from pathlib import Path
from typing import Any, Dict, List, Tuple
import pandas as pd, requests, sys, time, logging, json

# ---------------------------------------------------------------------------
# 1. Configuration & Logging Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("MigrationReconciler")

CONFIG = {
    "teradata_csv": r"data/teradata_ledger_extract.csv",
    "databricks_csv": r"data/databricks_ledger_extract.csv",
    "teradata_ddl": r"data/teradata_fx_ledger_ddl.sql",
    "databricks_ddl": r"data/databricks_fx_ledger_ddl.sql",
    "primary_key": "txn_id",
    "ollama_url": "http://localhost:11434/api/generate",
    "ollama_model": "llama3.1",  # Change to mistral, qwen2.5, etc., if preferred
    "ollama_timeout_seconds": 600,
    "max_sample_diffs": 5,       # Keep prompt small and focused for local LLM
}


# ---------------------------------------------------------------------------
# 2. Data & DDL Loading Functions
# ---------------------------------------------------------------------------
def load_csv_extract(file_path: str) -> pd.DataFrame:
    """Loads a CSV extract while preserving empty strings instead of auto-converting to NaN."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    logger.info(f"Loading extract: {file_path}")
    # keep_default_na=False ensures Teradata empty strings ('') stay distinct from NULLs
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def load_ddl_text(file_path: str) -> str:
    """Reads a SQL DDL file as plain text to give schema context to the AI."""
    path = Path(file_path)
    if not path.exists():
        logger.warning(f"DDL file not found ({file_path}). Continuing without DDL context.")
        return "DDL not provided."

    logger.info(f"Loading DDL: {file_path}")
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# 3. Deterministic Reconciliation Checks (Python handles the exact math)
# ---------------------------------------------------------------------------
def check_row_counts_and_duplicates(
    td_df: pd.DataFrame, dbx_df: pd.DataFrame, pk_col: str
) -> Dict[str, Any]:
    """Compares total row counts and checks for duplicate primary keys."""
    logger.info("Running Check 1: Row counts and primary key uniqueness...")

    td_count = len(td_df)
    dbx_count = len(dbx_df)
    dbx_duplicates = dbx_df[dbx_df.duplicated(subset=[pk_col], keep=False)]

    result = {
        "check_name": "Row_Count_And_Duplicates",
        "status": "PASS",
        "teradata_row_count": td_count,
        "databricks_row_count": dbx_count,
        "duplicate_pk_count_in_databricks": len(dbx_duplicates),
        "sample_duplicate_ids": dbx_duplicates[pk_col].unique().tolist()[:5],
    }

    if td_count != dbx_count or len(dbx_duplicates) > 0:
        result["status"] = "FAIL"

    return result


def check_financial_totals(td_df: pd.DataFrame, dbx_df: pd.DataFrame) -> Dict[str, Any]:
    """Compares sum of settled_amount across unique transactions to spot rounding drift."""
    logger.info("Running Check 2: Financial aggregation (settled_amount)...")

    # Deduplicate Databricks first so duplicate rows don't mask subtle rounding drift
    dbx_dedup = dbx_df.drop_duplicates(subset=[CONFIG["primary_key"]])

    td_total = pd.to_numeric(td_df["settled_amount"], errors="coerce").sum()
    dbx_total = pd.to_numeric(dbx_dedup["settled_amount"], errors="coerce").sum()
    difference = round(dbx_total - td_total, 4)

    return {
        "check_name": "Settled_Amount_Sum_Check",
        "status": "PASS" if difference == 0 else "FAIL",
        "teradata_total_settled": round(float(td_total), 4),
        "databricks_dedup_total_settled": round(float(dbx_total), 4),
        "net_drift": difference,
    }


def check_null_and_empty_drift(td_df: pd.DataFrame, dbx_df: pd.DataFrame) -> Dict[str, Any]:
    """Checks if empty strings ('') in Teradata became literal 'NULL' or blanks in Databricks."""
    logger.info("Running Check 3: Empty string vs. NULL drift on fee_code...")

    td_empty_count = int((td_df["fee_code"] == "").sum())
    dbx_empty_count = int((dbx_df["fee_code"] == "").sum())
    dbx_null_literal_count = int((dbx_df["fee_code"].str.upper() == "NULL").sum())

    status = "PASS"
    if td_empty_count != dbx_empty_count or dbx_null_literal_count > 0:
        status = "FAIL"

    return {
        "check_name": "Null_Vs_Empty_String_Check",
        "column": "fee_code",
        "status": status,
        "teradata_empty_string_count": td_empty_count,
        "databricks_empty_string_count": dbx_empty_count,
        "databricks_null_string_count": dbx_null_literal_count,
    }


def find_sample_row_mismatches(
    td_df: pd.DataFrame, dbx_df: pd.DataFrame, pk_col: str, max_samples: int
) -> List[Dict[str, Any]]:
    """Joins unique rows on primary key and captures column-level value differences."""
    logger.info("Running Check 4: Row-by-row column comparison...")

    dbx_dedup = dbx_df.drop_duplicates(subset=[pk_col])
    merged = pd.merge(td_df, dbx_dedup, on=pk_col, suffixes=("_td", "_dbx"))

    compare_cols = [col for col in td_df.columns if col != pk_col]
    sample_diffs: List[Dict[str, Any]] = []

    for _, row in merged.iterrows():
        row_diffs = {}
        for col in compare_cols:
            td_val = row[f"{col}_td"]
            dbx_val = row[f"{col}_dbx"]
            if td_val != dbx_val:
                # repr() makes hidden trailing spaces like 'WIRE_INTL  ' visible
                row_diffs[col] = {
                    "teradata": repr(td_val),
                    "databricks": repr(dbx_val),
                }

        if row_diffs:
            sample_diffs.append({pk_col: row[pk_col], "differences": row_diffs})

        if len(sample_diffs) >= max_samples:
            break

    return sample_diffs


# ---------------------------------------------------------------------------
# 4. AI Root-Cause Analysis via Local Ollama
# ---------------------------------------------------------------------------
def analyze_mismatches_with_ollama(
    report: Dict[str, Any], td_ddl: str, dbx_ddl: str
) -> str:
    """Sends the structured mismatch report and DDLs to a local Ollama LLM."""
    logger.info(f"Sending mismatch report to Ollama ({CONFIG['ollama_model']})...")

    prompt = f"""
You are a Senior Data Migration Engineer specializing in Teradata to Azure Databricks migrations.
Our automated Python reconciliation script compared a migrated FX Ledger table and found mismatches.

### 1. TERADATA SOURCE DDL:
{td_ddl}

### 2. DATABRICKS TARGET DDL:
{dbx_ddl}

### 3. RECONCILIATION MISMATCH REPORT (JSON):
{json.dumps(report, indent=2)}

### YOUR TASK:
Identify each distinct data migration issue shown in the report. For each issue, provide:
1. **Issue Name** (e.g., Banker's Rounding, Timezone Rollover, Encoding Mojibake, etc.)
2. **Root Cause** (Explain the technical difference between Teradata and Spark/Databricks)
3. **Recommended Fix** (Give a concrete Spark SQL or PySpark fix)

Keep your explanation clear, concise, and practical.
""".strip()

    payload = {
        "model": CONFIG["ollama_model"],
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.2},  # Low temperature for factual technical analysis
    }

    try:
        response = requests.post(
            CONFIG["ollama_url"],
            json=payload,
            timeout=CONFIG["ollama_timeout_seconds"],
        )
        response.raise_for_status()
        return response.json().get("response", "No response text returned by Ollama.")
    except requests.exceptions.ConnectionError:
        return (
            "ERROR: Could not connect to Ollama at http://localhost:11434. "
            "Make sure Ollama is running (`ollama serve`)."
        )
    except requests.exceptions.Timeout:
        return (
            "ERROR: Ollama request timed out after "
            f"{CONFIG['ollama_timeout_seconds']} seconds. "
            "Try a smaller model or increase CONFIG['ollama_timeout_seconds']."
        )
    except requests.exceptions.RequestException as err:
        return f"ERROR: Ollama API call failed: {err}"


# ---------------------------------------------------------------------------
# 5. Main Orchestrator
# ---------------------------------------------------------------------------
def run_reconciliation() -> None:
    """Main pipeline entry point."""
    start_time = time.time()
    pipeline_status = "FAILED"
    try:
        # Step 1: Load inputs
        td_df = load_csv_extract(CONFIG["teradata_csv"])
        dbx_df = load_csv_extract(CONFIG["databricks_csv"])
        td_ddl = load_ddl_text(CONFIG["teradata_ddl"])
        dbx_ddl = load_ddl_text(CONFIG["databricks_ddl"])

        # Step 2: Execute deterministic checks
        summary_checks = [
            check_row_counts_and_duplicates(td_df, dbx_df, CONFIG["primary_key"]),
            check_financial_totals(td_df, dbx_df),
            check_null_and_empty_drift(td_df, dbx_df),
        ]
        sample_row_diffs = find_sample_row_mismatches(
            td_df, dbx_df, CONFIG["primary_key"], CONFIG["max_sample_diffs"]
        )

        full_report = {
            "summary_checks": summary_checks,
            "sample_row_mismatches": sample_row_diffs,
        }

        print("\n" + "=" * 60)
        print("STEP 1: DETERMINISTIC PYTHON RECONCILIATION REPORT")
        print("=" * 60)
        print(json.dumps(full_report, indent=2))

        # Step 3: Trigger AI only if at least one check failed
        any_failure = any(c["status"] == "FAIL" for c in summary_checks) or len(sample_row_diffs) > 0

        if any_failure:
            print("\n" + "=" * 60)
            print("STEP 2: OLLAMA AI ROOT-CAUSE DIAGNOSIS")
            print("=" * 60)
            ai_diagnosis = analyze_mismatches_with_ollama(full_report, td_ddl, dbx_ddl)
            print(ai_diagnosis)
        else:
            logger.info("All reconciliation checks PASSED! Skipping AI analysis.")
        pipeline_status = "COMPLETED"
    except FileNotFoundError as fnf_err:
        logger.error(f"Missing required input file: {fnf_err}")
    except pd.errors.EmptyDataError as empty_err:
        logger.error(f"One of the CSV extracts is completely empty: {empty_err}")
    except KeyError as key_err:
        logger.error(f"Expected column missing from CSV extract: {key_err}")
    except Exception as exc:
        logger.exception(f"Unexpected error during reconciliation: {exc}")

    finally:
        # Runs every time—whether the pipeline succeeded or crashed
        elapsed_seconds = round(time.time() - start_time, 2)
        logger.info(
            f"Pipeline finished | Status: {pipeline_status} | Duration: {elapsed_seconds}s"
        )
        # In production (e.g., Teradata JDBC / Spark), you close DB connections here:
        # if td_conn: td_conn.close()

        if pipeline_status != "COMPLETED":
            sys.exit(1)  # Signals failure to CI/CD, Airflow, or Databricks Workflows


if __name__ == "__main__":
    run_reconciliation()