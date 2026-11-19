-- Azure Databricks Target DDL (Unity Catalog)
CREATE CATALOG IF NOT EXISTS prod_finance;
CREATE SCHEMA IF NOT EXISTS prod_finance.silver;

CREATE TABLE IF NOT EXISTS prod_finance.silver.fx_settlement_ledger (
    txn_id STRING NOT NULL,
    account_id STRING NOT NULL,
    counterparty_name STRING,
    currency STRING,
    raw_amount DECIMAL(18, 4),
    settled_amount DECIMAL(18, 2), -- Populated via Spark ROUND() which defaults to HALF_UP
    fee_code STRING,
    trade_date DATE, -- Derived after UTC timestamp conversion
    execution_ts TIMESTAMP, -- Stored in UTC
    CONSTRAINT pk_fx_ledger PRIMARY KEY (txn_id) NOT ENFORCED -- Informational only; does not block duplicate inserts!
)
USING DELTA
CLUSTER BY (trade_date, account_id)
COMMENT 'Migrated FX and wire settlement ledger from Teradata fin_core_db';
