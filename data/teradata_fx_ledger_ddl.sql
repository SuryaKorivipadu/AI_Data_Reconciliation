-- Teradata Source DDL (On-Premise)
CREATE MULTISET TABLE fin_core_db.fx_settlement_ledger (
    txn_id VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    account_id VARCHAR(15) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    counterparty_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
    currency CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
    raw_amount DECIMAL(18, 4),
    settled_amount DECIMAL(18, 2), -- Populated via Teradata ROUND() using Banker's Rounding (ROUND_HALF_EVEN)
    fee_code CHAR(11) CHARACTER SET LATIN NOT CASESPECIFIC,
    trade_date DATE FORMAT 'YYYY-MM-DD', -- Derived in local session timezone (America/New_York)
    execution_ts TIMESTAMP(6) -- Stored without timezone offset in EST/EDT
)
PRIMARY INDEX (txn_id)
PARTITION BY RANGE_N(trade_date BETWEEN DATE '2026-01-01' AND DATE '2026-12-31' EACH INTERVAL '1' DAY);
