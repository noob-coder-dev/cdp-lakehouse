-- Plumbing for hard problem #1: table, view and stream definitions. The guarantee lives in 01_cdc_apply.sql.
-- One table is shown (lending.loan_account). Every captured table gets the same shape, generated from its contract.

-- Bronze: every change connector v3 delivers from the clean topic, append-only. Created up front because the
-- connector needs the Iceberg table to exist; schema evolution lets it add new source columns (spec 01, rule 9).
CREATE ICEBERG TABLE IF NOT EXISTS bronze.lending__loan_account (
    loan_id            NUMBER(19,0),
    customer_id        NUMBER(19,0),
    principal_paise    NUMBER(19,0),
    outstanding_paise  NUMBER(19,0),
    status             STRING,
    remarks            STRING,              -- large text: can arrive as '__debezium_unavailable_value'
    updated_at         TIMESTAMP_NTZ(6),
    cdc_op             STRING,              -- c, u, d, r (r = snapshot read)
    cdc_version        NUMBER(38,0),        -- epoch, phase, position, row (CdcVersion.java)
    cdc_epoch          NUMBER(19,0),
    src_commit_ts_utc  TIMESTAMP_NTZ(3),
    raw_partition      NUMBER(10,0),
    raw_offset         NUMBER(19,0),        -- tie-breaker: Debezium's emission order within the partition
    record_metadata    OBJECT()
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'lake_s3'
    BASE_LOCATION = 'bronze/lending/loan_account'
    ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Silver: one row per key, including tombstones (is_deleted), so a late, older change can't bring a row back.
CREATE ICEBERG TABLE IF NOT EXISTS silver.lending__loan_account (
    loan_id            NUMBER(19,0) NOT NULL,
    customer_id        NUMBER(19,0),
    principal_paise    NUMBER(19,0),
    outstanding_paise  NUMBER(19,0),
    status             STRING,
    remarks            STRING,
    updated_at_utc     TIMESTAMP_NTZ(6),
    is_deleted         BOOLEAN NOT NULL,
    cdc_version        NUMBER(38,0) NOT NULL,
    raw_offset         NUMBER(19,0) NOT NULL,
    cdc_epoch          NUMBER(19,0) NOT NULL,
    src_commit_ts_utc  TIMESTAMP_NTZ(3),
    applied_at_utc     TIMESTAMP_NTZ(6)
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'lake_s3'
    BASE_LOCATION = 'silver/lending/loan_account'
    CLUSTER BY (loan_id);

-- What readers query: live rows only.
CREATE OR REPLACE VIEW silver.lending__loan_account_v AS
SELECT loan_id, customer_id, principal_paise, outstanding_paise, status, remarks, updated_at_utc, src_commit_ts_utc
FROM silver.lending__loan_account
WHERE NOT is_deleted;

-- Per-table floor (spec 01, rule 6): after a sweep in epoch E, changes from older epochs are dropped for good.
CREATE TABLE IF NOT EXISTS ctl.cdc_epoch_floor (
    table_name      STRING        NOT NULL,
    min_live_epoch  NUMBER(19,0)  NOT NULL,
    raised_at_utc   TIMESTAMP_NTZ NOT NULL
);

-- Bronze only ever receives inserts, so an append-only stream is enough. Its offset moves only when the MERGE
-- that reads it commits, so each change is consumed once; the version guard makes a second pass harmless anyway.
CREATE STREAM IF NOT EXISTS bronze.lending__loan_account__changes
    ON TABLE bronze.lending__loan_account
    APPEND_ONLY = TRUE;
