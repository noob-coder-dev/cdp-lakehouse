-- Plumbing for hard problem #2: tables, view, stage and file format.
-- The guarantee lives in spark/partner_file_loader.py (commit_load: one transaction per file).

CREATE FILE FORMAT IF NOT EXISTS ctl.parquet_ff TYPE = PARQUET USE_LOGICAL_TYPE = TRUE;

-- PySpark writes each load's validated rows to .../load_id=<uuid>/ with mode errorifexists; 7-day lifecycle.
CREATE STAGE IF NOT EXISTS ctl.partner_staging
    URL = 's3://cdp-staging/partner_rows/'
    STORAGE_INTEGRATION = s3_staging
    FILE_FORMAT = ctl.parquet_ff;

-- Every loaded version of every file, append-only. Superseded versions stay here for auditors.
CREATE ICEBERG TABLE IF NOT EXISTS bronze.partner_repayment_rows (
    load_id        STRING        NOT NULL,
    partner_id     STRING        NOT NULL,
    feed           STRING        NOT NULL,
    business_date  DATE          NOT NULL,
    seq            NUMBER(10,0)  NOT NULL,
    txn_id         STRING        NOT NULL,
    loan_id        STRING        NOT NULL,
    txn_date       DATE          NOT NULL,
    amount_paise   NUMBER(19,0)  NOT NULL,
    channel        STRING
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'lake_s3'
    BASE_LOCATION = 'bronze/partner/repayment_rows'
    CLUSTER BY (business_date, partner_id);

-- One row per slot (partner, feed, business date, sequence): which load is active. The claim MERGE in
-- commit_load is a compare-and-set on this row.
CREATE TABLE IF NOT EXISTS ctl.partner_file_slots (
    partner_id       STRING        NOT NULL,
    feed             STRING        NOT NULL,
    business_date    DATE          NOT NULL,
    seq              NUMBER(10,0)  NOT NULL,
    active_load_id   STRING        NOT NULL,
    active_revision  NUMBER(10,0)  NOT NULL,
    updated_at_utc   TIMESTAMP_NTZ NOT NULL
);

-- One row per arrival, whatever happened to it. Doubles as import_state for files (hard problem #3).
CREATE TABLE IF NOT EXISTS ctl.partner_file_loads (
    load_id          STRING        NOT NULL,
    partner_id       STRING,
    feed             STRING,
    business_date    DATE,
    seq              NUMBER(10,0),
    revision         NUMBER(10,0),
    sha256           STRING        NOT NULL,     -- of the decompressed bytes
    fingerprint      STRING,                     -- rows : paise total : order-independent checksum
    row_count        NUMBER(19,0),
    total_paise      NUMBER(19,0),
    status           STRING        NOT NULL,     -- LOADED, UNRECONCILED, DUPLICATE, STALE, CONFLICT, QUARANTINED
    reason           STRING,
    source_uri       STRING        NOT NULL,
    recorded_at_utc  TIMESTAMP_NTZ NOT NULL
);

-- What readers query: each slot's active version only. Flipping the pointer and inserting the rows commit in the
-- same transaction, so this view never shows half a file or two versions of one.
CREATE OR REPLACE VIEW silver.partner_repayments AS
SELECT r.*
FROM bronze.partner_repayment_rows AS r
JOIN ctl.partner_file_slots AS s ON s.active_load_id = r.load_id;
