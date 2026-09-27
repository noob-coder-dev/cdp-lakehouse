-- Plumbing for hard problem #3: the tables the trust gate reads and writes.
-- The guarantee lives in 04_trust_gate.sql and airflow/publish_gate.py.

-- What each business date needs, and by when. Generated daily from each source's calendar (stubbed).
CREATE TABLE IF NOT EXISTS trust.expected_arrivals (
    source         STRING        NOT NULL,   -- partner:NBFC_A:repayment, cdc:lending.repayment, stream:app_events, recon:collections
    unit           STRING        NOT NULL,   -- file sequence, hour, or 'day'
    business_date  DATE          NOT NULL,
    due_at_utc     TIMESTAMP_NTZ NOT NULL
);

-- Which sources feed each certified table.
CREATE TABLE IF NOT EXISTS trust.table_inputs (
    table_name  STRING NOT NULL,
    source      STRING NOT NULL
);

-- Results of the checks that aren't partner files: CDC day reconciliation, stream-hour audits ("every record
-- accounted for"), cross-source reconciliation, and approved waivers. Status is RECONCILED or BLOCKED.
CREATE TABLE IF NOT EXISTS trust.unit_checks (
    source           STRING        NOT NULL,
    unit             STRING        NOT NULL,
    business_date    DATE          NOT NULL,
    status           STRING        NOT NULL,
    reason           STRING,
    recorded_at_utc  TIMESTAMP_NTZ NOT NULL
);

-- One row per certified date: its total, and the latest input arrival it reflects.
CREATE TABLE IF NOT EXISTS trust.certified_dates (
    table_name        STRING        NOT NULL,
    business_date     DATE          NOT NULL,
    total_paise       NUMBER(19,0)  NOT NULL,
    inputs_as_of_utc  TIMESTAMP_NTZ NOT NULL,
    certified_at_utc  TIMESTAMP_NTZ NOT NULL,
    build_id          STRING        NOT NULL
);

-- Every certification, restatement and adjustment, with the inputs it read and the code that produced it.
CREATE TABLE IF NOT EXISTS trust.build_log (
    build_id          STRING        NOT NULL,
    table_name        STRING        NOT NULL,
    business_date     DATE          NOT NULL,
    action            STRING        NOT NULL,   -- CERTIFY, RESTATE, ADJUST
    total_paise       NUMBER(19,0),
    inputs_as_of_utc  TIMESTAMP_NTZ NOT NULL,
    code_version      STRING        NOT NULL,   -- git SHA
    built_at_utc      TIMESTAMP_NTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS trust.closed_months (
    table_name     STRING        NOT NULL,
    month_start    DATE          NOT NULL,
    clone_name     STRING        NOT NULL,     -- zero-copy clone: exactly what was reported
    closed_at_utc  TIMESTAMP_NTZ NOT NULL
);

-- The certified table: collections per business date (IST) and partner. It only ever holds certified dates.
CREATE TABLE IF NOT EXISTS gold.fin_daily_collections (
    business_date_ist  DATE          NOT NULL,
    partner_id         STRING        NOT NULL,
    collected_paise    NUMBER(19,0)  NOT NULL,
    txn_count          NUMBER(19,0)  NOT NULL,
    build_id           STRING        NOT NULL
);

-- Changes to dates in closed months, booked in the open period.
CREATE TABLE IF NOT EXISTS gold.fin_collection_adjustments (
    booked_business_date    DATE          NOT NULL,
    original_business_date  DATE          NOT NULL,
    partner_id              STRING        NOT NULL,
    delta_paise             NUMBER(19,0)  NOT NULL,
    build_id                STRING        NOT NULL,
    booked_at_utc           TIMESTAMP_NTZ NOT NULL
);
