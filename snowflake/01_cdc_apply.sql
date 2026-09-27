-- Hard problem #1: apply CDC changes to silver so that duplicates, replays, reordering and batching can't change
-- the result (spec: docs/specs/01-cdc-apply.md). Tables, view and stream: 00_cdc_tables.sql.
-- tests/test_cdc_apply_property.py runs the two marked statements below exactly as written.

CREATE OR REPLACE TASK silver.apply__lending__loan_account
    WAREHOUSE = wh_ingest
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('bronze.lending__loan_account__changes')
AS
-- apply-merge:begin
MERGE INTO silver.lending__loan_account AS t
USING (
    -- Rule 2: one change per key per batch (Snowflake rejects a MERGE where several source rows match one target).
    -- Rule 6: changes from epochs below the table's floor belong to a superseded timeline.
    SELECT *
    FROM bronze.lending__loan_account__changes
    WHERE cdc_epoch >= (SELECT COALESCE(MAX(min_live_epoch), 0)
                        FROM ctl.cdc_epoch_floor
                        WHERE table_name = 'lending.loan_account')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY cdc_version DESC, raw_offset DESC) = 1
) AS s
ON t.loan_id = s.loan_id
-- Rule 1: newer wins. Equal or older changes fall through every clause and are ignored.
-- Rule 3: a delete keeps its version as a tombstone and doesn't touch the last known values.
WHEN MATCHED
     AND (s.cdc_version > t.cdc_version OR (s.cdc_version = t.cdc_version AND s.raw_offset > t.raw_offset))
     AND s.cdc_op = 'd'
THEN UPDATE SET
     is_deleted        = TRUE,
     cdc_version       = s.cdc_version,
     raw_offset        = s.raw_offset,
     cdc_epoch         = s.cdc_epoch,
     src_commit_ts_utc = s.src_commit_ts_utc,
     applied_at_utc    = SYSDATE()
WHEN MATCHED
     AND (s.cdc_version > t.cdc_version OR (s.cdc_version = t.cdc_version AND s.raw_offset > t.raw_offset))
THEN UPDATE SET
     customer_id       = s.customer_id,
     principal_paise   = s.principal_paise,
     outstanding_paise = s.outstanding_paise,
     status            = s.status,
     -- Rule 8: an unchanged large value arrives as a placeholder; keep what silver already has.
     remarks           = IFF(s.remarks = '__debezium_unavailable_value', t.remarks, s.remarks),
     updated_at_utc    = s.updated_at,
     is_deleted        = FALSE,
     cdc_version       = s.cdc_version,
     raw_offset        = s.raw_offset,
     cdc_epoch         = s.cdc_epoch,
     src_commit_ts_utc = s.src_commit_ts_utc,
     applied_at_utc    = SYSDATE()
WHEN NOT MATCHED THEN INSERT (
     loan_id, customer_id, principal_paise, outstanding_paise, status, remarks, updated_at_utc,
     is_deleted, cdc_version, raw_offset, cdc_epoch, src_commit_ts_utc, applied_at_utc)
VALUES (
     s.loan_id, s.customer_id, s.principal_paise, s.outstanding_paise, s.status,
     NULLIF(s.remarks, '__debezium_unavailable_value'), s.updated_at,
     s.cdc_op = 'd',                        -- a delete for an unknown key still leaves a tombstone
     s.cdc_version, s.raw_offset, s.cdc_epoch, s.src_commit_ts_utc, SYSDATE());
-- apply-merge:end

ALTER TASK silver.apply__lending__loan_account RESUME;

-- Rule 6: the epoch sweep. Run by the runbook after a full snapshot of a table in epoch :new_epoch. Anything in
-- silver that the new epoch hasn't touched no longer exists at the source, so it goes, and the floor rises in the
-- same transaction so nothing from the old timeline can come back. Running it before the snapshot has been applied
-- is safe: a key's snapshot row (epoch :new_epoch) still passes the floor and re-inserts it.
-- epoch-sweep:begin
CREATE OR REPLACE PROCEDURE ctl.cdc_epoch_sweep(source_table STRING, silver_table STRING, new_epoch NUMBER)
RETURNS NUMBER
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    removed NUMBER DEFAULT 0;
BEGIN
    BEGIN TRANSACTION;
    DELETE FROM IDENTIFIER(:silver_table) WHERE cdc_epoch < :new_epoch;
    removed := SQLROWCOUNT;
    MERGE INTO ctl.cdc_epoch_floor AS f
    USING (SELECT :source_table AS table_name, :new_epoch AS epoch) AS s
    ON f.table_name = s.table_name
    WHEN MATCHED THEN UPDATE SET min_live_epoch = GREATEST(f.min_live_epoch, s.epoch), raised_at_utc = SYSDATE()
    WHEN NOT MATCHED THEN INSERT (table_name, min_live_epoch, raised_at_utc) VALUES (s.table_name, s.epoch, SYSDATE());
    COMMIT;
    RETURN removed;
END;
$$;
-- epoch-sweep:end
