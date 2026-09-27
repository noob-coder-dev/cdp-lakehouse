-- Hard problem #3: what the trust gate sees (spec: docs/specs/03-trust-gate.md).
-- airflow/publish_gate.py acts on these views. Tables: 03_trust_tables.sql.

-- Rule 4.1: for each expected unit, the latest meaningful arrival decides. Duplicate and stale files change nothing,
-- so they're left out; a later good arrival (a fixed correction) clears an earlier block.
CREATE OR REPLACE VIEW trust.import_state AS
WITH arrivals AS (
    SELECT 'partner:' || partner_id || ':' || feed AS source, seq::STRING AS unit, business_date,
           IFF(status = 'LOADED', 'RECONCILED', status) AS status, reason, recorded_at_utc
    FROM ctl.partner_file_loads
    WHERE status IN ('LOADED', 'UNRECONCILED', 'QUARANTINED', 'CONFLICT') AND business_date IS NOT NULL
    UNION ALL
    SELECT source, unit, business_date, status, reason, recorded_at_utc
    FROM trust.unit_checks
)
SELECT *
FROM arrivals
QUALIFY ROW_NUMBER() OVER (PARTITION BY source, unit, business_date ORDER BY recorded_at_utc DESC) = 1;

-- Rule 4.2: per certified table and date, expected versus reconciled units. A unit that never arrived counts as
-- not reconciled: absence can only be seen against an expectation.
CREATE OR REPLACE VIEW trust.date_completeness AS
SELECT i.table_name,
       e.business_date,
       COUNT(*)                                                 AS expected_units,
       COUNT_IF(s.status = 'RECONCILED')                        AS reconciled_units,
       COUNT_IF(s.status IS NULL AND e.due_at_utc < SYSDATE())  AS overdue_units,
       MAX(s.recorded_at_utc)                                   AS last_input_at_utc,
       ARRAY_AGG(IFF(s.status = 'RECONCILED', NULL,
                     e.source || '/' || e.unit || ': ' || COALESCE(s.status, 'missing')
                     || COALESCE(' (' || s.reason || ')', '')))  AS open_items
FROM trust.table_inputs AS i
JOIN trust.expected_arrivals AS e ON e.source = i.source
LEFT JOIN trust.import_state AS s
       ON s.source = e.source AND s.unit = e.unit AND s.business_date = e.business_date
GROUP BY i.table_name, e.business_date;

-- Rule 4.5: what readers and dashboard headers show. Complete-through is the day before the first incomplete date
-- after the last certified one (the unbroken run); fresh-as-of is reported next to it, never instead of it.
CREATE OR REPLACE VIEW trust.table_status AS
WITH certified AS (
    SELECT table_name, MAX(business_date) AS certified_through
    FROM trust.certified_dates
    GROUP BY table_name
)
SELECT d.table_name,
       c.certified_through,
       COALESCE(DATEADD(day, -1, MIN(IFF(d.reconciled_units < d.expected_units, d.business_date, NULL))),
                MAX(d.business_date))                                        AS complete_through,
       COALESCE(MAX(IFF(d.reconciled_units > 0, d.business_date, NULL)), c.certified_through) AS fresh_as_of,
       SUM(d.overdue_units)                                                  AS overdue_units
FROM trust.date_completeness AS d
LEFT JOIN certified AS c ON c.table_name = d.table_name
WHERE d.business_date > COALESCE(c.certified_through, '1900-01-01'::DATE)
GROUP BY d.table_name, c.certified_through;

-- Rule 4.4: month close. Refuses while any expected date of the month is uncertified. The clone is exactly what
-- was reported, and CREATE TABLE fails if it already exists, so a month can't be closed twice. From then on the
-- gate books adjustments instead of rebuilding the month's dates.
CREATE OR REPLACE PROCEDURE trust.close_month(table_name STRING, month_start DATE)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    uncertified NUMBER;
    clone_name STRING;
BEGIN
    SELECT COUNT(DISTINCT e.business_date) INTO :uncertified
    FROM trust.table_inputs AS i
    JOIN trust.expected_arrivals AS e ON e.source = i.source
    LEFT JOIN trust.certified_dates AS c ON c.table_name = i.table_name AND c.business_date = e.business_date
    WHERE i.table_name = :table_name AND DATE_TRUNC('month', e.business_date) = :month_start
      AND c.business_date IS NULL;
    IF (uncertified > 0) THEN
        RETURN 'refused: ' || uncertified || ' dates in the month are not certified';
    END IF;
    clone_name := table_name || '__close_' || TO_CHAR(month_start, 'YYYYMM');
    CREATE TABLE IDENTIFIER(:clone_name) CLONE IDENTIFIER(:table_name);
    INSERT INTO trust.closed_months (table_name, month_start, clone_name, closed_at_utc)
        VALUES (:table_name, :month_start, :clone_name, SYSDATE());
    RETURN 'closed as ' || clone_name;
END;
$$;
