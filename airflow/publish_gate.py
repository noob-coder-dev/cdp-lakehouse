"""Hard problem #3: the publish gate for a certified finance table (spec: docs/specs/03-trust-gate.md).

An Airflow task calls it after each input window:

    publish(conn, COLLECTIONS, today=date.today(), code_version=os.environ["GIT_SHA"])

It reads trust.date_completeness and writes gold, trust.certified_dates and trust.build_log. Targets Python 3.11.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class CertifiedTable:
    """How to rebuild one date of a certified table, check it, and book a change after close."""
    name: str
    rebuild: tuple[str, ...]    # run in the date's transaction, with %(day)s and %(build_id)s
    totals: str                 # (gold total, silver total) for %(day)s: must be equal, to the paise
    adjust: str                 # books new total - certified total - adjustments already booked, per partner


COLLECTIONS = CertifiedTable(
    name="gold.fin_daily_collections",
    rebuild=(
        "DELETE FROM gold.fin_daily_collections WHERE business_date_ist = %(day)s",
        """INSERT INTO gold.fin_daily_collections (business_date_ist, partner_id, collected_paise, txn_count, build_id)
           SELECT business_date, partner_id, SUM(amount_paise), COUNT(*), %(build_id)s
           FROM silver.partner_repayments WHERE business_date = %(day)s GROUP BY business_date, partner_id""",
    ),
    totals="""
        SELECT (SELECT COALESCE(SUM(collected_paise), 0) FROM gold.fin_daily_collections
                WHERE business_date_ist = %(day)s),
               (SELECT COALESCE(SUM(amount_paise), 0) FROM silver.partner_repayments WHERE business_date = %(day)s)""",
    adjust="""
        INSERT INTO gold.fin_collection_adjustments
            (booked_business_date, original_business_date, partner_id, delta_paise, build_id, booked_at_utc)
        SELECT %(today)s, %(day)s, partner_id, SUM(paise), %(build_id)s, SYSDATE()
        FROM (SELECT partner_id, amount_paise AS paise FROM silver.partner_repayments WHERE business_date = %(day)s
              UNION ALL
              SELECT partner_id, -collected_paise FROM gold.fin_daily_collections WHERE business_date_ist = %(day)s
              UNION ALL
              SELECT partner_id, -delta_paise FROM gold.fin_collection_adjustments
              WHERE original_business_date = %(day)s)
        GROUP BY partner_id
        HAVING SUM(paise) <> 0""",
)

DATES = """
SELECT d.business_date, d.reconciled_units = d.expected_units AS complete, d.last_input_at_utc,
       c.inputs_as_of_utc, m.month_start IS NOT NULL AS month_closed
FROM trust.date_completeness AS d
LEFT JOIN trust.certified_dates AS c ON c.table_name = d.table_name AND c.business_date = d.business_date
LEFT JOIN trust.closed_months AS m
       ON m.table_name = d.table_name AND m.month_start = DATE_TRUNC('month', d.business_date)
WHERE d.table_name = %(table)s AND d.business_date < %(today)s
ORDER BY d.business_date
"""

UPSERT_CERTIFIED = """
MERGE INTO trust.certified_dates AS t
USING (SELECT %(table)s AS table_name, %(day)s::DATE AS business_date) AS s
ON t.table_name = s.table_name AND t.business_date = s.business_date
WHEN MATCHED THEN UPDATE SET total_paise = %(total)s, inputs_as_of_utc = %(inputs_as_of)s,
    certified_at_utc = SYSDATE(), build_id = %(build_id)s
WHEN NOT MATCHED THEN INSERT (table_name, business_date, total_paise, inputs_as_of_utc, certified_at_utc, build_id)
    VALUES (s.table_name, s.business_date, %(total)s, %(inputs_as_of)s, SYSDATE(), %(build_id)s)
"""

MOVE_INPUTS_AS_OF = """
UPDATE trust.certified_dates SET inputs_as_of_utc = %(inputs_as_of)s
WHERE table_name = %(table)s AND business_date = %(day)s
"""

LOG_BUILD = """
INSERT INTO trust.build_log
    (build_id, table_name, business_date, action, total_paise, inputs_as_of_utc, code_version, built_at_utc)
VALUES (%(build_id)s, %(table)s, %(day)s, %(action)s, %(total)s, %(inputs_as_of)s, %(code_version)s, SYSDATE())
"""


def publish(conn, table: CertifiedTable, today: date, code_version: str) -> list[str]:
    """Certify, restate or adjust each date that needs it, in date order (spec 4.3). Returns what it did."""
    cur = conn.cursor()
    cur.execute(DATES, {"table": table.name, "today": today})
    done, advancing = [], True
    for day, complete, last_input, inputs_as_of, month_closed in cur.fetchall():
        if inputs_as_of is not None:                       # certified: act only on inputs that changed since
            if not complete or last_input is None or last_input <= inputs_as_of:
                continue                                   # unchanged, or the change is still blocked (open item)
            action = "ADJUST" if month_closed else "RESTATE"
        elif advancing and complete:
            action = "CERTIFY"
        else:
            advancing = False                              # the first gap: later dates wait (unbroken run)
            continue
        _apply(conn, table, action, day, last_input, today, code_version)
        done.append(f"{action} {day}")
    return done


def _apply(conn, table: CertifiedTable, action: str, day: date, inputs_as_of, today: date,
           code_version: str) -> None:
    """One date, one transaction: readers see the old date or the new one, never a mix. Moving inputs_as_of in
    the same transaction means a re-run finds nothing to do."""
    p = {"table": table.name, "day": day, "today": today, "build_id": str(uuid.uuid4()),
         "inputs_as_of": inputs_as_of, "action": action, "code_version": code_version, "total": None}
    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        if action == "ADJUST":                             # the month is closed: its rows never change
            cur.execute(table.adjust, p)
            cur.execute(MOVE_INPUTS_AS_OF, p)
        else:
            for statement in table.rebuild:
                cur.execute(statement, p)
            gold, silver = cur.execute(table.totals, p).fetchone()
            if gold != silver:                             # silver moved mid-build, or the build is wrong
                raise RuntimeError(f"{table.name} {day}: gold {gold} paise != silver {silver} paise")
            p["total"] = gold
            cur.execute(UPSERT_CERTIFIED, p)
        cur.execute(LOG_BUILD, p)
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise
