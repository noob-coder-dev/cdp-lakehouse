"""Tests for hard problem #2 (docs/specs/02-partner-files.md, section 6). Targets Python 3.11.

Part 1 runs on a local SparkSession: money is exact, broken files are quarantined whole, and the fingerprint
ignores row order and formatting.

Part 2 runs the load protocol against Snowflake (same configuration as test_cdc_apply_property.py): resends,
corrections, stale and conflicting revisions, and two revisions racing for one slot. The real claim, ledger and
classification SQL runs unchanged against scratch tables. Only the row source is swapped: staging Parquet in S3
is plumbing.
"""
from __future__ import annotations

import hashlib
import os
import sys
import threading
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark"))
loader = pytest.importorskip("partner_file_loader")      # needs pyspark, boto3 and snowflake-connector-python
FeedContract, Quarantine, validate_and_reconcile = loader.FeedContract, loader.Quarantine, loader.validate_and_reconcile

# ------------------------------------------------------------------------------------ part 1: local Spark

CONTRACT = FeedContract(feed="repayment", date_format="dd/MM/yyyy")
HEADER = "H|NBFC_A|repayment|2026-09-26|1|1"
GOOD = [HEADER, "D|T1|L9|26/09/2026|1,23,456.78|UPI", "D|T2|L7|25/09/2026|10|NACH", "T|2|12346678"]


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[2]").appName("partner-file-tests")
               .config("spark.sql.ansi.enabled", "true").getOrCreate())
    yield session
    session.stop()


def lines(spark, rows):
    return spark.createDataFrame([(r,) for r in rows], "value STRING")


def test_amounts_become_exact_paise(spark):
    header, details, proof = validate_and_reconcile(lines(spark, GOOD), CONTRACT)
    assert {r.txn_id: r.amount_paise for r in details.collect()} == {"T1": 12345678, "T2": 1000}
    assert (header.partner, header.business_date, header.revision) == ("NBFC_A", date(2026, 9, 26), 1)
    assert (proof.rows, proof.total_paise, proof.reconciled) == (2, 12346678, True)


@pytest.mark.parametrize("rows, reason", [
    (GOOD[:3], "trailer"),                                                 # truncated: the trailer never arrived
    (GOOD[:3] + ["T|2|12346679"], "trailer says"),                         # one paisa off
    ([HEADER, "D|T1|L9|26/09/2026|10.005|UPI", "T|1|1001"], "bad rows"),  # a cast alone would round to 10.01
    ([HEADER, "D|T1|L9|03/09/2026|10|UPI", "T|1|1000"], "bad rows"),      # 3 Sep: outside the 7-day window
    ([HEADER, GOOD[1], GOOD[1], "T|2|24691356"], "duplicate txn_id"),
    (["H|NBFC_A|refunds|2026-09-26|1|1"] + GOOD[1:], "wrong feed"),
])
def test_broken_files_are_quarantined_whole(spark, rows, reason):
    with pytest.raises(Quarantine, match=reason):
        validate_and_reconcile(lines(spark, rows), CONTRACT)


def test_paise_feeds_take_whole_numbers_only(spark):
    paise = FeedContract(feed="repayment", date_format="dd/MM/yyyy", amount_unit="paise")
    rows = [HEADER, "D|T1|L9|26/09/2026|12345678|UPI", "D|T2|L7|25/09/2026|1000|NACH", "T|2|12346678"]
    assert validate_and_reconcile(lines(spark, rows), paise)[2].total_paise == 12346678
    with pytest.raises(Quarantine, match="bad rows"):                        # rupees sent to a paise feed
        validate_and_reconcile(lines(spark, [HEADER, "D|T1|L9|26/09/2026|123.45|UPI", "T|1|12345"]), paise)


def test_fingerprint_ignores_row_order_and_formatting(spark):
    reformatted = [HEADER, "D|T2|L7|25/09/2026|10.00|NACH ", "D|T1|L9|26/09/2026|123456.78|UPI", "T|2|12346678"]
    first = validate_and_reconcile(lines(spark, GOOD), CONTRACT)[2]
    second = validate_and_reconcile(lines(spark, reformatted), CONTRACT)[2]
    assert first.fingerprint == second.fingerprint

# ------------------------------------------------------------------------------ part 2: Snowflake protocol

SCRATCH_DDL = """
CREATE TABLE {s}.partner_file_slots (partner_id STRING, feed STRING, business_date DATE, seq NUMBER(10,0),
    active_load_id STRING, active_revision NUMBER(10,0), updated_at_utc TIMESTAMP_NTZ);
CREATE TABLE {s}.partner_file_loads (load_id STRING, partner_id STRING, feed STRING, business_date DATE,
    seq NUMBER(10,0), revision NUMBER(10,0), sha256 STRING, fingerprint STRING, row_count NUMBER(19,0),
    total_paise NUMBER(19,0), status STRING, reason STRING, source_uri STRING, recorded_at_utc TIMESTAMP_NTZ);
CREATE TABLE {s}.staged (load_id STRING, txn_id STRING, loan_id STRING, txn_date DATE, amount_paise NUMBER(19,0),
    channel STRING);
CREATE TABLE {s}.rows (load_id STRING, partner_id STRING, feed STRING, business_date DATE, seq NUMBER(10,0),
    txn_id STRING, loan_id STRING, txn_date DATE, amount_paise NUMBER(19,0), channel STRING)
"""

V1 = [("T1", "L9", date(2026, 9, 26), 12345678, "UPI"), ("T2", "L7", date(2026, 9, 25), 1000, "NACH")]
V2 = [("T1", "L9", date(2026, 9, 26), 12345678, "UPI"), ("T2", "L7", date(2026, 9, 25), 1500, "NACH")]


def connect():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR", "snowflake"), role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"], database=os.environ["SNOWFLAKE_DATABASE"])


@dataclass
class Scratch:
    conn: object
    schema: str

    def run(self, conn, revision: int, rows: list, day: date, sha: str | None = None) -> str:
        """Stage `rows` as one file's content, then run the real commit_load for it."""
        content = "\n".join("|".join(map(str, r)) for r in rows)
        p = {"load_id": str(uuid.uuid4()), "source_uri": "s3://test", "partner": "NBFC_A", "feed": "repayment",
             "business_date": day, "seq": 1, "revision": revision,
             "sha256": sha or hashlib.sha256(content.encode()).hexdigest(),
             "fingerprint": f"{len(rows)}:{sum(r[3] for r in rows)}:{hash(frozenset(rows))}",
             "rows": len(rows), "total_paise": sum(r[3] for r in rows), "reconciled": True}
        conn.cursor().executemany(f"INSERT INTO {self.schema}.staged VALUES (%s, %s, %s, %s, %s, %s)",
                                  [(p["load_id"], *r) for r in rows])
        return loader.commit_load(conn, p)

    def visible(self, day: date) -> dict:
        cur = self.conn.cursor()
        cur.execute(f"SELECT r.txn_id, r.amount_paise FROM {self.schema}.rows r JOIN {self.schema}.partner_file_slots s"
                    f" ON s.active_load_id = r.load_id WHERE r.business_date = %s", (day,))
        return {txn: int(paise) for txn, paise in cur.fetchall()}

    def loads_kept(self, day: date) -> int:
        cur = self.conn.cursor()
        cur.execute(f"SELECT COUNT(DISTINCT load_id) FROM {self.schema}.rows WHERE business_date = %s", (day,))
        return cur.fetchone()[0]


@pytest.fixture(scope="module")
def sf():
    needed = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE")
    if any(name not in os.environ for name in needed):
        pytest.skip("Snowflake connection not configured")
    conn = connect()
    schema = f"PARTNER_TEST_{uuid.uuid4().hex[:8].upper()}"
    conn.cursor().execute(f"CREATE SCHEMA {schema}")
    for ddl in SCRATCH_DDL.format(s=schema).split(";"):
        conn.cursor().execute(ddl)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(loader, "INSERT_ROWS",
                   f"INSERT INTO {schema}.rows SELECT %(load_id)s, %(partner)s, %(feed)s, %(business_date)s, "
                   f"%(seq)s, txn_id, loan_id, txn_date, amount_paise, channel "
                   f"FROM {schema}.staged WHERE load_id = %(load_id)s")
        for name in ("CLAIM_SLOT", "RECORD_LOAD", "ACTIVE_LOAD"):
            mp.setattr(loader, name, getattr(loader, name).replace("ctl.", f"{schema}."))
        yield Scratch(conn, schema)
    conn.cursor().execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    conn.close()


def test_resends_corrections_and_late_revisions(sf):
    day = date(2026, 9, 26)
    assert sf.run(sf.conn, 1, V1, day) == "LOADED"
    assert sf.run(sf.conn, 1, V1, day) == "DUPLICATE"                        # renamed resend: same bytes
    assert sf.run(sf.conn, 1, V1, day, sha="re-exported") == "DUPLICATE"     # same data, different bytes
    assert sf.run(sf.conn, 1, V2, day) == "CONFLICT"                         # same revision, different data
    assert sf.visible(day) == {"T1": 12345678, "T2": 1000}
    assert sf.run(sf.conn, 2, V2, day) == "LOADED"                           # the correction
    assert sf.visible(day) == {"T1": 12345678, "T2": 1500}
    assert sf.run(sf.conn, 1, V1, day, sha="late-copy") == "STALE"           # an old revision arriving late
    assert sf.visible(day) == {"T1": 12345678, "T2": 1500}
    assert sf.loads_kept(day) == 2                    # v1 kept for auditors; losing loads left no rows behind


def test_two_revisions_racing_leave_exactly_one_active(sf):
    day, barrier, outcome = date(2026, 9, 27), threading.Barrier(2), {}

    def racer(revision, rows):
        conn = connect()                                                     # each loader has its own session
        barrier.wait()
        outcome[revision] = sf.run(conn, revision, rows, day)
        conn.close()

    threads = [threading.Thread(target=racer, args=(1, V1)), threading.Thread(target=racer, args=(2, V2))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert outcome[2] == "LOADED" and outcome[1] in {"LOADED", "STALE"}      # depends on who got the lock first
    assert sf.visible(day) == {"T1": 12345678, "T2": 1500}                   # never both, never v1
