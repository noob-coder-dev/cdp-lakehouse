"""Hard problem #2: load each partner file exactly once, all at once, and only if it reconciles to the paise.

Spec: docs/specs/02-partner-files.md. Runs on EMR (Python 3.11, Spark 3.5):

    spark-submit partner_file_loader.py --file s3://cdp-landing/nbfc_a/repayment/NBFC_A_REPAY_20260926.txt.gz \
        --feed repayment --date-format dd/MM/yyyy --staging s3://cdp-staging/partner_rows

Guarantee code: amount_to_paise, validate_and_reconcile, commit_load, classify_lost_claim. The rest is plumbing.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import os
import re
import uuid
from dataclasses import dataclass
from datetime import date, timedelta

import boto3
import snowflake.connector
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType

MONEY = r"^-?[0-9]+(\.[0-9]{1,2})?$"            # rupees, after removing grouping commas: at most two decimals
WHOLE = r"^-?[0-9]+$"                           # paise: whole numbers only
HEADER = re.compile(r"H\|(\w+)\|(\w+)\|(\d{4}-\d{2}-\d{2})\|(\d+)\|(\d+)")
TRAILER = re.compile(r"T\|(\d+)\|(-?\d+)")


class Quarantine(Exception):
    """The file is rejected whole; the reason goes to the ledger and to the partner."""

    def __init__(self, reason: str, header: Header | None = None):
        super().__init__(reason)
        self.header = header           # known once the header parsed, so the ledger can name the blocked slot


@dataclass(frozen=True)
class FeedContract:
    """A partner feed's parsing agreement, from the contract repo."""
    feed: str
    date_format: str                   # e.g. dd/MM/yyyy: never guessed from the data
    amount_unit: str = "rupees"        # detail amounts: "rupees" (1,234.50) or "paise" (123450); trailers are in paise
    max_back_days: int = 7             # detail dates must fall in [business_date - max_back_days, business_date]
    has_trailer: bool = True           # without one the file loads as UNRECONCILED and can't certify a day


@dataclass(frozen=True)
class Header:
    partner: str
    feed: str
    business_date: date
    seq: int
    revision: int


@dataclass(frozen=True)
class Proof:
    """What a file proved about itself. Becomes its ledger row."""
    rows: int
    total_paise: int
    fingerprint: str                   # same data in any order or formatting gives the same fingerprint
    reconciled: bool


def amount_to_paise(amount: Column, unit: str = "rupees") -> Column:
    """Exact paise, or null if the text isn't an amount in the agreed unit. Rupee amounts may have at most two
    decimals, so the DECIMAL(20,2) cast can't round; without that check Spark would turn '10.005' into 10.01 without
    an error. Paise amounts must be whole, so rupees sent to a paise feed fail here; paise sent to a rupee feed come
    out 100 times too big and fail the trailer check."""
    plain = F.regexp_replace(F.trim(amount), ",", "")    # handles 1,234.50 and 1,23,456.78 alike
    if unit == "paise":
        return F.when(plain.rlike(WHOLE), plain.cast(LongType()))
    if unit != "rupees":
        raise ValueError(f"unknown amount unit in the feed contract: {unit}")
    return F.when(plain.rlike(MONEY), (plain.cast(DecimalType(20, 2)) * 100).cast(LongType()))


def validate_and_reconcile(lines: DataFrame, contract: FeedContract) -> tuple[Header, DataFrame, Proof]:
    """Parse the file and prove it whole. Any failure raises Quarantine: one bad row blocks the whole file."""
    lines = lines.where(F.trim("value") != "")
    parts = F.split(F.regexp_replace("value", "^﻿", ""), r"\|")      # drop a byte-order mark
    kind = F.try_element_at(parts, F.lit(1))
    kinds = {r["kind"]: r["count"] for r in lines.groupBy(kind.alias("kind")).count().collect()}
    expected_trailers = 1 if contract.has_trailer else 0
    if set(kinds) - {"H", "D", "T"} or kinds.get("H") != 1 or kinds.get("T", 0) != expected_trailers:
        raise Quarantine(f"expected one header, {expected_trailers} trailer and detail rows only; found {kinds}")

    header_line = lines.where(kind == "H").select(F.regexp_replace("value", "^﻿", "")).first()[0]
    h = HEADER.fullmatch(header_line.strip())
    if not h or h[2] != contract.feed:
        raise Quarantine(f"unreadable header or wrong feed: {header_line!r}")
    header = Header(h[1], h[2], date.fromisoformat(h[3]), int(h[4]), int(h[5]))

    field = lambda i: F.trim(F.try_element_at(parts, F.lit(i)))           # noqa: E731
    details = lines.where(kind == "D").select(
        F.size(parts).alias("fields"),
        field(2).alias("txn_id"),
        field(3).alias("loan_id"),
        F.to_date(F.try_to_timestamp(field(4), F.lit(contract.date_format))).alias("txn_date"),
        amount_to_paise(field(5), contract.amount_unit).alias("amount_paise"),
        field(6).alias("channel"),
    ).cache()
    earliest = header.business_date - timedelta(days=contract.max_back_days)
    bad = ((F.col("fields") != 6)
           | (F.coalesce(F.col("txn_id"), F.lit("")) == "") | (F.coalesce(F.col("loan_id"), F.lit("")) == "")
           | ~F.coalesce(F.col("txn_date").between(earliest, header.business_date), F.lit(False))
           | F.col("amount_paise").isNull())
    s = details.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(bad, 1).otherwise(0)).alias("bad_rows"),
        F.countDistinct("txn_id").alias("distinct_txn"),
        F.sum("amount_paise").alias("total_paise"),              # 64-bit; ANSI mode fails on overflow, never wraps
        F.sum(F.xxhash64("txn_id", "loan_id", "txn_date", "amount_paise", "channel")
              .cast(DecimalType(38, 0))).alias("checksum"),      # order-independent: sum of per-row hashes
    ).first()
    if s["bad_rows"]:
        raise Quarantine(f"{s['bad_rows']} bad rows (field count, key, date outside window, or amount)", header)
    if s["distinct_txn"] != s["rows"]:
        raise Quarantine(f"duplicate txn_id: {s['rows'] - s['distinct_txn']} repeats inside the file", header)
    total = s["total_paise"] or 0
    if contract.has_trailer:
        trailer_line = lines.where(kind == "T").first()["value"].strip()
        t = TRAILER.fullmatch(trailer_line)
        if not t:
            raise Quarantine(f"unreadable trailer: {trailer_line!r}", header)
        if int(t[1]) != s["rows"] or int(t[2]) != total:
            raise Quarantine(
                f"trailer says {t[1]} rows / {t[2]} paise, file has {s['rows']} / {total} total", header)

    proof = Proof(s["rows"], total, f"{s['rows']}:{total}:{s['checksum'] or 0}", contract.has_trailer)
    return header, details.drop("fields"), proof


INSERT_ROWS = """
INSERT INTO bronze.partner_repayment_rows
    (load_id, partner_id, feed, business_date, seq, txn_id, loan_id, txn_date, amount_paise, channel)
SELECT %(load_id)s, %(partner)s, %(feed)s, %(business_date)s, %(seq)s,
       $1:txn_id::STRING, $1:loan_id::STRING, $1:txn_date::DATE, $1:amount_paise::NUMBER(19,0), $1:channel::STRING
FROM @ctl.partner_staging/load_id={load_id}/ (FILE_FORMAT => 'ctl.parquet_ff')
"""

CLAIM_SLOT = """
MERGE INTO ctl.partner_file_slots AS t
USING (SELECT %(partner)s AS partner_id, %(feed)s AS feed, %(business_date)s::DATE AS business_date,
              %(seq)s AS seq, %(revision)s AS revision, %(load_id)s AS load_id) AS s
ON t.partner_id = s.partner_id AND t.feed = s.feed AND t.business_date = s.business_date AND t.seq = s.seq
WHEN MATCHED AND s.revision > t.active_revision THEN UPDATE SET
    active_load_id = s.load_id, active_revision = s.revision, updated_at_utc = SYSDATE()
WHEN NOT MATCHED THEN INSERT
    (partner_id, feed, business_date, seq, active_load_id, active_revision, updated_at_utc)
    VALUES (s.partner_id, s.feed, s.business_date, s.seq, s.load_id, s.revision, SYSDATE())
"""

RECORD_LOAD = """
INSERT INTO ctl.partner_file_loads
    (load_id, partner_id, feed, business_date, seq, revision, sha256, fingerprint, row_count, total_paise,
     status, reason, source_uri, recorded_at_utc)
VALUES (%(load_id)s, %(partner)s, %(feed)s, %(business_date)s, %(seq)s, %(revision)s, %(sha256)s, %(fingerprint)s,
        %(rows)s, %(total_paise)s, %(status)s, %(reason)s, %(source_uri)s, SYSDATE())
"""


def commit_load(conn, p: dict) -> str:
    """The exactly-once step: the rows, the slot pointer and the ledger row commit together or not at all.
    Rows go in first (INSERTs don't block other loaders); the claim comes last, so its lock is short."""
    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        cur.execute(INSERT_ROWS.format(load_id=p["load_id"]), p)
        if cur.rowcount != p["rows"]:                            # a staged part-file went missing
            raise RuntimeError(f"inserted {cur.rowcount} rows, reconciled {p['rows']}")
        cur.execute(CLAIM_SLOT, p)                               # MERGEs on one table run one at a time
        inserted, updated = cur.fetchone()
        if inserted + updated != 1:                              # the slot already holds this revision or newer
            cur.execute("ROLLBACK")
            return classify_lost_claim(conn, p)
        status = "LOADED" if p["reconciled"] else "UNRECONCILED"
        cur.execute(RECORD_LOAD, {**p, "status": status, "reason": None})
        cur.execute("COMMIT")
        return status
    except Exception:
        cur.execute("ROLLBACK")
        raise


ACTIVE_LOAD = """
SELECT l.revision, l.sha256, l.fingerprint
FROM ctl.partner_file_slots AS s JOIN ctl.partner_file_loads AS l ON l.load_id = s.active_load_id
WHERE s.partner_id = %(partner)s AND s.feed = %(feed)s AND s.business_date = %(business_date)s AND s.seq = %(seq)s
"""


def classify_lost_claim(conn, p: dict) -> str:
    """Why the slot wasn't ours. A resend of the same data is a no-op; anything else waits for a person."""
    cur = conn.cursor()
    cur.execute(ACTIVE_LOAD, p)
    active_revision, active_sha, active_fingerprint = cur.fetchone()
    if p["sha256"] == active_sha or (p["revision"] == active_revision and p["fingerprint"] == active_fingerprint):
        status, reason = "DUPLICATE", f"same data as active revision {active_revision}"
    elif p["revision"] < active_revision:
        status, reason = "STALE", f"revision {p['revision']} is older than active revision {active_revision}"
    else:
        status, reason = "CONFLICT", f"revision {p['revision']} is already active with different data"
    cur.execute(RECORD_LOAD, {**p, "status": status, "reason": reason})
    return status


# ---------------------------------------------------------------------------------------------------- plumbing

def sha256_of(uri: str) -> str:
    """SHA-256 of the file's content, streamed from S3. Compressed files are hashed after decompressing: gzip
    stores the original file name inside the archive, so the same data zipped under two names differs."""
    bucket, key = uri.removeprefix("s3://").split("/", 1)
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
    stream = gzip.GzipFile(fileobj=body) if key.endswith(".gz") else body
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--feed", required=True)
    parser.add_argument("--date-format", required=True)
    parser.add_argument("--staging", required=True)
    parser.add_argument("--amount-unit", choices=["rupees", "paise"], default="rupees")
    parser.add_argument("--no-trailer", action="store_true")
    args = parser.parse_args()
    contract = FeedContract(args.feed, args.date_format, amount_unit=args.amount_unit,
                            has_trailer=not args.no_trailer)

    spark = SparkSession.builder.appName("partner-file-loader").config("spark.sql.ansi.enabled", "true").getOrCreate()
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"], role="PARTNER_LOADER",
        warehouse="WH_INGEST", database="CDP")
    conn.cursor().execute("ALTER SESSION SET LOCK_TIMEOUT = 300 STATEMENT_TIMEOUT_IN_SECONDS = 3600")

    p = {"load_id": str(uuid.uuid4()), "source_uri": args.file, "sha256": sha256_of(args.file),
         "partner": None, "feed": args.feed, "business_date": None, "seq": None, "revision": None,
         "fingerprint": None, "rows": None, "total_paise": None}
    seen = conn.cursor().execute("SELECT 1 FROM ctl.partner_file_loads WHERE sha256 = %s "
                                 "AND status IN ('LOADED', 'UNRECONCILED') LIMIT 1", (p["sha256"],)).fetchone()
    if seen:                                                     # cheap exit before any Spark work
        conn.cursor().execute(RECORD_LOAD, {**p, "status": "DUPLICATE", "reason": "exact bytes already loaded"})
        return
    try:
        header, details, proof = validate_and_reconcile(spark.read.text(args.file), contract)
    except Quarantine as q:
        if q.header:
            p.update(partner=q.header.partner, business_date=q.header.business_date, seq=q.header.seq,
                     revision=q.header.revision)
        conn.cursor().execute(RECORD_LOAD, {**p, "status": "QUARANTINED", "reason": str(q)})
        print("QUARANTINED:", q)
        return
    p.update(partner=header.partner, business_date=header.business_date, seq=header.seq,
             revision=header.revision, fingerprint=proof.fingerprint, rows=proof.rows,
             total_paise=proof.total_paise, reconciled=proof.reconciled)
    details.write.mode("errorifexists").parquet(f"{args.staging}/load_id={p['load_id']}")
    print(commit_load(conn, p))


if __name__ == "__main__":
    main()
