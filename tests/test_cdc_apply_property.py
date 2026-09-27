"""Property test for hard problem #1 (docs/specs/01-cdc-apply.md, section 6).

Claim: however a table's changes are duplicated, re-sent, replayed, reordered and split into batches, and even if
some changes from the old epoch never arrive, applying them with the real MERGE from snowflake/01_cdc_apply.sql
(plus the epoch sweep after a re-snapshot) leaves the live silver rows equal to the source's final state, and no
key's version ever goes backwards.

The generated histories include the case that breaks naive LSN ordering: a write that reaches the log just before
the snapshot point but commits just after it, so its position is below the snapshot's.

Runs the real SQL in a scratch schema in Snowflake. Standard tables stand in for the Iceberg ones; the MERGE
semantics are the same. Configure with SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD (or
SNOWFLAKE_AUTHENTICATOR), SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE. Targets Python 3.11.
"""
from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

snowflake_connector = pytest.importorskip("snowflake.connector")

SQL_FILE = Path(__file__).resolve().parents[1] / "snowflake" / "01_cdc_apply.sql"
SWEEP = "sweep"


def cdc_version(epoch: int, phase: int, position: int = 0, row: int = 0) -> int:
    """Same packing as CdcVersion.java: epoch * 10^27 + phase * 10^26 + position * 10^6 + row."""
    return epoch * 10**27 + phase * 10**26 + position * 10**6 + row


class Row(NamedTuple):
    """One change as bronze holds it. Field order matches INSERT_BATCH."""
    loan_id: int
    customer_id: int
    principal_paise: int
    outstanding_paise: int | None
    status: str | None
    remarks: str | None
    cdc_op: str
    cdc_version: int
    cdc_epoch: int
    raw_partition: int
    raw_offset: int


@dataclass(frozen=True)
class Scenario:
    steps: list                 # batches of rows to MERGE, with SWEEP markers in between
    expected: dict              # loan_id -> (outstanding_paise, status): the source table's final state


def replay_state(writes: list) -> dict[int, tuple[int, str]]:
    state: dict[int, tuple[int, str]] = {}
    for key, op, values, _position in writes:
        if op == "d":
            state.pop(key, None)
        else:
            state[key] = values
    return state


@st.composite
def scenarios(draw) -> Scenario:
    # 1. The source: committed writes with commit positions 10 apart. A write reaches the log up to 9 positions
    #    before it commits, so one that commits just after a snapshot point can sit below it (an open transaction).
    #    Two writes to the same key never overlap (row locks), so per key, log positions still increase.
    n_keys = draw(st.integers(1, 4))
    ops = draw(st.lists(
        st.tuples(st.integers(1, n_keys), st.booleans(), st.integers(0, 10**7),
                  st.sampled_from(["ACTIVE", "CLOSED", "WRITTEN_OFF"]), st.integers(0, 9)),
        min_size=1, max_size=20))
    alive: dict[int, tuple[int, str]] = {}
    writes = []                                     # (key, op, values, log position)
    for i, (key, delete, paise, status, lag) in enumerate(ops, start=1):
        if delete and key in alive:
            del alive[key]
            writes.append((key, "d", None, 10 * i - lag))
        else:
            writes.append((key, "u" if key in alive else "c", (paise, status), 10 * i - lag))
            alive[key] = (paise, status)
    n = len(writes)

    # 2. What Debezium emits. Epoch 1 streams the first `emitted` writes. Then the epoch is bumped: the connector
    #    restarts from its last committed offset (`resume`, possibly earlier), and a snapshot reads the state after
    #    `snap_at` commits. Rule 5 holds by construction: the new epoch's live tail starts at or before the snapshot.
    emitted = draw(st.integers(0, n))
    resume = draw(st.integers(0, emitted))
    snap_at = draw(st.integers(emitted, n))
    epoch1 = [(1, 1, key, op, values, pos) for key, op, values, pos in writes[:emitted]]
    live2 = [(2, 1, key, op, values, pos) for key, op, values, pos in writes[resume:]]
    snapshot = [(2, 0, key, "r", values, 0) for key, values in sorted(replay_state(writes[:snap_at]).items())]
    cut = draw(st.integers(0, len(live2)))
    resend_from = draw(st.integers(0, len(live2)))  # a Debezium restart re-sends a tail with new offsets
    emission = epoch1 + live2[:cut] + snapshot + live2[cut:] + live2[resend_from:]

    rows = []
    for offset, (epoch, phase, key, op, values, position) in enumerate(emission):
        paise, status = values if values else (None, None)
        rows.append(Row(key, key * 100, 5_000_000, paise, status, None, op,
                        cdc_version(epoch, phase, position), epoch, 0, offset))

    # 3. Some epoch-1 changes may never arrive (the snapshot plus the sweep must heal that); some rows are
    #    replayed verbatim, with the same offsets.
    lose = draw(st.booleans())
    if lose:
        rows = [r for r in rows if r.cdc_epoch != 1 or draw(st.booleans())]
    if rows:
        rows += draw(st.lists(st.sampled_from(rows), max_size=5))

    # 4. Any order, any batching, and the sweep somewhere in between (always, if changes were lost).
    rows = draw(st.permutations(rows))
    cuts = sorted(draw(st.lists(st.integers(0, len(rows)), max_size=4)))
    steps: list = [rows[a:b] for a, b in zip([0, *cuts], [*cuts, len(rows)])]
    if lose or draw(st.booleans()):
        steps.insert(draw(st.integers(0, len(steps))), SWEEP)
    return Scenario(steps, replay_state(writes))


def sql_block(name: str) -> str:
    """The exact statement between '-- <name>:begin' and '-- <name>:end' in the production SQL file."""
    match = re.search(rf"-- {name}:begin\n(.*?)\n-- {name}:end", SQL_FILE.read_text(), re.S)
    assert match, f"marker '{name}' missing from {SQL_FILE}"
    return match.group(1).strip().rstrip(";")


def in_scratch(sql: str, schema: str) -> str:
    return (sql.replace("bronze.lending__loan_account__changes", f"{schema}.batch")
               .replace("silver.lending__loan_account", f"{schema}.silver")
               .replace("ctl.cdc_epoch_floor", f"{schema}.cdc_epoch_floor")
               .replace("ctl.cdc_epoch_sweep", f"{schema}.cdc_epoch_sweep"))


SCRATCH_TABLES = """
CREATE TABLE {s}.batch (
    loan_id NUMBER(19,0), customer_id NUMBER(19,0), principal_paise NUMBER(19,0), outstanding_paise NUMBER(19,0),
    status STRING, remarks STRING, updated_at TIMESTAMP_NTZ(6), cdc_op STRING, cdc_version NUMBER(38,0),
    cdc_epoch NUMBER(19,0), src_commit_ts_utc TIMESTAMP_NTZ(3), raw_partition NUMBER(10,0), raw_offset NUMBER(19,0));
CREATE TABLE {s}.silver (
    loan_id NUMBER(19,0) NOT NULL, customer_id NUMBER(19,0), principal_paise NUMBER(19,0),
    outstanding_paise NUMBER(19,0), status STRING, remarks STRING, updated_at_utc TIMESTAMP_NTZ(6),
    is_deleted BOOLEAN NOT NULL, cdc_version NUMBER(38,0) NOT NULL, raw_offset NUMBER(19,0) NOT NULL,
    cdc_epoch NUMBER(19,0) NOT NULL, src_commit_ts_utc TIMESTAMP_NTZ(3), applied_at_utc TIMESTAMP_NTZ(6));
CREATE TABLE {s}.cdc_epoch_floor (
    table_name STRING NOT NULL, min_live_epoch NUMBER(19,0) NOT NULL, raised_at_utc TIMESTAMP_NTZ NOT NULL)
"""

INSERT_BATCH = ("INSERT INTO {s}.batch (loan_id, customer_id, principal_paise, outstanding_paise, status, remarks, "
                "cdc_op, cdc_version, cdc_epoch, raw_partition, raw_offset) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")


@dataclass
class Harness:
    conn: object
    schema: str
    merge_sql: str

    def run(self, sql: str, params=None) -> list:
        cur = self.conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else []

    def reset(self) -> None:
        for table in ("batch", "silver", "cdc_epoch_floor"):
            self.run(f"TRUNCATE TABLE {self.schema}.{table}")

    def apply(self, rows: list[Row]) -> None:
        self.run(f"TRUNCATE TABLE {self.schema}.batch")
        if rows:
            self.conn.cursor().executemany(INSERT_BATCH.format(s=self.schema), rows)
        self.run(self.merge_sql)

    def sweep(self, new_epoch: int) -> None:
        self.run(f"CALL {self.schema}.cdc_epoch_sweep(%s, %s, %s)",
                 ("lending.loan_account", f"{self.schema}.silver", new_epoch))

    def silver(self) -> dict[int, tuple]:
        rows = self.run(f"SELECT loan_id, cdc_version, raw_offset, is_deleted, outstanding_paise, status "
                        f"FROM {self.schema}.silver")
        return {int(k): (int(v), int(o), bool(d), p, s) for k, v, o, d, p, s in rows}


@pytest.fixture(scope="module")
def harness():
    needed = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE")
    if any(name not in os.environ for name in needed):
        pytest.skip("Snowflake connection not configured")
    conn = snowflake_connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR", "snowflake"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )
    schema = f"CDC_PROPTEST_{uuid.uuid4().hex[:8].upper()}"
    h = Harness(conn, schema, in_scratch(sql_block("apply-merge"), schema))
    h.run(f"CREATE SCHEMA {schema}")
    for ddl in SCRATCH_TABLES.format(s=schema).split(";"):
        h.run(ddl)
    h.run(in_scratch(sql_block("epoch-sweep"), schema))
    yield h
    h.run(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    conn.close()


@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(scenario=scenarios())
def test_silver_converges_to_the_source_whatever_the_delivery(harness, scenario):
    harness.reset()
    highest: dict[int, tuple[int, int]] = {}
    for step in scenario.steps:
        if step == SWEEP:
            harness.sweep(new_epoch=2)
        else:
            harness.apply(step)
        for key, (version, offset, *_rest) in harness.silver().items():
            assert (version, offset) >= highest.get(key, (-1, -1)), f"loan {key} went back to an older version"
            highest[key] = (version, offset)

    live = {key: (paise, status) for key, (_v, _o, deleted, paise, status) in harness.silver().items()
            if not deleted}
    assert live == scenario.expected
