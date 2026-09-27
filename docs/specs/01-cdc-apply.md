# Hard problem #1: Applying database changes safely

**Status:** approved and implemented. Summarised in section 8.1 of the design doc.
**Scope:** CDC from the service databases (MySQL/Postgres) into silver.
**Path:** Debezium → Kafka raw → Flink → Kafka clean → Snowflake connector v3 → bronze → MERGE → silver.

---

## 1. The guarantee

For every captured table, each silver row equals the source row at the **highest version seen for that key**, or is marked deleted if that version is a delete. This holds no matter how often changes are duplicated or replayed, in what order they arrive, or how they are split into batches. Silver never moves backwards, and a deleted row never comes back because an older change arrived late.

As a property: **silver depends on the set of changes seen, not on the order they were seen in.**

## 2. Why it's hard

| # | What goes wrong | Where it comes from | What a naive upsert does |
|---|---|---|---|
| 1 | The same change arrives twice | Debezium is at-least-once: after a restart it resends from its last committed offset. A replay after a Flink bug fix resends whole ranges. | Rewrites the row; if something newer arrived in between, the row goes back in time |
| 2 | An older change arrives after a newer one | Replays, resends, a re-snapshot running alongside the live stream | Overwrites new data with old |
| 3 | Several changes for one key in one batch | Normal traffic (a loan updated 3 times in 5 minutes) | Snowflake's MERGE fails on a nondeterministic match, or picks one at random |
| 4 | A deleted row comes back | A delete, then a late copy of an earlier update | Re-inserts the row |
| 5 | "Newer" is defined wrongly | **Timestamps:** clock skew, same-millisecond ties, MySQL binlog second precision. **Postgres `source.lsn`:** a per-row position that is *"not monotonic across overlapping/long-running transactions"* (Debezium PR #7764). A transaction still open when a snapshot is taken has LSNs **below** the snapshot point but commits **after** it. | A newer change is rejected as "older than the snapshot", so an update is lost |
| 6 | Positions stop being comparable | MySQL primary failover (new server, new binlog coordinates), a lost Postgres replication slot, a repartitioned topic | New changes get rejected, or old ones accepted, silently |
| 7 | Updates carry partial rows | Postgres leaves out unchanged large (TOAST) values unless configured not to; MySQL with `binlog_row_image=MINIMAL` | Real values are overwritten with a placeholder or null |
| 8 | A snapshot can't heal a missed delete | A snapshot lists only the rows that exist; it can't say a key is gone | Stale rows live forever |

All eight fail silently. Nothing crashes; a balance is just wrong.

## 3. Options weighed

| Option | Verdict | Why |
|---|---|---|
| Last arrival wins (plain upsert) | Rejected | Breaks on 1–4 |
| Kafka offset as the version | Rejected as the main version; kept as the tie-breaker | Debezium resends get new, higher offsets, so rows briefly go backwards. It also breaks if a topic is repartitioned or replayed through a new topic. |
| Source timestamp (`ts_ms`) | Rejected | Clock skew, ties, clock jumps at failover |
| Per-row source position alone (LSN / binlog position) | Rejected on its own | Correct between two writes to the same row, but wrong against a snapshot (row 5) and across failovers (row 6) |
| Commit LSN (`source.commit_lsn`) | Not available | Proposed in Debezium PR #7764, which was still open and unmerged in Sep 2026. It would simplify Postgres later. |
| A row-version column maintained by each app | Rejected as a platform rule | Robust, but we can't require every service table to have one |
| "Latest version per key" computed over the whole change log (dynamic table) | Rejected | Needs the full log forever. Bronze keeps 13 months in Snowflake, so keys untouched for 13 months would disappear. |
| Apply in Flink with per-key state, emitting upserts | Rejected | Flink state isn't the source of truth (restoring an older checkpoint replays data), and the Snowflake connector only inserts, so a MERGE is needed anyway |
| **Version = (epoch, phase, source position, row), with raw offset as tie-breaker; a MERGE with a version guard, tombstones and an epoch floor** | **Chosen** | Handles 1–8, and each rule is small and testable |

## 4. The design

### 4.1 The version

One number per change, computed once, in Flink:

```
cdc_version = epoch × 10^27  +  phase × 10^26  +  position × 10^6  +  row      -- NUMBER(38,0)
```

| Part | Meaning | Postgres | MySQL | Snapshot row (`op = 'r'`) |
|---|---|---|---|---|
| epoch | Which timeline the position belongs to; stamped at capture | `__cdc_epoch` | `__cdc_epoch` | same |
| phase | 0 = snapshot read, 1 = live change | 1 | 1 | 0 |
| position | Where the change sits in the source log | `source.lsn` | binlog file number × 10^10 + `source.pos` | 0 |
| row | Row within one log event | 0 | `source.row` | 0 |

**Digit widths:**
- position < 10^20 (a 64-bit LSN fits)
- binlog `pos` < 10^10 (binlog files are at most 1 GB)
- row < 10^6

Values outside these ranges are rejected, so no part can spill into the next.

**Tie-breaker:** ties on `cdc_version` are broken by `raw_offset`, the change's offset in the raw Kafka topic. Within an epoch, a key's changes always sit in one partition, so this is Debezium's own emission order.

### 4.2 The rules

1. **Newer wins.** A change is applied only if its (`cdc_version`, `raw_offset`) is greater than the row's current pair. Equal or older changes are ignored. This makes replays and duplicates harmless, and makes arrival order irrelevant.
2. **One change per key per batch.** Before the MERGE, each batch keeps only the highest-version change per key.
3. **Deletes leave a tombstone.**
   - A delete sets `is_deleted = TRUE` and keeps the delete's version, so a late, older update loses.
   - A delete for an unknown key inserts a tombstone.
   - A newer insert revives the row.
   - Readers use a view that hides tombstones.
4. **Within an epoch, snapshot rows lose to live changes; across epochs, they beat everything older.** This fixes the "transaction still open during the snapshot" problem without needing a commit LSN:
   - Within an epoch, live changes are a complete, ordered tail of the log. So a key's highest live change is always its true state.
   - A key with no live change in the epoch hasn't changed since the epoch began, so its snapshot row is its true state.
5. **Every snapshot starts a new epoch.** Runbook order:
   1. Bump the epoch in the connector config.
   2. Trigger the snapshot (Debezium's incremental-snapshot signal).

   The connector restarts from its last committed offset. So the new epoch's live changes start at or before the snapshot, which is what rule 4 needs.
6. **An epoch sweep heals missed deletes.** After a full snapshot of a table in epoch E, one transaction does both of these:
   - deletes every silver row whose epoch is below E (whatever the snapshot and the new epoch's changes didn't touch no longer exists at the source)
   - raises the table's floor to E

   The MERGE drops any change whose epoch is below the floor, so nothing from the old timeline can come back. If the sweep runs too early, it heals itself: the key's snapshot row still arrives later and re-inserts it, because epoch E passes the floor.
7. **Flink refuses to guess across a topology change.** The connector also stamps the MySQL server it expects (`__cdc_expected_server`). If `__server_id` differs, Flink sends the change to a HOLD topic and raises an alert. Runbook:
   1. Bump the epoch and the expected server.
   2. Replay the HOLD topic under the new epoch. All held changes come from the new server, so their positions are comparable with each other.

   Two related rules:
   - Repartitioning a topic is forbidden without an epoch bump and a re-snapshot.
   - A lost Postgres replication slot means a connector reset, a new epoch and a snapshot.
8. **Partial rows keep the existing value.** Columns that can carry Debezium's `__debezium_unavailable_value` placeholder are merged column by column: a placeholder keeps the current silver value. Onboarding checks the preconditions: `binlog_row_image=FULL` on MySQL, and `REPLICA IDENTITY FULL` on Postgres tables with large columns.
9. **Schema changes:** added columns flow into bronze, because connector v3 evolves Avro schemas. They reach silver only when the table's contract adds them. If a required column goes missing, the change goes to HOLD.
10. **Every table needs a primary key**, or a key defined with `message.key.columns`. Onboarding rejects tables without one.

### 4.3 How the version travels

- **Capture (configuration, no code):**
  - Debezium's `ExtractNewRecordState` flattens each record and adds `__op`, the position fields and `__source_ts_ms`.
  - Deletes become rows with `__deleted = true`.
  - Kafka Connect's `InsertField$Value` stamps `__cdc_epoch`, and `__cdc_expected_server` for MySQL.
- **Flink (Java):**
  - reads the raw topics; the function keeps no state and runs at any parallelism, because the MERGE resolves order by version
  - validates each record and computes `cdc_version`
  - adds `cdc_version`, `cdc_op`, `cdc_epoch`, `src_commit_ts_utc`, `raw_partition` and `raw_offset`
  - writes to the clean topic, DLQ or HOLD. There is **one exactly-once Kafka sink**, so everything from a checkpoint commits together.
  - the per-partition audit records (offset ranges plus counts) belong to hard problem #3's "every record accounted for" check. They are described there, not coded here.
- **Snowflake:**
  - connector v3 appends to the bronze Iceberg table
  - an append-only stream on bronze feeds a task that runs the MERGE every 5 minutes
  - the sweep is a stored procedure that the runbook calls

## 5. The code

Line counts exclude comments, blank lines and imports. The Java builds against Flink 1.20.0 on Java 17.

**Guarantee code (about 220 lines):**

| File | What it enforces | Lines |
|---|---|---|
| `flink-cdc-normalizer/.../CdcVersion.java` | The version definition: epoch and phase (rules 4–5), position encoding, range checks | 51 |
| `flink-cdc-normalizer/.../CdcNormalizeFunction.java` | Validation, and routing to clean, DLQ or HOLD (rules 7 and 9) | 100 |
| `snowflake/01_cdc_apply.sql` | The MERGE task (rules 1–3, the floor from rule 6, rule 8) and the sweep procedure (rule 6) | 70 |

**Plumbing (about 200 lines):**
- `CdcNormalizerJob.java`: wiring, and the one exactly-once Kafka sink
- `CleanRecordCodec.java`: Avro and Schema Registry
- `RawRecord.java`, `OutRecord.java`
- `snowflake/00_cdc_tables.sql`: tables, view and stream

Stubbed: the contract registry (one example table), and the Debezium/Connect settings, which are configuration rather than code.

## 6. Tests

| Test | What it asserts | How it fails if the design is wrong |
|---|---|---|
| **Property test (real code):** Python + Hypothesis, against Snowflake | Generate random source histories: inserts, updates, deletes, re-inserts, transactions still open during a snapshot, and one epoch change. Turn them into change events, then duplicate them, resend them with new offsets, shuffle them and split them into random batches. After applying every batch with the real MERGE, assert two things: (a) the live silver rows equal the source's final state, and (b) no key's version ever decreases between batches. | Remove the version guard → an older change wins. Use physical deletes instead of tombstones → a deleted row comes back. Put snapshot rows on the same timeline as live changes → the open-transaction case loses an update. Skip the per-batch reduction → Snowflake's nondeterministic-merge error. Hypothesis shrinks each failure to a minimal history. |
| Version ordering (JUnit, planned) | A higher epoch always wins; a live change beats a snapshot row in the same epoch; MySQL file rollover orders correctly (`000009:999` < `000010:4`); out-of-range values are rejected | Wrong digit widths let a position spill into the phase or epoch |
| Routing (JUnit, planned) | A missing op, epoch or key → DLQ. A server mismatch or a missing required column → HOLD. Kafka tombstones are filtered and counted. | A mismatched change reaches the clean topic and gets ordered on the wrong timeline |
| Every record accounted for | Moved to hard problem #3 | — |
| Sweep (SQL, planned) | After a snapshot in epoch E and a sweep, keys missing from the snapshot are gone; an old-epoch change arriving afterwards is dropped; a sweep run too early heals itself | Without the floor, a late old change brings a swept key back |
| Partial rows (SQL, planned) | A placeholder value never overwrites a real one | Placeholder text shows up in silver |

## 7. Limits (stated in the design doc)

- **Each table converges on its own.** A transaction that touches several tables can be seen half-applied across silver tables. That's hard problem #6, not solved here; finance only reads complete, gated days.
- **Silver lags the source** by the pipeline's latency (target: under 10 minutes, 95% of the time).
- **Epoch changes are a runbook step, with a human in the loop.** During a MySQL failover, that database's changes wait in HOLD until someone confirms the new server.
- **Correctness depends on three things:**
  - Debezium sending each key's changes to one partition, in commit order
  - Postgres row locks, which stop two writes to the same row from overlapping
  - the runbook order in rule 5
- **Future simplification:** if Debezium ships a commit LSN (PR #7764), rule 4 could become a plain position comparison for Postgres.

## 8. To verify when pinning versions

- The exact `ExtractNewRecordState` options and added field names in the pinned Debezium version (`add.fields`, `delete.tombstone.handling.mode`)
- Whether MySQL `source.pos` is per event or per transaction. The raw-offset tie-breaker covers both cases.
- How connector v3 maps Avro `decimal(38,0)` onto an Iceberg `NUMBER(38,0)` column
- Append-only streams on Snowflake-managed Iceberg tables

**Sources:**
- [Debezium PR #7764: expose the transaction commit LSN (open)](https://github.com/debezium/debezium/pull/7764)
- [Debezium PostgreSQL connector docs](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
