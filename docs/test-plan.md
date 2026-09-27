# Test plan

For each hard problem: what we assert, and how the test would fail if the design were wrong. Two of the tests are real code, and the rest are planned. Details are in each spec's section 6.

**Running the real tests** (Python 3.11; they haven't been run for this submission):

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r tests/requirements.txt
export SNOWFLAKE_ACCOUNT=… SNOWFLAKE_USER=… SNOWFLAKE_PASSWORD=… SNOWFLAKE_WAREHOUSE=… SNOWFLAKE_DATABASE=…
.venv/bin/pytest tests/
```

- The local Spark tests need only `pyspark`.
- The Snowflake tests skip themselves when the connection variables aren't set. When they run, they create a scratch schema and drop it afterwards.

## #1 Applying database changes safely

| Test | Status | What it asserts | How it fails if the design is wrong |
|---|---|---|---|
| **Silver converges whatever the delivery** (`tests/test_cdc_apply_property.py`) | Real code (Hypothesis, against Snowflake; runs the MERGE and sweep text straight from `snowflake/01_cdc_apply.sql`) | Random source histories: inserts, updates, deletes, re-inserts, transactions still open during a snapshot, and one epoch change. The changes are duplicated, re-sent with new offsets, replayed, shuffled and split into random batches, and some old-epoch changes are lost (then the sweep runs). Assert that the live silver rows equal the source's final state, and that no key's version ever decreases between batches. | Remove the version guard → an older change wins. Use physical deletes → a deleted row comes back. Put snapshot rows on the same timeline as live changes → the open-transaction case loses an update. Skip the per-batch reduction → Snowflake's nondeterministic-merge error. Drop the floor → a lost-and-late old change brings back a swept key. Hypothesis shrinks each failure to a minimal history. |
| Version ordering | Planned (JUnit) | A higher epoch always wins; a live change beats a snapshot row in the same epoch; MySQL binlog rollover orders correctly (`000009:999` < `000010:4`); out-of-range parts are rejected | Wrong digit widths let a position spill into the phase or epoch |
| Routing | Planned (JUnit) | A missing op, epoch or key → DLQ. A MySQL server mismatch or a missing contract column → HOLD. Kafka tombstones are skipped. | A mismatched change reaches the clean topic and gets ordered on the wrong timeline |
| Sweep | Planned (SQL) | After a snapshot in epoch E and a sweep, keys absent from the snapshot are gone, and a later old-epoch change is dropped. A sweep that runs too early heals itself. | Without the floor, a late old change brings back a swept key |
| Partial rows | Planned (SQL) | Debezium's unavailable-value placeholder never overwrites a real value | Placeholder text appears in silver |
| Connector and transactional topics | Planned (pre-production) | The pinned Snowflake connector v3 reads a Flink exactly-once topic with `read_committed`: no stalls, and counts match | The connector stalls on offset gaps, which Snowflake's docs warn about |

## #2 Loading partner files exactly once

| Test | Status | What it asserts | How it fails if the design is wrong |
|---|---|---|---|
| **Money, validation and fingerprint** (`tests/test_partner_files.py`, part 1) | Real code (local Spark) | `1,23,456.78` → 12345678 paise. Quarantined: `10.005`, a missing trailer, a trailer one paisa off, a date outside the window, a duplicate `txn_id`, a wrong feed in the header. A paise feed accepts `12345678` and quarantines `123.45` (rupees sent by mistake). The same data reordered and reformatted gives the same fingerprint. | Drop the pattern check → `10.005` loads as 1001 paise. Sum as doubles → totals drift. Ignore the trailer → a truncated file loads. Hash raw text instead of normalised rows → a re-export becomes a conflict. |
| **Load protocol** (part 2) | Real code (against Snowflake; the real claim, ledger and classification SQL) | Resend (same bytes) → DUPLICATE. Re-export → DUPLICATE. Same revision with different data → CONFLICT. Revision 2 → only v2's rows visible. A late revision 1 → STALE. v1 stays in bronze, and losing loads leave no rows. Two revisions racing from two sessions → v2 active either way, never both. | Claim first and insert later without the rollback → half-loaded files become visible. Claim with INSERT instead of MERGE → concurrent loaders both win. Delete-then-insert corrections → a reader sees an empty day. |
| No trailer | Planned | Loads as UNRECONCILED, and the day isn't certified | The weaker guarantee gets hidden |
| Staged count | Planned | A missing staged Parquet part → inserted ≠ reconciled count → rollback | A partial file becomes visible |

## #3 The trust gate

All tests for #3 are planned, not coded.

| Test | What it asserts | How it fails if the design is wrong |
|---|---|---|
| Unbroken run | Dates 1, 2 and 4 complete, 3 missing → certified through 2, and 4 waits | Certifying 4 → month-to-date totals are silently short |
| Missing is incomplete | An expected file that never arrived → the date isn't complete, and an alert fires once it's past due | Using arrivals only → an absent file looks like a quiet day |
| Loaded ≠ reconciled | UNRECONCILED or CONFLICT blocks the date, with its reason | An unreconciled day gets certified |
| Correction before close | New input after certification, month open → the date is rebuilt; two builds in `build_log`; totals equal the new silver | Stale certified numbers |
| Correction after close | Month closed → gold rows and the close clone unchanged; one adjustment with the exact delta and the original business date | Reported numbers change |
| Idempotent re-run | Running the gate twice → no second build and no second adjustment | Double counting |
| Totals check | Silver changes mid-build → rollback; the date keeps its old certified value | Gold and silver disagree |
| Blocked correction | A correction for a certified date is quarantined → the certified numbers stand, and an open item shows | Numbers withdrawn, or the problem hidden |
| Every record accounted for | A gap between audit offset ranges (a lost exactly-once transaction) → that hour is BLOCKED | Silent data loss |
