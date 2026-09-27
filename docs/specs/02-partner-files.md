# Hard problem #2: Loading partner files exactly once

**Status:** approved and implemented. Summarised in section 8.2 of the design doc.
**Scope:** daily partner and vendor file drops (~500M rows/day in total).
**Path:** SFTP/S3 landing → PySpark on EMR (Python 3.11) → Parquet in S3 staging → **one Snowflake transaction** → bronze + file ledger.
**Code budget:** about 120 lines of guarantee code.

---

## 1. The guarantee

Every partner file becomes visible **exactly once and all at once, and only if it matches its trailer to the paise**. A corrected file replaces the version it corrects in a single step. At no moment can a reader see half a file, two versions of the same file, or a file that didn't reconcile.

## 2. Why it's hard

| # | What goes wrong | What a naive loader does |
|---|---|---|
| 1 | The same file arrives twice, maybe renamed (partner resends; S3 event notifications are at-least-once; Airflow retries a task) | Loads it twice, doubling a day's collections |
| 2 | A corrected file arrives for a day already loaded | Appends it (double counting), or deletes the old rows then inserts the new ones, leaving a gap readers can see |
| 3 | An *older* revision arrives after a newer one (a delayed SFTP retry) | Replaces the correction with the mistake |
| 4 | The file is half uploaded, or truncated | Loads the part that arrived; totals are silently low |
| 5 | Amounts like `"1,23,456.78"` (Indian digit grouping), `"10.005"`, or rupees where paise were expected | A float or decimal cast rounds `10.005` to `10.01` without an error, and the paise stop reconciling |
| 6 | Ambiguous dates (`03/04/2026`) | Parses 3 April as 4 March, and the rows land in the wrong period |
| 7 | Two loaders process the same slot at once (a retry while the first run is still going; two revisions landing together) | Both "win", so two versions are active |
| 8 | The load dies halfway (Spark or Snowflake) | Leaves partial rows that nobody cleans up |

## 3. Options weighed

| Option | Verdict | Why |
|---|---|---|
| Snowflake COPY load metadata (skips files it has already loaded) | Rejected as the guarantee | It keys on the file's name and lasts 64 days. A renamed resend loads again, and it knows nothing about corrections. |
| "Run once" from the scheduler or S3 events | Rejected | Both are at-least-once by design |
| Delete the day's rows, then insert the correction | Rejected | Needs a lock on the whole table, destroys the audit trail, and a failure between the two steps shows a gap |
| Spark writing to Snowflake with the Spark connector | Rejected | Can't put the rows and the ledger update in one transaction |
| Spark writing the Iceberg table directly | Rejected | Breaks the single-writer rule: Snowflake is the only writer of every table |
| **Identity from content; append-only rows; a per-slot "active version" pointer flipped in the same transaction that inserts the rows** | **Chosen** | Handles 1–8. Superseded versions stay in bronze for auditors. |

## 4. The design

### 4.1 Identity comes from content, never from the file name

| Term | Meaning | Source |
|---|---|---|
| `sha256` | The file's exact bytes, **after decompressing** (gzip stores the original file name inside the archive, so the same data zipped under two names has different bytes) | Streamed from S3 |
| slot | (partner, feed, business date, sequence): the place the file fills. A daily feed has one slot per day; an hourly feed has 24. | The file's **header line** (`H|partner|feed|business_date|seq|revision`) |
| revision | The partner's version number for that slot | Header |
| fingerprint | Row count + paise total + a checksum over the normalised rows that doesn't depend on row order (the sum of each row's `xxhash64`). The same data re-exported with different line endings, digit grouping, row order or compression gets the same fingerprint. | Computed during validation, in the same pass |

Renaming a file changes nothing, because identity comes from the bytes and the header. A re-export changes the bytes but not the fingerprint. The fingerprint isn't cryptographic: it catches accidental differences, not deliberate forgery, which this design doesn't try to stop.

### 4.2 What happens to each arriving file

| Situation | Outcome | Visible change |
|---|---|---|
| Same `sha256` already loaded (active or superseded) | DUPLICATE (checked before any Spark work) | None |
| Slot empty | NEW → validate, reconcile, load | The file's rows appear |
| Slot active with a lower revision | CORRECTION → validate, reconcile, load | Old rows disappear and new ones appear, in one step |
| Slot active with a higher revision | STALE → quarantined | None |
| Slot active with the same revision, different bytes, **same fingerprint** (a re-export) | DUPLICATE | None |
| Slot active with the same revision, **different fingerprint** | CONFLICT → quarantined; the partner must confirm | None |
| Any validation or reconciliation failure | QUARANTINED, the whole file | None; the day stays incomplete in `import_state` (#3), with the reason |

### 4.3 Validation and reconciliation (PySpark)

A file is all or nothing. **One bad row quarantines the whole file**: zero tolerance for finance feeds, and a sample of the bad rows goes to the partner.

- **Record types:** exactly one header (`H`) and one trailer (`T|row_count|total_paise`). Every other line is a detail (`D`). A truncated file has no trailer, so it fails here.
- **Money:**
  - Remove the digit-grouping commas, which handles both `1,234.50` and `1,23,456.78`.
  - Require at most two decimals (`^-?\d+(\.\d{1,2})?$`) **before** casting to `DECIMAL(20,2)` and multiplying by 100. The regex check is what makes the cast exact; without it, Spark rounds `10.005` silently.
  - Totals are summed as 64-bit integers, with ANSI mode on, so an overflow fails instead of wrapping.
  - The unit comes from the partner's agreement: rupee strings as above, or whole paise (`123450`). A mix-up can't load:
    - rupees sent to a paise feed fail the whole-number check
    - paise sent to a rupee feed come out 100 times too big and fail the trailer check
- **Dates:** parsed with the format in the partner's contract, then checked against a window (business date − 7 days to business date). A day-month swap lands weeks outside the window and fails.
- **Duplicates inside a file:** `txn_id` must be unique. Duplicates quarantine the file; they are never silently removed, because they point to a partner-side bug.
- **Reconciliation:** detail row count = trailer count, and the sum of `amount_paise` = trailer total, both exactly.

### 4.4 The exactly-once step (one Snowflake transaction)

```
BEGIN
  1. INSERT this load's rows into bronze from the staged Parquet     (INSERTs don't block other loaders)
     check: rows inserted = the reconciled count                     (catches missing staged files)
  2. MERGE the slot pointer: take the slot only if it's empty, or its active revision is lower
     check: exactly 1 row inserted or updated; otherwise ROLLBACK and classify (duplicate / stale / conflict)
  3. INSERT the load into the file ledger as LOADED
COMMIT
```

- **Readers** query `silver.partner_repayments`: bronze rows joined to the slot's active load. The pointer flip and the rows commit together, so readers never see half a file or two versions.
- **Two loaders at once:** Snowflake runs MERGEs on the same table one at a time, so the second loader's MERGE sees the first one's committed pointer and loses cleanly. The rows go in first and the claim comes last, so the lock is held only briefly.
- **A crash at any point:**
  - Before COMMIT: Snowflake rolls everything back. Orphaned staged Parquet expires after 7 days.
  - After COMMIT: the ledger row committed with the data, so a retry sees DUPLICATE.
- **The ledger doubles as `import_state` for files** (#3). Each load has a unit (the slot), a row count, a paise total and a status.

### 4.5 Partners without a trailer (A7's weaker guarantee)

- The load goes in as **UNRECONCILED**. Its rows are visible to analysts (trusted tier), but the day can't be certified until finance reconciles it another way, for example against the partner's settlement report, with maker-checker sign-off.
- A `.done` marker file is required before processing, to avoid reading half-uploaded files.

## 5. The code (about 120 lines of guarantee code)

Line counts exclude comments, blank lines and imports. The code is written for Python 3.11 and Spark 3.5 and hasn't been run.

| File | What it enforces | Lines |
|---|---|---|
| `spark/partner_file_loader.py`: `amount_to_paise`, `validate_and_reconcile` | Exact money; the record-type, date-window and duplicate checks; the trailer reconciliation; the fingerprint (4.1, 4.3) | ~60 |
| `spark/partner_file_loader.py`: `commit_load`, `classify_lost_claim` and their SQL | The one transaction, and the outcomes (4.2, 4.4) | ~60 |

**Plumbing:**
- the contract and header dataclasses, streaming `sha256` from S3, and `main`
- `snowflake/02_partner_tables.sql`: the bronze rows table, the slot and ledger tables, the active view, the stage and file format

## 6. Tests

| Test | What it asserts | How it fails if the design is wrong |
|---|---|---|
| **Money, reconciliation and fingerprint (real code, local Spark):** `tests/test_partner_files.py` part 1 | `1,23,456.78` → 12345678 paise; `10.005` → quarantined, never rounded; missing trailer → quarantined; trailer total off by 1 paisa → quarantined; a date outside the window → quarantined; duplicate `txn_id` → quarantined; wrong feed in the header → quarantined; a paise feed accepts `12345678` and quarantines `123.45`; the same data reordered and reformatted → same fingerprint | Drop the regex check → `10.005` loads as 1001 paise. Sum as doubles → totals drift. Ignore the trailer → a truncated file loads. Hash raw text instead of normalised rows → a re-export becomes a CONFLICT. |
| **Load protocol (real code, against Snowflake):** part 2 | Resend (same bytes) → DUPLICATE; re-export (different bytes, same data) → DUPLICATE; same revision with different data → CONFLICT; revision 2 → the view shows only v2's rows; a late revision 1 → STALE; v1 stays in bronze, and losing loads leave no rows. Two revisions racing from two sessions → v2 active either way, and never both. | Claim first and insert later without the rollback → half-loaded files become visible. Claim with INSERT instead of MERGE → concurrent loaders both win (INSERTs don't take the blocking lock). Delete+insert corrections → a reader sees an empty day. |
| Date window (planned) | A swapped day and month is rejected | Wrong-period rows get loaded |
| No trailer (planned) | Loads as UNRECONCILED; the day isn't certified | The weaker guarantee gets hidden |

## 7. Limits (stated in the design doc)

- **Loads commit one at a time at the claim step.** The lock is short, because the rows go in first. At dozens of files a night this is fine; with thousands of small files, the slots table would become a queue.
- **Hashing a multi-GB file** takes seconds to tens of seconds on the driver. That's accepted.
- **One bad row blocks the whole file.** That's the right call for finance, but it means partner-quality issues show up as blocked days. That's intended and visible.
- **Detection relies on the partner's trailer.** A partner who sends a wrong trailer that matches wrong data won't be caught here; the cross-source reconciliation in #3 (our records against their settlement file) is the backstop.
- **If a loader dies while holding its transaction open,** its lock stays until the session ends. Loaders therefore set a short `LOCK_TIMEOUT` and `STATEMENT_TIMEOUT_IN_SECONDS`.

## 8. To verify when pinning versions

- `SELECT … FROM @stage/path (FILE_FORMAT => …)` inside an explicit transaction, used in an INSERT
- That a MERGE blocked on a table lock evaluates against the committed state once it gets the lock (the concurrency test asserts this)
- MERGE result columns ("number of rows inserted" / "number of rows updated") as returned by the Python connector
