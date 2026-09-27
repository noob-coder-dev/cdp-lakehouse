# Hard problem #3: The trust gate

**Status:** approved and implemented. Summarised in section 8.3 of the design doc.
**Scope:** certified finance tables. The worked example is `gold.fin_daily_collections`: collections per business date (IST) and partner, in paise.
**Builds on:** a pattern I run in production: an import-state table in Vertica where freshness = the latest loaded hour and completeness = the end of the unbroken run of loaded hours.
**Code budget:** about 100 lines of guarantee code.

---

## 1. The guarantee

A certified table shows a business date **only when every expected input for that date has arrived and reconciled, in an unbroken run** from the last certified date. Once a date is certified:
- A later change to its inputs **rebuilds it in place** while its month is open.
- After month close, a later change becomes an **adjustment** in the open period.

A closed month never changes, and every change is logged.

## 2. Why it's hard

| # | What goes wrong | Why a timestamp or a row count doesn't catch it |
|---|---|---|
| 1 | A table was written a minute ago but is missing yesterday's partner file | Freshness is not completeness |
| 2 | Something that never arrived | Nothing in the data says it's missing. Only an expectation ("partner A sends one file per day by 06:00") can |
| 3 | A file arrived but didn't reconcile, or a correction is waiting on the partner | "Loaded" is not "correct" |
| 4 | A day is complete but the day before it isn't | Month-to-date totals are silently short. Hence the unbroken-run rule (the gap rule I use in production) |
| 5 | A partner corrects a day after it was certified | The numbers change under finance's feet, or never change at all |
| 6 | A correction arrives after the month closed | Rewriting a closed month changes what was reported to management or regulators |
| 7 | One day is rebuilt twice, or an adjustment is booked twice | Double counting |
| 8 | Different checks live in different places (file trailers, CDC recon, stream audit windows, partner settlement recon) | Nobody sees one answer to "can this day be trusted?" |

## 3. Options weighed

| Option | Verdict | Why |
|---|---|---|
| Freshness only ("last loaded at …") | Rejected | Misses 1–4 |
| Data quality checks alone (null counts, row counts) | Rejected as the gate; kept as inputs to it | They say whether the rows present look right, not whether all rows arrived |
| Publish everything and flag problems | Rejected for finance | Finance chose "late beats wrong" (A5) |
| Recompute closed months freely | Rejected | A closed month is a reported number |
| Snowflake dynamic tables with a target lag | Rejected for certified tables | They refresh when inputs change, not when inputs are *complete and reconciled* |
| **Every check becomes an expected unit that must be RECONCILED; certify in date order and stop at the first gap; rebuild before close, adjust after** | **Chosen** | One definition of "trusted" covering 1–8. Adding a check is adding an expected unit. |

## 4. The design

### 4.1 Everything is an expected unit

`trust.expected_arrivals` lists what each date needs and by when. It is generated daily from each source's calendar.

| Source | Unit | RECONCILED when | Written by |
|---|---|---|---|
| Partner file | (partner, feed, sequence) per business date | The latest meaningful arrival for the slot is LOADED (it reconciled to its trailer) | The loader's ledger, `ctl.partner_file_loads` (#2) |
| CDC table | The business date | Silver has applied changes past midnight IST (Debezium heartbeats keep this moving on quiet tables), **and** the day's count and paise total match the source read replica | CDC recon job → `trust.unit_checks` |
| Event stream | Each hour | Flink's "hour closed" marker arrived, **and** every record is accounted for: per raw offset range, in = clean + DLQ + HOLD + filtered, bronze holds the clean count, and consecutive ranges have no gaps | Stream audit check → `trust.unit_checks` (moved here from #1) |
| Cross-source recon | The business date | Our records match the partner's settlement file within 0 paise | Recon job → `trust.unit_checks` |

For each unit, **the latest meaningful arrival decides**:
- LOADED or RECONCILED → the unit is reconciled.
- UNRECONCILED, QUARANTINED, CONFLICT or BLOCKED → the unit blocks certification, and its reason is shown.
- DUPLICATE and STALE arrivals change nothing, so they're ignored.
- A later good arrival (a fixed correction) clears an earlier block.

### 4.2 Completeness, freshness and the unbroken run

- **Date complete** for a table = every expected unit, across all sources in `trust.table_inputs` for that table, is RECONCILED.
- **Complete through** = the day before the first incomplete date after the last certified date (the gap rule I use in production, applied across sources).
- **Fresh as of** = the latest date with any reconciled input. It's reported next to "complete through", never instead of it.
- **Open items:** each incomplete date lists what's missing or blocked, with reasons. Missing and past due → alert, even if nothing arrived at all.

### 4.3 The publish gate (run by Airflow after each input window)

1. **Changed certified dates first.** For every certified date whose inputs changed after it was certified (`last_input_at > inputs_as_of`), in date order:
   - if the date isn't complete (for example the correction was quarantined): leave the certified numbers standing and show the open item.
   - if the month is open: **rebuild** the date (a restatement).
   - if the month is closed: **book an adjustment**. The delta is (new total − certified total − adjustments already booked for that date), dated in the open period, carrying `original_business_date`.
2. **Then advance.** From the last certified date + 1 up to yesterday: certify each date that is complete, and **stop at the first one that isn't**. A later complete date waits, so the unbroken run holds.
3. **Each rebuild or certification is one transaction:**
   - delete the date's gold rows and insert them fresh from silver
   - check that the gold total equals the silver total, exactly
   - write a `build_log` row: inputs as of when, code version, metric versions
   - record the date in `trust.certified_dates`

   Readers see the old date or the new one, never a mix. Deleting then inserting is right here, but was rejected for partner files in #2. The difference: gold is small and derived, and bronze keeps the audit record.
4. **Nothing is done twice.** `inputs_as_of` moves forward in the same transaction as the rebuild or adjustment. Adjustments are computed against the certified total *plus adjustments already booked*, so a second run finds a delta of zero and books nothing.

### 4.4 Month close

`trust.close_month(table, month)`:
- refuses to run unless every date in the month is certified
- clones the gold table (zero-copy) as `…__close_YYYYMM`, which is exactly what was reported
- marks the month closed

From then on, the gate never touches that month's rows (rule 4.3.1).

### 4.5 What readers see

`trust.table_status` shows, per table:
- certified through
- complete through
- fresh as of
- the open items with their reasons

Dashboards show the first three in their header. Certified tables only ever contain certified dates, so a reader can't query a half-loaded day by accident.

## 5. The code (about 180 lines of guarantee code; the target was about 100)

Line counts exclude comments, blank lines and imports. The code is written for Python 3.11 and hasn't been run.

| File | What it enforces | Lines |
|---|---|---|
| `snowflake/04_trust_gate.sql` | `trust.import_state` (the latest arrival decides), `trust.date_completeness` (expected vs reconciled, open items), `trust.table_status`, `trust.close_month` | 70 |
| `airflow/publish_gate.py` | The gate: certified dates with changed inputs are handled in date order (rebuild or adjust), then new dates advance and stop at the first gap; one transaction per date; exact totals check; the worked example's rebuild, totals and adjustment SQL | 108 |

**Plumbing:** `snowflake/03_trust_tables.sql`, which defines the expected arrivals, table inputs, unit checks, certified dates, build log, closed months, the gold table and its adjustments. Generating the expected arrivals from source calendars is stubbed.

## 6. Test plan (written, not coded; the brief asks for real code for the hardest one or two tests, which are #1's and #2's)

| Test | What it asserts | How it fails if the design is wrong |
|---|---|---|
| Unbroken run | Dates 1, 2 and 4 complete, 3 missing → certified through 2, and 4 waits | Certifying 4 → month-to-date totals are silently short |
| Missing counts as incomplete | An expected file that never arrived → the date isn't complete, and an alert fires once past due | Using only arrivals, not expectations → an absent file looks like a quiet day |
| Loaded ≠ reconciled | UNRECONCILED (no trailer) or CONFLICT → blocks the date, with its reason | Treating "arrived" as good → an unreconciled day gets certified |
| Correction before close | v2 arrives after the date was certified, month open → the date is rebuilt; `build_log` shows two builds; totals equal the new silver | Not detecting changed inputs → stale certified numbers |
| Correction after close | Same, month closed → the gold rows and the close clone are unchanged; one adjustment row with the exact delta and `original_business_date` | Rebuilding closed months → reported numbers change |
| Idempotent re-run | Running the gate twice → no second build and no second adjustment | Not moving `inputs_as_of`, or not netting existing adjustments → double counting |
| Totals check | Silver changes between the delete and the insert (simulated) → the transaction rolls back, and the date stays at its old certified value | Committing without the exact check → gold and silver disagree |
| Every record accounted for | A lost exactly-once transaction (a gap between audit ranges) → that hour is BLOCKED | Counting only rows that arrived → silent data loss |

## 7. Limits (stated in the design doc)

- **An old incomplete date blocks everything after it.** A partner who never sends one file stops certification from that date on. That's intended: finance must either chase the file or approve a documented waiver (a maker-checker row in `trust.unit_checks`). The waiver is described here but not coded.
- **Expected arrivals are only as good as the calendars they're generated from.** A new partner feed that nobody registered is invisible to the gate. Feed onboarding therefore registers the calendar, which is part of the contract.
- **Adjustments are booked per date and partner at the grain of this table.** Other finance tables need their own adjustment grain.
- **The gate processes one table at a time, in date order.** At this platform's size (a few certified tables, a few hundred dates a year), that's fine.
