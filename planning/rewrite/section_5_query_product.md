# SQL query product — checkpoint plan

Status: checkpoints 1–2 owner accepted; checkpoint 3 ready for owner verification. Updated 2026-09-16 after the bounded five-date screen. This replaces the earlier, broader section-five specification. Build query access first; plots, history-rendering tools, report writing and strict-run exports are outside this task.

## Product

Make the existing cloud-VM dataset queryable with **SQL and Python**, using DuckDB over its completed Parquet files. Connect through SSH or a notebook running on the VM. A publicly exposed SQL server, web UI and duplicate database storage are not required.

A researcher can list fields/units, select dates, execute SQL, and retrieve symbols, dates, sessions, timestamps and measured values. Python and the CLI use the same tables. Default to **all represented sessions: premarket, RTH and after-hours**. No implicit activity gate. Dates refer to trading dates in America/New_York; timestamps retain their exact UTC endpoint identity.

Expose `features`, `members` and `feature_catalog`. Reuse the existing 27-field registry, units and validity masks. Invalid measurements are unavailable to numeric predicates; valid zeros remain zero. Distinguish unavailable data from a valid query with no matches. Select date/member files before scanning, project only needed columns, and do not recalculate features at the query boundary.

Ordinary SQL supports comparisons, grouping and window functions. For example, a trailing minute above a trade-rate threshold can be a SQL query or saved view; it does not require a new stored binary feature. Such a query must check consecutive valid seconds and relevant breaks. Persist a new feature only when reuse, computation cost or missing underlying measurements justify it. This duration example is explanatory, not another implementation requirement.

## How checkpoints run

Assign **one checkpoint at a time to one Sol agent (`gpt-5.6-sol`)**. No parallel implementation agents or automatic progression. The agent completes the assigned work, tests it, and stops for the owner's verification. Do not launch agents merely because this plan exists.

Each checkpoint handoff must be short: what now works, one copyable command or Python example, actual result and runtime where relevant, tests/limitations, source commit and installed release if applicable. Include the exact next checkpoint scope. Maintain one small completion record alongside this plan with status `pending`, `ready for owner verification`, or `owner accepted`. Record acceptance only after the owner explicitly gives it. Agent self-review is not owner acceptance.

The next checkpoint starts only after owner acceptance. Routine fixes within the assigned checkpoint do not require repeated permission. If blocked, identify the concrete problem rather than adding another framework or silently broadening scope.

## Checkpoint 1 — Open and query a small subset

**Build:** a package API that opens a date/member-scoped DuckDB handle on the VM, with `features`, `members` and `feature_catalog`. Expose the existing measurements and reason masks, using a metadata catalog of completed files. Reuse existing registry/reader logic where useful; do not require integration of the separate strict-run engine. Use one complete symbol-day from the existing accepted 24-member pilot for the first real test, retaining all sessions.

**Test:** execute a simple threshold query, a count by session, and a field/unit listing. Check a small fixture for threshold boundaries, null versus zero, and date/member selection. Compare a few real returned keys/values with direct reads of the selected source columns. Missing or inconsistent inputs must fail clearly. Keep required base joins keyed and avoid reading base/support when a feature-only query does not need them.

**Owner verifies:** one Python example returns actual matching timestamps and values; the session counts show that no RTH-only filter was inserted. An empty threshold result is acceptable when the source comparison agrees. Stop here.

## Checkpoint 2 — Usable installed SQL and Python access

**Build:** install the query package in an isolated VM release and add a small CLI for field discovery and executing SQL files with explicit date bounds. Python returns bounded Arrow batches or a result object that can be converted to a DataFrame for small results. Provide a short SSH/notebook connection guide and a reusable SQL example. Keep table names and field names the same in both interfaces. Preserve existing CLI meanings.

Proposed Python shape, finalized during implementation:

```python
with open_tape_database(catalog, start_date="YYYY-MM-DD", end_date="YYYY-MM-DD") as db:
    result = db.sql("SELECT symbol, session_date, endpoint_time FROM features LIMIT 10")
    print(result.df())
```

Default sessions are all; optional session restrictions must be explicit. Cap terminal previews and stream larger exports to Parquet. Save the SQL, selected scope, source identity and row count with an export. This is trusted researcher access through SSH, not a hosted multiuser SQL service.

**Test:** run the same query through CLI and Python outside the development checkout and compare results. Verify an empty date and a valid zero-match query. Confirm date scoping avoids unrelated members. Record setup/verification time separately from query time and measure memory on the small subset. Run relevant existing tests; no full-corpus verification or benchmark suite is required.

**Prepare:** use member metadata to propose five represented trading dates and every completed member on those dates, without choosing dates for their query results. Default to five consecutive represented dates centered around the median represented date; name the actual dates in the handoff. Report projected rows, input bytes, runtime and output size from the small test, within an explicit job budget.

**Owner verifies:** copyable CLI and Python examples work, and the proposed five-date scope/budget is acceptable. Acceptance of the stated run scope authorizes checkpoint 3; do not ask again for the same permission. Stop here.

## Checkpoint 3 — Five-date illustrative screen

**Run:** the screen below across the accepted five dates, all completed members on those dates and all represented sessions. All seven conditions must hold at the same one-second endpoint, with every required feature valid. No minimum matching duration and no implicit additional gate.

| Measurement | Condition |
|---|---:|
| Five-second midpoint RMS, 30-second half-life | **>10 bps** |
| Mean full quoted spread, 30-second half-life | **<100 bps** |
| RMS / mean full quoted spread, 30-second half-life | **>2** |
| Movement participation, 30-second half-life | **≥0.4** |
| Eligible trade rate, 30-second half-life | **≥10 trades/s** |
| Quote-event age p90, trailing 60 seconds | **≤2 seconds** |
| Eligible-trade age p90, trailing 60 seconds | **≤2 seconds** |

Use the registry's canonical field names:

```sql
SELECT symbol, session_date, session, endpoint_time,
       midpoint_rms_5s_bps_hl30s, quoted_spread_bps_hl30s,
       midpoint_rms_5s_to_spread_hl30s, movement_participation_hl30s,
       trade_rate_per_second_hl30s,
       quote_age_p90_seconds_window60s, trade_age_p90_seconds_window60s
FROM features
WHERE midpoint_rms_5s_bps_hl30s > 10
  AND quoted_spread_bps_hl30s < 100
  AND midpoint_rms_5s_to_spread_hl30s > 2
  AND movement_participation_hl30s >= 0.4
  AND trade_rate_per_second_hl30s >= 10
  AND quote_age_p90_seconds_window60s <= 2
  AND trade_age_p90_seconds_window60s <= 2;
```

The key/session aliases above are proposed public names; feature names come from the existing registry. The spread ceiling is **100 bps for this agreed example**. Check the original tape-characterization repository read-only for the historical screen and note any difference; do not silently substitute 200 bps or legacy feature meanings. Historical matching is not a prerequisite to running the agreed screen. These are exploratory descriptive thresholds, not calibrated trading rules. Participation does not establish direction or continuous movement.

**Return:** matching observations with the displayed values, plus a small symbol/date/session summary containing represented, eligible, unavailable and matching seconds and first/last matching timestamps. Retain valid zero-match members and distinguish members with no eligible inputs. Include date-level distinct matching-symbol counts and actual scan time/memory. First/last matches do not imply continuous matching between them; one-second observations are not independent signals.

**Test:** confirm exported rows satisfy every predicate and summary counts reconcile with the selected population and match count. Spot-check source values. Preserve an empty result instead of tuning thresholds to manufacture matches.

**Owner verifies:** the actual SQL and returned rows answer the requested question, and runtime is practical. Completion means SQL/Python querying works over the tested scope. It does not claim full-corpus performance, profitable signals, report completion or acceptance of the entire rewrite. Stop; any plots or subsequent research are a separate task.

## Implementation boundaries and starting points

Follow the existing [VM-first instructions](../../docs/vps-operations.md): inspect identity, active jobs, branches and dirty worktrees; use a development worktree/environment; preserve existing work and installed releases. Source synchronization uses Git. Keep detailed rows/catalogs private. No new acquisition, raw replay, feature rebuild, full-corpus scan, upload, deletion or publication is included.

The accepted release has 5,208 symbol-days and 299,980,800 rows. Use its completed-member metadata, not the legacy or raw-migration population. Initial known reuse points are A's registry/reader at `96d74da8d37b8f6956db16405fc03bc621a453d9` and B's optional predicate core at `09056be8b30925e14ac23baf263db265ad74edfe`. Refresh repository state before choosing the implementation baseline. A's reference is pilot-specific: add a separately identified catalog/reference for arbitrary dates rather than disabling pilot checks. Preserve existing source/schema integrity checks and record the selected release identity; do not build a second verification framework.

For small real tests, reuse [Phase 4's existing bounded workload limits](phase_4.md): one worker/thread, 300-second hard wall limit, 2 GiB sampled RSS stop / 3 GiB hard ceiling, DuckDB 256 MiB memory with at most 1 GiB spill, 4 GiB owned output/scratch, 8 GiB read budget including verification, and 20 GiB free-disk reserve. Check combined use with active jobs. A resource stop requires diagnosis, not automatic retries or larger limits. The five-date budget is proposed in checkpoint 2 and accepted before execution.

This owner-directed plan supersedes the earlier requirement to integrate strict runs, plotting and broad acceptance machinery before delivering simple SQL access. Existing feature mathematics, validity, timing and dataset identities remain governed by the [endpoint/EW contract](../../docs/contracts/endpoint-ew-v1.md). Keep verification proportional to these three concrete deliverables.
