# Causal cohort query contract

Extracted normative sections from `compact_tape_query_projection_spec.md`, design `local_tape_retrieval_v2`, semantics `tape_cohort_hysteresis_v1`. The corresponding config, reader, state machine and output modules are implemented and tested in this repository. This document does not assert that every larger-system requirement of the source design was implemented.

The older 60s initial-config example below is retained as grammar context. The maintained example is [the selected 300s config](../config/tape_cohort_300s_ms3_p050_selected_v1.json), with participation 0.50/0.40 and movement/spread 3.0/2.4. Neither defines a trade model. See real-data access (source-only document `data-access.md`; not bundled) for the historical release restriction.

## 4. Query configuration and immutable identity

### 4.1 Supported grammar

Each condition identifies one exact feature name from the eighteen-field schema
and its unit. Conditions are conjunctive (AND); no arbitrary expression strings,
SQL fragments, OR groups, cross-feature arithmetic or future outcomes in V1.
Each feature may have an entry lower/upper bound and a continuation lower/upper
bound, with independently specified inclusive/exclusive operators. Missing bound
means unbounded, not zero. At least one bound is required per condition. All
conditions use the same declared set of fields for entry and continuation.
Mixed 60s/300s fields are allowed and each retains its own mask. No automatic
same-horizon activity gate from the distribution report is imported.

Validate exact schema names, unique features, registry units, finite numeric
bounds, nonempty bound intervals, and that the entry interval is a subset of the
continuation interval including boundary inclusivity. Reject booleans as numeric
values, negative zero and unknown config fields. Age/rate/movement bounds must
respect their nonnegative domain; participation bounds stay in [0,1]. Missing
continuation bounds inherit the corresponding entry bounds only during explicit
normalization; persist the expanded form before hashing.

Confirmation settings are `entry_confirm_seconds` and `exit_confirm_seconds`,
integers in [0,300], excluding booleans. Here “seconds” means consecutive
one-second observations:
`N_entry=max(1,entry_confirm_seconds)`, `N_exit=max(1,exit_confirm_seconds)`.
Three passes at t,t+1,t+2 confirm at t+2, not t+3. Zero and one have identical
immediate semantics; canonical normalization maps one to zero before hashing.
There is no minimum-duration post-filter in V1. Return every positive-duration
active window. Do not introduce a five-minute minimum to obtain five-minute means.

### 4.2 User-selected initial report cohort

The architecture and state semantics are fixed. The numerical cohort is an
explicit descriptive cohort, not a validated market boundary. On 2026-09-10 the
user selected five consecutive entry passes, five exit failures, continuation
floors at 80% of entry and a spread cap of 125 bps, retaining the strict thresholds
below. These are the required initial implementation/pilot settings:

| Feature (60s) | Entry | Continuation |
|---|---|---|
| `movement_mean_5s_bps_60s` | >= 10 bps | >= 8 bps |
| `movement_mean_to_spread_60s` | >= 1.5 | >= 1.2 |
| `trade_rate_60s` | >= 10 trades/s | >= 8 trades/s |
| `dollar_rate_60s` | >= 5000 dollars/s | >= 4000 dollars/s |
| `quoted_spread_mean_bps_60s` | > 0 and <= 100 bps | > 0 and <= 125 bps |

Entry confirmation: 5 consecutive strict passes. Exit confirmation: 5 consecutive
continuation failures. The 80% floors, 125 bps cap and five-second counts are
user-selected starting choices; they are not empirical findings. No freshness,
participation or 300s requirement is silently added. The grammar supports those
features if explicitly requested in a new config.

The complete initial config to implement is below. Unit strings are exact values
from the existing feature registry; do not substitute display abbreviations:

```json
{
  "schema": "tape_cohort_config_v1",
  "semantics_version": "tape_cohort_hysteresis_v1",
  "eligibility": "post_discovery_and_requested_zero_masks_v1",
  "decision_session": "extended_0400_2000_ET",
  "entry_confirm_seconds": 5,
  "exit_confirm_seconds": 5,
  "conditions": [
    {
      "feature": "movement_mean_5s_bps_60s", "unit": "bps",
      "entry": {"lower": 10.0, "lower_inclusive": true, "upper": null, "upper_inclusive": true},
      "continuation": {"lower": 8.0, "lower_inclusive": true, "upper": null, "upper_inclusive": true}
    },
    {
      "feature": "quoted_spread_mean_bps_60s", "unit": "bps",
      "entry": {"lower": 0.0, "lower_inclusive": false, "upper": 100.0, "upper_inclusive": true},
      "continuation": {"lower": 0.0, "lower_inclusive": false, "upper": 125.0, "upper_inclusive": true}
    },
    {
      "feature": "trade_rate_60s", "unit": "trades/second",
      "entry": {"lower": 10.0, "lower_inclusive": true, "upper": null, "upper_inclusive": true},
      "continuation": {"lower": 8.0, "lower_inclusive": true, "upper": null, "upper_inclusive": true}
    },
    {
      "feature": "dollar_rate_60s", "unit": "USD/second",
      "entry": {"lower": 5000.0, "lower_inclusive": true, "upper": null, "upper_inclusive": true},
      "continuation": {"lower": 4000.0, "lower_inclusive": true, "upper": null, "upper_inclusive": true}
    },
    {
      "feature": "movement_mean_to_spread_60s", "unit": "dimensionless",
      "entry": {"lower": 1.5, "lower_inclusive": true, "upper": null, "upper_inclusive": true},
      "continuation": {"lower": 1.2, "lower_inclusive": true, "upper": null, "upper_inclusive": true}
    }
  ]
}
```

Normalize feature order to `compact_product_schema.FEATURES` order, numbers to
finite binary64 values, unused inclusivity to true, and zero to positive zero.
Hash UTF-8 JSON with sorted keys, compact separators, `ensure_ascii=False`, and
`allow_nan=False`. `query_hash` hashes the complete normalized semantic config.
Names, comments, pilot date, release identity, cache path and presentation style
are not semantic query fields and are excluded from this hash.

A separate study lock records query hash, release identity, pilot/development
dates, intended reporting dates, config status (`candidate` or `locked`), and user
selection/approval record. Candidate changes receive new hashes and remain
available for comparison. Before the six-month prevalence run, lock the exact
config; do not tune it after examining corpus results. Include pilot/development
dates in descriptive totals but disclose their use, and optionally report the
remaining dates separately. This is not an independent predictive validation.

`query_run_hash` binds query hash, release/control hashes, routed membership,
mode (checkpoint/pilot/range), prefix limits if any, output schema version and
transitive query implementation/runtime identity. The implementation identity
hashes a sorted mapping of relative paths to SHA-256 for all new query modules and
all reused local reader/schema/validation/storage/supervision dependencies, plus
Python/NumPy/PyArrow/DuckDB versions and output schema hashes. Enumerate that file
closure in code and test that changing a dependency invalidates result reuse.
Do not substitute the Git commit alone (the working tree can be dirty), or hash
unrelated research documents. `window_id` hashes query hash,
partition identity, entry-confirmed endpoint and per-partition window ordinal.
Thus a window has the same ID in a pilot and range using the same source/query;
run identity changes with scope. Cache identity never depends on query thresholds.

## 5. Endpoint input, eligibility and exact time conventions

### 5.1 Required projected batch

Decode keys `session_date`, `symbol`, `interval_end_ns`; selected numerical fields
and their masks; and these five common context fields:

```text
continuity_segment_id
halt_interval_active
post_discovery_eligible
primitive_quote_source_file_accepted
primitive_trade_source_file_accepted
```

Decode `midpoint` only for trace/chart output. Preserve source types/nullability.
A batch API must return keys, raw values and Arrow null masks, reason masks and
context, not only an eligible-value array. Ephemeral NumPy NaN may represent an
Arrow null internally only when the independent null bitmap is retained. Stored
NaN/Inf is illegal, including in excluded diagnostics. Do not coerce null to zero.

Require exact feature schema, row count, ordered unique keys, full canonical grid
and known masks. Validate metadata discovery against `post_discovery_eligible`.
Require all physical non-null fields non-null. Reject a zero mask with a null,
nonfinite/out-of-domain value or contradictory required source/halt context.
Respect feature-specific source requirements from the product contract: do not
invent a global trade-source gate for a quote-only query. For the initial five
fields both quote and trade acceptance are required by its dependencies.

For each requested f:

```text
analysis_valid_f = (mask_f & 223) == 0  # diagnostic only; allows bit 32
query_eligible_f = mask_f == 0 and post_discovery_eligible
available = every requested query_eligible_f and every requested value finite
strict = available and every entry bound passes
continuation = available and every continuation bound passes
```

Reject negative, null, noninteger or unknown masks before bit arithmetic. Bits:
undefined=1, source_unaccepted=2, immature=4, insufficient_support=8,
active_halt=16, carried_history=32, nonpositive_spread=64,
zero_total_movement=128. Finite excluded observations remain raw diagnostics and
are unavailable to this query. A valid zero is an economic observation: e.g.
M=0 can fail M>=10 while null participation from zero total movement is irrelevant
unless participation was requested. Requested unavailable values force a reset;
unrequested unavailable fields do not affect membership.

When a requested movement/spread ratio and both same-horizon components are
already projected, assert on zero-mask rows that R=M/S using the inherited
rtol=1e-10, atol=1e-12. Reject a contradiction; do not replace stored R with a new
value. Do not fetch otherwise unrequested support just for this identity check.
This algebraic check is not independent reconstruction of M or S.

Do not push economic predicates into a SQL WHERE that removes rows before state
processing. SQL/Arrow may project columns and route dates/symbols. Every ordered
endpoint, including failures and unavailable rows, must reach the state machine.

### 5.2 Endpoint coverage versus causal active time

For a complete date the source endpoints are 04:00:01, ..., 20:00:00 ET, 57,600
rows. Row t describes [t-1s,t) and becomes knowable at t. The rolling feature uses
its inherited historical support; neither t-60s nor t-300s is a query activation.

A decision at endpoint t<20:00 applies to [t,t+1s), subject to the next observed
boundary. An active window is [entry_confirmed_at, exit_effective_at), with
integer-second duration. Its member endpoints are exactly t in that half-open
span. Do not include the endpoint that confirms exit. Pending-exit failures before
confirmation ARE member endpoints, explicitly labeled. Pending-entry rows before
confirmation are not members. Duration equals active member count on valid grids.

The terminal 20:00 row is still validated and included in observed-endpoint
availability/strict accounting, but MUST NOT start a candidate, activate a window,
count as an active member or extend a span past close. Close any active window at
20:00 with `session_close` before applying terminal economic state transitions.
An endpoint qualifying only at 20:00 is endpoint evidence, not an active window.
This corrects the old `last_endpoint+1s=20:00:01` error.

Two session labels are deliberately distinct:

- source stratum uses t-1s: endpoint 09:30 is premarket; endpoint 16:00 is RTH;
- active-time stratum uses t: decision second starting 09:30 is RTH and starting
  16:00 is after-hours.

09:30 and 16:00 do not reset. Split active-time summaries by overlap with stratum
boundaries without splitting window identity. V1 routes full dates, not intraday
slices. Presentation filters may clip display but never regroup/reinitialize.
Historical halt/discovery provenance must be disclosed: causal feature endpoints
do not prove the historical overlay itself was received live. These are grid-level
state intervals, not subsecond execution/latency guarantees.

## 6. Hysteresis state machine: normative algorithm

### 6.1 State and boundary precedence

Maintain only the current partition's previous endpoint/continuity, state
`OUT|PENDING_ENTRY|ACTIVE|PENDING_EXIT`, candidate first endpoint/count, exit-trigger
endpoint/count, current window ordinal and streaming summaries. No list of window
members, no list of a date's windows and no confirmation-sized raw-row buffer are
needed. A separate optional trace sink receives transitions as they occur.

At every endpoint validate input first, then apply these boundaries in order:

1. At 20:00, close with `session_close` as described above.
2. A symbol/date change flushes the previous partition before initializing the
   next; production partitions must end at 20:00. Never carry state across dates.
3. Non-increasing/duplicate endpoints are structural errors, not a new episode.
4. A forward clock gap closes an active span at `previous_endpoint+1s`, with
   `clock_gap`, right-censored=true. Reset entry/exit counters. Do not extend to
   the newly observed endpoint. Complete-product grid validation then rejects the
   partition, so no production result containing this corruption can commit.
5. A continuity-ID change closes the old active window at current t with
   `continuity_change` and resets all counters. The current row may start a NEW
   candidate/window if available and strict; it cannot continue the old window.
6. An active halt closes at t with `active_halt` and clears all counters. Do not
   evaluate economic entry on this row, even if a malformed value is finite.
7. Any requested unavailability closes at t with `unavailable:<primary>` and
   clears all counters. Never use exit confirmation to bridge it.
8. Otherwise apply economic transitions below to the available row.

04:00 is an initialization boundary, not an observed endpoint in this product.
Do not synthesize a 04:00 measurement. A halt-active row always prevents entry;
post-halt finite carried values cannot enter until their requested masks return
zero. Do not duplicate the feature builder's halt warmup or add a new cooldown.

When boundaries coincide, retain `boundary_causes` containing all observed causes
in the above order and use the first applicable cause as `exit_reason`. For
unavailability, primary precedence is pre_discovery, source_unaccepted, immature,
insufficient_support, carried_history, nonpositive_spread, undefined,
zero_total_movement. Active halt has already taken precedence. Retain each field's
full mask and the union; a primary reason is only a mutually exclusive accounting
label. A malformed mask/schema is an error, not ordinary unavailability.

### 6.2 Available-row transition table

Perform the transition first; accumulate the current row as a member only if the
post-transition state is ACTIVE or PENDING_EXIT. Thus a confirming entry row is
included and a confirming exit row is excluded.

| Prior state | Condition | Action | Current row a member? |
|---|---|---|---|
| OUT | strict=false | Stay OUT; clear candidate | No |
| OUT | strict=true, N_entry=1 | Open at t, candidate_start=t, state ACTIVE | Yes |
| OUT | strict=true, N_entry>1 | candidate_start=t, entry_count=1, PENDING_ENTRY | No |
| PENDING_ENTRY | strict=false | Clear candidate/count, OUT, even if continuation=true | No |
| PENDING_ENTRY | strict=true, count+1<N_entry | Increment consecutive count | No |
| PENDING_ENTRY | strict=true, count+1=N_entry | Open at t, preserve candidate_start, ACTIVE | Yes |
| ACTIVE | continuation=true | Stay ACTIVE | Yes |
| ACTIVE | continuation=false, N_exit=1 | Close at t, trigger=t, confirmation=t, OUT | No |
| ACTIVE | continuation=false, N_exit>1 | trigger=t, exit_count=1, PENDING_EXIT | Yes |
| PENDING_EXIT | continuation=true | Cancel pending exit/count/trigger, ACTIVE | Yes |
| PENDING_EXIT | continuation=false, count+1<N_exit | Increment consecutive exit count | Yes |
| PENDING_EXIT | continuation=false, count+1=N_exit | Close at t, confirmation=t, OUT | No |

After an economic exit there is no same-row reentry: strict implies continuation,
so that row cannot satisfy strict. A pending-exit recovery requires continuation,
not renewed strict entry. Exit failures are consecutive failures of the aggregate
AND continuation predicate; different features may fail on successive seconds.
Record failing feature bits on every trace row, and the first-trigger plus final
confirmation failing bits in the window record.

For each member update strict-pass, continuation-pass and pending-exit counts,
and per-feature min/max/sum/first/last. All member values are available and finite.
Use compensated floating sums for means; no exact feature-value median in V1.
Counts are integers. Set `last_member_endpoint_ns=t`. On closure:

```text
end_exclusive_ns = exit_effective_at_ns
active_seconds = (end_exclusive_ns - entry_confirmed_at_ns) / 1_000_000_000
active_seconds == member_endpoint_count
member_endpoint_count == continuation_pass_count + pending_exit_count
strict_pass_count <= continuation_pass_count
last_member_endpoint_ns == end_exclusive_ns - 1_000_000_000
```

Discard no positive-duration window. Zero-duration closures indicate an ordering
bug except an intentionally clipped checkpoint result; do not silently repair
production output. Candidate-only runs are counted separately, never as windows.

The initial config's candidate and final pending-exit counts must also be exposed
in trace output when no active window is emitted. Daily entry accounting uses
`started = confirmed + cancelled_economic + cancelled_boundary`; count each
candidate exactly once. `confirmed` equals number of opened windows. Boundary
cancellation includes close, halt, unavailable and continuity; add per-cause
counters rather than merging these with economic entry failure.

### 6.3 Exact worked oracle

Use local endpoint offsets in seconds; all rows available, no boundaries,
N_entry=3, N_exit=3. S=strict, C=continuation (S implies C):

| t | S | C | State after row | Action/member |
|---|---|---|---|---|
| 1 | true | true | PENDING_ENTRY | candidate 1; not member |
| 2 | true | true | PENDING_ENTRY | count 2; not member |
| 3 | false | true | OUT | cancel entry; not member |
| 4 | true | true | PENDING_ENTRY | candidate 4 |
| 5 | true | true | PENDING_ENTRY | count 2 |
| 6 | true | true | ACTIVE | confirm/open 6; member |
| 7 | false | true | ACTIVE | member |
| 8 | false | false | PENDING_EXIT | trigger 8; member |
| 9 | false | true | ACTIVE | cancel exit; member |
| 10 | false | false | PENDING_EXIT | trigger 10; member |
| 11 | false | false | PENDING_EXIT | count 2; member |
| 12 | false | false | OUT | confirm exit; NOT member |

Expected window: candidate=4, entry=6, trigger=10, exit=12, span [6,12), duration
6 seconds, members 6..11, strict_count=1, continuation_count=3 (6,7,9),
pending_exit_count=3 (8,10,11). The cancelled exit at 8 remains in the trace.
Changing row 11 to unavailable closes [6,11) immediately, with five members and
no economic exit confirmation. Changing row 11 to a new continuity segment closes
[6,11) and permits only a new entry process on row 11.

### 6.4 Censoring and strict-run evidence

Scheduled 04:00/20:00 boundaries, known unavailability, halt and continuity change
are explicit causes, not unknown censoring. Full-date production windows are
normally uncensored. A missing observation or an intentionally truncated prefix
has unknown adjacent state and is censored. Checkpoint EOF closes at last t+1s
with `selection_boundary` and right-censored=true, capped at session close; such
windows are fixture outputs and cannot enter population summaries. A checkpoint
beginning after 04:00 must label startup truncation and cannot claim equivalence
to a full-date state reconstruction without a replayed prefix.

In parallel, maintain an independent one-row-lookbehind strict-run reducer over
all observed endpoints, including the terminal 20:00 measurement. A strict run
records first/last passing endpoint, endpoint count, preceding/following boundary
cause and source identity. It never bridges a nonpass, unavailable row, gap or
continuity change. These are endpoint runs, not causal active intervals: for
endpoints a..b, represented source-second coverage is [a-1s,b), endpoint_count is
(b-a)/1s+1. Never label [a,b+1s) as an executable/active span. This compact output
provides the exact one-second strict qualification windows without an endpoint
corpus. Terminal-only strict qualification is a valid one-endpoint strict run.

## 7. Verified cache, catalog, and date processing

### 7.1 Cache layout and ownership

Use a user-configurable root, separate from other tasks' scratch:

```text
<cache>/
  cache_owner.json
  catalog.duckdb
  cache.lock
  objects/<feature_sha256>/features.parquet
  objects/<feature_sha256>/admission.json
  manifests/<manifest_sha256>.json
  attempts/<attempt_id>/attempt.json
  attempts/<attempt_id>/*.partial
  temp/
```

Create and validate an owner UUID/root marker. Paths must be root-relative,
contained beneath the owned root, and free of traversal/symlink escape. A cache
command never recursively cleans an arbitrary supplied path. An existing root
without a recognized marker must not be adopted silently.

A single exclusive OS-backed cache lock prevents concurrent mutation/query pin
races. Release it automatically on process exit; a text PID file alone is not a
lock. First implementation serializes API commands sharing a cache. Do not hold
another task's lock or touch its processes. The lock covers catalog transactions,
view creation, source pinning and query execution. A paused CLI does not leave
files falsely pinned forever; reconcile prior-run pins under the exclusive lock.

Cache key is full feature object SHA; admission record also binds bytes, schema,
row count, object key and marker identity. Catalog tables (small control data):

- `source_releases(release_identity PK, control_hashes_json, complete, created_at)`.
- `source_members(release_identity, partition_identity, session_date, symbol,
  feature_sha, feature_bytes, manifest_sha, support_sha, metadata_json)`; composite
  primary key `(release_identity,partition_identity)`, unique `(release_identity,date,symbol)`.
- `cached_objects(feature_sha PK, relative_path, size_bytes, admission_hash,
  verified_at, last_used_at, state)`; state is admitted/missing/quarantined.
- `cache_pins(run_id,feature_sha)` with composite primary key.
- `query_runs(run_id PK, query_hash, query_run_hash, source_release_identity,
  mode, state, result_relative_root, manifest_hash)`.

Do not store credentials in these tables. Query manifests belong with results;
cache eviction must never delete committed query results or their lineage.

### 7.2 Admission and reuse algorithm

For each routed member:

1. Resolve exact metadata through frozen release membership. Fetch the small
   manifest by its identity on a miss. Verify body SHA/length and compatibility
   with the accepted calculations, schema hashes and both object identities.
   Cached manifests may be reused after local hash and compatibility checks.
2. If features are cached, check local SHA/length and admission binding once per
   cache object per command before reading. Detect local changes; never rely only
   on filename, mtime or ETag. Recheck requested-column grid/domain validity during
   the query scan. An intact warm cache makes zero remote requests.
3. On miss, reserve bytes, create a private attempt, GET the whole feature object
   in <=1 MiB blocks while hashing, verify response metadata/body/length. Reject
   a feature object exceeding the 128 MiB admission cap before transfer. No HEAD
   is needed when the GET verifies the same facts. No support GET. Normal cold
   count is two GETs/member: marker and features (fewer when marker cached).
4. Before admission, inspect full physical schema and row-group metadata. Cache
   admission covers verified bytes only. During the subsequent query pass, stream keys/context,
   selected values and masks, validating the canonical grid as each batch arrives.
   A complete production query must exhaust and verify all 57,600 rows before
   committing any result. Byte-cache admission is not full semantic acceptance;
   admission metadata says `body_verified`, with semantic checks recorded in the
   query result. This avoids a second key-only scan and does not constitute
   reconstruction. A later structural failure quarantines the cache entry.
   Reject row groups >25,000, projected chunks >16 MiB uncompressed, or summed
   projected row-group bytes >64 MiB before decoding. Apply the source reader's
   exact post-discovery metadata check. Do not recompute features.
5. Write/hash admission metadata, fsync files, atomically rename within the same
   filesystem, then commit catalog state. Catalog visibility follows filesystem
   commit. A crash between these operations leaves a verified orphan that can be
   reconciled under lock; never expose a .partial file.
6. Pin while read; unpin and update LRU when done. The first implementation pins
   at most the current member during a batch query, not every date in the range.

Use finite connection/read timeouts and disable nested unbounded SDK retries.
At most three attempts for transient reads, delays 1s/5s, each in a new bounded
private attempt. Identity, schema, coverage and resource failures are not retried.
Retained failed bytes count toward quota. Exhaustion marks the date failed and
stops the batch with a nonzero exit; it never converts failure into zero matches.

### 7.3 Quota and eviction

Default cache limit: 1 GiB INCLUDING cached manifests and incomplete attempts.
Preserve at least 3 GiB filesystem free at all times, including other tasks'
usage. Before every transfer/output reservation, compare both quota and actual
free disk. Evict least-recently-used unpinned admitted feature objects only; tie
break by SHA. Delete only individually cataloged, ownership-checked paths.
An evicted object is recoverable from its immutable R2 identity. Retain tiny
lineage controls; keep resident control records within a 64 MiB budget and persist
additional metadata in the catalog rather than loading it all. Unknown files
are not evicted.

Routine eviction of this tool's verified downloaded copies is part of its cache
implementation scope. It does not authorize canonical T/Q deletion, other caches,
user files, report outputs or any remote deletion. Incomplete/quarantined attempts
require an explicit `cache reconcile` action before owned deletion/retry; do not
silently accumulate/retry them. If pinned/retained bytes prevent admission, stop
with required/available bytes rather than exceeding limits.

### 7.4 Date/range transaction

Process dates ascending and members `(symbol,partition_identity)` ascending. Each
symbol-day is an independent state-machine input. Each date writes private result
parts and summaries. Verify complete routed member/row counts, window identities,
strict-run reconciliation and output hashes; write date manifest last; atomically
rename the private date directory to its immutable committed path. Add catalog
reference afterward. One date never needs a date-wide raw-row sort or materialization.

Stream provisional result events to the caller with `date_status=provisional`;
only `date_committed` makes them accepted. Saved Parquet files are canonical
outputs; stdout is not a resume ledger. On failure preserve previous committed
dates, mark the overall run incomplete and exclude the failed date from final
population claims. Do not report the mean over only successful dates as the
requested six-month mean. Diagnostic partial summaries must be labeled incomplete.

`--resume` verifies frozen controls/config/code, committed date manifests and all
result object hashes. Skip committed dates; replay an uncommitted date from its
first member after reconciling its bounded owned attempt. Do not serialize live
Python state or resume at an arbitrary raw endpoint. A crash after date rename
but before catalog transaction is recovered by verifying that date manifest and
registering it. Detect duplicate membership and competing date commits; never
merge by directory glob. New dates or corrected partitions require a new run.

### 7.5 DuckDB inspection versus state processing

Create a `tape_endpoints` view from an explicit verified list of currently staged
paths, never a recursive glob. A `stage-date` command must finish all member
body admissions plus a bounded full key/context grid validation per member before
announcing that the SQL view covers the complete requested date. That full-date
operation requires the approved pilot/range scope; it is not a checkpoint shortcut.
V1 SQL inspection supports one staged date at a time and pins its files
for the inspection command; the maximum current date fits within the cache.
Expose derived query eligibility and source/decision strata as SQL columns.
`stage-date` prepares/admission-checks files and reports coverage at completion;
it does not promise indefinite residency after its process exits. `open_date`
revalidates residency and full key/context grid evidence for its source identity,
pins the whole date, creates the view and owns the SQL
inspection session until its context exits. Drop session views before unpinning.
Do not store an apparently current persistent endpoint view after the session.
The CLI/API owns exact coverage metadata; eviction cannot make an active SQL
inspection silently incomplete. If a future date is larger than the cache quota,
`open_date` fails with required bytes rather than exposing a partial view; the
streaming production query can still process that date member by member.

Production hysteresis uses the projected Arrow batch iterator in source order.
DuckDB is not required to express recursive state transitions in SQL. Use DuckDB
for inspecting endpoints and querying committed window/daily Parquet results.
Set one thread, 256 MiB memory limit and 256 MiB maximum temp size per connection;
these are not persisted globally by DuckDB and must be applied every connection.
Do not use `fetchall()`/Pandas for endpoint data. Result fetches use bounded batches.
