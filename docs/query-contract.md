# Causal cohort query contract

The implemented semantics are `tape_cohort_hysteresis_v1`. The installed query reads verified compact releases and processes every ordered endpoint. These are descriptive selections; neither confirmation nor eventual duration establishes an executable trading model or persistence of future local conditions.

## Configuration

The [selected 300s configuration](../config/tape_cohort_300s_ms3_p050_selected_v1.json) uses five consecutive entry passes and five consecutive continuation failures:

| Feature | Entry | Continuation |
|---|---:|---:|
| Mean five-second movement | ≥10 bps | ≥8 bps |
| Mean quoted spread | >0 and ≤100 bps | >0 and ≤125 bps |
| Trade rate | ≥10/s | ≥8/s |
| Dollar rate | ≥5,000 USD/s | ≥4,000 USD/s |
| Movement/spread | ≥3.0 | ≥2.4 |
| Movement participation | ≥0.50 | ≥0.40 |

These thresholds were selected using the [five-date development pilot](../reports/pilot.md). They are not independently validated predictive boundaries. Mixed horizons and alternative explicit thresholds remain supported by the grammar.

### Supported grammar

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


Normalize feature order to `compact_product_schema.FEATURES` order, numbers to
finite binary64 values, unused inclusivity to true, and zero to positive zero.
Hash UTF-8 JSON with sorted keys, compact separators, `ensure_ascii=False`, and
`allow_nan=False`. `query_hash` hashes the complete normalized semantic config.
Names, comments, pilot date, release identity, cache path and presentation style
are not semantic query fields and are excluded from this hash.


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


## Verified releases and output evidence

The supported local workflow reconciles an explicit expected-member inventory against completed partitions. Duplicate, unexpected, corrupt and missing members fail verification. Partial session-start fixtures require explicit prefix handling and cannot be presented as full-session historical releases. A requested historical release hash remains an optional exact replay constraint; compatible new releases receive their own identities.

The query emits interval and strict-run artifacts, availability and supply accounting, zero-match members, and concentration evidence. Raw unavailable endpoints are retained in accounting and reach the state machine. The run binds normalized query settings, release identity and installed source/runtime identity. See [reproduction](dataset-build.md) for commands and [architecture](architecture.md) for bounded processing.

The retained remote cache reader additionally verifies object bodies, schema/grid constraints and release controls before admission; it has explicit cache ownership and quotas. Its historical replay helpers are internal compatibility code. The supported local release command does not require a remote scan, a particular old release hash or an unexplained private selection database.
