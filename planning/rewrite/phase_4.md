# Phase 4 — New-feature retrieval and research workflow

Status: implementation specification, 2026-09-15. The owner selected sequential execution: implement and review A, then B, then C, then D. This document specifies all four checkpoints so they share one contract; it does not claim any checkpoint is implemented. Generate each execution prompt when its predecessor is accepted. Do not start four implementation agents concurrently.

## 1. Outcome and governing contracts

Deliver an installed workflow that reads the completed endpoint/EW release, selects observations with explicit feature rules, exports strict matching runs and coverage, and generates independently checked small-data distributions and figures. The same interfaces must support later full-population execution without changing their statistical meaning.

Read [product direction](README.md), [master phases](phases.md), [Phase 0](phase_0.md), [endpoint/EW contract](../../docs/contracts/endpoint-ew-v1.md), and the applicable sections of [pipeline implementation](feature_pipeline_implementation.md). Their feature mathematics remain authoritative. This specification adds downstream selection, retrieval and report behavior; it does not change stored feature calculations. Legacy compact readers/results retain their existing identities.

The root README remains the historical report until Phase 5 replaces it with verified new-population findings. Phase 4 outputs are explicitly pilot or synthetic evidence. No prediction, executable expectancy, estimator optimization, new acquisition, base replay, feature rebuild, web application, data upload/deletion, or publication is required by this phase.

### Inspected baseline and completed data

The running-build source baseline was `ea2e16128225a91ceb6003fcb3f6ef80985ba8e0`; wheel SHA-256 was `a506c2ad2fd7aabeb6d214a876f6741f303d8bc713fb34a4b6dbbc83d85cafe9`. The accepted plan hash was `e45b3d94c76191d3ed6dd20496dfc1c581e6fac4a7a94a69d9fc325488c2b09a`; population hash was `d59f56fe11d414d632a6cdd02b373fcd0c21dec65f8ed6b8d0d277260d00441e`.

Read-only review on 2026-09-15 reconciled exactly 5,208 expected and completed/verification-passed symbol-days, zero missing/extra/failed members, and 299,980,800 rows in each of base, features and support. All base/feature manifests were checked for completion, contract/member identity and companion existence/size. The recorded job took 5,342.46 seconds with eight workers, peak process-tree RSS 3,404,500,992 bytes, and output size 69,384,625,884 bytes. These are completed-build evidence, not Phase 4 acceptance or independent numerical reconstruction of every row. Member manifests still label independent reconstruction `pending`; bounded Phase 2/3 reference calculations are separate evidence.

Operational locators on the verified project VM:

- Control: `/srv/tape-data-product/control/full-admitted-ea2e161-20260915/`; read `accepted-plan/plan.json`, `completion-verification.json`, and the read-only `run-ledger.sqlite`.
- Data: `/srv/tape-data-product/derived/endpoint-ew-v1-ea2e161-20260915/{base,features}`.
- Installed build: `/opt/tape-data-product/releases/integrated-ea2e161/`.

These paths are operator inputs, never package defaults or runtime dependencies. Keep member catalogs, absolute-path receipts and detailed rows in ignored/private VM artifacts. Do not commit them. Refresh identities before execution. The old local/VM main checkout at `462e2e9` is behind the integrated implementation; do not branch new implementation from it merely because it is the default checkout.

The accepted 5,208 members differ from the 7,085 paired T/Q migration population and the legacy report population. Preserve the accepted denominator; do not silently substitute either earlier population. Reconciliation of the other members belongs in the final population narrative, not a fabricated zero-match count.

## 2. Sequential implementation checkpoints

| Checkpoint | Owns | Completion boundary |
|---|---|---|
| A | Pilot manifest, endpoint/EW release adapter, shared registry mapping and validated projected reader | Installed reader passes independent fixtures and bounded real-pilot checks; interface documented for B/C |
| B | Feature predicates, strict runs, coverage/contributions, reproducible exports | Independent rule/run cases and pilot queries pass using A's reader |
| C | Marginal ECDFs, joint distributions, weighting, figures | Independent numerical/denominator checks and rendered pilot figures pass using A's reader |
| D | End-to-end integration, example inspection, report rehearsal and Phase 5 preparation | Installed synthetic raw-to-report and real pilot workflows reproduce; measured full-run proposal ready |

One implementation task owns each checkpoint. Review the specification and checkpoint evidence independently as required by the master plan; a bounded read-only reviewer is compatible with sequential implementation and must not launch data jobs or write competing code. Record findings and resolutions. Acceptance records identify source revision, wheel, tests, measurements, limitations and the next checkpoint's interface. Do not produce a second standalone spec for each checkpoint.

## 3. Shared data and selection contract

### 3.1 Tables and registry

Accepted schemas are `tape_base_1s_v1`, `tape_features_endpoint_ew_v1`, and `tape_feature_support_ew_v1`. Keys are `(session_date, symbol, interval_end_ns)`; UTC nanoseconds remain integers through filtering and joins. Do not round timestamps through floating point.

Use `contracts.registry.query_registry(config)` as the canonical 27-measurement registry: nine EW families at each of hl30s/hl120s, six freshness p90 fields at window60s/window300s, and three unsmoothed current ages. Validate configuration from the release. Do not copy a hard-coded legacy feature list or expose internal EW sufficient statistics as headline features.

The shared adapter must explicitly map each measurement to its table, value column, reason mask, unit, feature family, half-life or window, source dependencies from registry `Feature.sources`, and optional coverage dependencies. In particular:

- Derived features live in `features.parquet`; masks are `<full_feature_name>_reason_mask`.
- Current `trade_age_seconds`, `quote_age_seconds`, `midpoint_change_age_seconds` live in `base.parquet`; masks are respectively `trade_age_reason_mask`, `quote_age_reason_mask`, `midpoint_change_age_reason_mask`, not `<value_name>_reason_mask`.
- Preserve midpoint-age status, observation origin and lower-bound metadata for inspection; a lower bound is not an exact age.
- Support fields are diagnostics, not additional mandatory eligibility gates. Return participation shares return support; RMS/spread discloses both return and spread support. Age sample counts/elapsed slots retain fixed-window semantics.
- Baseline exact schema types, including decimal share volume, must survive projections; do not coerce all base columns into floats.

Published values are finite/nonnull exactly when their accepted reason mask is zero. Contradictory values/masks and unknown mask bits fail validation rather than being repaired. Valid zero RMS/activity/spread remain zeros. Undefined participation and zero-spread ratios remain unavailable. Do not apply a universal all-feature complete-case mask, current-input gate, or legacy transaction gate.

### 3.2 Time and session selection

An observation labeled `t` summarizes `[t-1s,t)` and uses source events strictly before `t`. Its feature value is an endpoint measurement; this does not prove historical client delivery latency. Selection reads stored histories without resetting/recalculating them.

Session selection uses the stored second's interval: RTH includes rows with `09:30:00 ET < t <= 16:00:00 ET`; premarket has `04:00:00 ET < t <= 09:30:00 ET`; after-hours has `16:00:00 ET < t <= 20:00:00 ET`. These are reporting strata of this stored contract, not a new exchange-calendar/early-close correction. Use date-aware `America/New_York` conversion and verify UTC/date consistency. In particular the row ending 09:30:00 summarizes the final premarket second. Keep any future exchange-calendar policy separately versioned.

For explicit UTC selection, name the API bounds `endpoint_start_ns` inclusive and `endpoint_stop_ns` exclusive: `start <= t < stop`. Document the distinction from market-session boundary selection. Subsets must expose selection/member/segment boundaries to run reducers. No run crosses a member, session-selection segment, or unselected interval. Missing physical rows inside declared selected coverage are corruption, not inferred inactivity.

### 3.3 Population and discovery timing

Require an explicit population timing mode in saved configurations. Initial examples use `historical_membership`: all selected stored session rows from the accepted historical symbol-day membership, subject to the requested session/time selection. Label it retrospective member selection. This is suitable for descriptive historical analysis; it does not reproduce a live screener's availability.

Offer `nominal_post_discovery` and `receipt_post_discovery` only through an explicit validated metadata mapping. Preserve nominal endpoint and actual known-at/receipt clocks separately. A timestamp absent from a member cannot be inferred from its filename, first trade, first nonnull feature, or a string such as `nominal_historical`.

The inspected baseline context has `discovery.eligibility_basis=nominal_historical`, `receipt_known_at=unavailable`, and retained historical membership, but no usable discovery timestamp. A must audit the selected pilot contexts. A request for a timing mode lacking required evidence fails preflight with member/count/reason details; it must not silently fall back to full-session eligibility. Historical-membership mode remains usable. An optional future identity-bound metadata sidecar can add recovered clocks without changing stored features, but external evidence recovery is not an A prerequisite.

When supported clocks exist, nominal mode selects `t > nominal_discovery_endpoint_ns`; receipt mode selects `t > max(nominal_discovery_endpoint_ns, receipt_known_at_ns)` and requires both clocks and their provenance. This preserves the strictly-prior information convention; exposure represents selected endpoint observations, not exact post-discovery execution time. Test receipt before/after nominal time, equality and fractional-second clocks. Discovery selection never resets EW or age history.

Preserve `unverified_missing_original_vendor_pagination_receipts`, historical halt/continuity information basis, missing receipt times, and member selection lineage in receipts. Numerical validity and source-retrieval completeness are distinct. Do not upgrade lineage because hashes or query tests pass.

### 3.4 Shared accounting

Keep member selection, timing selection, feature validity and feature predicate results separate. Report planned/available/missing members before row analysis; corruption fails rather than being counted as a valid zero match. For selected rows, a query has eligible inputs only when all features used by its predicates are valid. Then `selected = eligible + unavailable` and `eligible = matching + nonmatching`. A query with no eligible rows has a null match fraction, not zero. A member with eligible rows and no matches is a valid zero-match member.

For a marginal distribution use that field's own validity; for a pair use exactly the pair's joint validity. Explicit conditioning gates add their own dependencies and separate denominators. Reason-bit tallies may overlap and must be labeled; they are not a disjoint partition unless an explicit exclusive classification is added.

## 4. Checkpoint A — Pilot and shared reader

### A1. Deterministic pilot selection

Build a versioned metadata-only reference manifest for 24 complete symbol-days, with no feature-value threshold, copied Parquet tables, or raw decode. Use the fixed accepted 5,208-member population and immutable admission/source descriptors, not whichever outputs happen to be visible in a directory.

Algorithm `month_event_rank_quartiles_v1`:

1. Reconcile plan membership and completed/verification-passed ledger membership. Verify plan/admitted-index hashes and member descriptor hashes. Event count is admitted quote rows plus admitted trade rows from `source_pair.streams`; it counts raw records, not eligible trades or feature matches.
2. For each month March–August 2026, sort members by `(event_count, session_date, symbol)`. Assign zero-based rank `i` among `n` to stratum `min(3, floor(4*i/n))`. This is a rank stratum; ties are deterministically split, not estimated numerical quartile cutoffs.
3. Process months ascending, then strata 0..3. Rank candidates by SHA-256 of UTF-8 `phase4-pilot-v1|YYYY-MM-DD|SYMBOL`, with date/symbol tie-breaks. Pick the first symbol not previously chosen, if one exists in that stratum; otherwise take the first candidate and record the symbol-repeat fallback. Do not replace members because their later feature values are inconvenient.
4. Require one member per stratum, 24 total, all with complete declared 57,600-row sessions. Missing strata, unsupported coverage or required metadata produce a specific selection failure rather than silent shrinking/resampling.
5. Bind algorithm/seed, month/stratum/rank/event-count metadata, input population/plan identities, selected ordered member keys, base/feature/context manifest hashes and output hashes/schema identities. Store canonical JSON identity separately from relocatable filesystem locators.

Expected pilot size is 1,382,400 rows per table. It diversifies workload and time; it is not a probability-weighted estimate of the corpus. Complete sessions retain startup, quiet periods and unavailable measurements. Keep synthetic edge cases separate from the real pilot. No need for all edge cases to occur naturally in the selected members.

### A2. Release/reference format and trust

Add an endpoint/EW reference format distinct from `local_compact_release_v1`. Reuse safe-path, canonical JSON, hash, schema, completion and stage utilities where their behavior applies. Read-only adapters must not require raw T/Q access, sibling repositories, the implementation checkout, or a live build ledger after the reference has been constructed.

The reference binds release/configuration and base/feature implementation identities; member keys, declared coverage, companion hashes, and the feature manifest's consumed base-manifest hash; context/selection/source lineage; pilot-selection identity; and verification evidence. Reject incompatible mixed configurations, synthetic/real mixtures, duplicate members, prefix/full substitution, path traversal and incorrect base-feature bindings. Validate full expected membership at construction; A's pilot reference intentionally declares its own 24 members and parent population, not a falsely complete 5,208-member release.

Separate immutable content identity from data-root locations so relocation under explicitly supplied trusted roots preserves identity. Resolve and check paths against those roots. Do not allow arbitrary untrusted manifest paths to escape them. Absolute VM paths belong in private operator locator files.

Construction must verify selected companion bytes/hashes, schemas, keys/grid and values/masks using bounded existing validators or extracted reusable logic. Check baseline verification functions before reuse: some calculation verifiers require raw descriptors or the current calculator implementation identity. Do not weaken them globally; create a properly scoped read-only consumer validation path when necessary. A reader implementation change must not force feature recomputation or require its own calculator code hash to equal the producer's hash.

Each read session must verify the reference/manifest identities and consumed files. Correctness-first default is full consumed-file hash verification once per read session, followed by bounded projections and stable file-identity checks across use. An explicitly scoped in-process verification handle may reuse verification for unchanged files during that session; do not persist trust based only on size/mtime. Record validation I/O separately from query scan I/O. Durable verification caches and faster trust policies require a separately reviewed design, not an implicit shortcut for latency targets.

### A3. Installed interfaces

Implement a namespaced endpoint/EW reader API and CLI without changing legacy command meaning. Proposed ownership is new `query/endpoint_release.py`, `query/endpoint_reader.py`, `query/endpoint_selection.py`, plus an additive CLI adapter. Inspect actual CLI structure and adjust module locations if needed; record the final names in A's completion record.

Required public operations (names below are the intended interface unless inspection finds a concrete conflict):

- `build_endpoint_reference(...)`: constructs the pilot/reference from explicit operator inputs and returns identity/accounting; private plan/catalog adapters remain out of generic row logic.
- `open_endpoint_reference(path, *, expected_identity, data_roots)`: validates and yields a scoped reader handle.
- `iter_endpoint_batches(handle, *, fields, selection, include_support=False, include_run_boundaries=False, batch_size=4096)`: yields typed Arrow batches with keys, requested values and their masks, explicit selection flags/segment identity, and requested support/inspection dependencies. With `include_run_boundaries=True`, include `halt_active` and each required source's `quote/trade_continuity_id` and `quote/trade_continuity_break_in_second` from base. Required sources are the union of requested measurement sources; B requests predicate dependencies separately from optional display-only dependencies when deciding run boundaries.
- `describe_endpoint_fields(config)`: returns the complete field/table/mask/unit/dependency mapping.
- Additive CLI group `tape-product endpoint-data` with `pilot`, `verify`, `fields`, and `inspect` operations. `inspect` requires explicit member/time selection and capped output; it is not B's feature-predicate query command.

Selection configuration includes explicit members/dates if narrowing the reference, sessions, optional endpoint bounds and population timing mode. Reject unknown fields, reversed bounds, unknown sessions/modes and requests for absent members. Defaults in examples must be saved explicitly. Keep feature predicates out of A.

The reader projects only required columns/companions. Joining tables by position alone is insufficient: validate exact key equality, monotonicity, uniqueness and expected grids even when Parquet row-group/batch boundaries differ. Prefer aligned streaming over whole-day hash joins; one member at a time, bounded buffers. Feature-only reads should not decode all base values; context and key verification still apply. Preserve nonmatching/unavailable observations in selected spans for B. Emit an explicit segment boundary for omitted selection intervals; session filtering must not create accidental adjacency.

### A4. Independent verification and real-pilot acceptance

Use literal small tables with expected outputs authored independently of production reader/helpers. Include:

| Case | Expected evidence |
|---|---|
| Equal keys, unequal row-group boundaries | Correct joins across batches 1, 7 and 4096 with identical logical rows |
| Missing/duplicate/reordered/wrong-member key | Specific failure; no silent inner-join row loss |
| Corrupt bytes/schema, mixed config, wrong base hash, path escape | Reference/read rejected before accepted results |
| Valid zero, unavailable value, lower-bound midpoint age | Correct values and exact masks; no invented age or zero |
| All 27 registry measurements | Correct table/mask/unit mapping, including current-age mask exceptions |
| Single-feature projection | Unrelated invalid features do not suppress the requested value |
| Source-specific run metadata | Quote-only measurement across a trade-only interruption, and the reverse, expose correct continuity without imposing unrelated validity |
| Session and UTC endpoints | Literal checks around 09:30/16:00, winter/summer offsets, selection inclusivity and fractional discovery time |
| Unknown discovery clock | Historical mode works; requested discovery mode fails with reason |
| Pilot determinism | Input shuffle leaves selection unchanged; ties/repeated symbols and missing strata follow A1 |
| Relocation and mutation | Trusted-root relocation preserves content identity; changed companion invalidates the read session |
| Installed use | Works outside checkout with raw roots inaccessible; legacy reader tests remain valid |

On real data, inventory all 5,208 members only through bounded metadata reads. Select and validate the 24-member pilot, scan its complete keys/grid once per companion, and reconcile the declared 1,382,400 rows per table. Report aggregate per-field valid/zero/null counts and observed source/discovery metadata limitations, not distribution claims.

Compare production projections with a separate direct Arrow or SQL read for fixed first/middle/last eight-row intervals of each selected member. Check all 27 queryable measurements/masks and requested support at those intervals without calling the production adapter to generate expectations. This checks reading/alignment, not raw-to-feature numerical reconstruction. Include full pilot batch-size invariance at 7 and 4096; use batch size 1 on synthetic fixtures only.

Accept A only when the installed API/CLI, immutable pilot, independent tests and resource measurements pass. Record final API/config/receipt schemas, selected pilot identity, exact source/wheel, rows/bytes, verification versus scan time, peak RSS and limitations in `phase_4_a_completion.md`; update implemented docs. Do not mark B–D or Phase 4 complete. Commit reviewed implementation in the development worktree; sharing to GitHub follows explicit task scope.

## 5. Checkpoint B — Rules, strict runs and exports

Consume A's reader and registry; do not add a second validity/population implementation. Support conjunctions of finite numeric feature comparisons (`>`, `>=`, `<`, `<=`, equality) and explicit range endpoints. Reject unknown fields, nonfinite cutoffs and contradictory ranges. Multiple named queries are separate selections; arbitrary expression evaluation, OR syntax, confirmation and relaxed continuation are deferred unless separately specified. Illustrative rules are not calibrated thresholds.

Match input eligibility requires exactly the predicate dependencies. Requesting extra display columns does not change matches. Null predicates are unavailable, not a negative numeric value. Keep selected endpoints flowing through the run reducer; predicate pushdown cannot delete false/unavailable rows needed to end runs.

A strict run is consecutive one-second matching endpoints in one selected segment/member, with no required-source continuity break or halt between them. Emit first and last matching endpoint, `match_count`, represented interval `[first_endpoint-1s,last_endpoint)`, and closure reason. Distinguish represented duration (`match_count` seconds) from elapsed time between first and last signals (`last-first`). Entry-time information is first endpoint; eventual run duration and last endpoint are retrospective. Mark selection/member truncation as censored. Break on relevant source continuity even if a historical feature remains numerically publishable; unrelated source interruptions must not impose an unrequested dependency.

Outputs: immutable query config and identity; matching observations with keys/values/masks; strict runs; member-level selected/eligible/matching/nonmatching/unavailable counts; symbol/date contributions; lineage and schema metadata. Stream Parquet/JSONL outputs; cap optional CSV/display exports. Query identity binds reference, selection, predicates, semantics and implementation. Empty results must still have schema and full accounting.

Independent tests cover threshold equality, valid zero versus unavailable, false/missing middle row, batch-boundary runs, member/session/continuity boundaries, irrelevant display fields, no-eligible versus zero-match members, and a hand-counted multisymbol example. Include `midpoint_rms_5s_to_spread_hl30s > 2` as a synthetic/illustrative query; no positive real match is required for acceptance. Demonstrate a reproducible pilot export and verify its values against direct source projections.

## 6. Checkpoint C — Distributions and figures

Implement numerical reducers separately from rendering, using A's selection/validity contract. Begin with unconditional RTH historical-membership pilot distributions. No inherited legacy transaction gate. User-specified activity filters are explicit additional analyses with their own saved denominator; do not select them to manufacture appealing findings.

Required marginal output covers all 27 registry measurements with units and separate hl30s/hl120s or window60s/window300s labels. Valid zeros remain in the distribution. An empty valid population yields an empty table and unavailable statistics. Primary ECDF is `F(x)=count(valid values <= x)/N_valid`, with ties coalesced across batch boundaries. Report selected, valid, unavailable and zero counts alongside quantiles; quantify source/feature exclusions separately.

Equal-symbol-day sensitivity is `F_equal(x)=(1/M) sum_m F_m(x)`, over the M members with at least one valid selected observation for that field. Each observation in member m has weight `1/(M*n_m)`. Report omitted zero-valid members. Do not substitute the ECDF of member medians. For joint distributions, use pair-valid n_m for this weighting. Unit-test a case where pooled seconds and equal-member weighting deliberately differ.

Initial joint panels, at matching EW views, are RMS versus spread; RMS versus participation; trade rate versus spread; trade rate versus separate bid/ask displayed sizes; and fast versus slow RMS on pair-valid endpoints. Avoid the redundant RMS-versus-RMS/spread relationship as primary evidence of an independent association. Pair weighting and validity must be explicit. Each panel reports sample/member counts and concentration. Independent one-second observations must not be claimed: returns overlap and features are smoothed.

For pilot mechanics use fixed, versioned bins with explicit underflow/overflow and zero handling. Before full execution, record final units/bin edges/axis scales and how they were selected. Pilot-informed axes are exploratory configuration, not preselected corpus evidence. Numeric tables retain all valid observations even when log axes omit zero from the visual; show zero mass separately. Do not silently winsorize tails or replace exact ECDFs with histogram approximations.

Reuse bounded tie reducers, sorting and plotting utilities where correct; replace legacy field validation/gates. Exact ECDF sorting is O(N log N) external work with O(N) bounded temporary disk; fixed-bin joints are O(N) work and O(number of bins) resident state. Process one exact field at a time. Verify literal ties, zeros, nulls, weighted examples, pair-specific missingness, bins at edges, overflow, empty populations and reconciliation of plotted totals to tables. Render and inspect every pilot figure. Final figure selection may be smaller than numerical coverage to keep the report readable.

## 7. Checkpoint D — Installed report rehearsal

Integrate A–C in one installed workflow: invented raw tape → accepted base/features → endpoint reference → rule/export → feature-history inspection → marginal/joint tables and figures. No network or sibling-checkout access. Use a separate real-pilot workflow on existing feature tables; do not replay real T/Q merely to demonstrate the reader.

Prepare a pilot report fragment explaining the measurements, timing, denominator, an illustrative query, strong/weak examples and limitations. Do not overwrite the historical root report with pilot findings. For real raw-tape inspection, define exact selected members/time intervals and I/O budget before the bounded inspection; no whole-day raw loading. Synthetic tape inspection is mandatory; real examples must retain their actual lineage.

Record example-selection procedure and label any examples chosen after inspecting results. Show symbol/date concentration and selection effects. Do not present a favorable example as typical behavior without population evidence.

D records full-population execution parameters, expected scan/sort/output/storage costs and query performance measurements. Phase 5 execution requires its measured full-run confirmation; completion of the earlier feature build is not blanket authorization for all future corpus sorts or publication.

## 8. Resource and performance contract

Before every VM job, follow [VPS connection preflight](../../docs/vps-operations.md), inspect branch/dirty worktrees and active jobs, and refresh free memory/disk. Use a separate checkout/environment. Never edit an installed release, mutate completed data or change another job's environment. Local documentation-only edits are allowed; implementation/testing are VM-first.

A's bounded real-data scope is metadata inventory of 5,208 members and decoded reads of the fixed 24 pilot members only. Enforce per measured invocation: one worker, library threads 1, CPU quota 200%, process-tree RSS stop 2 GiB with cgroup hard ceiling 3 GiB and no swap, 15-minute runtime limit, 8 GiB total read-byte budget (including hashes), 4 GiB owned output/scratch cap, and at least 20 GiB free-disk reserve. Inspect projected companion sizes before execution; if the deterministic pilot exceeds the budget, report the exact projection and propose a revised budget rather than silently resampling. Stop and diagnose resource failures; do not restart automatically. Log cumulative resource use across justified attempts.

Default output batch size is 4096; A's public reader maximum is 4096 for this phase. Resident state is O(batch * projected columns + fixed member/selection state). Metadata indexing may use SQLite with an 8 MiB cache; no full event arrays, whole-day DataFrames, or corpus-sized joins. B keeps bounded run state and disk-backed contribution totals. C initially uses DuckDB memory limit 256 MiB, one thread and at most 1 GiB spill within the 4 GiB overall cap. Total process-tree RSS includes native engines and plotting.

Performance targets are engineering targets, not permission to skip integrity checks. On an already verified in-process reader handle, target <=2s for a ten-second single-member projection and <=30s for one member's full-session four-field scan. B targets <=120s for a four-predicate scan over the 24-member pilot. C targets <=300s for one pilot-field exact ECDF under its budget. A reports first-use verification time separately; it is bounded by the 15-minute measured-invocation cap. A target miss requires explanation and an accepted optimization/revised target before that checkpoint is called performance-accepted.

Measure first-use and warm repeated reads without dropping shared OS caches. Call a run cold only when cache state is actually controlled and documented; fresh process is not cold disk. Record wall/CPU time, projected rows/columns, logical/physical bytes where observable, peak process-tree RSS, spill/output size and cache/verification state. Full-corpus latency targets and budgets must be set from these measurements before Phase 5 runs.

## 9. Completion, integration and next prompts

The implementation task must first check that this specification and required decisions are present in its VM Git baseline. Local uncommitted plans are not automatically on the VM. Integrate planning changes through a scoped Git commit/fetch or local Git bundle when remote sharing is outside scope; never copy source files as the normal synchronization method. Preserve pre-existing owner edits, especially AGENTS.md. Do not stage all local planning changes indiscriminately.

After each checkpoint, update this document only for resolved interfaces/decisions, add concise checkpoint evidence, and update implemented `docs/`. Preserve remaining work as pending. Generate the next prompt using the accepted source revision and actual interfaces. The prompts reference this document rather than repeating all semantics.

Phase 4 accepts only after A–D pass, an independent review resolves correctness findings, installed synthetic and real-pilot workflows reproduce, figures and denominators reconcile, and a bounded measured Phase 5 proposal is ready. The final corpus findings/report and any public release remain separate work.

### Specification review, 2026-09-15

A bounded independent read-only review checked governing-contract consistency, sequential ownership and A's implementability. Incorporated both material findings: receipt eligibility uses the maximum of nominal discovery and receipt clocks, and A explicitly exposes source-specific continuity metadata needed by B. Local document links and whitespace checks passed. This is specification review, not implementation, pilot construction or data acceptance.
