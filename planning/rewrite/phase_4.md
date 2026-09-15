# Phase 4 — New-feature retrieval and research workflow

Status: report-first implementation specification, updated after review of A's final handoff on 2026-09-15. A's reviewed fixed-pilot reader handoff is complete at VM commit `96d74da8d37b8f6956db16405fc03bc621a453d9`; this does not accept B/C/D or a full-universe query release. Phase 4's primary outputs are usable queries, population accounting and real-data figures. A–D describe code ownership, not a serial prerequisite chain for exploratory research. The report-first scope in §2 supersedes earlier instructions that real figures must wait for all synthetic/report infrastructure.

## 1. Outcome and governing contracts

Deliver reproducible queries and empirical report outputs from the completed endpoint/EW release: universe coverage over time, real-data feature relationships and distributions, and worked threshold selections with coverage and contribution accounting. Begin with bounded pilot execution and measured scaling before full-population analysis. Integrate these into the installed product; strict matching runs support later interval examples and are not a prerequisite for observation queries or figures.

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

### Accepted A handoff and actual execution boundary

Read the VM handoff branch's `planning/rewrite/phase_4_a_completion.md` and `docs/endpoint-data.md`. Implementation commit is `e3ca68fa85cca4b5690fa8865bfe853cec28e3db`; installed source is `b3d467dad943d82aca6c861d4e3a3de9199f6841`; final documentation commit is `96d74da8d37b8f6956db16405fc03bc621a453d9`. The installed wheel SHA-256 is `f88fe930bc91dc5057ac7fbe5a019a01ea4f8078439b7cde1a3c4644ed705c8e`, under `/opt/tape-data-product/releases/phase4-a-b3d467d/`.

Pilot identity is `4a410bd8321cfe0466dc7ecd02c8a5762a679c320d64376c24cea076465ae832`, at `/srv/tape-data-product/control/phase4-a-52b5b35-20260915/pilot-reference-v1-deterministic`. It contains 24 unique symbols, 1,382,400 rows per table and 72 session segments. Recorded evidence includes 345 passing tests, 19 focused reviewer tests, full-pilot batch equality and direct agreement on 576 fixed-interval rows across 99 columns. The installed verification blocked access to raw data and the development checkout. This is reader validation, not empirical feature findings or corpus-wide independent numerical reconstruction.

The implemented reference is deliberately pilot-specific: open enforces the ordered six-month × four-stratum structure and full-session bounds. B/C must not claim an arbitrary-member or full-universe reference is already implemented. Use this pilot immediately. A separately versioned explicit-member reference or scoped manifest-bound SQL adapter is needed before expanding membership; do not disable pilot checks or rebuild the features. Metadata population counts can still use the full accepted plan/ledger without opening it as an A reference.

A hashes companions once per handle and streams member data before narrow time filtering; it has no row-group time pruning or durable verification cache. Its 76.9-second all-field acceptance pass includes output hashing, sample capture and comparisons, so it is not a clean query benchmark. Its warm-state 0.47-second ten-second and 0.55-second full-member probes do not establish full-universe throughput. Use the exact-workload benchmark gate in §8 before scaling; do not reopen A for speculative optimization.

## 2. Implementation checkpoints and dependency order

### Report-first scope — owner clarification, 2026-09-15

The purpose is to recreate the original empirical report with the new data structure and feature definitions. Reference documents inspected are `docs/tape_data_product/report_revised_draft.md` and `report.md` in the sibling `tape-characterization` repository, and this repository's historical root README. The sibling is a read-only methodology/visual reference, never a runtime dependency. Its report covers universe breadth over time, marginal distributions by session, joint movement/spread/participation relationships and activity bands, and worked retrieval. Its draft also used real development subsets before full-data aggregation. Synthetic figures were test artifacts, not a substitute for the empirical deliverable.

**Deliver in this practical order:**

1. **Population and query access:** a repeatable SQL or CLI path over explicit completed-member files, plus daily/monthly symbol-day counts, distinct symbols, date coverage and represented time. The complete accepted inventory can supply member counts without scanning feature rows. Keep acquired/selected/completed populations distinct; broader-market selection shares require their actual reference-universe and screening records. Do not reuse the old 6,222-member counts or 0.90% selection share as new-release findings.
2. **Real joint-distribution preview immediately:** reuse the existing fixed 24-member sample if its exact membership and companion identities are available. Select complete symbol-days before examining values, retain all their represented sessions, and plot the already-computed features. No new sample-construction project is required. Explicitly label this stratified development sample; do not claim it represents corpus frequencies. A documented seeded sample is also technically valid for later expansion, but selecting the first filesystem members is not random. Do not resample to recover a desired shape.
3. **Worked filter and population reduction:** use `midpoint_rms_5s_to_spread_hl30s > 1`, and separately hl120s, as the initial user-requested descriptive example. Report starting members, members with any valid input, members with at least one matching second, eligible/matching/unavailable time, and retained fractions by date/session. An any-match symbol-day is not a whole day labeled above threshold. Any minimum matching duration is a separate explicit condition. Report matching/represented and matching/eligible fractions separately. Ratio validity requires the stored ratio's own valid mask and finite value; zero spread remains unavailable.
4. **Session ECDFs and activity-conditioned joint pages:** adapt the original report's useful panels after seeing the first real joint preview. Keep the same fixed sample for paired comparisons. Start pooled and split by premarket/RTH/after-hours, not RTH alone. Begin unconditionally with field/pair validity; any activity gate and bands must be explicit, with retained-time accounting. Then measure and propose the full-population rerun and report replacement.

These are feature calculations and descriptive statistics, not a trained predictive model. No training or recalculation is needed to inspect stored values. New five-second endpoint RMS / EW spread replaces mean absolute TWAP movement / rolling spread; a threshold of 1 retains the interpretation “measured movement scale exceeds mean full quoted spread” but its frequency is newly measured. Half-lives 30s/120s are not the legacy 60s/300s rolling horizons. Never carry old correlations or numerical findings forward as expected results.

### First real figure set and report correspondence

| Original report view | New preview / adaptation |
|---|---|
| Daily acquisition-universe share and coverage | Daily/monthly accepted member counts now; separate screening/reference-share reconstruction only with matching evidence |
| Movement versus spread with M/S reference lines | RMS versus full mean spread at hl30s and hl120s; constant RMS/spread lines including 1 |
| Movement versus participation | RMS versus the corresponding EW magnitude participation at each half-life |
| Movement versus trade rate | RMS versus corresponding EW eligible trade rate |
| Movement/spread and movement/participation by activity band | Same panels using explicit corresponding EW trade-rate bands; each panel's required fields and denominator disclosed |
| Session ECDF grids | New measurements split by session, with half-life and freshness-window labels clearly separated |
| Filtered universe and worked retrieval | Threshold counts, matching rows and breadth over time; strict runs later if needed for interval examples |

The first real preview requires the first three joint relationships at both half-lives (six panels), with count/coverage tables, probability mass per bin, explicit zero/off-axis mass and rendering inspection. Displayed-size, share/dollar-rate and fast/slow comparisons can follow. Exact ECDFs for every field, equal-member sensitivity, all activity-band pages and an installed raw-to-report demo are not gates for delivering these six real-data panels. Do not label a visual ridge a verified positive correlation without suitable numerical analysis; report the actual shape, including weak or absent relationships.

### Report structure and plot selection after A

Retain the original report's narrative with limited changes, rather than inventing a new research program:

1. **Purpose and acquired population:** explain the research question and acquisition screen, then show daily accepted symbol-day counts and a monthly coverage table. Separate reference-universe, acquired, admitted and completed populations wherever evidence supports reconciliation. Display missing/unresolved provenance explicitly rather than inventing a complete funnel.
2. **What the features measure:** group movement, friction/displayed liquidity, activity and freshness. Distinguish the five-second return lag, EW half-lives, startup requirements and fixed p90 windows. Describe numerical coverage separately from represented hours. A 16-hour full member has 57,600 represented seconds, not 16 hours of valid values for every field.
3. **What combinations occur:** present the six-panel RMS/spread, RMS/participation and RMS/trade-rate preview first during research. The final report may retain its old ECDF-before-joints order if that reads better. Interpretation is conditional on the actual plotted population, not expected historical shapes.
4. **What is new or complementary:** use a compact displayed-liquidity view and a fast/slow feature-history example when they explain information missing from the older report. These are follow-on analyses of existing measurements, not additional base fields or model training.
5. **Research use:** show the saved RMS/spread >1 query, its daily breadth and time retention, and a reproducible example. Report both matching/represented and matching/valid-input shares, plus unavailable time. An any-match member count is distinct from matching duration; do not imply a whole day satisfies the condition.
6. **Limits and reproducibility:** population selection, incomplete historical retrieval/knowledge evidence, overlapping observations, concentration and measured execution costs. Link detailed methodology and all-field tables outside the main narrative.

Plot priorities and interpretation:

| Plot | Decision and interpretation |
|---|---|
| RMS vs spread, both half-lives | Keep as the main economic-context plot. Positive log axes with constant RMS/spread lines (0.1, 1, 10 where visible); zero mass handled separately. Pair-valid spread/RMS population differs from ratio-valid population when spread is zero. Annotate the >1 fraction using an explicitly ratio-valid denominator, not a hidden change to heatmap normalization. |
| RMS vs participation, both half-lives | Keep as magnitude versus concentration. Use linear participation [0,1], positive log RMS and explicit zeros. Both use the same return history: writing Q=EW(r²), A=EW(|r|), RMS=sqrt(Q), P=A²/Q gives RMS*sqrt(P)=A when defined. They are different summaries, not independent measurements; scaling all returns changes RMS but leaves P unchanged. Their joint shape is descriptive, not independent validation or evidence of directional structure. |
| RMS vs trade rate, both half-lives | Keep as movement versus eligible transaction activity. Common smoothing/overlap and symbol composition can contribute to the pattern. Preserve quiet/zero-rate mass; do not prefilter for activity before the unconditional preview. |
| Spread vs displayed bid size and spread vs displayed ask size | Add one compact follow-on figure to show the new displayed-liquidity dimension, with sides separate and shares labeled. This tests whether wider/narrower quotes coexist with different displayed sizes. Absolute shares vary by symbol/price/lot conventions; the plot does not measure executable capacity or establish a causal relationship. No new imbalance or combined-depth feature is needed. |
| Fast/slow RMS and spread on the same real time interval | Prefer a small feature-history example with both views and marked query crossings over a main-report fast-vs-slow correlation matrix. It explains responsiveness and retained history. Use the same endpoints where both views are valid; do not call either a forecast or assume the slow curve is always smaller. Example selection is retrospective and documented. |
| Trade rate vs trade-age p90 | Optional supporting plot when explaining bursty vs more continuous trading. EW half-life and p90 window must each be named; a chosen hl30/window60 or hl120/window300 pairing is a reporting choice, not identical weighting. Do not impose the trade-age cutoff before using this panel to examine inactivity. |
| Share rate vs trade rate; dollar rate vs share rate | Supporting only, not headline independent-correlation evidence: these reuse the same eligible transaction totals and are linked through trade size/price. Keep their marginal summaries available. |
| Ratio vs its numerator/denominator; all-pairs correlation matrix | Do not prioritize. Arithmetic coupling, duplicated horizons and unequal validity can create impressive but uninformative structure. A matrix is not needed to recreate or explain this product. |

Preserve the old heatmaps' probability-mass interpretation: percentage of the panel's eligible seconds per bin, not density per unit of x/y. Share bin edges, axis limits and color normalization across compared fast/slow panels, recording configuration after the pilot. Keep zeros and off-axis observations in totals and disclose them; do not plot a false zero position on a logarithmic axis. Use non-overlapping categories for displayed, zero-excluded and other off-axis counts so they reconcile. Show per-panel valid seconds/member counts and unavailable fractions without a large wall of captions.

For comparisons claiming a half-life effect, supplement each-view-own-valid results with common-endpoint validity for that pair; different startup/support eligibility must not masquerade as a smoothing effect. Keep this pairing local to the compared variables, never an all-27-field complete-case gate. For session comparisons report contributing symbol-days and concentration; if a main relationship is heavily concentrated, add the already-specified equal-member sensitivity before claiming it is broad. No significance testing treating seconds as independent samples is required.

Keep session ECDFs but split them into readable families: (a) RMS, spread, ratio and participation; (b) trades/shares/dollars and displayed sizes; (c) current ages and fixed-window freshness. The 27 queryable measurements need not all occupy the root report. Store all-field numerical summaries in supporting artifacts; initial selected ECDFs can follow the joint preview. Do not force current ages into fast/slow labels or describe 60s/300s p90 windows as EW horizons.

Revisit the old transaction gate only as a separately labeled conditioned analysis. A proposed rate>=1/s plus age-p90<=2s gate retains its illustrative role; it is not inherited automatically or recalibrated silently. Explicitly choose its EW half-life/p90 window, record gate eligibility/retention and mark the corresponding ECDF truncations as imposed. Unconditional activity bands may include [0,1), [1,10), [10,30), [30,100), and >=100 trades/s, with zero mass disclosed; reporting fewer panels must not remove omitted bands from baseline accounting. Full activity-band pages are useful only if they add insight beyond the first plots.

### Data access and correctness boundary

Use A's committed reader if available. If A is still closing product-level acceptance, a bounded direct Arrow/DuckDB path over the same explicit completed partitions may produce the research preview. Keep the SQL/script, member list, release/configuration identity, projected columns, masks, selection, bin edges and numerical output identities. Validate file/member/schema/coverage identities and relevant key/value/mask consistency; compare selected rows/counts independently. Do not read unverified replacement files, silently substitute prefixes, glob an uncontrolled directory, infer missing discovery timestamps, or bypass a demonstrated defect in the actual path used.

A thin fixed-release research adapter using installed contract definitions is allowed; a second generalized registry/reader system is not. Feature-only plots need only feature values, keys and masks, with metadata for population/session accounting. Base/support joins are added only when requested measurements or diagnostics require them. Document research-adapter limitations; do not claim it has completed all generic reader acceptance. Migrate the preview to the shared reader once available without changing its statistical meaning.

Historical full-session analysis must be labeled as such because current inspected contexts lack usable discovery clocks. The original report's post-discovery timeline is a different population. Missing discovery/reference-universe evidence should produce a clear limitation on reproducing those specific sections, not block all current feature plots. A later recovered identity-bound clock mapping can support separate post-discovery results.

### Ownership, existing work and closing scope

- **A:** reviewed fixed-pilot reader handoff is complete at the revision recorded above. Consume that evidence and preserve its documented limitations. Do not reopen A for generalized capabilities or repeat its checks unless a new finding or relevant code change requires it.
- **B:** prioritize population summaries, usable observation queries and the RMS/spread >1 reduction example. SQL or CLI is acceptable for initial research; strict-run machinery and generic export features must not delay the first correct counts and observations.
- **C:** prioritize the six-panel real-data joint preview and its denominators now. Synthetic tests remain small correctness checks. C does not depend on B or full A acceptance when the documented direct path is sound. Broader ECDF/weighting infrastructure follows the first empirical deliverable.
- **D:** assemble the population narrative, new feature definitions, actual figures/findings and worked query into a real-data report rehearsal. Installed-demo polishing follows the empirical workflow.

Keep independent implementation in separate worktrees; do not modify another task's files or copy uncommitted source. Serialize data-intensive VM runs. The real preview is restricted to the fixed 24-member sample under §8's budgets, with fresh concurrent-resource checks; inventory population summaries are metadata-only. Full-corpus feature scans/sorts still require their measured projection and explicit confirmation. No automatic full-corpus run or publication follows from this clarification.

The detailed A–D requirements below remain a reusable implementation backlog and source of correctness semantics. Their broad interface/coverage requirements do not supersede this research-first delivery order. Record partial deliverables and deferred requirements honestly rather than either blocking useful real results or declaring the entire product accepted.

| Checkpoint | Owns | Completion boundary |
|---|---|---|
| A | Pilot manifest, endpoint/EW release adapter, shared registry mapping and validated projected reader | Installed reader passes independent fixtures and bounded real-pilot checks; interface documented for B/C |
| B | Feature predicates, strict runs, coverage/contributions, reproducible exports | Independent rule/run cases and pilot queries pass using A's reader |
| C | Real-data joint preview first, then marginal ECDFs and broader comparisons | Independent numerical/denominator checks and rendered real-pilot figures; shared-reader integration recorded separately if initially using the direct path |
| D | Empirical report rehearsal, example inspection, integration and Phase 5 preparation | Real pilot population/figures/query reproduce; measured full-run proposal ready; installed-demo work tracked separately |

One implementation task owns each checkpoint. Review the specification and checkpoint evidence independently as required by the master plan; a bounded read-only reviewer must not launch data jobs or write competing code. Record findings and resolutions. Acceptance records identify source revision, wheel, tests, measurements, limitations and the next checkpoint's interface. Do not produce a second standalone spec for each checkpoint.

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

Implement numerical reducers separately from rendering, using A's selection/validity contract. Begin with the real-data joint preview in §2, pooled across represented sessions and then separated by session under historical-membership selection. No inherited legacy transaction gate. User-specified activity filters are explicit additional analyses with their own saved denominator; do not select them to manufacture appealing findings.

Eventual marginal output covers all 27 registry measurements with units and separate hl30s/hl120s or window60s/window300s labels. Valid zeros remain in the distribution. An empty valid population yields an empty table and unavailable statistics. Primary ECDF is `F(x)=count(valid values <= x)/N_valid`, with ties coalesced across batch boundaries. Report selected, valid, unavailable and zero counts alongside quantiles; quantify source/feature exclusions separately.

Equal-symbol-day sensitivity is `F_equal(x)=(1/M) sum_m F_m(x)`, over the M members with at least one valid selected observation for that field. Each observation in member m has weight `1/(M*n_m)`. Report omitted zero-valid members. Do not substitute the ECDF of member medians. For joint distributions, use pair-valid n_m for this weighting. Unit-test a case where pooled seconds and equal-member weighting deliberately differ.

Initial joint panels follow §2: RMS versus spread, RMS versus participation and RMS versus trade rate at both EW views. Later panels include trade rate versus spread, trade rate versus separate bid/ask displayed sizes, and fast versus slow RMS on pair-valid endpoints. Avoid the redundant RMS-versus-RMS/spread relationship as primary evidence of an independent association. Pair weighting and validity must be explicit. Each panel reports sample/member counts and concentration. Independent one-second observations must not be claimed: returns overlap and features are smoothed.

For pilot mechanics use fixed, versioned bins with explicit underflow/overflow and zero handling. Before full execution, record final units/bin edges/axis scales and how they were selected. Pilot-informed axes are exploratory configuration, not preselected corpus evidence. Numeric tables retain all valid observations even when log axes omit zero from the visual; show zero mass separately. Do not silently winsorize tails or replace exact ECDFs with histogram approximations.

Reuse bounded tie reducers, sorting and plotting utilities where correct; replace legacy field validation/gates. Exact ECDF sorting is O(N log N) external work with O(N) bounded temporary disk; fixed-bin joints are O(N) work and O(number of bins) resident state. Process one exact field at a time. Verify literal ties, zeros, nulls, weighted examples, pair-specific missingness, bins at edges, overflow, empty populations and reconciliation of plotted totals to tables. Render and inspect every pilot figure. Final figure selection may be smaller than numerical coverage to keep the report readable.

## 7. Checkpoint D — Installed report rehearsal

First integrate the real-data population summary, feature figures and worked query into a report rehearsal. Subsequently integrate A–C in one installed demonstration: invented raw tape → accepted base/features → endpoint reference → rule/export → feature-history inspection → marginal/joint tables and figures. No network or sibling-checkout access. Use a separate real-pilot workflow on existing feature tables; do not replay real T/Q merely to demonstrate the reader.

Prepare a pilot report fragment explaining the measurements, timing, denominator, an illustrative query, strong/weak examples and limitations. Do not overwrite the historical root report with pilot findings. For real raw-tape inspection, define exact selected members/time intervals and I/O budget before the bounded inspection; no whole-day raw loading. Synthetic tape inspection is mandatory; real examples must retain their actual lineage.

Record example-selection procedure and label any examples chosen after inspecting results. Show symbol/date concentration and selection effects. Do not present a favorable example as typical behavior without population evidence.

D records full-population execution parameters, expected scan/sort/output/storage costs and query performance measurements. Phase 5 execution requires its measured full-run confirmation; completion of the earlier feature build is not blanket authorization for all future corpus sorts or publication.

## 8. Resource and performance contract

Before every VM job, follow [VPS connection preflight](../../docs/vps-operations.md), inspect branch/dirty worktrees and active jobs, and refresh free memory/disk. Use a separate checkout/environment. Never edit an installed release, mutate completed data or change another job's environment. Local documentation-only edits are allowed; implementation/testing are VM-first.

### Next B/C jobs: actual-workload benchmark before expansion

The first real B/C results must also measure the production analysis path, not A's acceptance checker. Restrict feature reads to the existing 24-member pilot and set a **300-second hard wall limit per real workload**, one worker/library thread, at most 200% CPU, sampled process-tree RSS stop 2 GiB, 3 GiB hard memory limit, no swap, 4 GiB total owned output/scratch and 20 GiB free-disk reserve. DuckDB keeps its 256 MiB engine memory limit and <=1 GiB spill within that total. Apply the read-byte accounting below. These limits supersede the older 15-minute A invocation limit for initial B/C real workloads. On a stop, record the reason and smallest proposed fix; do not automatically retry, add workers, increase time/memory, or expand sample membership.

Benchmark (i) metadata population aggregation without decoding features, (ii) exact ratio>1 counts at both half-lives with capped matching-row examples, and (iii) six-panel fixed-bin joint aggregation with coverage/session/contributor counts. Within the histogram workload, compute panels from one union-of-required-columns scan where practical, then render from the small saved bin tables. Counts may share this pass too. Reuse an in-process verified handle; do not rehash the release or rescan all columns per panel. Do not run all-field support joins, output-digest loops, tiny-batch invariance or per-observation Python dictionaries as the normal report computation.

Record setup/file-verification time, scan/reducer time, rendering time, rows and projected bytes, peak memory, spill/output size and cache state separately. Use the smallest correct projected path; benchmark an equivalent manifest-bound DuckDB aggregation if the shared reader is slow. Compare identical predicates, members, masks and denominators; differing results are a correctness problem before a speed comparison. No durable verification bypass is implied.

Return the actual pilot counts and real figures with timings. Do not present a scaled estimate as measured corpus performance. Before any larger feature sample or full-universe scan, propose exact membership/read/time budgets and obtain the applicable external-run confirmation. Full scaling evidence should cover more than one sample size and member-size/date variation, account for per-file setup/hash costs, and avoid treating warm-cache tiny reads as cold-corpus evidence. No 5,208-member run is authorized by finishing A or by this spec.

Exact ECDF sorting, all-field exports and complete match-row exports are separate workloads, not hidden side effects of histogram/count queries. Budget and measure one chosen ECDF first; do not queue 27 sorts automatically. Reuse saved numerical aggregates for plot/style revisions without rereading feature files.

### A's completed acceptance budget and shared bounds

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

### Report-design and A-handoff review, 2026-09-15

Reviewed the owner-supplied final handoff against the VM's clean `96d74da` branch, completion record and installed-interface documentation. Inspected the prior report narrative and rendered ECDF, movement/spread/participation and activity-band figures. An independent read-only report-design review agreed with retaining the core structure and six-panel first preview, adding a compact displayed-size view, separating fixed-age windows from EW views, disclosing moment coupling and using matched validity for explicit half-life comparisons. Incorporated its scope corrections to the outcome and D summaries. Added actual-workload/time-limit requirements following the owner's concern about unmeasured multi-hour report jobs. This review performed no feature-data scan and makes no new empirical finding or fresh numerical-validation claim.
