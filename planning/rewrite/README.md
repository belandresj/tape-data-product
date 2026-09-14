# Tape research database: product direction

**Status: product decisions selected and Phase 0 contract package implemented on 2026-09-14; Phase 1 environment and bounded transport checks are complete; production replay/features remain pending.** This document records the agreed MVP scope, feature construction, and research architecture. The choices below are selected directions, not competing candidates. Phase 0 mathematical and schema decisions are closed; later measured operational limits and explicitly deferred choices remain open in their owning phases. This document does not replace the existing [V1 feature contract](../../docs/reference/v1/feature-contract.md), historical results, or query semantics. Intentional semantic changes require a new version and validation; preserve the existing eighteen compact 60s/300s fields and their definitions under their existing identity.

Follow the [master phase plan](phases.md) for implementation order, VPS/data migration, agent responsibilities, per-phase specification requirements, and completion criteria. The [storage and migration plan](storage_and_migration.md) prioritizes corpus scans and efficient feature rebuilds and separates early raw transfer from full calculation approval. This README owns product decisions; the phase plan organizes their implementation.

## Goal and researcher workflow

Build an efficiently queryable cloud research database at one-second resolution. Researchers specify measurable tape characteristics and retrieve matching observations, time ranges, and symbol-dates for inspection or downstream research. A reusable trade/quote replay engine makes it practical to add base measurements when a new question requires information absent from the one-second data.

Example: “Within regular hours, find symbol-dates containing observations with high EW five-second midpoint volatility, at least ten eligible trades per second, and narrow quoted spreads.” Results identify the matching times, measured values, eligible coverage, and contributions by symbol/date. Any minimum matching-time or duration requirement must be explicit.

The workflow is: choose dates/population → specify feature conditions → retrieve matches and coverage → inspect feature histories and optionally raw tape → export a reproducible selection. Symbol-date results summarize matching observations; they do not label an entire day as one tape type. Defaults use strict matching observations/runs. Confirmation and relaxed continuation, if offered, remain separately defined options. Eventual duration and other retrospective summaries are not knowable at entry.

The product describes observed conditions. It does not promise persistent regimes, direction, execution quality, or trading profitability. Persistence research informs limitations rather than determining whether the dataset is useful. Initially, searches cover the existing acquisition-screened population, not the entire equity market; unavailable data and valid zero-match members remain distinct.

## Selected one-second base measurements

The [accepted Phase 0 schema](phase_0_schema_review.md) records exact column names, types, units, null rules and reason codes (approved 2026-09-14). It specifies three logical tables and 27 queryable measurements. The installed contracts package implements the schema and lossless decimal admission checks. Individual historical source precision/units remain subject to member admission; this is not a completed data release.

Each row ending at `t` summarizes `[t−1s,t)`. Endpoint quotes and ages use events strictly before `t`. Time-weighted average prices (TWAPs) weight reconstructed quote states by their valid duration, not by quote-message count.

| Group | Selected stored measurements | Purpose |
|---|---|---|
| Identity | Symbol, session date, UTC second-ending timestamp | Stable joins, partition selection, and timing |
| Price during the second | Bid TWAP, ask TWAP, midpoint TWAP; common valid quote duration | Prevailing-price context and reusable alternative inputs; not headline volatility inputs |
| Quote at the endpoint | Endpoint bid and ask, endpoint quote validity | Latest observed prices; endpoint midpoint and spread are derived |
| Quoted friction | Integral of instantaneous full spread in bps-seconds; valid spread duration | Exact duration-weighted spread in bps and alternative smoothing |
| Displayed liquidity | Separate bid/ask displayed-size integrals in shares-seconds and supported durations; endpoint bid/ask sizes and size validity | Reusable EW mean displayed sizes and current displayed-size context |
| Transactions | Eligible trade count, share volume, and dollar volume | Trade/share/dollar rates; average trade size and eligible-trade VWAP |
| Freshness | Endpoint ages since the latest eligible trade, quote event, and observed valid midpoint change | Transaction, quote, and price-update freshness |
| Observation quality | Trade/quote source acceptance, continuity/reset information, halt status; midpoint-age status and observation origin/lower bound | Distinguish missing observations, valid zeros, stale states, and interruptions |

Discovery timing/provenance and constant source information belong in associated member metadata, with eligibility accessible to queries. Bid/ask/midpoint TWAPs share support so their arithmetic identities hold. Spread has its own documented validity rule: mean percentage spread cannot generally be recovered from mean bid/ask prices. No valid duration means unavailable, not zero.

Displayed sizes must be normalized to shares using verified source units and retain the source's exact best-quote size semantics. Size validity is explicit rather than inferred from price validity. These measurements describe displayed best-quote liquidity, not full order-book depth, queue position, hidden liquidity, or executable capacity. Endpoint size imbalance can be derived from the stored sizes, but is not an additional headline MVP dimension.

Midpoint-change age distinguishes known age, continuously observed no-change with a lower bound, and unobservable state. Current ages remain unsmoothed. Keep essential observation quality; regenerate horizon-specific maturity/support diagnostics instead of retaining every legacy diagnostic in the base table.

Five-second returns and any future alternative lags are derived from endpoint midpoint observations with explicit support across the required history. Separate bid/ask TWAPs, endpoint quotes, exact shares, and displayed-size accumulators require raw replay; the current compact release does not contain all selected columns. One-second summaries cannot recover subsecond paths, order-book depth, trade–quote interactions, or different event-eligibility populations.

## Main queryable dimensions

| Dimension | Selected queryable features | Frozen construction |
|---|---|---|
| Volatility | EW five-second endpoint-midpoint volatility, in bps | Square five-second log returns observed every second, take their EW mean, then square root |
| Movement participation | Concentration/participation of those same return magnitudes | Squared EW mean absolute return divided by EW mean squared return, using identical weights and support |
| Quoted friction | Mean full quoted spread in bps | Decay spread integrals and supported durations, then divide |
| Transaction activity | Trades/s, shares/s, dollars/s | Decay per-second totals and supported exposure, then divide |
| Displayed liquidity | Separate mean displayed bid and ask sizes, in shares | Decay size integrals and their supported durations, then divide |
| Volatility relative to spread | EW five-second volatility / compatible EW mean full spread | Divide the current estimates; no additional smoothing of the ratio |
| Freshness | Exact current trade/quote/midpoint-change ages; ordinary trailing-window age p90s | Current ages are unsmoothed; p90s use unweighted supported endpoint ages, with no EW smoothing |

### Endpoint volatility and exponential weighting

Use the endpoint midpoint rather than the one-second midpoint TWAP for headline returns. This choice adopts the conventional sampled-price input without requiring an endpoint-versus-TWAP comparison experiment. TWAP remains useful base data, but neither empirical equivalence nor endpoint superiority is claimed. Midpoint sampling at five seconds is not a formal microstructure-noise correction; the MVP measures observed midpoint variation, not an unbiased estimate of latent efficient-price volatility.

Let `m(t)` be the finite positive midpoint of the prevailing reconstructed quote state using events strictly before `t`, when that state is valid. An invalid prevailing state is unavailable; do not skip it to recover an older valid quote. At each one-second row endpoint, calculate the supported return

\[
r_t=10{,}000\log\frac{m(t)}{m(t-5s)}.
\]

Return support requires valid prevailing midpoints at both endpoints and no halt or declared source-continuity break across the lag. Brief invalid quote states between valid endpoints do not by themselves invalidate the return or reset EW history. An invalid sampled endpoint makes returns using that endpoint unavailable; never substitute an older valid quote. Quote exposure and midpoint-change age retain their own validity rules. This endpoint-return clarification was accepted on 2026-09-14.

The return lag is **five seconds**, and publication/update cadence is **one second**. These returns overlap. One jump can affect several return observations; counts are not independent sample counts. Do not substitute disjoint five-second blocks, TWAP returns, or a long price EMA in this definition.

For a selected decay half-life `h`, let `w_u` be weights proportional to `2^{-(t-u)/h}`, normalized over the supported return history admitted by the initialization/reset contract. Define

\[
Q_t=\sum_u w_u r_u^2,\qquad
A_t=\sum_u w_u|r_u|,\qquad
\sigma_{5,t}=\sqrt{Q_t}.
\]

Square the returns before weighting; do not subtract their local mean. The headline volatility is an **EW RMS five-second return in bps**, not a disjoint-return realized-variance sum, an annualized estimate, a one-minute-equivalent scale, or a forecast. For an expanding history without truncation, a bounded implementation can decay an unnormalized squared-return sum and its weight sum by `lambda = 2^(-1/h)` each second, add the new supported squared return and its unit weight, then divide. Startup and unsupported observations require explicit weight/support accounting rather than silently seeding unavailable history with zeros.

EW weighting is selected for volatility, spread, rates, and displayed-size averages. Newer observations receive more weight regardless of their magnitude. Large returns matter more because they are squared, separately from recency weighting. Half-life means an observation's weight halves after that elapsed time; it is not a hard window length.

The two default EW views are **fast: 30-second half-life** and **slow: 120-second half-life**. Apply the view's half-life to squared and absolute return moments, spread, trade/share/dollar rates, and displayed bid/ask sizes. Participation and volatility/spread are derived from those corresponding estimates without additional smoothing. The five-second return lag and one-second update cadence remain unchanged.

These defaults favor somewhat smoother recent-condition measurements than 20s/100s half-lives; they are design choices, not empirically optimized settings. With mature, fully observed, untruncated EW history, average observation ages are approximately 43s and 173s, compared with approximately 30s and 150s for the legacy 60s/300s rolling windows. Older observations retain diminishing weight. Common half-lives align recency weighting, not the response speed of every derived feature: after nonzero returns cease entering a mature fully observed history, RMS decays approximately with a 2h half-life because it is the square root of the squared-return mean. Freshness p90s retain their separate 60s/300s fixed windows.

For spread, rates, and displayed sizes, apply each second's decay weight to both its numerator and supported duration before dividing. Do not EW-average per-second ratios with unequal exposure. Valid zero-trade seconds contribute zero totals and positive supported time. Invalid time contributes no fabricated zero observation, but elapsed time still ages existing state. Quote-only and trade-only features retain their own support; do not impose one universal complete-case mask.

### Derived participation and volatility/spread

\[
P_t=\frac{A_t^2}{Q_t},\qquad X_t=\frac{\sigma_{5,t}}{S_t},
\]

where `S_t` is the current EW mean full quoted spread in bps at the same half-life. Volatility and participation use exactly the same supported returns and weights. Participation describes concentration of magnitudes in that weighted history, not price-path structure, direction, an active-time fraction, or independent information count. For supported all-zero returns, volatility is zero and participation is null. Do not add an epsilon to define it. Mean absolute movement `A_t` remains an internal sufficient statistic, **not a headline query dimension**; `P_t = A_t^2/Q_t` does not add an independent third movement moment.

The volatility/spread ratio is defined only when both components are available and spread is strictly positive. It compares five-second return scale with full quoted friction; it does not measure a capturable move or expectancy. The same half-life makes the recency settings compatible, but return and spread support remain separately disclosed. Apply **no additional EW smoothing** to participation or volatility/spread, and do not average historical ratios. The new volatility/spread field must not reuse the legacy mean-absolute-movement/spread identity or thresholds as if their meanings were unchanged.

### Data coverage and feature validity

Use **data coverage** in reader-facing descriptions for how much required history has usable observations; this is the quantity called support in the equations above. **Feature validity** is the binary decision that a feature meets its data, startup-history, and mathematical requirements and can be published. Coverage and validity are distinct: a fully observed all-zero return history has valid zero RMS but undefined participation.

Usable data come from an accepted source, satisfy the measurement's value/eligibility rules, and meet its continuity requirements. Activity requires reliably observed time and eligible trades; an observed second without trades is a valid zero. Returns require finite positive valid endpoint midpoints and the required continuity across the lag. Spread and displayed sizes require quote time satisfying their respective validity rules. Exact age percentiles require known endpoint ages under the event-observation contract. Missing data must not become observed inactivity.

Track coverage separately for each feature and distinguish insufficient startup history from missing observations. For EW features, specify usable weight/exposure relative to possible wall-clock weight/exposure and the minimum requirements for publication. Decaying numerator and denominator through missing observations does not by itself lower their ratio; explicit coverage and current-availability rules determine when a retained estimate is unavailable. Default coverage thresholds and the interruption policy are selected below and encoded by the completed Phase 0 contract package; production replay must implement those rules without reopening them.

### Accepted availability and robustness defaults

The following choices were accepted on 2026-09-14 and are specified in [Phase 0](phase_0.md). They are encoded in the installed contract package; production replay/EW calculation is still pending.

- Initial/post-halt startup: 60s for the fast EW view and 300s for the slow view, separate from the 30s/120s half-lives. Normalize untruncated EW state over actual supported observations. Default coverage is 90% for spread and 80% for other EW families, represented as explicit versioned configuration. Measure exclusion and recovery on representative data before any later threshold revision.
- Preserve and decay EW state through known bounded feed gaps; advance possible wall-clock exposure and remove unusable support. Suppress affected publication during the declared outage, invalidate cross-gap returns and restart affected event-age origins. On source recovery, sufficient historical coverage can restore EW publication without another full startup. Retain session/halt resets and the separately specified age-window reset policy.
- A historical summary need not have a usable newest numerical observation when its own coverage and source requirements are met. Crossing a coverage threshold must not erase estimator state. Current measurements retain separate validity.
- Quote-event age measures time since a structurally accepted quote message with trustworthy timing, independently of price/size validity. Midpoint-change age retains continuous valid-price observation requirements.
- Otherwise valid numeric locked quotes (positive bid equals ask) contribute zero full spread with positive supported duration. Crossed/nonfirm states remain invalid; contradictory lock flags are explicitly classified rather than coerced to zero. RMS/spread remains undefined for zero mean spread.
- Localize malformed or unfamiliar record effects where their semantic consequences can be bounded. Preserve trustworthy raw records and unaffected measurements. Unknown trade eligibility must not become observed zero activity, and an unknown quote state must not reveal an older valid quote. Identity errors, untrustworthy clocks and unbounded semantic ambiguity remain explicit failures for affected dependencies.

### Eligible current activity and trade reporting age

Retain the existing maximum reporting age of **one second**, inclusive: an otherwise eligible trade requires `0 <= sip_timestamp - participant_timestamp <= 1_000_000_000 ns`. This measures execution/report age at the SIP, not age at our output endpoint or the user's local receipt time. Preserve existing condition and correction eligibility; correction/cancel action records are not new executions.

Eligible trades enter the second containing their SIP timestamp, contributing count, shares and dollars together. Older reports remain in raw source data but contribute no current activity and do not reset eligible-trade age. They are never backdated into an execution-time bucket. A reliably observed second with only known ineligible/late reports is valid zero eligible activity; unknown eligibility or missing source observation is not. The maximum age belongs in the versioned configuration, and operational exclusion accounting must distinguish these cases.

Acquisition preserves timestamped reports; replay performs the semantic/timeliness filtering. Historical SIP-clock replay can therefore include late reports in its raw input while the selected activity features count only timely eligible reports. It does not establish exact historical delivery to a particular client.

### Freshness and numerical contract

Keep exact current ages and ordinary unweighted trailing-window p90s as separate readings. Use the existing 60s/300s fixed-history freshness views; these are window lengths, not EW half-lives. The p90 is the ordinary linear-interpolated percentile of supported endpoint ages, not a percentile of event-to-event gaps, a weighted quantile, or an EW average of successive p90s. Midpoint-age lower bounds do not enter exact-age percentiles. Retain explicit maturity and support requirements, and the distinction between no observed change and unobservable history.

Retain all three age p90s: trade age captures time spent without an eligible trade, quote age captures time without a quote refresh, and midpoint-change age captures time without an observed midpoint change even when quotes refresh. Their purpose is to reveal upper-tail inactivity that average transaction rates can conceal. They do not measure the longest gap, and gaps occupying less than roughly 10% of the sampled window may not be reflected in p90. No additional no-midpoint-change-duration query feature is required.

The completed Phase 0 package encodes initialization and normalization, untruncated history, reset behavior, coverage/startup defaults, numerical representation, and the source-unit admission contract. Actual source-member units, precision, observation coverage, and knowledge-time evidence remain admission checks during transfer and replay; they are not unresolved feature mathematics. Production replay and feature calculation must preserve the accepted clock and transition rules: unsupported elapsed time never disappears, returns never bridge declared continuity breaks, and halt/reset boundaries never become observed quiet time. The selected endpoint input, five-second lag, 30s/120s default EW half-lives, EW feature families, and fixed-window freshness construction are closed decisions.

### MVP exclusions

Exclude price-path structure, trend/reversal/coherence features, multi-day relative activity baselines, detailed trade composition (including TRF fractions and classified-share coverage), signed trade flow/order-flow imbalance, effective-spread and other trade–quote interaction features, new subsecond path/change-count accumulators, and formal microstructure-noise models. These can motivate later versioned extensions through the replay engine; they are not required to complete this rewrite. Their exclusion is a scope choice, not evidence that they lack research value or trading edge. Do not add an endpoint-versus-TWAP experiment, estimator tournament, predictive-edge study, or formal noise-correction implementation as an MVP acceptance gate.

## Research architecture and completion criteria

Use three layers: immutable raw T/Q → versioned one-second base table → versioned queryable features. The selected compute environment is an OVHcloud VPS with user-reported 8 vCPUs, 24 GB RAM, a 200 GB SSD, and Ubuntu 26.04. Verify the actual environment and free resources before sizing production jobs; set explicit server memory, concurrency, and disk limits from representative measurements. The separate development-machine limits below remain unchanged. Persist the working base/feature tables on the VPS so ordinary feature queries and report calculations use local data without repeated R2 staging. Keep transfer and archive operations separate from calculation and retrieval. R2 remains durable storage. Users provide their own authorized data/access. Database engine, physical partitions, and serving interface follow measured workloads.

Decide full raw-T/Q retention on the VPS from a current object inventory and a measured total disk budget covering raw inputs, new base/features, report outputs, temporary build/query space, system use, and growth. Transfer directly from R2 to the VPS with resumable, identity-verified copies; copying data does not require deleting the R2 archive. Do not mirror every historical derived release merely to run the new product. Early raw staging may precede production calculators after a measured representative transfer, refreshed inventory, conservative disk reserve and explicit bulk-transfer confirmation under the storage and migration plan. A measured representative production build and complete capacity projection remain required before the full external calculation run.

### Repository report and research evidence

The repository-root README is the final research report and product showcase, not a general codebase README or installation landing page. It must explain the research problem and acquired population, introduce the feature mathematics and interpretations, present recalculated marginal and joint distributions under the new feature identity, and demonstrate a reproducible feature-rule query with inspected tape examples. Include coverage, symbol/date concentration, limitations, and links to detailed methodology, architecture, and reproduction instructions. Readers should learn what the measurements reveal and how to use them, not only how to install the software.

Regenerate the relevant existing distribution analyses for the new endpoint/EW features, with explicit populations, activity filters, availability denominators, and separate half-life/freshness-window labels. Preserve historical results under their legacy identity until replacement results have been computed and verified; old charts are not evidence for new-feature behavior. The report must distinguish descriptive usefulness from predictive or executable trading evidence. Detailed operational documentation belongs in supporting docs rather than displacing the report at the repository root.

### Calculation and validation

Replay preserves tested event eligibility, ordering, and causal alignment. Compute requested quote accumulators together in one sequential pass and trade accumulators in another; coordinate streams only when a measurement needs trade–quote alignment. Researchers can add an accumulator without rewriting acquisition, timing, validation, or output handling. Reuse correct existing code; a new schema does not require a numerical-engine rewrite.

The MVP quote pass produces endpoint prices/sizes, TWAP prices, spread and size integrals with their own support, and exact freshness/quality state. The trade pass produces eligible counts, shares, dollars, and freshness. No new trade–quote join is required by the selected dimensions. The feature layer derives five-second endpoint returns, EW moments and exposure-weighted summaries, participation, volatility/spread, and fixed-window freshness p90s from the base table. Changing supported feature half-lives or freshness windows must not require raw replay. Carry resumable estimator state or reconstruct the necessary prefix so partition/query boundaries do not silently restart EW history.

Preserve legacy data/feature identities and historical report meanings. Publish the rewrite under a distinct schema/configuration identity covering price input, return lag/cadence, decay, units, support, initialization, resets, quantile definitions, source size semantics, and all transitive calculation dependencies. Query metadata must distinguish EW half-life from fixed freshness-window length, expose component availability, and support all selected dimensions through the installed package/CLI rather than only an ad hoc notebook or cloud table. Do not impose unrequested feature availability on a query.

Jobs must use bounded batches/state and resumable per-member outputs, never whole-corpus materialization. Every detailed data-job specification must state time/memory complexity, largest allocations, cache/spill/disk limits, and peak process-tree RSS acceptance. Measure the exact path on a representative session-start sample and obtain the required confirmation before full external runs. Retain input identities and small completion records; do not require copying the entire old release before building the new table.

Expected construction is linear in streamed events/seconds for a fixed number of feature views, using per-stream quote/trade state, a fixed five-second return history, constant-size EW accumulators per view, and bounded freshness histories/quantile structures. Exact p90 processing must have an explicit bounded update algorithm and complexity; do not repeatedly materialize growing histories. Projected batches default to 4,096 rows and may not exceed 25,000. Target at most approximately 2 GiB process-tree RSS and terminate before 3 GiB on the development machine, with live monitoring. Representative acceptance records must include rows, elapsed time, peak RSS, and full-run memory/transfer/disk projections before the existing full external-run confirmation checkpoint.

The revision is complete when a researcher can reproduce a selection through the installed Python/CLI interface; inspect units, timing, validity and lineage; add and validate a base measurement through bounded replay; and develop feature variations without replaying T/Q unnecessarily. Acceptance must include:

- Independent numerical reconstruction on small fixtures of endpoint returns, EW squared and absolute moments, exposure-weighted spread/rates/sizes, participation, volatility/spread, and ordinary freshness p90s. Verify strict timestamp boundaries, overlap, startup, valid zeros, missing support, halt/reset behavior, unequal exposure, and identical results across batch/resume boundaries.
- Historical reconstruction under the new identity, with legacy report reconciliation explicitly separated from new-feature results. Changed volatility/participation/ratio definitions are not expected to reproduce legacy movement values. Verify unchanged legacy fields under their existing contract; do not relabel old empirical evidence as new-feature validation.
- Reproducible queries across the selected dimensions, with availability, zero-match members, timing, source/configuration lineage, and symbol/date contributions. Numeric parameter selection and memory-safe exact freshness processing must be specified before acceptance runs.
- A root research report with independently checked numerical tables and regenerated distribution figures for the new features, plus a reproducible query/tape example. State the population, feature availability, configuration identities, and limits of each result.
- Measured cold/warm query latency, bytes read, build throughput, disk use, and peak process-tree RSS. Query performance targets must be set in the delivery specification before acceptance runs, with bounded local caches and cloud spill/disk limits.

Completion establishes measurement correctness, reproducible retrieval, extensibility, and measured resource behavior. It does not require demonstrating endpoint superiority over TWAP, persistent regimes, predictive edge, or executable expectancy. Cloud deployment alone is not completion.
