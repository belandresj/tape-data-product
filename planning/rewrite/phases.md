# Product rewrite: master phase plan

**Status: master plan established on 2026-09-14; Phases 0 and 1 complete within their contract and bounded-transport scopes; Phases 2–5 remain pending.** This document turns the agreed [product direction](README.md) into ordered deliverables for the rewrite. The new production features, full migration and replacement research report remain unimplemented.

## 1. Product and repository destination

Build a research database of one-second equity tape measurements, with an installed Python/CLI interface for reproducible feature-rule queries and a reusable raw trade/quote replay engine. Run builds, repeated queries, and report calculations against persistent local data on the OVHcloud VPS. R2 remains the durable archive; routine feature queries must not stage objects from R2.

**The repository-root README is the final research report and product showcase, not a general codebase README or installation landing page.** It must teach an interviewer or researcher what the measurements mean, show recalculated distributions and relationships, and demonstrate the product in use. Installation, package navigation, operating procedures, and detailed engineering documentation belong in supporting docs. A short reproduction link in the report is appropriate; setup instructions must not replace the research narrative.

The final report must cover:

1. The research problem, acquisition screen, observed population, and coverage.
2. Feature definitions, units, concise mathematics, real-time timing, and strong/weak examples.
3. Recalculated marginal distributions and joint relationships for the new features.
4. A reproducible rule-based selection, matching observations/runs, and inspected tape examples.
5. Findings, symbol/date concentration, selection effects, and what remains unproven.
6. Links to reproduction, detailed methodology, architecture, and measured validation.

Keep historical charts and results attributed to their existing feature identity until verified replacements exist. Recomputing a figure under a new definition is new research evidence, not reproduction of the old numerical result. Descriptive distributions do not establish predictive power or executable expectancy.

## 2. Authority, project root, and specification workflow

- This repository, `tape-data-product`, is the rewrite root. New Codex tasks and worktrees should start here; the VPS should use a checkout of this same Git repository.
- The sibling `tape-characterization` repository is available for historical reference and migration of private operational records. It must not become an import, runtime, or test dependency. The sibling `capitulation` repository is not a product dependency.
- The [product direction](README.md) owns selected product behavior and equations. This master plan owns phase order and completion requirements. Each phase implementation spec supplies exact interfaces, algorithms, tests, and resource budgets without silently changing those decisions.
- The [implemented V1 contract](../../docs/reference/v1/feature-contract.md) and [existing query contract](../../docs/reference/v1/query-contract.md) continue to govern legacy identities. Historical eighteen-field meanings, masks, timing, and reports must not be relabeled as new-feature outputs.
- Write the next phase's implementation spec after inspecting the current integrated code. Do not write every detailed phase spec upfront. Settle shared schemas and interfaces early, then incorporate measured findings into downstream specifications.
- Track the master plan, phase specifications, accepted decisions and concise completion records in `planning/rewrite/`. Keep credentials, private catalogs, detailed market rows, temporary handoffs, raw operational logs and machine snapshots in ignored `local_docs/` or `private/`, outside the installed package.
- Before parallel implementation, preserve a reviewed Git baseline containing the applicable plans and instructions. Keep private evidence snapshots separately. Start each worktree from the reviewed baseline; do not assume a clone contains uncommitted local decisions, credentials or source receipts.

## 3. Decisions already selected

The equations and full selected base-column requirements are in the product direction; this table is a navigation summary, not a second mathematical contract.

| Area | Selected behavior |
|---|---|
| Resolution and price input | One-second rows; strictly prior endpoint midpoint; five-second log returns sampled every second, with overlap disclosed |
| EW defaults | Fast half-life 30s; slow half-life 120s; common recency settings for moments, spread, transaction rates, and displayed sizes |
| Movement | RMS five-second return and magnitude participation; mean absolute movement remains an internal statistic |
| Ratios | Current RMS divided by corresponding mean full quoted spread; no additional smoothing of participation or RMS/spread |
| Freshness | Current trade/quote/midpoint-change ages unsmoothed; all three ordinary age p90s retain 60s/300s rolling windows |
| Coverage terminology | Data coverage measures usable history; feature validity is the binary publication decision; observed zero is different from missing data |
| Queries | Rules on the selected features, such as RMS/spread > 2; strict matches/runs by default, with separately defined optional confirmation/continuation |
| Extensibility | Raw T/Q → persisted one-second base → versioned features; changing a supported half-life does not require raw replay |
| Research scope | Recalculate distributions and demonstrate retrieval; no predictive-edge, endpoint-versus-TWAP, or estimator-tournament acceptance gate |
| Presentation | Root README is the final report/showcase, supported by detailed methodology and reproducibility docs |

Do not add new mean-absolute-movement query requirements, no-midpoint-change-duration searches, path/direction features, or a new query UI as prerequisites to completing the agreed rewrite. Preserve observation-origin/lower-bound metadata already required by freshness semantics.

## 4. Architecture and migration boundaries

The package should expose clear responsibilities: acquisition, storage/integrity, replay, feature calculation, query/export, and report analysis. Centralize versioned schemas and defaults. Public APIs should not require users to understand historical experiment names. Exact filenames and package moves belong in the relevant implementation spec, not an upfront bulk rename.

Current reusable assets include bounded event decoding, eligibility/timing behavior, freshness state, integrity checks, release membership accounting, strict-run reduction, report reducers, and installed CLI/testing infrastructure. The current feature API still builds legacy compact outputs directly from raw data. The independent base layer, expanded quote/size columns, endpoint RMS/EW construction, and new feature registry remain implementation work.

Historically named modules are still transitive dependencies of current calculation and verification code. Extract shared behavior with regression evidence before isolating legacy implementation. Preserve numerical meaning while issuing new source/run identities when material code or layout changes. Do not replace tested event semantics simply to obtain cleaner filenames.

### VPS and data placement

The user selected OVHcloud with **8 vCPUs, 24 GB RAM, 200 GB SSD, Ubuntu 26.04**. Phase 1 verified provisioning, key-based access and isolated Python 3.13 installation. See [Phase 1 completion](phase_1_completion.md) for the executed environment and sample limits; full corpus capacity remains unproven.

Keep code separate from persistent raw/base/feature datasets, control records, report outputs, and bounded temporary storage. Prefer retaining the complete canonical raw T/Q copy locally if the measured total disk budget permits. Leave unnecessary historical derived releases in R2. Raw retention is a capacity decision, not a requirement that every bucket object be mirrored.

The 2026-09-14 read-only listing found approximately 43.7 GB of canonical T/Q, 38.6 GB under the historical tape-product prefix, and 53.6 GB under the older tape-feature prefix (decimal GB). Canonical T/Q had 7,085 symbol-date pairs with both object names present. These are planning snapshots of object sizes/presence, not verified source coverage or content identities; refresh and reconcile the selected inventory before migration. Do not equate every object within the historical date range with the exact accepted report membership.

Transfer directly from R2 to the VPS using resumable, identity-verified copies. Include selected-member, discovery, reference-population, halt/continuity, and source-lineage records; Parquet filenames alone are insufficient. Verify coverage evidence against the actual available receipts and manifests. Do not fabricate missing provenance to satisfy a newer admission schema. Preserve R2 objects; migration does not imply remote deletion or credentials in Git.

## 5. Phases and acceptance

| Phase | Main deliverable | Dependency |
|---|---|---|
| 0 | Exact contract, interfaces, and preserved baseline | Current product direction and code review |
| 1 | Reproducible VPS environment and verified small transfer | Can overlap Phase 0; full transfer deferred |
| 2 | Raw replay to independently verified one-second base | Phase 0; VPS measurements need Phase 1 |
| 3 | Base to new features, independently reconstructed | Accepted base schema and Phase 2 |
| 4 | Complete small-data query and report workflow | Accepted feature schema and Phase 3 |
| 5 | Measured corpus build, queries, and final research report | Phases 1–4 and required external-run confirmation |

### Phase 0 — Finish the contract and preserve the baseline

Phase 0 is complete; see the [specification](phase_0.md) and [completion evidence](phase_0_completion.md). Exact contract definitions and validation helpers are installed; production replay/features remain Phases 2/3. The user accepted robustness-review items 1–5 and retained the one-second activity reporting-age cap; Phase 0 owns those contracts, with runtime implementation in the applicable later phases.

Resolve initialization/normalization, finite-history truncation or untruncated state, minimum coverage and startup history, current availability, interruption/reset behavior, and displayed-size units/validity. Specify session boundaries and continuity across reporting strata, quote initialization, lag support including within-second invalidity, and discovery/halt information-clock treatment. Reuse existing semantics where intended; make every intentional change explicit.

Define base and feature schema identities, validity/reason representation, checkpoint state, public build interfaces, and package boundaries. Choose the exact bounded p90 algorithm. Inspect existing source and test dependencies, capture the current revision, and run the appropriate bounded baseline verification without claiming historical regeneration.

**Accept when:** no implementation agent must invent a mathematical availability rule; schemas and dependencies are explicit; legacy preservation checks and the resource strategy are specified. Record unresolved product choices for the user rather than silently selecting them during coding.

### Phase 1 — Prepare the VPS and prove data access

Complete for environment and bounded transfer scope; see the [specification](phase_1.md) and [completion record](phase_1_completion.md). Historical source-admission gaps and corpus budgets remain later obligations.

Verify the server environment and install the package reproducibly, including its supported Python version rather than assuming the OS default is compatible. Configure access and secrets outside Git, persistent data directories, capped scratch space, and live process-tree monitoring. Inventory selected source/control records and compare them with available R2 objects.

Use an explicit small representative transfer set to validate direct download, resumability, content identity, and available coverage metadata. Measure bytes, elapsed time, throughput, temporary disk, and peak RSS. Run the installed offline demonstration on the server. Set preliminary server budgets and the transfer concurrency limit; raw listings alone do not justify a corpus copy.

**Accept when:** the server runs the installed product and reads verified local inputs, with documented environment and measured sample transfer behavior. This phase does not claim the new feature product or authorize full migration.

### Phase 2 — Build reusable replay and the one-second base

Extract shared timing, event eligibility, quote-state, freshness, and interruption behavior. Implement one quote pass and one trade pass producing the selected base measurements. Add endpoint bid/ask and sizes, separate price TWAPs, exact share totals, and displayed-size integrals/durations with their independent validity. No trade–quote join is required by this feature set.

Persist versioned base partitions and completion records through the installed interface. Demonstrate adding a small accumulator without replacing the replay machinery. Check event ordering, exact timestamps, unequal exposure, zero activity, invalid/recovered states, and halt/reset boundaries using small independently calculated fixtures.

**Accept when:** base values and validity reconstruct independently, required semantic events survive aggregation, and batching/resumption preserves output. Measure the exact production path on a bounded representative external prefix before projecting corpus output size or resources.

### Phase 3 — Calculate the new feature views

Implement endpoint five-second returns, EW squared/absolute moments, exposure-weighted spread/rates/sizes, participation, RMS/spread, and ordinary freshness p90s from the base dataset. Apply the Phase 0 coverage/startup/reset rules. Persist estimator/configuration identities and sufficient checkpoint state or reconstruct the required prefix across partition/query boundaries.

Use fixtures with steady movement, an isolated jump, burst-then-quiet activity, unequal valid durations, missing data, and all-zero returns. These verify response mechanics and mathematical correctness; they do not optimize half-lives against future outcomes.

**Accept when:** independent reconstruction verifies numbers, nulls, and validity; 30s/120s EW and 60s/300s freshness metadata are unambiguous; changing half-life needs no raw access; restart/batch boundaries do not change results. Legacy definitions remain separately identifiable and verified.

### Phase 4 — Connect retrieval and research outputs

Extend the feature registry, validated readers, query rules, strict-run defaults, session/date/population selection, and exports for the new identity. Preserve unavailable-versus-zero-match accounting and symbol/date contributions. Do not impose unrequested feature availability or let filtering discard boundaries needed for run reconstruction. Define the timestamp meaning of returned observations and intervals.

Adapt marginal/joint-distribution calculations and plotting to the new fields. Specify populations, activity filters, weighting, denominators, units, and figure lineage before examining corpus results. Retain the existing types of informative distribution comparisons where meaningful; old numerical cutoffs are not automatically calibrated to new movement definitions.

**Accept when:** the installed offline workflow goes from invented raw tape through base/features to a rule such as RMS/spread > 2, a reproducible export, a feature-history/tape inspection, and small-data report figures. Specify cold/warm query workloads, latency targets, and scan/spill budgets before full acceptance. A separate web application is not required.

### Phase 5 — Migrate, build, measure, and publish the report

Combine measured transfer/base/feature/report costs into the full selected-corpus projection. Account for system use, raw retention, base/features, catalogs, reports, temporary spill, rebuild overlap, and growth. Refresh membership and source identities. Present representative row counts, elapsed time, peak process-tree RSS, transfer/disk projections, and obtain the required full external-run confirmation.

Then transfer the selected corpus resumably and build versioned per-member outputs on the VPS. Completion must reconcile expected members, failures, exclusions, and valid zero-match members. Verify new historical features and keep legacy reconciliation separate. Measure actual throughput, peak memory/disk use, and cold/warm queries against persistent local tables.

Regenerate the numerical distributions and figures, inspect their rendering and accounting, select clearly attributed tape examples, and rewrite the root README as the final report/showcase. Link claims and figures to the accepted release/configuration and reproducible calculations. Report actual descriptive findings, including weak or concentrated relationships, rather than assuming the new features improve upon the old ones.

**Accept when:** the report and working query product use the same verified new release; resource/performance evidence is recorded; ordinary queries do not stage R2 data; reproduction works from the installed package without sibling checkouts. No automatic remote publication, data deletion, or profitability claim follows from completion.

## 6. Resource contract for every data-intensive phase

Each implementation spec must state time/memory complexity, largest in-memory objects, streaming/batching strategy, disk/cache/spill limits, restart behavior, and peak process-tree RSS acceptance. Use projected batches of 4,096 rows by default, never more than 25,000. Do not retain full raw symbol-days, duplicate Arrow/Pandas representations, or growing per-event arrays.

For a fixed number of views, target O(E + N·F) replay/feature work plus the declared exact-quantile cost, where E is input events, N is output seconds, and F is the fixed view/feature count. Memory must be bounded by batches, fixed lag/age histories, and estimator state, not E or corpus N. Quantile and report sorting algorithms must explicitly bound resident state and external spill; do not infer their complexity from the EW portion of the pipeline.

On the 8 GiB development machine, target at most approximately 2 GiB process-tree RSS and terminate before 3 GiB. On the VPS, establish explicit measured per-worker and aggregate limits, leaving OS/query/disk-cache headroom; 24 GB RAM is not permission for unbounded processing or eight simultaneous workers. Start data execution single-process and increase concurrency only through an explicit resource-validated plan.

Before full external work, measure the exact production path on a representative bounded session-start sample and inspect allocations for full-input scaling. Present measurements and projections before requesting the user's full-run confirmation. After a resource stop, inspect surviving work and fix the scaling failure before proposing a rerun. Full-run authorization does not imply deletion or public release of private market data.

## 7. Codex execution and per-phase specification

Use one main rewrite task to own integration and this plan. A separate VPS task can prepare Phase 1 while the main task resolves Phase 0. Use a reviewer for each phase's contract and implementation evidence. Give independent code-writing tasks separate worktrees and explicit module ownership; a worktree does not isolate shared VPS data or resource use.

Only one task should control external transfers/builds until aggregate concurrency has been measured and explicitly configured. Do not launch downstream agents against changing schemas. Report tooling preparation may overlap feature implementation after its inputs are fixed, but corpus figures must wait for accepted new-feature output.

For each phase: inspect current code → write/review its spec → implement → independently review calculations and integration → resolve findings → accept the phase and prepare the next one. Routine code fixes do not require repeated user approval. Product/mathematical changes and the full external-run checkpoint require the relevant user decision.

Each phase spec must include:

- Deliverable, exclusions, dependencies, and the exact baseline revision.
- Referenced product decisions and any unresolved choices that block implementation.
- Input/output schemas, clocks, validity, and public interfaces.
- Existing code to reuse, planned changes, and legacy compatibility checks.
- Algorithms, resource bounds, representative sample, and acceptance measurements.
- Independent examples/tests with expected results and restart/batch checks.
- Completion criteria and a concise evidence record distinguishing synthetic tests, external measurements, and research findings.

Do not mark a phase complete solely because code exists or tests pass. Record which acceptance criteria were actually met and which remain pending. Keep operational transcripts and detailed private evidence outside the public repo; publish concise sanitized methodology and validation evidence that helps a reviewer assess the finished product.
