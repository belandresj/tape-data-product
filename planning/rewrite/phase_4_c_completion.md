# Phase 4 checkpoint C — implementation evidence

Status: independent reducers and synthetic rendering are implemented; real-pilot integration and checkpoint acceptance remain pending checkpoint A.

## Implemented behavior

Marginals use each field's own reason mask. The exact table coalesces equal values after an external DuckDB sort and reports `F(x)=count(X<=x)/N_valid`. It preserves valid zero, emits an empty typed table for an empty valid population, and reports selected, valid, unavailable, zero, contributing-member, zero-valid-member, overlapping reason-bit, and quantile accounting. Equal-symbol-day sensitivity is the mean of member ECDFs: an observation in member m has weight `1/(M*n_m)`.

Joint histograms use exactly pair validity. They retain pooled counts in fixed resident arrays and disk-backed SQLite member cells for equal-symbol-day weights based on pair-valid `n_m`. Outputs include single-field and pair denominators, zero atoms, explicit positive underflow/finite/overflow bins, omitted members, and maximum member share plus observation-share Herfindahl concentration. No clipping or winsorization occurs.

Rendering consumes reconciled tables rather than selecting data. Captions state scope, population, estimator half-life/window, units, weighting, availability, zero mass, lineage, and concentration. ECDF display reduction preserves both sides of material jumps and bounds cumulative-probability displacement to 0.01 percentage point; numerical tables remain exact.

## Verification

Source baseline: `52b5b3502c9567b120a92316f47a8642a718e11c`. Checkpoint A was still uncommitted, so no A code was copied or used. Synthetic cases independently cover ties split across batches, zeros, nulls, empty marginals, pair-specific missingness, exact bin edges and tails, zero-valid members, and a deliberate pooled/equal-member difference. Focused tests: 4 passed in 2.05 seconds wall time and 282,312 KiB maximum RSS, including pytest, DuckDB, Arrow, and Matplotlib. Full pre-review suite: 330 passed in 97.81 seconds.

Two generated synthetic figures were visually inspected. An initial footer collision was found and fixed; the figures were re-rendered and checked for legible axes, labels, legends, denominators, zero mass, tails, and unclipped lineage.

Independent read-only review found one high-severity ECDF-decimation problem and medium findings concerning reason accounting, concentration disclosure, and zero-row members. The implementation now uses mass-error-bounded ECDF reduction, reports overlapping reason bits, renders concentration, and accepts authoritative expected members. Source-family exclusion summaries still require A's registry mapping.

## Resource design and pending acceptance

Exact ECDF work is one field at a time: O(N log N) external sort, 256 MiB DuckDB memory, one thread, and at most 1 GiB spill for the pilot. Joint work is O(N) with resident O(batch + bins) state and disk-backed O(nonempty member-bin cells) state. The pilot scratch ceiling is intentionally not claimed adequate for the 299,980,800-row full population; Phase 5 must set a measured explicit preflight and scratch budget.

Pending dependency: merge A's reviewed commit, consume its actual reader/registry/selection interfaces, add source-family exclusion categories, orchestrate all 27 RTH historical-membership marginals and the required joint panels, run only the fixed 24-member immutable pilot after A acceptance and when no competing data job is active, reconcile plot/table totals, measure I/O/RSS/spill, visually inspect every pilot figure, independently review the integration, and verify the integrated wheel. No 5,208-member analysis was run. No real-data finding or checkpoint-C acceptance is claimed.
