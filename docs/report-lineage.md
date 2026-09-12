# Report reproduction and figure lineage

The [root README](../README.md) is the historical research report. Its seven PNGs
remain imported historical outputs; the installed package has not regenerated
the six-month study. The local workflow regenerates numerical tables and seven
figures on invented data. That verifies implementation and accounting, not the
historical observations or an executable trading edge.

## Run the analysis stages

Follow [dataset-build.md](dataset-build.md) to produce a verified local release
and screening stages. Then run:

```sh
tape-product report aggregate --release /path/to/release \
  --screen /path/to/screen --output /path/to/new-analysis \
  --tape-config config/report_tape_gpus_cast.json
tape-product report verify --input /path/to/new-analysis
tape-product report render --aggregate /path/to/new-analysis --output /path/to/new-figures
tape-product report verify --input /path/to/new-figures
```

Paths inside the tape configuration are relative to the command working
directory. Replace the example T/Q and partition paths with explicitly staged,
verified local artifacts. No command scans R2 or reads another checkout.
For multiple dates, `--screen` accepts a JSON file containing
`{"screens": ["screen-2026-06-18", "screen-2026-06-19"]}`; these paths are relative
to that inventory file. Screening must cover exactly the selected symbol/date
members in the release. Reference members with no minute bars remain in the
population denominator. Missing, duplicate and unexpected release members are
errors. Omitting `--screen` explicitly leaves the population figure unavailable;
omitting `--tape-config` explicitly leaves the tape figure unavailable.

`--config` accepts an optional JSON object with `batch_size` (1–4,096), explicit
`axes`, and `expected_release_hash` for replay of an already accepted local
release. The default bin edges are the recovered historical report axes in
[report_axes.json](../config/report_axes.json). New package/runtime identities
produce new release and analysis identities. The imported historical release
hash is provenance, not a fabricated identity for a newly calculated release.

Outputs are immutable directories. `stage.json` binds input identities,
effective settings, installed implementation/dependencies and all final output
bytes. `numerical/manifest.json` additionally binds selection, units, axes,
feature definitions and exact numerical artifacts. Interrupted directories
remain evidence; choose a new output after diagnosing the failure.

## Figures and headline tables

All analysis code is in `tape_data_product.analysis`. `pipeline.aggregate_report`
reads accepted partitions; `pipeline.render_report` consumes saved intermediates.
The table below identifies the more specific numerical and rendering functions.
“Imported” refers to the existing historical report assets, never synthetic
outputs produced by a local demonstration.

| Report output | Numerical producer and required inputs | Saved numerical intermediate | Offline renderer and checks | Historical status |
|---|---|---|---|---|
| Population share and universe coverage table (§2) | `pipeline.population_from_screen`, indexed `reconcile_screen_membership`; verified screening denominator/selection/reference records and exact release membership | `population.json`, `headline_tables.json`, `screen_members.sqlite` | `rendering.render_population`; overall share is sum selected / sum reference, daily counts bounded and validated | Imported; historical reference membership and screen stages must be supplied |
| GPUS/CAST June 18 tape and comparison table (§3.2) | `tape.aggregate_tape`; canonical T/Q plus completed compact partitions, exact interval recipe below | `tape/tape.json` (all retained plot events and 300 count bars), `tape/table.json` (common and feature-specific means) | `tape.render_tape`; raw/stored trailing activity, endpoint M/S and exact print/bar count reconciliation | Imported; newly implemented bounded adapter; historical T/Q/partitions not staged or rerun |
| 60s ECDF grid (§4) | `aggregates.ReportPlotAggregates`, `ecdf.exact_table`; all release rows, 60s activity filter and each feature’s eligibility | `numerical/exact/60s/*.parquet`, `coverage/populations.json` | `rendering.render_ecdf`; tied atoms, zeros and session denominators verified; streamed reduction and exported SVG geometry checked | Imported; requires complete accepted historical release |
| 300s ECDF grid (§4) | Same numerical functions, independently evaluated 300s filter | `numerical/exact/300s/*.parquet` and eligibility counts | Same renderer, separate denominators and horizon | Imported; requires complete accepted historical release |
| Movement/spread/participation joint page (§5) | `aggregates.FixedJoint`; same-horizon activity filter intersected with exactly each plotted pair | `numerical/joints/{60,300}s/movement_{spread,participation}.{npz,json}` | `rendering.render_joint`; integer cell mass, zero atoms, tails, orientation, own-panel normalization | Imported; requires complete accepted historical release |
| Activity/movement/spread joint page (§5) | Same accumulator; movement–trade-rate baseline and movement–spread by four rate bands | `movement_trade_rate` and `movement_spread_rate_bands` arrays/notes | Same renderer; all four bands reconcile to baseline although ≥100/s is not displayed | Imported; requires complete accepted historical release |
| Activity/movement/participation joint page (§5) | Same accumulator, movement–participation by four rate bands | `movement_participation_rate_bands` arrays/notes | Same renderer; all-zero movement excluded only where participation is required | Imported; requires complete accepted historical release |
| Retained-time table (§4) | Horizon-specific `selection.transaction_gate`; every post-discovery endpoint including failures/unavailability | `numerical/coverage/gate.json`, `headline_tables.json` | `verification.verify_numerical`; pass + fail + unavailable reconciles to post-discovery time | Imported supporting counts; regeneration requires historical release |
| Observation-status table (supplement) | Explicit support-field status per endpoint; separate rolling-age eligibility | `numerical/coverage/midpoint_status.json`, `populations.json` | `verification.verify_numerical`; exact-known / no-change lower-bound / unobservable partition the supplied rows | Imported supporting counts; regeneration requires historical support files |
| Contributor concentration (supplement) | SQLite accumulation for every gate, feature, pair and rate-band population by symbol, date and symbol-day | `numerical/coverage/concentration.json`; retained `work/contributors.sqlite` | Integer endpoint counts, maximum/top-five share and sum of squared contributor shares | Imported supporting counts; regeneration requires historical release |

The original report publication manifest records historical release identity
`c9abea6e3faea62e2471e8b8b883494c612cc4d48b4e44e82d9a7b47dd9aae50`,
numerical identity `e64f6043cec4f70517db41fe8d63907a2436d013ce72e2872151d8f47e514812`,
6,222 members and 358,387,200 represented feature seconds. Existing supporting
JSON counts cannot recover missing exact ECDF atoms or joint cells. Supply the
complete accepted historical feature/support release and verified screening
records, or a hash-verified original numerical bundle, to regenerate historical
plots. Do not treat local synthetic output as replacement empirical evidence.

## GPUS/CAST recipe

[report_tape_gpus_cast.json](../config/report_tape_gpus_cast.json) preserves
GPUS on 2026-06-18 `[09:50:00,09:55:00)` ET and CAST on the same date
`[10:03:00,10:08:00)` ET. These examples were selected retrospectively. No
deterministic original selection rule is asserted. The recovered earlier
comparison generator also contained a different CAST interval; that earlier
interval is not substituted for the final report’s documented 10:03 example.

Prices are rebased by `10,000 × (price / first valid midpoint − 1)` for display.
Quotes use causal SIP states with the last stable sequence at a tied timestamp;
invalid quote states make gaps. Every eligible trade in `[start,end)` contributes
to the faint price points and the one-second bars. T/Q hashes must equal the
partition’s calculation inputs; the recipe also carries the original GPUS/CAST
T/Q hashes. The table contains interval trade count / 300, feature-specific means
and common-five-feature means. The common fields are movement, spread, endpoint
movement/spread, trade rate and dollar rate at 300s. An average endpoint M/S is
not replaced by the ratio of average movement to average spread.

Stored trailing-feature endpoints are `[start,end)` (300 endpoints), so their
history precedes the displayed tape. The first endpoint’s raw trade-rate check
uses `[start−300s,start)`; a trade stamped exactly at an endpoint belongs to the
following one-second row. Full historical reconstruction must initialize from
the session start and retain declared halts/continuity boundaries.

## Numerical and resource guarantees

The activity filter is independently `trade_rate_H ≥ 1/s` and
`trade_age_p90_H ≤ 2s`, requiring eligible inputs at that horizon. Each ECDF adds
only its own feature eligibility; each joint adds only its plotted pair.
Participation is undefined when total movement is zero. Midpoint-age lower
bounds never enter exact-age p90. The report does not reuse a query mask or
require all eighteen fields at once.

Exact ECDF tables external-sort one feature at a time and merge ties across
batch boundaries. They retain zeros, per-session integer counts and cumulative
counts. Display reduction scans the saved table twice with 4,096-row batches,
retains a bounded number of vertices and verifies both left and right limits
against every exact atom, with ≤0.05 percentage-point numerical reduction error.
SVG coordinate-rounding error is measured separately. Backend path
simplification is disabled. Valid mass outside displayed ranges remains in all
normalization; joint notes separately account for exact zero, positive
underflow, finite cells and overflow. Rate-band boundaries 10, 30 and 100/s are
left-inclusive in the next band.

Aggregation is O(NF + NP) for fixed-bin work plus O(FN log N) external sorting,
with O(NF) retained disk. Resident state consists of one projected batch,
fixed histogram arrays, an 8 MiB SQLite contributor cache and a 256 MB DuckDB
sort; the spill limit is 1 GB, one thread. Large inputs can exceed the explicit
spill quota and fail rather than expanding RAM. Rendering retains bounded
ECDF display data, fixed joint arrays and one canvas; membership display is
capped at 10,000 members and population at 10,000 dates. Tape extraction is
O(T+Q) through each slice end, with 600 fixed counters and a hard 250,000 display
events per stream per pair. It fails instead of silently subsampling events.

The final fresh-wheel demonstration ran outside the checkout with network and source-access guards, generating 1,440 feature endpoints, 21,600 trade rows, 1,442 quote rows and seven verified figures in 24.395 seconds at 300.44 MiB peak process-tree RSS (50 ms samples). See [verification](verification/README.md) for exact evidence. These are synthetic measurements; no full historical scan has been measured or accepted. Before any full external run, measure the exact production path on an accepted session-start sample and obtain the separate full-run confirmation.
