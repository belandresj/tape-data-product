# Equity Tape Characterization — Technical Supplement

Supporting definitions, observation-status counts, and figure verification for the [main report](../README.md). Section and figure numbers refer to that report. The empirical report assets were imported from the completed research analysis; this standalone checkout did not rerun the six-month dataset calculations.

## Feature Contract and Eligibility

The [Tape Data Product V1 feature contract](../docs/reference/v1/feature-contract.md) provides exact equations, source clocks, support requirements, reset behavior, and zero/null semantics for the legacy product. The movement calculation retains 56 candidate overlapping changes at 60s and 296 at 300s under its defined window convention.

Midpoint observation status distinguishes exact known age, an observed period without a change that supplies only a lower bound, and unobservable time. The table below reports endpoint counts in the active trading population, separately for each horizon’s selection. Status is an endpoint observation; the horizon changes which endpoints are selected, not the definition of status.

| Selection | Session | Exact age known | No change observed (lower bound) | Unobservable | Rolling midpoint-age p90 eligible |
|---|---|---:|---:|---:|---:|
| 60s | Premarket | 14,996,285 | 72,656 | 35,050 | 15,019,867 |
| 60s | Regular hours | 29,653,328 | 23,805 | 4,594 | 29,655,140 |
| 60s | After-hours | 6,075,316 | 53,279 | 19,599 | 6,081,358 |
| 60s | All sessions | 50,724,929 | 149,740 | 59,243 | 50,756,365 |
| 300s | Premarket | 14,106,134 | 64,033 | 31,046 | 14,169,383 |
| 300s | Regular hours | 27,177,922 | 20,456 | 3,364 | 27,190,475 |
| 300s | After-hours | 5,645,515 | 44,306 | 17,406 | 5,689,428 |
| 300s | All sessions | 46,929,571 | 128,795 | 51,816 | 47,049,286 |

The first three count columns partition the selected endpoints. The final column is a separate rolling-history eligibility count, not a fourth status: it requires sufficient exact ages and mature history. Exact age at the current endpoint is neither equivalent to nor required by rolling-p90 availability. Lower bounds never enter the exact-age p90.

Across all 246,150,282 post-discovery endpoints, before the active trading selection, 242,730,857 have known exact age (98.61%), 1,310,270 have an observed no-change lower bound (0.53%), and 2,109,155 are unobservable (0.86%). These endpoint-status counts are identical across horizons before selection. Supporting status rows are present for every counted endpoint. The [observation-status counts](report_assets/midpoint_status.json) also provide the full post-discovery breakdown by session; [feature eligibility counts](report_assets/populations.json) report rolling-p90 availability and other panel denominators separately.

## Figure Sources and Verification

Plot percentages include valid observations outside the displayed axis ranges, including valid zeros. Participation is undefined for all-zero movement histories, so those histories are excluded from participation plots. Histories with no observed midpoint change provide an age lower bound, which is excluded from exact-age calculations. The first movement bin can combine zeros and small positive values in movement/spread and movement/activity plots.

Activity-band shares use observations with valid measurements for the plotted pair and trade rate; their baseline includes the undisplayed ≥100 trades/s band. In the illustrative comparison in Section 3.2, the reported movement/spread value is the average of the trailing ratios, rather than the ratio of the two displayed averages.

The [publication manifest](report_assets/publication_manifest.json) records source artifact identities and SHA-256 checksums for all seven figures in the main report and the supporting count files. The five ECDF and joint-distribution figures are copied unchanged from the completed March–August render; their saved export checks passed. The manifest includes those verification results and the ECDF display-error bounds. Export verification checks the rendered output; it is not an independent reconstruction of the underlying features.

The [analysis-selection coverage](report_assets/gate.json) supplies the retained-time percentages in Section 4. The [panel accounting](report_assets/populations.json) records feature and joint-population eligibility, and [contributor concentration](report_assets/concentration.json) records contribution counts, maximum shares, top-five shares, and the sum of squared contributor shares for each population. The figures and supporting counts are included in the report_assets directory alongside these documents for repository-based viewing.
