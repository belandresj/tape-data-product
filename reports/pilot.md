# Five-date participation pilot

Adding movement participation entry **≥0.50** and continuation **≥0.40** to the 300-second movement/activity/friction cohort retained **80.2% of baseline active time** and changed membership on each date with supply. It distinguishes movement concentration within this cohort; it does not demonstrate better future price paths or trading expectancy.

The shared query requires entry/continuation movement ≥10/8 bps, positive spread ≤100/125 bps, trade rate ≥10/8 per second, dollar rate ≥5,000/4,000 per second, and movement/spread ≥3.0/2.4. Entry and exit use five consecutive passes/failures. All variants use the same eligibility and boundary semantics.

| Participation entry / continuation | Active stock-seconds | Baseline retention | Windows | Sum of daily qualifying-symbol counts | Dates with supply |
|---|---:|---:|---:|---:|---:|
| None | 12,177 | 100.0% | 30 | 21 | 3 / 5 |
| 0.40 / 0.32 | 11,759 | 96.6% | 31 | 21 | 3 / 5 |
| **0.50 / 0.40** | **9,760** | **80.2%** | **23** | **17** | **3 / 5** |
| 0.60 / 0.48 | 3,079 | 25.3% | 8 | 7 | 2 / 5 |

The 235 searched symbol-days cover March 13, April 8, June 18, July 20 and August 4, 2026. March and July have zero matches in every variant. For March, April, July and August, dates were chosen by acquired symbol-day count closest to that month's median, earliest date breaking ties; June 18 was existing development data. The dates and tested thresholds are not independent predictive validation.

Participation was unavailable at 751,273 otherwise baseline-eligible decision seconds, but was available at all 12,177 baseline active endpoints. Observed membership differences therefore reflect thresholds and state timing rather than missing participation at baseline-active endpoints. The selected variant has 162.67 active stock-minutes over five dates, or 32.53 per date. Daily counts sum stock-days, not unique symbols across dates. Windows and overlapping endpoints are dependent observations.

Concentration remains substantial: the largest symbol contributed 41.5%, 26.2% and 42.6% of selected active time on April 8, June 18 and August 4 respectively. These dates do not establish broad, reliable daily supply. No direction, price-level, outcome, future-return or minimum-duration filter was used; eventual window duration is not knowable at entry.

The retained [twenty aggregate rows](pilot-aggregates.json) are sufficient to reproduce this table. [Provenance](pilot-provenance.json) records the exact historical release, selected-inventory hash, twenty query/run identities and source manifest/summary hashes. The curation checked completed local manifests and reconciled saved verification counts with these aggregates; it did not rerun the feature corpus or independently reclassify the original endpoints. The original pilot reported 551.6 MiB peak process-tree RSS; that is inherited historical evidence, separate from this checkout's synthetic measurements.

Authorized reproduction: obtain the exact private release controls and calculation allowlist, generate each variant with [pilot_configs.py](../scripts/pilot_configs.py), then plan each of these five dates using the [documented real-data workflow](../docs/data-access.md). The copied query semantics are unchanged; the expanded implementation-identity closure requires new plans. Original run plans must not be resumed in this checkout.

No six-month cohort results are presented. Costs, fills, entry/exit execution, latency, adverse excursion, win rate, payoff, profit factor and executable expectancy remain untested. The table is an aggregate summary, not a market-data redistribution license.
