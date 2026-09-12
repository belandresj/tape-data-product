# Tape data product: implemented feature contract

Contract identity: `tape_data_product_v1`. Compact physical layout: `tape_product_compact_v1`. The retained calculator, compact writer and causal query implement the compact 60s/300s feature product. The standalone verification uses synthetic fixtures; this does not assert that real data were regenerated or that every historical methodology is implemented. The equations and feature definitions below are preserved from the source contract.

See [layout and validation](../compact-layout.md), [query behavior](../query-contract.md), and [data access](../data-access.md). Packaged semantic metadata preserves historical definition identities without requiring implementation documents at runtime.

## Product goal

Turn equity trades and NBBO quotes into continuous, interpretable measurements that let researchers retrieve **stock-time observations or intervals** matching their requirements. The initial population is the acquired universe selected for volatility and activity, observed after verified discovery. A volatile symbol-day does not imply continuously active trading or movement large relative to quoted friction.

The product describes movement magnitude, movement concentration, quoted friction, transaction activity, and event freshness. Clients choose their own filters. Retrieval results disclose eligible time, selected time, symbol/day/date coverage, and exclusions. Time coverage differs from the fraction of stocks with any qualifying observation.

These are descriptive measurements, not directional signals, profitability scores, executable-capacity estimates, or guarantees of continued movement. Customer demand and empirical usefulness remain to be established.

## Clock, resolution, and support

The dataset has one row per second over `[04:00,20:00)` America/New_York: 57,600 rows per complete symbol-day. A row ending at t represents `[t−1s,t)` and is knowable at that endpoint on the SIP information clock. Events stamped exactly t belong to the following row. Premarket, RTH, and after-hours are reporting strata; history does not reset merely at 09:30 or 16:00.

Compute **nine rolling features at H ∈ {60,300} seconds: eighteen numerical fields**, plus reusable one-second measurements and quality context. Use 60 seconds for recent conditions and 300 seconds for slower context. These are two views of the same definitions, not eighteen independent dimensions.

Five seconds is the movement lag in both horizons. Let m(u) be the valid **duration-weighted midpoint of the one-second interval ending at u**, and define

\[
d_u=10^4\left|\log\frac{m(u)}{m(u-5s)}\right|.
\]

Let D_H(t) contain the supported observations under the implemented H−4 movement-window convention: 56 candidate changes for 60s and 296 for 300s. Preserve its exact endpoint, source, maturity, and continuity semantics when reconstructing observations. Let n be the number of supported changes, including supported zeros.

The five-second changes are observed every second and **overlap**. They are not disjoint five-second blocks, independent samples, or components that sum to the net H-second return. A single price jump may affect multiple observations. All feature interpretations concern supported observations in the stated history.

Primary analysis requires post-discovery eligibility, per-feature support and maturity, no active halt, and fully rebuilt wall-clock history. Retain source acceptance, continuity, halt, and carried-history diagnostics independently. Finite values containing carried history are not primary trailing-wall-clock observations. Nominal discovery completion and verified received-at timing are distinct; historical halt annotations do not establish live availability.

## Frozen rolling features

For each H, let W_H(t) denote the preceding history and I_H(t) its supported activity seconds. Let N_u and V_u be eligible trade count and eligible dollar volume in second u. All inputs to the mean movement/spread ratio use the same H.

| # | Feature / output name | Definition | Interpretation |
|---|---|---|---|
| 1 | Mean five-second movement, `movement_mean_5s_bps_{H}s` | M = sum(d_u)/n | Average magnitude of short-scale midpoint movement, in bps. |
| 2 | Movement participation, `movement_participation_{H}s` | P = sum(d_u)² / (n × sum(d_u²)) | How broadly movement magnitude is distributed across supported observations. Dimensionless; interpret jointly with M. |
| 3 | Mean quoted spread, `quoted_spread_mean_bps_{H}s` | Duration-weighted full spread over valid unlocked quote time | Typical quoted friction, in bps; not realized execution cost. |
| 4 | Trade rate, `trade_rate_{H}s` | Sum of eligible trade counts / supported seconds | Eligible reported transactions per second; not uniform arrival spacing. |
| 5 | Dollar rate, `dollar_rate_{H}s` | Sum of eligible dollar volume / supported seconds | Eligible reported dollars per second; not executable capacity. |
| 6 | Trade-age p90, `trade_age_p90_seconds_{H}s` | Linear p90 of supported endpoint trade ages | Upper-tail time since the last eligible trade, in seconds. |
| 7 | Quote-age p90, `quote_age_p90_seconds_{H}s` | Linear p90 of supported endpoint quote ages | Upper-tail time since the latest quote under the source-event contract, in seconds. An unchanged-price refresh can reset age. |
| 8 | Midpoint-change-age p90, `midpoint_change_age_p90_seconds_{H}s` | Linear p90 of supported exact-event endpoint midpoint-change ages | Upper-tail time since an observed valid instantaneous midpoint change, in seconds; measures updating rather than displacement magnitude. |
| 9 | Mean movement/spread, `movement_mean_to_spread_{H}s` | R = M/S, for valid positive S | Average five-second movement relative to same-horizon mean quoted friction. A derived comparison, not an independent coordinate. |

For spread, let b(v), a(v) be the instantaneous reconstructed bid and ask, q(v)=(a(v)+b(v))/2, and J(v) indicate valid unlocked spread support. Precisely,

\[
S_H(t)=\frac{\int_{W_H(t)}J(v)\,10^4[a(v)-b(v)]/q(v)\,dv}
{\int_{W_H(t)}J(v)\,dv}.
\]

The midpoint q(v) in this expression is instantaneous; m(u) in the movement definition is a one-second duration-weighted mean. M/S compares summaries and does not establish contemporaneous trading opportunity or a capturable move.

## Movement participation

For a supported, mature window with positive total movement, define

\[
P_H(t)=\frac{\left(\sum_{d\in D_H(t)}d\right)^2}
{n\sum_{d\in D_H(t)}d^2}
=\frac{1}{1+\operatorname{CV}(d)^2},
\]

where CV uses the population variance over the same n observations, including zeros. There is no movement cutoff, spread-multiple cutoff, or percentile parameter. The five-second lag, history horizons, and squared weighting are fixed methodological choices.

- `1/n ≤ P ≤ 1` when defined. Equal positive magnitudes give P=1; one positive observation and n−1 zeros give P=1/n.
- If k observations have the same positive magnitude and the rest are zero, P=k/n. With unequal positive magnitudes, P is an effective participation measure, not an observed active-time fraction or an effective independent sample size.
- Multiplying every movement by the same positive constant leaves P unchanged. Equal tiny movements and equal large movements both produce P=1; use M to distinguish magnitude.
- No supported observations or zero total movement makes P null with an explicit reason. An all-zero supported window still has M=0 and remains distinguishable from missing data. Do not add an epsilon to manufacture a defined ratio.
- Squared weighting makes concentrated large observations influential. Input-quality checks and representative validation must assess this behavior; do not silently clip or winsorize source values.
- P ignores ordering and direction. A sequence and its reordered version have the same P. It does not distinguish one long consolidation from many short pauses, or establish that a stock advanced steadily.

For illustration, 80% equal positive observations and 20% zeros produce P=0.8; 10% equal positive observations and 90% zeros produce P=0.1, even if their means are equal. These are algebraic examples, not empirical findings.

## Event ages and observation status

Current age at endpoint t is elapsed time since the relevant event strictly before t. Store current trade, quote, and midpoint-change ages once per endpoint, in seconds. These answer **freshness now**; rolling p90s describe the recent history of freshness. Ages can exceed H and must not be clipped to it.

Midpoint-change age uses eligible quote events and the **instantaneous** valid reconstructed midpoint. An unchanged-midpoint refresh does not reset age; a within-second change and reversal both do. Use established numeric representation and ordering without an added price-change tolerance. Initial valid price state seeds comparison but is not an observed change.

Expose midpoint-age status separately from numeric age:

| Status | Numeric meaning |
|---|---|
| `known` | An eligible valid midpoint change was observed; exact age is available. |
| `no_change_observed` | Valid midpoint state has been continuously observed since a recorded observation start, but no change has been observed. Exact age is null; observed duration without change is available as a lower bound under the source-coverage assumption. |
| `unobservable` | Valid continuous observation is unavailable; neither exact age nor an ongoing lower bound is supplied. |

An invalid midpoint transition clears comparison, exact age, and the no-change observation origin. Valid recovery seeds a new observation period; it does not count movement across the gap. Halts and declared continuity breaks also reset comparison. Quote initialization before 04:00 may seed price but cannot extend observed duration before 04:00. Never count unsupported time as observed no-change time.

Midpoint-age rolling windows reset at halts and continuity breaks and require fresh H-second maturity. Ordinary invalid ages occupy unsupported wall-clock slots. Lower bounds do not enter exact-age p90s. Require accepted quote source, no active halt, H elapsed slots since initialization/reset, and at least `ceil(0.8H)` supported exact ages: 48 or 240.

Trade and quote ages retain their verified upstream event and reset semantics. Their p90s likewise require maturity and at least 80% supported endpoint ages. Age p90 is a time-sampled age statistic, not a percentile of event-to-event gaps. No Poisson-arrival assumption is imposed.

For n sorted supported ages x_(1),...,x_(n), linear p90 uses h=(n−1)×0.9, j=floor(h), a=h−j, and (1−a)x_(j+1)+a x_(min(j+2,n)).

## Stored one-second measurements

Retain these supporting measurements in the final dataset, rather than publishing only rolling summaries:

| Measurement | Required content |
|---|---|
| Midpoint and movement | One-second duration-weighted midpoint, midpoint support duration, five-second absolute log movement, and support/boundary metadata. |
| Quoted spread | Integral of valid unlocked spread in bps-seconds, valid unlocked duration in seconds, and source acceptance. Their ratio gives that second's supported mean spread; sums allow correctly duration-weighted aggregation. |
| Transactions | Eligible trade count and dollar volume for each second, with source acceptance. Accepted no-trade seconds are observed zeros. |
| Current ages | Trade, quote, and exact midpoint-change age in seconds; midpoint observation status, observation-start timestamp, and observed no-change duration where applicable. |
| Identity and quality | Symbol/date/endpoint, source and feature identities, support, continuity, halt/reset, maturity, discovery, and historical/live-availability metadata. |

These are supporting observations, not additional rolling headline features. They allow researchers to reconstruct the defined movement, activity, spread, participation, and age summaries without replaying raw T/Q, using the documented masks and reset rules. Computations requiring subsecond paths or different event populations may still require raw data.

Store supported zeros as zero. Persist undefined values as native null, with per-feature validity and reason flags; never persist NaN or infinity. Participation inherits the mean's movement support and maturity gates and additionally requires positive total movement. Support counts are not independent sample counts. Do not force every query onto one universal complete-case population.

## Calculation and resource guarantees

The mean source conversion is `midpoint_movement_bps_per_30s_{H}s / 6`; the multiplier is normalization, not a measured 30-second return. Source trade/quote age p90s expressed in milliseconds are divided by 1,000. Reuse is checked against schemas and manifests.

Changes to a normative equation, population, clock, support gate, denominator, zero/null rule or membership require a contract revision and matching tests and identities. Integrity, independent numerical reconstruction and external-data acceptance remain separate claims.

Canonical raw storage and acquisition are documented in [acquisition](../acquisition.md). The durable store is Cloudflare R2; local storage remains usable for every calculation stage. Current resource bounds and measured acceptance are in [architecture](../architecture.md) and [verification](../verification/README.md). Full external-data runs require a representative production-path measurement and confirmation; this contract does not itself establish memory compliance.
