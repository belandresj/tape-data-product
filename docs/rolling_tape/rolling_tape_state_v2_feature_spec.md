# Rolling Tape State V2 — Frozen Feature Contract

**Status:** normative feature contract; implementation conformance and acceptance pending  
**Product identity:** `rolling_tape_state_v2`  
**Session:** `[04:00,20:00)` America/New_York  
**Input and output cadence:** one causal row per symbol-second  
**State horizons:** 60 observable seconds and 300 observable seconds

## 1. Purpose

Rolling Tape State V2 is a direction-neutral description of whether an equity
tape offers observable midpoint movement relative to displayed transaction
cost, executed activity, displayed top-of-book liquidity, and temporally
continuous quote and trade event streams.

It is a measurement product. It does not predict returns, select a direction,
define an entry or exit, estimate fill probability, or label a tape active or
inactive. A client may use the measurements to construct a universe or to
condition a separate forecasting or execution model.

The compact economic state has five groups:

1. midpoint movement in basis points and current full-spread units;
2. concentration of midpoint movement through time;
3. eligible trade and dollar throughput;
4. separately measured displayed bid and ask notional plus flow-to-depth; and
5. quote usability, quote-event continuity, trade-event continuity, and
   observed execution cost.

Every rolling baseline is measured over one of two horizons:

- 60 observable seconds for the fast/current tape state; and
- 300 observable seconds for the slow/established tape state.

The substring `per_30s` in movement field names is a common rate unit. It does
not define a third estimation horizon.

## 2. Client and product boundary

The ordinary client activity controls are three concepts:

```text
maximum quote-age p90
maximum trade-age p90
minimum eligible dollar rate
```

Clients apply those thresholds to the 60-second fields, the 300-second fields,
or both. V2 does not publish an `activity_mode` field or combine client
thresholds into a universal activity label.

Trade rate remains descriptive context for clients that care about print
count. Usable-NBBO fraction is quote-quality context, not a fourth default
activity control. Movement, side-specific displayed notional, flow-to-depth,
and spread fields support separate opportunity, capacity, and cost filters.

Client-selected thresholds, threshold-conditioned activity episodes,
session-to-date baselines, full-day baselines, and universe membership are
downstream products and are not part of this contract.

## 3. Normative configuration

| Parameter | Value |
|---|---:|
| Publication cadence | 1 second |
| Fast state horizon | 60 observable seconds |
| Slow state horizon | 300 observable seconds |
| Current execution-cost horizon | 60 consecutive wall-clock seconds |
| Fine movement scale | 5 seconds, evaluated at every 1-second phase |
| Movement-concentration scale | 1-second midpoint changes |
| Movement-concentration tail | Largest 10% of valid changes |
| Common movement-rate unit | Per 30 seconds |
| Fast/slow support threshold | 80% |
| Execution-cost trust threshold | 90% |
| Accepted-halt treatment | Pause state clocks; rebuild current cost layer |

The 80% and 90% thresholds create support or trust metadata. They do not erase
an otherwise mathematically computable economic estimate.

## 4. Causality and row semantics

Each row ending at `t` uses only events observable by `t`. SIP timestamp is the
causal clock. Participant timestamps never backdate information.

An accepted symbol-day has exactly 57,600 canonical interval-end rows:

```text
04:00:01 ET, 04:00:02 ET, ..., 20:00:00 ET
```

The row ending at `t` represents source interval `[t-1s,t)`. An event stamped
exactly `t` belongs to the following source interval.

The quote initializer may use the latest accepted quote in `[03:55,04:00)` ET
to establish the prevailing state and quote age at 04:00. Trades before 04:00
never contribute to V2.

There is no automatic reset at 09:30 or 16:00. Continuity resets at the
extended-session boundary, an accepted upstream source-continuity break, or an
accepted halt boundary.

## 5. Required one-second primitive boundary

The feature builder consumes canonical one-second primitives keyed by:

```text
session_date
symbol
interval_end
```

Required quote primitives are:

```text
midpoint_twap
spread_twap_bps
midpoint_valid_frac
spread_valid_frac
locked_frac
quote_age_end_ms
displayed_bid_notional_twap
displayed_ask_notional_twap
displayed_nbbo_notional_valid_frac
continuity_segment_id
quote_source_file_accepted
```

Required trade primitives are:

```text
eligible_trade_count
eligible_share_volume
eligible_dollar_volume
eligible_trade_age_end_ms
effective_spread_share_bps_sum
effective_spread_contributing_shares
trade_source_file_accepted
```

The accepted-halt overlay supplies:

```text
halt_interval_active
halt_interval_id
halt_resume_boundary
historical_ex_post_overlay
halt_registry_version
halt_registry_config_hash
```

Every primitive preserves native nulls. Missing midpoint, spread, displayed
depth, quote age, or trade age is not zero. An accepted source second with no
eligible trades has zero eligible trade count, share volume, and dollar volume.

## 6. Canonical eligible-trade population

Trade rate, dollar rate, trade age, and effective spread use one eligible-trade
population. An eligible trade requires:

- finite positive trade price;
- finite positive analytic share size, including accepted fractional shares;
- accepted correction semantics;
- known trade-condition codes;
- nonnegative participant-to-SIP latency no greater than one second; and
- deterministic SIP/sequence ordering within the trade stream.

During RTH `[09:30,16:00)` ET, every attached condition must be in:

```text
{0, 3, 14, 36, 37, 41, 60}
```

During `[04:00,09:30)` and `[16:00,20:00)` ET, Form T condition `12` is also
accepted:

```text
{0, 3, 12, 14, 36, 37, 41, 60}
```

Condition `13` is not accepted.

## 7. Observable-time state windows

For `H` in `{60,300}`, retain the latest `H` observable, non-halt one-second
rows in the current ordinary continuity epoch.

An accepted halt pauses both state clocks:

- halt seconds do not enter activity rates as zeros;
- raw 60-second and 300-second states are carried across halt rows;
- no movement or age primitive bridges the halt or reopening boundary;
- post-resumption observations progressively displace retained pre-halt
  observations; and
- provenance identifies carried and mixed pre/post-halt state.

Consequently, an `H`-observation state can span more than `H` wall-clock
seconds after a halt. It must not be described as a strict wall-clock lookback.

The current 60-second execution-cost layer is different: it requires 60
consecutive wall-clock rows in the current ordinary continuity/post-halt epoch.
It is unavailable during a halt and rebuilds from post-resumption rows.

## 8. Midpoint movement

### 8.1 One-second TWAP midpoint

Let `m_u` be the time-weighted average of the valid prevailing NBBO midpoint in
the source second ending at `u`.

The midpoint is valid only for a finite positive two-sided firm/open quote that
is not crossed and does not contain a rejected or unknown quote condition.
Locked states retain a valid midpoint. One-sided, nonfirm, closed, crossed, and
rejected states do not.

### 8.2 Five-second phase movement

For every eligible one-second endpoint `u`, define:

\[
r_5(u)=10{,}000\log\left(\frac{m_u}{m_{u-5s}}\right),
\qquad
a_5(u)=|r_5(u)|.
\]

A five-second primitive is valid when:

1. both exact endpoint midpoints are finite and positive;
2. all five source rows exist and are source-accepted;
3. the endpoints and source rows occupy one ordinary continuity epoch;
4. no accepted halt boundary lies inside the primitive; and
5. the primitive does not bridge halt generations.

Midpoint-valid duration is reliability metadata, not a movement-publication
gate. A valid flat primitive contributes zero.

Every one-second phase is retained. With mature full support, the exact nominal
candidate counts are:

```text
60-second state  -> 56 overlapping five-second movements
300-second state -> 296 overlapping five-second movements
```

The earliest mature primitive may use the exact midpoint boundary immediately
before the oldest retained source-second row. That boundary is not an
additional retained observation. This endpoint/source-row convention controls
the movement warm-up and candidate counts.

### 8.3 Movement in basis points per 30 seconds

Let `P_H(t)` be the valid five-second movements retained at row `t`. When
`P_H(t)` is nonempty:

\[
\boxed{
midpoint\_movement\_bps\_per\_30s_H(t)
=6\frac{\sum_{u\in P_H(t)}a_5(u)}{|P_H(t)|}
}.
\]

The factor six converts mean absolute five-second movement into a common
30-second rate. This is phase-averaged gross movement, not net displacement,
event-level path length, or a count of executable price changes.

### 8.4 Movement in current full spreads per 30 seconds

When the current `quoted_spread_bps_60s(t)` from Section 14 is finite and
strictly positive:

\[
\boxed{
midpoint\_movement\_in\_spreads\_per\_30s_H(t)
=\frac{midpoint\_movement\_bps\_per\_30s_H(t)}
{quoted\_spread\_bps\_60s(t)}
}.
\]

Both horizons use the same current 60-second full-spread denominator. The
fast-to-slow ratio therefore isolates movement-rate acceleration or compression
rather than a change between two cost estimators.

The coordinate measures observed gross movement relative to displayed cost.
It does not imply predictability, fillability, captured spread crossings, or
positive expectancy after fees, latency, adverse selection, and market impact.

## 9. Midpoint-movement concentration

For every candidate one-second endpoint `u` in horizon `H`, define:

\[
x_1(u)=10{,}000\left|\log\left(\frac{m_u}{m_{u-1s}}\right)\right|.
\]

The change is valid only when both exact endpoints are finite and positive,
source-accepted, and in the same ordinary continuity epoch and halt generation.
Valid zero changes remain in the population.

Let `X_H(t)` be the valid population, `n=|X_H(t)|`, and:

\[
k=\max(1,\lceil0.10n\rceil).
\]

Let `Top_k(X_H)` contain the `k` largest values. When total valid movement is
positive:

\[
\boxed{
midpoint\_movement\_top\_10pct\_share_H(t)
=\frac{\sum_{x\in Top_k(X_H)}x}{\sum_{x\in X_H}x}
}.
\]

Values near `0.10` indicate movement distributed broadly through the horizon.
Higher values mean a small number of seconds contributed a disproportionate
share. Values near `1.00` mean nearly all movement occurred in a small tail.

The statistic uses linear movement contributions. It is null when no valid
one-second changes exist or total valid movement is zero. Subsecond changes
that reverse entirely inside one second are outside this metric.

## 10. Eligible trade and dollar rates

Let `c_s` and `d_s` be eligible trade count and eligible dollar volume in a
valid observable source second. For the retained horizon:

\[
\boxed{trade\_rate_H(t)=\frac{\sum_s c_s}{N_{activity}}},
\qquad
\boxed{dollar\_rate_H(t)=\frac{\sum_s d_s}{N_{activity}}}.
\]

`N_activity` is the number of source-valid observable activity seconds, not the
nominal horizon and not the wall-clock span across an accepted halt. An
accepted no-trade second contributes zero to the numerator and one to the
denominator.

Rates publish whenever at least one valid activity second exists. Dollar rate
is the default economic activity level. Trade rate remains client-visible
print-count context.

## 11. Displayed bid and ask notional

For source second `s`, let:

- `B_s` be time-weighted displayed bid price times displayed bid size;
- `A_s` be time-weighted displayed ask price times displayed ask size; and
- `f_s` be the fraction of the second with valid positive two-sided displayed
  depth.

For each horizon:

\[
\boxed{
mean\_displayed\_bid\_notional_H(t)
=\frac{\sum_s B_sf_s}{\sum_s f_s}
},
\]

\[
\boxed{
mean\_displayed\_ask\_notional_H(t)
=\frac{\sum_s A_sf_s}{\sum_s f_s}
}.
\]

The estimates publish when total valid displayed-depth duration is positive.
Coverage is:

\[
displayed\_notional\_valid\_fraction_H(t)=\frac{\sum_s f_s}{H}.
\]

Bid and ask notional remain separate client-facing coordinates. Displayed NBBO
notional is not full depth, executable capacity, queue position, hidden
liquidity, or fill probability.

## 12. Flow to displayed depth

When both displayed-notional estimates are finite and their sum is positive:

\[
\boxed{
flow\_to\_displayed\_depth_H(t)
=\frac{dollar\_rate_H(t)}
{mean\_displayed\_bid\_notional_H(t)
+mean\_displayed\_ask\_notional_H(t)}
}.
\]

Units are displayed-book equivalents per second. Higher values mean more
reported dollar flow relative to displayed NBBO inventory. This can reflect
rapid replenishment, a thin book being consumed, or both. Interpret the field
with side-specific displayed notional, spread, effective spread, and quote
quality.

## 13. Quote usability and event-stream continuity

### 13.1 Usable-NBBO fraction

For the observable horizon:

\[
\boxed{
usable\_nbbo\_fraction_H(t)
=\frac{\sum_s midpoint\_valid\_frac_s}{H}
}.
\]

This is a duration-weighted availability measurement. A locked but otherwise
valid midpoint contributes usable duration. A crossed, one-sided, nonfirm,
closed, rejected, or unknown quote state does not.

A stale but otherwise valid quote still contributes usable duration. Quote age
measures freshness separately. Dividing by nominal `H` makes incomplete
warm-up visible rather than renormalizing it away.

### 13.2 Quote age

At endpoint `u`, `quote_age_end_ms(u)` is elapsed SIP-clock time since the most
recent accepted raw quote event while the prevailing price state is valid.
Every accepted raw refresh resets age, including a same-price/same-size refresh.
If there is no prior quote event in the current continuity/halt generation or
the prevailing price state is invalid, quote age is null.

Let `Q_H(t)` be the finite quote-age endpoint observations retained in horizon
`H`. When it is nonempty:

\[
\boxed{
quote\_age\_p90_H(t)=Q^{linear}_{0.90}(Q_H(t))
}.
\]

Use NumPy's linear quantile convention. Lower values mean that quotes were
refreshed more recently through most of the horizon. The p90 is conditional on
finite observations; `usable_nbbo_fraction_H` discloses whether a usable quote
was present through the horizon.

### 13.3 Eligible-trade age

For row endpoint `u`, let `T^-(u)` be the greatest SIP timestamp strictly less
than `u` belonging to an eligible trade in the same session, ordinary
continuity epoch, and halt generation. When it exists:

\[
eligible\_trade\_age\_end\_ms(u)
=\frac{u-T^-(u)}{1\text{ ms}}.
\]

An event stamped exactly `u` belongs to the next source interval and cannot
reset age at `u`. A valid no-trade second increases age; it does not make age
null. Before the first eligible trade in the applicable generation, age is
null.

Let `T_H(t)` be the finite trade-age endpoint observations retained in horizon
`H`. When it is nonempty:

\[
\boxed{
trade\_age\_p90_H(t)=Q^{linear}_{0.90}(T_H(t))
}.
\]

Lower values mean eligible trades occurred with shorter temporal gaps through
most of the horizon. Trade age measures event continuity; dollar rate measures
economic throughput. Neither substitutes for the other.

The implementation retains finite quote-age and trade-age observation counts
for support tests and manifest diagnostics. Those counts are audit metadata,
not additional client activity controls.

## 14. Current 60-second execution-cost layer

The current cost layer uses exactly the 60 consecutive wall-clock source rows
in `[t-60s,t)`. The window must be complete, source-accepted, outside an
accepted halt, and inside one current ordinary continuity/post-halt epoch.

### 14.1 Quoted full spread

For second `s`, define unlocked valid spread duration:

\[
u_s=\max(spread\_valid\_frac_s-locked\_frac_s,0).
\]

When total unlocked duration is positive:

\[
\boxed{
quoted\_spread\_bps_{60s}(t)
=\frac{\sum_s spread\_twap\_bps_s\,spread\_valid\_frac_s}
{\sum_su_s}
}.
\]

Its coverage is:

\[
quoted\_spread\_valid\_fraction_{60s}(t)=\frac{\sum_su_s}{60}.
\]

The spread estimate publishes with any positive unlocked duration once the
structural 60-row window exists. `quoted_spread_valid_60s` is true at coverage
of at least `0.90`.

### 14.2 Effective spread

For eligible trade `i`, let `P_i` be price, `Q_i` analytic shares, and `M_i^-`
the latest finite positive price-valid midpoint with strictly earlier SIP
timestamp. An equal-SIP quote cannot be the reference.

Define unsigned full effective spread:

\[
ES_i^{bps}=20{,}000\frac{|P_i-M_i^-|}{M_i^-}.
\]

Over `[t-60s,t)`:

\[
\boxed{
effective\_spread\_bps_{60s}(t)
=\frac{\sum_iQ_iES_i^{bps}}{\sum_iQ_i}
}.
\]

The denominator contains effective-spread-contributing shares. Coverage is:

\[
effective\_spread\_share\_coverage_{60s}(t)
=\frac{\text{contributing eligible shares}}
{\text{all eligible shares}}.
\]

The estimate publishes whenever both eligible and contributing shares are
positive inside a structurally complete window. `effective_spread_valid_60s`
is true at coverage of at least `0.90`. With no eligible shares, effective
spread and coverage are null.

### 14.3 Effective-to-quoted spread

When both inputs are finite and quoted spread is positive:

\[
\boxed{
effective\_to\_quoted\_spread_{60s}(t)
=\frac{effective\_spread\_bps_{60s}(t)}
{quoted\_spread\_bps_{60s}(t)}
}.
\]

Below one means recent executions averaged inside the displayed full-spread
scale; near one means observed effective and quoted full spreads were similar;
above one means executions averaged farther from their strictly prior midpoint
than the current displayed full spread. This is not implementation shortfall
or post-trade adverse selection.

## 15. Fast-to-slow context

Publish the following ratios when the slow denominator is finite and strictly
positive:

\[
midpoint\_movement\_fast\_to\_slow
=\frac{midpoint\_movement\_in\_spreads\_per\_30s_{60s}}
{midpoint\_movement\_in\_spreads\_per\_30s_{300s}},
\]

\[
trade\_rate\_fast\_to\_slow
=\frac{trade\_rate_{60s}}{trade\_rate_{300s}},
\]

\[
dollar\_rate\_fast\_to\_slow
=\frac{dollar\_rate_{60s}}{dollar\_rate_{300s}},
\]

\[
flow\_to\_depth\_fast\_to\_slow
=\frac{flow\_to\_displayed\_depth_{60s}}
{flow\_to\_displayed\_depth_{300s}}.
\]

Above one means the fast state exceeds the slow state; below one means it has
compressed. The component levels remain mandatory. These ratios are
standardized derived views, not additional activity controls. Quote-age and
trade-age ratios are not part of V2 because their absolute time units are more
interpretable.

## 16. Support, maturity, and provenance

### 16.1 State and family support

For each `H`, publish these audit fields:

```text
state_observed_second_count_H
state_observed_second_fraction_H
state_mature_H
movement_valid_5s_count_H
movement_valid_5s_fraction_H
movement_valid_1s_count_H
movement_valid_1s_fraction_H
activity_valid_second_count_H
activity_valid_second_fraction_H
quote_age_observation_count_H
trade_age_observation_count_H
movement_support_valid_H
activity_support_valid_H
displayed_notional_support_valid_H
```

`state_mature_H` is true when `H` observable rows exist in the current ordinary
continuity epoch.

The family support rules are:

\[
movement\_support\_valid_H
=
\left(state\_observed\_second\_count_H\ge\lceil0.80H\rceil\right)
\land
\left(movement\_valid\_5s\_count_H\ge\lceil0.80(H-4)\rceil\right)
\land
\left(movement\_valid\_1s\_count_H\ge\lceil0.80H\rceil\right),
\]

\[
activity\_support\_valid_H
=activity\_valid\_second\_count_H\ge\lceil0.80H\rceil,
\]

\[
displayed\_notional\_support\_valid_H
=
\left(state\_observed\_second\_count_H\ge\lceil0.80H\rceil\right)
\land
\left(displayed\_notional\_valid\_fraction_H\ge0.80\right).
\]

The exact movement minimums are 48 observed seconds, 45 valid five-second
movements, and 48 valid one-second movements for `H=60`; and 240, 237, and 240
respectively for `H=300`. Activity minimums are 48 and 240 valid seconds.

`movement_valid_5s_fraction_H` uses nominal denominator `H-4`.
`movement_valid_1s_fraction_H`, `activity_valid_second_fraction_H`, and
`state_observed_second_fraction_H` use nominal denominator `H`.

Age observation counts disclose quantile sample size to auditors but are not
client activity controls. Numeric estimates publish below support thresholds
whenever their equations are mathematically defined.

### 16.2 Halt provenance

For each horizon, publish:

```text
state_pre_halt_observation_fraction_H
state_post_halt_observed_seconds_H
state_contains_pre_halt_history_H
state_fully_post_halt_H
state_carried_forward_during_halt_H
```

Also publish:

```text
halt_interval_active
halt_interval_id
seconds_since_halt_resume
current_cost_post_halt_observed_seconds_60s
historical_ex_post_overlay
```

Before the first accepted halt, pre-halt fraction is zero and fully-post-halt is
true. During a halt, raw observable-time state can be carried, but the current
60-second execution-cost fields and cost-normalized movement are null. After
resumption, mixture fields remain authoritative until retained observations
and required boundary endpoints are entirely post-halt.

### 16.3 Source and methodology provenance

Every artifact must bind:

- raw trade and quote paths, row counts, sizes, mtimes, and SHA-256 identities;
- source acceptance and duplicate-key results;
- upstream market-state version and source hash;
- V2 primitive version and configuration hash;
- V2 feature-model version, configuration hash, and this contract's SHA-256;
- eligible-trade-population version;
- halt-registry version, configuration, accepted interval IDs, and content
  hashes;
- halt-treatment version;
- source-code hashes and Git revision; and
- output row count, key checks, null counts, support summaries, and artifact
  hashes.

Per-row identity fields may be projected away by clients only when the same
identities remain in Parquet schema metadata and the run manifest.

## 17. Exact client-facing schema

For each `H` in `{60s,300s}`:

```text
midpoint_movement_bps_per_30s_H
midpoint_movement_in_spreads_per_30s_H
midpoint_movement_top_10pct_share_H
trade_rate_H
dollar_rate_H
mean_displayed_bid_notional_H
mean_displayed_ask_notional_H
flow_to_displayed_depth_H
quote_age_p90_H
trade_age_p90_H
usable_nbbo_fraction_H
```

Current execution-cost fields:

```text
quoted_spread_bps_60s
effective_spread_bps_60s
effective_to_quoted_spread_60s
```

Standardized fast-to-slow views:

```text
midpoint_movement_fast_to_slow
trade_rate_fast_to_slow
dollar_rate_fast_to_slow
flow_to_depth_fast_to_slow
```

The coverage, support, halt, and identity fields in Section 16 and the following
cost-specific qualifiers are mandatory audit metadata, not additional economic
coordinates:

```text
displayed_notional_valid_fraction_H
quoted_spread_valid_fraction_60s
quoted_spread_valid_60s
effective_spread_share_coverage_60s
effective_spread_valid_60s
```

## 18. Null, zero, and numerical rules

1. Persist unavailable numeric values as native Arrow null, never NaN or
   infinity.
2. Missing or invalid midpoint observations never become zero movement.
3. A valid flat movement state produces zero movement level and null movement
   concentration.
4. An accepted no-trade second contributes zero trade and dollar flow.
5. A no-trade second after a prior eligible trade increases trade age.
6. Before the first eligible trade in the applicable generation, trade age is
   null.
7. Quote and displayed-depth coverage below a trust threshold does not erase an
   otherwise computable estimate.
8. A ratio is null when its denominator is missing, nonfinite, or nonpositive.
9. The current cost layer requires a complete same-epoch 60-row wall-clock
   window.
10. Observable-time 60-second and 300-second raw state does not update during
    accepted halt rows.
11. No movement or event-age primitive bridges an accepted halt, invalid
    endpoint, rejected source row, or ordinary continuity reset.
12. All aggregations use deterministic population definitions and stable key
    order.

## 19. Interpretation boundaries

V2 describes tape conditions available at each publication time. It does not
establish predictive power or executable expectancy.

In particular:

- movement in spreads is not a count of captured spread crossings;
- movement concentration describes temporal concentration, not direction;
- a low quote age does not prove the quote was continuously usable;
- a low trade age does not prove economically large flow;
- trade count is affected by print fragmentation and reporting behavior;
- displayed notional is not executable capacity;
- flow-to-depth can reflect replenishment, thinness, or both;
- effective spread is not post-trade adverse selection; and
- fast-to-slow compression is context, not a trading signal.

## 20. Halt-registry semantics

Historical construction must verify and reuse the separately versioned accepted
halt registry and overlay. V2 must not infer halts from T/Q inactivity.

The accepted registry is historical, ex-post information. The per-row
`historical_ex_post_overlay` field and manifest identity prevent a downstream
user from treating accepted-halt labels as contemporaneously available live
signals. A live implementation requires a separately specified real-time halt
source and its publication latency.

## 21. Contract acceptance

This feature contract is normative for `rolling_tape_state_v2`. An output may
be described as conforming only after:

1. the standalone primitive adapter, halt clock, feature builder, runner,
   tests, Parquet artifacts, and manifests implement this document exactly;
2. deterministic movement, concentration, activity, depth, event-age, cost,
   null, support, maturity, and halt tests pass;
3. shared market-state and accepted-halt-registry regression suites pass;
4. the pinned BIRD `2026-04-15` acceptance run passes the matching
   implementation specification;
5. two independent BIRD builds have identical logical Parquet content and
   stable manifest identities; and
6. both full runs remain below the repository's approximately 4 GiB peak
   resident-memory ceiling.

Until all conditions hold, existing code and generated artifacts are historical
development outputs and must not be described as conforming frozen V2 outputs.
