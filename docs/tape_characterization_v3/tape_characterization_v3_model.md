# Tape Characterization V3 — Economic Tape Cohort Model

**Status:** candidate model; V3 pilot implemented with synthetic verification, real-data acceptance pending  
**Proposed product identity:** `economic_tape_state_v3`  
**Session:** `[04:00,20:00)` America/New_York  
**Publication cadence:** one causal row per symbol-second  
**State horizons:** 60 observable seconds and 300 observable seconds

## 1. Product objective

V3 is a direction-neutral index of economically comparable equity tapes. It
should let a researcher retrieve symbol-seconds with similar movement scale,
displayed friction, event activity, continuity, and top-of-book capacity, then
apply the researcher's own path, directional, forecasting, or execution model.

The product does not claim that neighboring tapes have the same alpha or that a
model transfers between them. It supplies the economic comparison set needed
to test that claim. It is not primarily a global opportunity rank, a
contraction detector, or a path-shape classifier.

## 2. Relationship to Rolling Tape State V2

V3 should reuse the causal clocks, eligible-trade population, halt treatment,
null semantics, and most 60-second and 300-second measurements from
`rolling_tape_state_v2`.

The main changes in interpretation and membership are:

1. raw movement in basis points and quoted spread remain separate primary
   coordinates;
2. movement divided by spread and fast divided by slow remain derived views,
   not independent similarity inputs;
3. temporal movement concentration and other path-shape fields are excluded
   from the core cohort vector;
4. spread is described over both the 60-second and 300-second horizons;
5. point-in-time tick geometry and semantic NBBO update intensity are proposed
   additions; and
6. similarity uses historically anchored coordinates rather than a rank within
   the contemporaneous cross-section.

## 3. Candidate economic coordinates

`H` is each horizon in `{60s, 300s}`. Raw fields remain client-visible even
when a direction-neutral composite is used for cohort distance.

| Family | Primary coordinates | Role |
|---|---|---|
| Movement | `midpoint_movement_bps_per_30s_H` | Gross direction-neutral midpoint movement scale |
| Displayed friction | `quoted_spread_bps_H`, `quoted_spread_ticks_H` | Cost geometry in proportional and discrete units |
| Tick geometry | `minimum_price_variation_dollars`, `minimum_price_variation_bps` | Point-in-time price discreteness |
| Trade activity | `trade_rate_H` | Eligible print intensity |
| Quote activity | `nbbo_state_change_rate_H` | Semantic displayed-state churn rather than raw message traffic |
| Event continuity | `trade_age_p90_H`, `quote_age_p90_H` | Tail gaps in eligible trades and accepted quote messages |
| Dollar capacity | `dollar_rate_H` | Reported notional throughput |
| Displayed capacity | `mean_displayed_bid_notional_H`, `mean_displayed_ask_notional_H` | Side-specific MBBO displayed notional |
| Execution geometry | `effective_spread_bps_H` | Secondary, trade-conditioned location of executions relative to the prior midpoint |

Direction-neutral similarity retains bid and ask displayed notional as two
separate primary coordinates. V3 must not replace them with their sum,
geometric mean, minimum, ratio, or another composite. Two tapes can have the
same total displayed notional while placing materially different capacity on
the two sides; combining the fields would erase that distinction. Keeping the
coordinates separate does not assign directional meaning. It preserves the
observed execution geometry so the distance can report whether the bid side,
ask side, or both differ.

### 3.1 Semantic quote activity

The existing `quote_update_count` counts raw accepted quote messages, including
same-price/same-size refreshes. That raw rate remains a feed diagnostic and is
not a primary V3 coordinate.

`nbbo_state_change_rate_H` should count accepted updates that change at least
one economically visible MBBO state component. The implementation should also
retain these decomposition diagnostics:

```text
nbbo_price_change_rate_H
nbbo_size_change_rate_H
raw_quote_message_rate_H
```

Quote age and state-change rate are complementary. Quote age measures gaps in
the accepted quote stream; state-change rate measures how rapidly displayed
prices or sizes actually change.

For accepted quote update `j`, define its semantic MBBO signature as:

```text
S_j = (
  bid price, ask price, bid size, ask size,
  bid exchange, ask exchange,
  one-sided, nonfirm, closed, condition-invalid,
  locked, crossed, price-state-valid, depth-state-valid
)
```

These components are the existing canonical semantics produced by
`build_market_state.py`. A repeated raw message whose signature is unchanged
is not a semantic state change. A change in any component is one state-change
event, regardless of how many components changed in that message. Float
comparisons use the canonical market-state equality rules: two missing values
are unchanged, while a transition between missing and finite is a change.

Let `G(j)` identify the quote's ordinary continuity epoch and accepted-halt
generation. With `j-` denoting the immediately preceding accepted quote in the
same `G(j)`, define:

\[
c_j=
\begin{cases}
1,&j-\text{ exists and }S_j\ne S_{j-},\\
0,&\text{otherwise}.
\end{cases}
\]

The first accepted quote in a continuity/halt generation initializes state and
does not count as churn. For observable non-halt source second `s`:

\[
C_s=\sum_{j:\,sip_j\in[s-1s,s)}c_j.
\]

An accepted quote source with no semantic change in `s` contributes observed
zero. A rejected or unavailable quote-source second contributes null. Let
`O_H(t)` be V2's retained observable, non-halt seconds and let `N_Q` be the
number of quote-source-valid seconds in `O_H(t)`. When `N_Q>0`:

\[
\boxed{
nbbo\_state\_change\_rate_H(t)
=\frac{\sum_{s\in O_H(t)}C_s}{N_Q}
}.
\]

The rate is null when `N_Q=0`. Publish
`nbbo_state_change_observation_count_H=N_Q` and:

\[
nbbo\_state\_change\_support\_valid_H
=N_Q\ge\lceil0.80H\rceil.
\]

`nbbo_price_change_rate_H` and `nbbo_size_change_rate_H` use the same clock,
denominator, and generation resets. The price diagnostic counts a message when
bid or ask price changes; the size diagnostic counts it when bid or ask size
changes. `raw_quote_message_rate_H` counts every accepted raw quote message,
including repeated signatures and generation-initializing messages.

### 3.2 Displayed-friction horizons

Displayed spread is a current execution-friction measurement. Unlike the
observable-time activity and movement states, it uses a complete consecutive
wall-clock window:

\[
W_H(t)=[t-H,t),\qquad H\in\{60s,300s\}.
\]

The window must contain exactly `H` source seconds, remain within one ordinary
continuity and post-halt generation, contain no accepted halt second, and have
accepted quote-source coverage throughout. For second `s`, let:

```text
u_s = duration in seconds where spread state is valid AND not locked
a_s = integral of spread_bps over that same valid AND not locked duration
```

When total unlocked valid duration is positive:

\[
\boxed{
quoted\_spread\_bps_H(t)
=\frac{
\sum_{s\in W_H(t)}a_s
}{
\sum_{s\in W_H(t)}u_s
}
}.
\]

Coverage and validity are:

\[
quoted\_spread\_valid\_fraction_H(t)
=\frac{\sum_{s\in W_H(t)}u_s}{H},
\]

\[
quoted\_spread\_valid_H(t)
=\mathbf{1}\!\left\{
quoted\_spread\_valid\_fraction_H(t)\ge0.90
\right\}.
\]

The numeric estimate publishes whenever the structural window exists and its
denominator is positive, even below 90% coverage; the validity flag remains
false below 90%. When a structurally valid accepted-source window has zero
unlocked duration, coverage is observed zero, the estimate is null, and
validity is false. When the structural or accepted-source requirement fails,
the estimate and coverage are null and validity is false. Both horizons use
`joint_valid_unlocked_duration_v3_1`. The 60-second V3 field is deliberately
versioned separately from legacy V2: subtracting all locked duration from
valid-spread duration is incorrect when locked states are themselves invalid.
Explicitly locked positive-spread states also contribute neither numerator nor
denominator. V2 artifacts remain unchanged; V3 makes no exact-alias claim.

### 3.3 Tick and price-regime semantics

Price is retained as metadata rather than used as a dominant similarity axis.
Its principal economic effects enter through tick size in basis points,
spread in ticks, displayed notional, and per-share costs expressed in basis
points.

Tick size must be point-in-time and may change intraday when the applicable
minimum-price-variation regime changes, including around one dollar. For every
second `t`:

```text
minimum_price_variation_bps(t)
  = 10,000 * minimum_price_variation_dollars(t) / midpoint(t)
```

A 60-second or 300-second window that crosses a tick-regime change should
aggregate each source second using its contemporaneous tick and publish a
`tick_regime_transition_H` audit flag.

The repository does not currently have an authoritative point-in-time
minimum-price-variation input. V3 must add and version that reference before
labeling a field as official tick size. An observed quote increment may be
published only under an explicitly different name and must not be represented
as the regulatory minimum.

### 3.4 Effective spread and coverage

For each horizon, V3 should publish:

```text
effective_spread_bps_H
effective_to_quoted_spread_H
effective_spread_share_coverage_H
effective_spread_valid_H
```

`effective_spread_bps_H` is the eligible-share-weighted unsigned full effective
spread relative to the strictly prior SIP-known midpoint. Share coverage is:

```text
effective_spread_share_coverage_H
  = eligible shares with a valid effective-spread measurement
    / all eligible shares
```

Coverage is a validity gate, not a similarity coordinate. It is required
because initialization, quote gaps, halts, and invalid quote states can leave
some eligible trades without a valid strictly prior midpoint. A low-coverage
effective-spread estimate may describe an unrepresentative subset of traded
shares.

Effective spread is a secondary execution coordinate rather than a universal
base-cohort requirement. The base economic cohort remains available when
effective-spread coverage is inadequate; an execution-refined neighborhood may
add `effective_spread_bps_H` when coverage is sufficient. The ratio to quoted
spread remains a derived view and must not be added as an independent distance
input when both component spread fields are already present.

Rolling Tape State V2 currently implements effective spread only over 60
seconds. The 300-second field is a proposed V3 addition, not an existing V2
capability.

### 3.5 Initial September 1 to September 2 pilot base

The first V3 experiment uses September 1 as the historical reference and
September 2 as the held-out comparison date. For each horizon
`H in {60s, 300s}`, its base-distance inputs are:

```text
midpoint_movement_bps_per_30s_H
quoted_spread_bps_H
trade_rate_H
nbbo_state_change_rate_H
trade_age_p90_H
quote_age_p90_H
dollar_rate_H
mean_displayed_bid_notional_H
mean_displayed_ask_notional_H
```

These 18 fields form six economic families: movement, displayed friction,
trade-and-quote activity, event continuity, dollar capacity, and displayed
capacity. Family weighting must prevent the two side-specific displayed fields
from receiving extra influence merely because displayed capacity has more
columns. Bid and ask remain independently visible in both the distance
decomposition and the published raw values.

The pilot excludes tick geometry until an authoritative point-in-time MPV
reference exists. It also excludes effective spread from the universal base
distance, retains price as metadata, treats movement/spread and fast/slow
ratios as derived views, and excludes all directional and path-shape fields.
Quality and support fields gate whether a comparison is valid; they are not
additional distance coordinates. This subsection records an experiment
contract, not an implemented or promoted V3 product.

All pilot inputs other than semantic NBBO activity and the corrected quoted
spread at both horizons inherit their exact definitions from the frozen Rolling Tape State V2
specification:

| Pilot input | Normative V2 definition |
|---|---|
| `midpoint_movement_bps_per_30s_H` | [Section 8.3](../rolling_tape/rolling_tape_state_v2_feature_spec.md#83-movement-in-basis-points-per-30-seconds) |
| `trade_rate_H`, `dollar_rate_H` | [Section 10](../rolling_tape/rolling_tape_state_v2_feature_spec.md#10-eligible-trade-and-dollar-rates) |
| `mean_displayed_bid_notional_H`, `mean_displayed_ask_notional_H` | [Section 11](../rolling_tape/rolling_tape_state_v2_feature_spec.md#11-displayed-bid-and-ask-notional) |
| `quote_age_p90_H`, `trade_age_p90_H` | [Sections 13.2 and 13.3](../rolling_tape/rolling_tape_state_v2_feature_spec.md#132-quote-age) |

## 4. Derived views, not primary coordinates

The following remain useful client filters and explanatory fields:

```text
movement_in_spreads_H = midpoint_movement_bps_per_30s_H / quoted_spread_bps_H
movement_fast_to_slow = midpoint_movement_bps_per_30s_60s
                        / midpoint_movement_bps_per_30s_300s
trade_rate_fast_to_slow
dollar_rate_fast_to_slow
flow_to_displayed_depth_H
effective_to_quoted_spread_H
```

They must not be included as additional independent similarity inputs when
their numerator and denominator components are already present. In particular,
movement divided by spread can map a low-movement/tight-spread tape and a
high-movement/wide-spread tape to the same value, while small absolute spread
changes near zero can cause large ratio changes.

## 5. Quality gates and audit fields

Quality determines whether a coordinate is trustworthy; it is not another
economic cohort dimension. V3 retains V2's family-specific support, maturity,
halt, source, and coverage fields, including:

```text
usable_nbbo_fraction_H
state_observed_second_count_H
state_mature_H
movement_support_valid_H
activity_support_valid_H
displayed_notional_support_valid_H
quote_age_observation_count_H
trade_age_observation_count_H
effective_spread_share_coverage_H
effective_spread_valid_H
locked/crossed and spread-valid coverage
halt and post-halt provenance
```

Client eligibility profiles may apply thresholds to quote age, trade age,
dollar throughput, displayed capacity, and coverage. V3 should publish the
components and version each profile rather than declare one universal notion
of a tradable tape.

### 5.1 Initial pilot comparison gate

Raw feature publication continues to follow the V2 rule: publish a numeric
estimate whenever its equation is defined, and publish support separately.
The September 1 to September 2 pilot uses a stricter all-coordinate gate for
distance calculation. A row is `pilot_base_comparison_eligible` only when, for
both `H=60s` and `H=300s`:

1. `state_mature_H`, `movement_support_valid_H`,
   `activity_support_valid_H`, `displayed_notional_support_valid_H`, and
   `nbbo_state_change_support_valid_H` are true;
2. `quoted_spread_valid_H` is true;
3. `quote_age_observation_count_H >= ceil(0.80 H)` and
   `trade_age_observation_count_H >= ceil(0.80 H)`;
4. `halt_interval_active` is false and `state_fully_post_halt_H` is true; and
5. all 18 pilot coordinates are finite and nonnegative.

The pilot does not compute a partial distance when one family is unavailable;
the row is ineligible for comparison. A valid observed zero remains zero,
including a zero trade rate, dollar rate, or semantic state-change rate. No
minimum economic activity, maximum event age, price cutoff, rank threshold, or
future information enters this base gate. Those remain separately versioned
client filters or later sensitivity tests.

Unavailable numeric values remain native Arrow null. NaN and infinity are
never persisted. Support below a threshold does not erase an otherwise
computable raw feature; it prevents that row from entering this pilot's
distance calculation.

## 6. Similarity and cohort construction

V3 publishes interpretable raw values plus a separately versioned cohort
index. The initial cohort method should use soft economic neighborhoods rather
than immediately forcing every row into a hard statistical cluster.

### 6.1 Initial pilot transformation

All 18 pilot coordinates are nonnegative and use a dimensionless `log1p`
transformation. For feature `j` in its frozen published unit, let `u_j` be one
unit: one basis point for bps fields, one event per second for event rates, one
millisecond for ages, one dollar per second for dollar rate, and one dollar for
displayed notional. Define:

\[
y_j(x)=\log\left(1+\frac{x}{u_j}\right).
\]

Using only eligible September 1 reference observations, estimate the median
`m_j` and interquartile range `IQR_j = Q_0.75(y_j)-Q_0.25(y_j)` with the linear
quantile convention. Freeze those values and transform both dates as:

\[
z_j(x)=\frac{y_j(x)-m_j}{IQR_j}.
\]

September 2 never contributes to its own transformation parameters. The pilot
does not winsorize or clip transformed observations. If any required
`IQR_j` is zero or nonfinite, scaler fitting fails rather than silently adding
an arbitrary denominator floor. The scaler artifact records every unit,
median, IQR, source date, eligible-row count, feature order, and contract hash.

This is now exact for the two-date pilot. A later production V3 reference will
re-estimate the same versioned transformation on a larger multi-day development
period and validate it on later dates.

### 6.2 Initial pilot family distance

The 18 coordinates form these six families:

```text
movement:            movement 60s, movement 300s
displayed friction:  quoted spread 60s, quoted spread 300s
activity:             trade rate 60s/300s, NBBO state-change rate 60s/300s
event continuity:    trade age 60s/300s, quote age 60s/300s
dollar capacity:     dollar rate 60s, dollar rate 300s
displayed capacity:  bid notional 60s/300s, ask notional 60s/300s
```

For tapes `a` and `b`, family `f`, and its feature set `J_f`, define:

\[
D_f^2(a,b)=\frac{1}{|J_f|}\sum_{j\in J_f}
\left(z_j(a)-z_j(b)\right)^2.
\]

The universal pilot distance gives each family equal weight:

\[
\boxed{
D_{base}(a,b)=
\sqrt{\frac{1}{6}\sum_{f=1}^{6}D_f^2(a,b)}
}.
\]

Averaging within a family prevents activity or displayed capacity from
dominating merely because it has more columns. Bid and ask displayed notional
remain separate differences inside the displayed-capacity family. The output
must publish `D_base` and all six `D_f` values so users can see why two tapes
are near or far. The pilot uses no covariance estimate, learned metric,
contemporaneous rank, effective spread, derived ratio, or clustering model.

The broader development procedure remains to fit a multi-day historical
reference, publish neighbor distance or membership strength alongside any
coarse cohort label, test stability on held-out dates, and only then consider
Gaussian mixtures or another clustering model.

Contemporaneous cross-sectional ranks may be published as optional discovery
views. They are not stable cohort coordinates because their meaning changes
with that second's selected universe.

## 7. What V3 deliberately leaves to the client

The core cohort vector excludes direction, net displacement, route efficiency,
movement concentration, trend, reversal, contraction, expansion, and future
returns. Those can be joined as strategy-specific labels or derived products.

Consequently, V3 can support the statement that two tapes had similar observed
economic and activity structure. It cannot, without a separate transfer test,
support the stronger statement that the same predictive or execution model
works on both.

## 8. Promotion requirements

The candidate feature set becomes frozen only after all of the following are
resolved on development dates and confirmed out of sample:

1. a point-in-time tick/MPV reference and intraday transition semantics;
2. exact semantic NBBO price-change and size-change event definitions;
3. the family weighting and distance behavior of the separate bid-side and
   ask-side displayed-notional coordinates;
4. incremental-information and redundancy tests for semantic quote activity;
5. the effective-spread population and minimum share-coverage rule at both
   horizons;
6. frozen transformations, family weights, and distance behavior;
7. cohort stability across dates, price/tick regimes, and symbols; and
8. explicit evidence that broadening the acquisition universe adds economic
   regimes rather than mostly adding unusable tape.

No clustering algorithm, cohort count, rank threshold, or model-transfer claim
is frozen by this document.

## 9. Implementation and resource boundary

The [pilot implementation specification](pilot_implementation_spec.md) fixes
query selection, ties, provenance and resource acceptance. Implementation
reuses V2's measurement definitions and bounded horizon containers, but replaces
its full-quote-day allocation path with projected Parquet reads, batches
of at most 25,000 rows, fixed-size rolling state, no full-day Pandas copies,
single-process acceptance, and live peak-RSS measurement. The working target is
at most 2 GiB RSS and the hard stop is 3 GiB.

Before a full multi-day cohort build, the exact production path must be measured
on a representative bounded sample. The report must include sample rows,
elapsed time, peak RSS, projected output size, and the largest fixed and
batch-sized allocations.
