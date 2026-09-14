# Phase 0 accepted schema: measurements, types, nulls and reasons

**Status: column names, types, units, null rules and reason codes accepted by the user on 2026-09-14; implemented in `tape_data_product.contracts`. Exact share-total type decimal128(38,9) is frozen as the supported lossless input contract; individual source precision is checked before member completion.** Uses the accepted [product direction](README.md) and [Phase 0 rules](phase_0.md). This review closes field naming/type/null/reason choices only; full transition/checkpoint interfaces remain separate Phase 0 work.

## 1. Organization and conventions

Three logical tables: `base_1s`, `features_1s`, `feature_support_1s`, plus member metadata. Physical files/partitioning remain a later implementation choice. The feature query view joins current ages from base, so these are exposed without physically duplicating them. No legacy columns are added merely for compatibility.

Every logical row is keyed by:

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| `session_date` | string | No | Canonical YYYY-MM-DD New York session date |
| `symbol` | string | No | Source/member symbol |
| `interval_end_ns` | int64 | No | UTC Unix ns; row summarizes `[t−1s,t)`; endpoint state uses events strictly before t |

One-second timestamps align to the grid; no duplicate keys. Physical key types retain convenient existing package joins. Dates are validated canonical strings; dictionary encoding does not change the logical type. No extra `timestamp` alias is physically stored. Session/reporting stratum can be derived from the key and session calendar.

Prices, dollar totals, integrals, EW features and displayed sizes use finite float64. Shares are not converted to integers when fractional. Durations/timestamps use int64 nanoseconds. Counts use int64. Exact share totals are selected as decimal128(38,9): 29 integer digits and 9 fractional digits, with lossless input/total checks. **Provider documentation does not establish a universal maximum decimal scale.** The installed source-admission helper verifies lossless representability for each quantity. Individual historical members remain unverified until admission. More-precise inputs require an explicit schema version rather than silent rounding, dropped eligible trades or claims of exact recovery from float-only inputs. EW share rates are float64 summaries, not exact decimal arithmetic claims.

All measurements are USD equities. Currency and source size-unit conversion identity are required member metadata. All float64 NaN/Inf values are illegal on disk. Prices/midpoints must be positive; spreads/rates/ages/durations are nonnegative. Zero is meaningful when supported.

## 2. One-second base measurements

`N` means never null; `Y` means null when the measurement has no admissible value. For a measurement tuple sharing a mask, either all members are present or all are null. Support durations themselves are always present, including zero. Float precision does not promise bit-exact TWAP arithmetic identities; common support establishes those identities mathematically, checked with the declared numerical tolerance.

| Exact column(s) | Arrow type | Null? | Unit / meaning |
|---|---|---|---|
| `bid_twap_usd`, `ask_twap_usd`, `midpoint_twap_usd` | float64 | Y | USD/share; common valid-price exposure; midpoint TWAP constructed consistently with bid/ask TWAPs |
| `price_valid_duration_ns` | int64 | N | Valid common bid/ask/midpoint exposure in the second, 0..1e9 |
| `price_twap_reason_mask` | uint16 | N | Shared publication reason for the three TWAPs |
| `bid_end_usd`, `ask_end_usd` | float64 | Y | Valid prevailing two-sided endpoint price; no older good-quote fallback |
| `price_end_reason_mask` | uint16 | N | Shared endpoint-price reason; endpoint midpoint/spread derive from these prices |
| `spread_integral_bps_seconds` | float64 | Y | Integral of instantaneous full spread over valid spread exposure; valid numeric locks contribute zero |
| `spread_valid_duration_ns` | int64 | N | Spread exposure, 0..1e9; includes otherwise valid numeric locks |
| `spread_integral_reason_mask` | uint16 | N | Reason for unavailable spread integral |
| `bid_size_integral_shares_seconds` | float64 | Y | Normalized displayed bid-size integral over its supported duration |
| `bid_size_valid_duration_ns` | int64 | N | Bid-size exposure, 0..1e9 |
| `bid_size_integral_reason_mask` | uint16 | N | Reason for unavailable bid-size integral |
| `ask_size_integral_shares_seconds` | float64 | Y | Normalized displayed ask-size integral over its supported duration |
| `ask_size_valid_duration_ns` | int64 | N | Ask-size exposure, 0..1e9 |
| `ask_size_integral_reason_mask` | uint16 | N | Reason for unavailable ask-size integral |
| `bid_size_end_shares`, `ask_size_end_shares` | float64 | Y | Per-side current normalized displayed sizes, strictly positive when valid |
| `bid_size_end_reason_mask`, `ask_size_end_reason_mask` | uint16 | N | Independent endpoint-size reasons |
| `trade_count_1s` | int64 | Y | Eligible timely trades in supported activity exposure |
| `share_volume_1s` | decimal128(38,9), per-source admission required | Y | Exact total of eligible trade quantities subject to verified source precision |
| `dollar_volume_1s_usd` | float64 | Y | Sum of eligible price × shares, USD; not an exact decimal-dollar promise |
| `activity_valid_duration_ns` | int64 | N | Reliable exposure for this shared eligible activity population, 0..1e9 |
| `activity_reason_mask` | uint16 | N | Shared reason for count/share/dollar tuple |
| `trade_age_seconds` | float64 | Y | Endpoint t minus SIP time of latest eligible timely trade report |
| `quote_age_seconds` | float64 | Y | Endpoint t minus latest structurally accepted quote-event SIP time, independent of price validity |
| `midpoint_change_age_seconds` | float64 | Y | Endpoint t minus latest observed valid midpoint-change event time |
| `trade_age_reason_mask`, `quote_age_reason_mask`, `midpoint_change_age_reason_mask` | uint16 | N | Separate current-age reasons |
| `midpoint_age_status` | uint8 | N | 0 unobservable, 1 no_change_observed, 2 known |
| `midpoint_observation_start_ns` | int64 | Y | Start of current continuously valid midpoint observation, present in statuses 1 and 2 |
| `midpoint_age_lower_bound_seconds` | float64 | Y | Only status 1: `(t−midpoint_observation_start_ns)/1e9`; never enters exact-age p90 |

**Partial exposure:** numerators describe only their disclosed supported duration. If a spread is 10 bps for 0.9 supported seconds and invalid for 0.1s, store integral 9 bps-seconds, duration 900,000,000ns, mask 0. Do not null the whole row or imply one second of exposure. If exposure is zero, integral is null and mask includes `NO_SUPPORTED_DATA`. Internal reducers add zero mass/exposure for such a row; storage does not label an unavailable integral as a measured zero.

A valid locked second has integral 0, duration 1e9, mask 0. A reliably observed no-eligible-trade second has count/share/dollar totals 0, positive exposure, mask 0. Known late reports are outside the activity population and do not remove exposure. Unknown trade eligibility cannot simply be omitted while its time remains fully supported. Phase 0's transition/exclusion table must specify bounded uncertain intervals; if an exact affected subsecond interval cannot be established, mark its containing second's activity exposure unsupported and exclude all its activity totals consistently. This conservative fallback is confined to that population/time, not the whole member. If trustworthy count exists but eligible size/dollar contributions are uncertain, the common activity population becomes unsupported; per-total populations would be a separately specified extension.

An unusable local record does not add a nonzero publication reason to a partially supported base numerator that remains admissible. Its defect is reflected in lost exposure and bounded audit/quality evidence. Publication reasons are not counts of every bad event encountered.

## 3. Minimal per-second observation context

These columns accompany base measurements and are available to feature/query readers. The context is an input to the state-transition contract; IDs are not interchangeable with EW resets.

| Column | Type | Null? | Meaning |
|---|---|---|---|
| `quote_source_status`, `trade_source_status` | uint8 | N | 0 unverified, 1 accepted, 2 unavailable; structurally trusted observation at endpoint, independently by stream |
| `quote_observed_duration_ns`, `trade_observed_duration_ns` | int64 | N | Structurally observed stream exposure, 0..1e9; semantic support can be smaller |
| `quote_continuity_id`, `trade_continuity_id` | int64 | N | Nonnegative source-continuity segment at endpoint; changes cannot be bridged by relevant event ages/returns |
| `quote_continuity_break_in_second`, `trade_continuity_break_in_second` | bool | N | Declared source break occurs in `[t−1s,t)`; a brief bad numeric quote alone is not such a break |
| `halt_active` | bool | N | Whole-second closure when accepted halt overlaps the second, retaining selected conservative overlap policy |
| `halt_id` | string | Y | Metadata-linked interval ID when `halt_active`; null otherwise |

Accepted source status describes observation trust, not universal validity of its payload. A structurally accepted crossed quote can have quote age and unavailable midpoint. Accepted historical metadata remains distinguishable from measured local receipt evidence.

Halt and continuity overlays with precise boundaries and information-time provenance are identity-bound member metadata/control records. The one-second flags/IDs must agree with them. Do not infer outages from quiet tape, old quotes, or gaps in sequence numbers alone. The feature layer checks lag boundaries using these fields/overlays, not `price_valid_duration_ns == 1e9` throughout the lag.

## 4. Published features

Exactly nine EW features per half-life. Replace `{h}` with **30 and 120**, yielding 18 separate nullable float64 columns. Literal column order is the following order for h=30, then the same order for h=120.

| Exact name template | Unit | Meaning |
|---|---|---|
| `midpoint_rms_5s_bps_hl{h}s` | bps | Square root of EW mean squared five-second endpoint returns |
| `movement_participation_hl{h}s` | dimensionless | Squared EW absolute-return mean / EW squared-return mean |
| `quoted_spread_bps_hl{h}s` | bps | EW spread integral / EW valid spread seconds |
| `trade_rate_per_second_hl{h}s` | trades/s | EW eligible count / EW supported activity seconds |
| `share_rate_per_second_hl{h}s` | shares/s | EW eligible shares / EW supported activity seconds |
| `dollar_rate_usd_per_second_hl{h}s` | USD/s | EW eligible dollars / EW supported activity seconds |
| `bid_size_mean_shares_hl{h}s` | shares | EW bid-size integral / EW supported bid-size seconds |
| `ask_size_mean_shares_hl{h}s` | shares | EW ask-size integral / EW supported ask-size seconds |
| `midpoint_rms_5s_to_spread_hl{h}s` | dimensionless | Current RMS / corresponding current mean spread |

Add six nullable float64 columns, units seconds, in this order for w=60 then w=300:

- `trade_age_p90_seconds_window{w}s`
- `quote_age_p90_seconds_window{w}s`
- `midpoint_change_age_p90_seconds_window{w}s`

Every one of these 24 physically stored feature columns has its own nonnull uint16 `<feature_column>_reason_mask`, in the same order after numerical columns. The query view also exposes `trade_age_seconds`, `quote_age_seconds`, `midpoint_change_age_seconds` and their masks from base: **27 queryable measurements**. No EW smoothing of ages, no extra smoothing of ratios. Mean absolute return is internal state, not an additional public feature.

Derived inspection fields `midpoint_end_usd`, `spread_end_bps`, per-second eligible VWAP and average trade size are computed from base by readers, with dependency/domain checks; they are not added to the stored headline feature schema in this review.

## 5. Coverage/support table

Keyed identically to the feature table; raw inputs are not needed to explain publication. Avoid repeating the same coverage for every derived feature.

For each h in {30,120} store these **nonnull float64** fields:

- `return_usable_weight_hl{h}s`, `return_possible_weight_hl{h}s` — observation weights, dimensionless.
- For f in {`spread`, `activity`, `bid_size`, `ask_size`}: `{f}_usable_exposure_seconds_hl{h}s`, `{f}_possible_exposure_seconds_hl{h}s` — EW supported/possible seconds.

All are nonnegative; usable ≤ possible within declared numerical tolerance; possible=0 means coverage undefined. Reader-facing coverage is derived as usable/possible and is not redundantly persisted. Return RMS and participation share return coverage; all three activity rates share activity exposure; the ratio exposes both return and spread coverage.

Store nonnull int64 `quote_ew_startup_elapsed_seconds` and `trade_ew_startup_elapsed_seconds`: wall-grid startup ages for quote-derived families and activity respectively. Session/accepted halt resets restart both; bounded source gaps restart neither. This avoids making an affected-source rebuild erase the other source’s startup history. Any safety rebuild must restore the prior valid state or explicitly restart only its affected clock under the transition contract.

For a in {`trade`, `quote`, `midpoint_change`} and w in {60,300}, store nonnull int32 `{a}_age_sample_count_window{w}s` and `{a}_age_elapsed_slots_window{w}s`. Each is 0..w; elapsed is capped at w. Startup and coverage remain distinct: elapsed=w is mature; sample_count≥ceil(0.8w) is enough exact-age coverage. Current exact age need not be available for its historical p90 to pass these rules.

No stored per-feature boolean `valid` is needed: it derives from the mask. Public readers may expose that convenience field. Source-specific current availability comes from base context. Restart sufficient state is separate from these diagnostics and is defined in the checkpoint work item.

## 6. Reason codes and null contract

Use one versioned uint16 bitmask vocabulary, independent of legacy bit values. Multiple independently applicable reasons may be ORed. Codes are stable within the new schema identity; unknown bits are structural errors. Public readers decode masks to the strings below so users do not need to interpret integers.

| Bit value | Name | Applies when |
|---:|---|---|
| 1 | `SOURCE_UNAVAILABLE` | Required source observation unavailable at the relevant current publication point |
| 2 | `SOURCE_UNVERIFIED` | Required source trust/admission cannot be established |
| 4 | `HALT` | Selected halt policy closes the row |
| 8 | `STARTUP` | The measurement's required startup history is incomplete |
| 16 | `LOW_COVERAGE` | History is mature enough to assess coverage, but supported weight/exposure/sample fraction is below threshold |
| 32 | `NO_SUPPORTED_DATA` | No usable exposure, lag input or supported history exists for this measurement |
| 64 | `INVALID_CURRENT_VALUE` | Actual current measurement fails value/semantic validity; used for endpoint/base current outputs, not as a blanket history gate |
| 128 | `NO_OBSERVED_EVENT` | No qualifying event has been observed within trusted continuity to establish this exact age |
| 256 | `MIDPOINT_AGE_LOWER_BOUND_ONLY` | Continuous valid midpoint observation exists but no change has been seen; exact age is unknown |
| 512 | `CONTINUITY_BREAK` | A direct observation/return would bridge a declared continuity break |
| 1024 | `ZERO_RETURN_VARIATION` | Supported return magnitudes are all zero, so participation is undefined; RMS remains valid zero |
| 2048 | `ZERO_SPREAD` | Otherwise available mean spread is zero, so RMS/spread is undefined |

No `LATE_TRADE` publication bit: a known late trade is outside the selected population and does not invalidate otherwise observed activity. No `PRE_DISCOVERY` or `CARRIED_HISTORY` bit: discovery is selection eligibility; known gaps preserve decayed EW state under explicit coverage, and accepted halt resets remove pre-halt EW history. Mathematical validity is distinct from query selection.

A historical feature **does not inherit** current endpoint/age invalidity by blindly ORing base masks. It evaluates its own history, source and domain rules. A ratio combines the reasons of its two historical components; it adds `ZERO_SPREAD` only when the spread component is otherwise valid and zero. Participation adds `ZERO_RETURN_VARIATION` only when supported/mature/covered RMS would otherwise be publishable. Failure to evaluate a predicate is not proof of every downstream failure.

For each published value or shared-value tuple:

- `reason_mask == 0` iff the value is nonnull, finite, in domain and admissible under that measurement's rules.
- `reason_mask != 0` iff the public value is null. Multiple genuine independent failures may be represented, but derived consequences are not blindly added.
- Zero exposure has a zero-valued duration, null numerator/mean, and nonzero reason. Positive supported exposure can produce an actual zero numerator/value with mask zero.
- Lower-bound metadata is permitted with an unavailable exact midpoint age: its validity follows `midpoint_age_status`, not the exact-age mask.
- Overflow, malformed output masks, source hash mismatch and broken keys are integrity/implementation errors, not ordinary market-data reasons. Do not suppress them as a null feature and declare successful completion.

Examples: supported unchanged midpoint → RMS 0 / mask 0; participation null / 1024. Supported locked quotes → mean spread 0 / mask 0; RMS/spread null / 2048 if RMS itself is available. Midpoint continuously unchanged since observation began → exact age null / 256 and a nonnull lower bound. A crossed current quote can yield null endpoint prices / 64 and a valid quote-event age / 0. Enough historical support can keep RMS/p90 nonnull despite their current primitive being unavailable.

## 7. Metadata and decision boundary

Constant details are stored once per member/release, not repeated every second: base/feature schema IDs, semantic configuration and hash, source hashes/admission status, currency, size units/conversion, source decimal precision evidence, selection/discovery endpoint and knowledge basis, session boundaries, halt/continuity overlay identities and retrospective/live provenance, synthetic/prefix/full coverage and calculation identity. Query output exposes discovery eligibility alongside feature validity. Missing measured receipt time stays unknown and is not inferred from SIP time.

This schema review deliberately does not freeze the complete manifest/checkpoint JSON structures or serving layout. Those belong to the remaining Phase 0 interface/state work. It also does not certify precision/units of unseen canonical files. The definition review and contract implementation are complete. See [the implemented contract](../../docs/contracts/endpoint-ew-v1.md) and [completion evidence](phase_0_completion.md). The source precision/unit admission rule is implemented; no unseen canonical object is certified by this schema. Complete manifest/runtime writers and feature calculation follow in Phases 2/3; restart initially reconstructs incomplete members from session start.
