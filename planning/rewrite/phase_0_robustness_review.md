# Phase 0 robustness review

2026-09-14. Planning evidence only; no production code or canonical data changed. The endpoint-return clarification is accepted and recorded in README.md and phase_0.md. The user accepted items 1–5 for Phase 0 on 2026-09-14; the product README and phase_0.md incorporate them. Item 6 retains the existing one-second maximum trade reporting age at the user’s direction. Optional source-order repair is not selected.

## Principle

Scope an impairment to the measurements, events and time that it affects. Preserve valid estimator history when its interpretation remains sound. Distinguish a malformed individual value, an unknown semantic code, a bounded observation gap, and an untrustworthy source identity. A tolerance policy must not convert missing trades to observed zeros or invent valid prices. This review identifies structural risks; it has not measured their frequency in the external corpus.

## Findings and recommendations

### 1. One unfamiliar or missing field can stop a whole stream build

`features/direct_frozen_product.py::events` raises on any unknown quote condition/indicator, trade condition, or correction code. `acquisition/vendor.py` also raises on unknown response fields and null required prices/sizes. These checks can turn a localized issue into a failed member build; they are candidates for redesign in the new product.

Selected: preserve trustworthy raw records and emitting explicit local semantic exclusions rather than rejecting a whole day merely for one unfamiliar record. Distinguish storage/schema acceptance from per-feature semantic usability. A missing bid size should not destroy valid price or ask-size evidence. An additive vendor field should not automatically be considered corrupt; preserve it through an explicitly versioned acquisition policy instead of silently dropping it.

Do not treat an unknown quote condition as harmless or keep an older good quote behind it. If it might change price validity, current price state becomes unknown until a usable update restores it. If an unknown trade record might be an execution, exclude its numerical contribution and conservatively mark affected transaction exposure uncertain; otherwise exclusion plus a full denominator would falsely count observed inactivity. Unknown correction effects may not be localizable: if their scope cannot be bounded, stop the affected dependency or escalate its source status. Never claim all unknown records are safely skippable.

Keep errors for unverifiable identities, uninterpretable clocks, conflicting event keys, and incomplete output commit state. Separate build failure from raw-data preservation.

### 2. Blanket EW reset after every source gap is stricter than required

A known one-second feed gap should not necessarily erase several minutes of valid EW history and impose another 300s startup. Distinguish estimator history from price/event continuity.

Selected: retain and decaying EW state through a bounded explicitly represented outage, with zero usable exposure and advancing possible exposure. Suppress affected current publication during a declared outage. Invalidate returns across the gap and reseed event-age origins after observation resumes. Once source observation resumes, historical summaries may publish if their weighted coverage is sufficient; do not add a full startup delay solely because one known interval is missing.

Do not bridge the gap with a return merely because the EW accumulators survive it. Short gaps can be handled without an arbitrary short/long duration threshold: weighted coverage already deteriorates with gap duration. Boundaries with changed/unknown symbol identity, invalid estimator checkpoints, or untrustworthy time require rebuilding/reset. Keep session and accepted halt resets as the current policy; changing post-halt economic history is a separate decision. No automatic resumption after a resource guard stop follows from this data policy.

Age-window historical samples can similarly occupy fixed wall-clock slots while current age origins restart; this is distinct from continuing an exact age across the gap. The accepted age-reset contract remains unchanged until this alternative is explicitly selected.

### 3. Price-gated quote age conflates feed freshness with price validity

`all_feature_month_core.AgeReplay.end_interval` and the primitive builder require a valid prevailing price state for current quote age. Yet a clearly timestamped crossed quote proves that a quote event was observed recently.

Selected: define new-product quote age as time since a structurally accepted quote event, independent of price validity; retain price validity separately. A trustworthy quote timestamp can establish message freshness even when its price/size content is unusable. This is a semantic change from V1, not a correction to legacy results. Midpoint-change age still requires valid continuous midpoint observation and must not be relaxed to match message age. Source uncertainty or untrustworthy timestamps still prevent an exact quote-event age.

### 4. 90% spread coverage can be sensitive to clustered invalid time

The accepted draft transfers V1's 90% spread / 80% other-family thresholds to EW coverage. Equal thresholds do not imply equal tolerance over wall time.

For fully observed mature EW state followed by k whole unsupported seconds, coverage is approximately `2**(-k/h)` (infinite-history limit). At h=30s, 90% is crossed after about 4.56s, hence the fifth full missing second; 80% after about 9.66s, hence the tenth. At h=120s, the corresponding times are 18.24s and 38.63s. Startup has different finite possible-weight accounting. No summary reset is necessary merely because it crosses a threshold; retain state and allow recovery as coverage improves.

One fully missing second accounts for about 2.28% of mature fast-view weight, so isolated small defects do not generally break an 80% gate. Clustered defects have more effect. These are algebraic response calculations, not observed corpus defect rates.

Selected: keep accepted thresholds provisionally, making them explicit configuration rather than hard-coded invariants, and measuring feature-specific exclusion time/recovery in representative data. Do not lower thresholds merely to increase match count. We already retain base measurements and quality evidence so feature validity does not erase the underlying observations.

### 5. Excluding every valid lock from spread can magnify the coverage issue

V1 excludes explicit/numeric locked states from spread exposure even when the midpoint is valid. That is coherent with V1's unlocked-spread identity, but should not be assumed necessary for every full-spread estimator.

Selected: include zero spread and positive exposure when both positive prices are equal and otherwise satisfy quote validity. Keep crossed/nonfirm states unavailable. A condition-only lock flag with unequal displayed prices is contradictory and must not be silently coerced to zero; the accepted semantics must specify it separately.

This avoids treating well-defined zero-width observations as missing exposure. It changes the spread definition and is selected for the new version. The RMS/spread ratio must still be unavailable for zero mean spread; a tiny positive spread is not proof of executable opportunity. Do not add an epsilon denominator. The new product includes otherwise valid numeric locks; the legacy unlocked-only identity remains unchanged.

### 6. Late trades and strict source ordering deserve classification, not blanket relaxation

`build_market_state.activity_trade_semantics` applies a participant-to-SIP reporting delay of 0..1s and a selected condition population. This can exclude delayed reports from transaction activity. It is a population choice inherited from current-pressure research, not a general truth that late reports are corrupt. Resolved by the user: retain timely eligible reports, with maximum participant-to-SIP age of one second inclusive. Assign by SIP time and never backdate late reports; known late exclusions do not invalidate reliable zero eligible activity. Do not silently widen eligibility: late reports can refer to old executions and correction/cancel action rows are not new trades. Quantify exclusions before changing this population.

The decoder also requires strictly increasing `(sip_timestamp, sequence_number)`. Known duplicate transport deliveries or input sorting defects can sometimes be repaired in an explicit bounded normalization stage, with identity, counts, and provenance. Replay itself still needs deterministic ordering. Do not deduplicate by price/size, silently choose between conflicting records with the same key, or sort by execution timestamp and backdate knowledge. Current canonical objects remain immutable; any repair is a new derived artifact with a new identity.

## Changes this review does not propose

No arbitrary price spike removal, generic stale-quote timeout, crossing-return interpolation, zero filling of unknown activity, clamping of ratios, or bridging of strict query runs across unavailable observations. Separate feature robustness from query presentation: optional gap-tolerant run summaries must explicitly label gaps rather than claiming uninterrupted matches. Preserve source byte checks, unit verification, real-time timestamp boundaries, and true zero-versus-undefined distinctions.

## Evidence and next verification

Findings come from local source inspection and the earlier semantic tests, which were not rerun for this document-only change. No external data were accessed for this review. Accepted changes need independent fixtures showing that defects affect only intended outputs: an invalid intermediate quote; a missing size on one side; a known feed gap without unnecessary EW erasure; trustworthy quote-event timestamps with invalid prices; an unknown semantic event with explicitly bounded uncertainty; and locks versus crossed quotes. Measure exclusions and recovery on the required representative sample before drawing conclusions about actual brittleness.
