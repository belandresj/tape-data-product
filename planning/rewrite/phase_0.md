# Phase 0 implementation specification: close the contract

**Status: Phase 0 contract implementation complete, 2026-09-14. Production replay/features remain Phases 2/3. See [completion evidence](phase_0_completion.md).**

## 1. Purpose and authority

Most product decisions are already made. The [product direction](README.md) owns the selected measurements, equations, defaults, workflow, architecture, and exclusions. Phase 0 translates those choices into an exact contract, executable schema/configuration checks, and a preserved compatibility baseline. It is not another feature-design study and does not implement the production replay or EW engine.

The [master plan](phases.md) owns sequencing. Existing behavior remains governed by [the implemented feature contract](../../docs/reference/v1/feature-contract.md), [compact layout](../../docs/reference/v1/compact-layout.md), and [query contract](../../docs/reference/v1/query-contract.md). Proposed choices below must not change legacy identities or become implementation defaults merely because they appear in this draft.

Phase 0 delivers:

1. An accepted numerical availability and clock contract, with a decision record for the few choices the README explicitly leaves open.
2. Exact versioned base, feature, metadata, reason, and checkpoint schemas; public interface definitions and package dependency boundaries.
3. Small independent examples and contract tests, plus bounded regression evidence for the existing product.
4. A reproducible code baseline and a separately preserved snapshot of ignored planning documents.

Excluded: VPS provisioning, raw transfers, historical builds, production replay/EW implementation, query redesign, report regeneration, empirical parameter optimization, and changes to AGENTS.md. These belong to later phases or separate explicit scope.

## 2. Already selected; do not reopen

| Contract area | Read directly from the product README |
|---|---|
| Sampling | One-second intervals `[t−1s,t)`; prevailing quote strictly before the endpoint; five-second endpoint log returns published every second |
| Movement | EW RMS, not mean absolute movement or demeaned standard deviation; participation uses the same absolute/squared moments and weights |
| Recency | 30s/120s EW half-lives for moments, spread, rates, and sizes; ratios have no extra smoothing |
| Freshness | Three exact current ages and three ordinary linear-interpolated p90s at each of 60s/300s; midpoint-age lower bounds excluded from exact quantiles |
| Base | Separate price TWAPs, endpoint quotes/sizes, spread and size integrals/exposures, counts/shares/dollars, freshness and observation quality |
| Queries | Explicit feature rules; strict matching observations/runs by default; only requested feature dependencies gate availability |
| Architecture | Immutable raw T/Q → persisted one-second base → versioned features; local VPS queries, R2 archive, installed package/CLI |
| Claims | Measurement and descriptive retrieval; no prediction, profitability, endpoint/TWAP comparison, or noise-model acceptance gate |

Do not duplicate all equations in several documents. The final contract references the README equations and specifies their missing initialization, support, numerical, and interruption details.

## 3. Inspected implementation and compatibility boundaries

Paths below are relative to the repository root and describe the current working tree, not proposed new behavior.

| Existing code | What it provides / what must change later |
|---|---|
| `features/api.py::build_partition`, `build_inventory` | Installed raw-to-compact entry points, source/selection admission and immutable-input checks. Keep existing calls and outputs compatible; introduce separate base-to-feature interfaces. |
| `features/direct_frozen_product.py::events` | Bounded projected decoding, strict `(sip_timestamp, sequence_number)` ordering, unknown-code rejection, session-aware trade eligibility. Current quote projection discards bid/ask/size fields after decoding, so it cannot directly supply the new base. |
| `features/economic_tape_state_v3.py::primitives` | Session grid, pre-session quote seeding, halt-overlap closure, state clearing, event cursors. Its midpoint output is a TWAP, not the selected endpoint input. Whole-source tail validation occurs on complete builds. |
| `features/build_market_state.py::_quote_batch_semantics` and `activity_trade_semantics` | Existing price/condition and eligible-trade rules. Locked quotes can have valid prices; crossed/nonfirm/one-sided states cannot. Legacy depth validity requires both sizes positive; this is not yet the new per-side size contract or proof of source units. |
| `features/all_feature_month_core.py::AgeReplay` | Exact subsecond midpoint-change observation, known/no-change/unobservable states, spread integration, 80% freshness support. Preserve observed invalid/recovered transitions, including same-timestamp transitions. |
| `features/economic_tape_state_v3.py::RollingValues` | Exact p90 using a bounded queue and sorted list. Binary search does not make insertion/deletion O(log H): list shifts cost O(H). Reuse the quantile logic with declared bounds. |
| `features/direct_frozen_product.py::Reducer`, `rolling_tape_state_v2_halt_clock.py` | Legacy rolling histories omit accepted halt rows and can retain pre-halt history. This must not silently become the new EW wall-clock contract. |
| `features/compact_product_schema.py`, `compact_product.py` | Legacy 18 fields, separate support, reason masks, schema/calculation identities, integrity versus numerical audit distinction. New schemas must have distinct identities. |
| `query/api.py`, `tape_cohort_state.py`, `tape_cohort_reader.py`, `query/release.py` | Ordered endpoint processing, strict-run reduction, causal confirmation, verified membership and unavailable/zero-match accounting. Legacy selected hysteresis configuration remains legacy. |
| `analysis/`, `stages.py`, `storage/catalog.py` | Reusable reducers, stage receipts and canonical admission. New contract code must not import report calculations or operational runners. |

Current calculation dependencies include `api → direct_frozen_product → all_feature_month_core → economic_tape_state_v3 → build_market_state`, and schema/moment dependencies on historical modules. Phase 0 records these dependencies; Phase 2 extracts shared behavior with parity tests before any removal. A bulk module rename is not a Phase 0 deliverable.

## 4. Remaining decisions, revised against the first implementation

**Accepted revision, 2026-09-14:** The user accepted D1–D7 with subsequent endpoint-return and robustness refinements. The table below is the current selected direction; it supersedes the earlier draft recommendations. Phase 0 supplies exact schemas, state transitions and verification requirements for these decisions; production replay/features follow in Phases 2/3. The [robustness review](phase_0_robustness_review.md) retains rationale and limits.

| ID | Plain-language question | Selected behavior |
|---|---|---|
| D1 | How do averages start, and when is old history discarded? | Normalize over actual usable observations with no fabricated zeros. Retain untruncated recursive EW state within a session/halt-reset period; known bounded observation gaps age rather than erase that state. |
| D2 | How long do we wait, and how much missing data do we tolerate? | Default initial/post-halt startup is 60s fast / 300s slow, distinct from 30s/120s half-lives. Default EW coverage is 90% spread, 80% returns/rates/each size side. Thresholds are explicit versioned configuration, with exclusion/recovery measured on representative data. Crossing a coverage threshold changes publication, not estimator memory. Fixed age windows retain full 60s/300s maturity and 48/240 exact supported samples. |
| D3 | Does one missing current observation invalidate a historical summary? | No blanket current-value gate. Require trustworthy dependencies, no active halt, startup, coverage, and mathematical domain. Current measurements have their own validity. Suppress affected publication during a declared source outage; on recovery historical summaries can return when coverage permits. Ordinary unusable values remove only their own support. |
| D4 | What survives a halt or feed gap? | Session/accepted halt boundaries reset as selected. A known bounded feed gap preserves and decays EW moments/exposures, advances possible weight, invalidates returns crossing the gap and restarts affected event-age origins. Do not impose a new EW startup solely for that gap. Preserve unaffected source families. Untrustworthy clocks, source/identity changes and invalid checkpoints require explicit rebuild/reset. Age-window resets remain as previously selected; preserving age-window samples across declared gaps is not additionally authorized by the EW proposal. |
| D5 | How do freshness and p90 work? | Reuse exact event timing, within-second midpoint changes/reversals, midpoint observation lower bounds and bounded linear p90. New quote-event age is time since a structurally accepted quote message, independent of price/size validity; that rule also defines samples for quote-age p90. Midpoint-change age still requires continuous valid midpoint observation. Trade age remains since the latest eligible timely trade report. |
| D6 | What do sizes mean? | Per-side positive finite size support plus valid two-sided price state; one side's missing size does not invalidate the other. Verify source units; preserve decimal trade-share quantities and existing decimal-size precedence. Known ineligible trades do not create missing exposure merely because they fail the activity population filter. |
| D7 | What about locks/crosses and local malformed records? | Otherwise valid positive equal bid/ask prices contribute zero spread with positive exposure. Crossed/nonfirm states remain unavailable. A lock flag with unequal prices must be explicitly classified and never forced to zero. Localize understood bad/unknown-record effects; preserve source records and unaffected measurements instead of failing a day automatically. Unknown semantic effects must not be silently treated as harmless. Retain strict checks for identity, interpretable clocks, conflicting keys and output integrity. |

### Activity reporting-age contract

Retain `max_trade_reporting_age_ns = 1_000_000_000` as the default, included in the semantic configuration identity. For an otherwise eligible trade, require `0 <= sip_timestamp - participant_timestamp <= max_trade_reporting_age_ns`. Negative or unavailable/untrustworthy reporting-age inputs are not eligible; timestamp uncertainty is accounted for separately from a known late-report exclusion. Keep the existing condition and causal correction-payload population; correction/cancel action rows are not new executions.

Bucket eligible trades by **SIP timestamp** in `[t−1s,t)`, including their count, shares and dollars together. A report over the maximum age contributes to none of those activity totals and does not reset eligible-trade freshness. Never backdate it into its execution-time bucket. A report within the maximum age can cross a second boundary and still count in its SIP-arrival bucket. The threshold is reporting age at the SIP, not age at the output endpoint or network arrival at a user's machine.

A reliably observed second containing only known late/ineligible reports is valid zero eligible activity. Unknown eligibility or source loss is different and must not be turned into a fabricated zero. Preserve source records and bounded exclusion accounting, including lateness, condition and correction reasons, outside the headline MVP feature set.

The installed acquisition transport writes structurally validated records in SIP order and preserves participant/SIP timestamps, conditions, corrections and size inputs; it does not filter late records out of the raw tape. Replay applies `activity_trade_semantics` and the second reducer consumes only eligible trades. Thus historical retrieval is a SIP-clock replay containing reports with different execution ages, followed by a timely-activity filter. It is not proof of the exact historical vendor/network delivery stream. No per-record local receipt timestamp or guarantee of an unchanged historical vendor snapshot is inferred.

### What the review actually established

- `direct_frozen_product.Reducer.push` implements spread coverage with `duration * 10 >= 9 * h * NS`. `test_exact_duration_thresholds_do_not_fail_from_subsecond_roundoff` checks exactly 90% spread and 80% displayed-depth support. The initial blanket 80% proposal would have weakened spread admission without a reason.
- `test_age_eighty_percent_support_is_not_shortened_clock` deliberately makes the last 12 of 60 current ages unavailable, keeps the p90 finite with 48 supported ages, then makes it null when support drops to 47. Historical-summary validity and exact-current-age validity are separate. The initial D3 proposal contradicted this tested design.
- `test_maturity_counts_halt_carry_and_boundary`, the `carried_history` reason mask, and the query contract establish that carried values are not eligible primary query observations. Keeping diagnostic state is not the same as treating a halt as normal quiet trading.
- The old five-second movement calculation compares supported one-second TWAP endpoints; it does not prove uninterrupted instantaneous midpoint observability throughout the lag. The accepted new endpoint-return rule below does not require uninterrupted valid intermediate quotes. Only endpoint usability and declared continuity/halt boundaries gate the lag; midpoint-change age remains a different continuous-observation measurement.
- The old combined depth gate is appropriate to that combined metric; applying it unchanged to independent new bid/ask size measurements would unnecessarily discard the valid side.
- `_analytic_trade_size` preserves decimal-size precedence but casts both paths to float64. That is insufficient to promise exact preservation of decimal source quantities; this review does not establish a material error in historical dollar-rate results.

### Source-unit evidence (checked 2026-09-14)

[Massive's current stock quote documentation](https://www.massive.com/docs/rest/stocks/trades-quotes/quotes) defines stock quote sizes in shares. Its [unit-change notice](https://www.massive.com/blog/change-stocks-quotes-round-lots-to-shares) describes the November 3, 2025 transition from round lots to shares and regeneration of historical flat files. Therefore use a factor of one for a source verified to have share units; never multiply every historical object by 100. Saved object provenance matters because historical files may have been regenerated; a trading date alone does not establish the units of an archived object. This review did not inspect or migrate canonical R2 objects and does not establish their units individually.

[Massive's trade documentation](https://www.massive.com/docs/rest/stocks/trades-quotes/trades) defines `decimal_size` as a string containing the fractional trade size. The installed acquisition schema already retains it as a string. Preserve exact decimal input for totals, with a declared bounded scale/precision and explicit rejection of unsupported values; do not silently truncate or infer precision already lost in an old float-only source.

The final D4 contract must supply a transition table for: session start/end; 09:30 and 16:00 reporting boundaries; ordinary value invalidity/recovery; quote-source loss/recovery; trade-source loss/recovery; missing grid rows; halt entry, overlapping partial seconds and first fully open second; discovery; and checkpoint resume. For every event, state which price/event clocks, five-second lag, EW families, and age windows reset, decay, retain or become unavailable. Missing physical rows should fail grid validation; an explicitly represented unavailable second still advances time.

Recommended retained session behavior: build 04:00–20:00 America/New_York with UTC ns endpoints and DST-aware conversion; no estimator reset merely because reporting strata change at 09:30/16:00. Retain the latest prevailing quote within the existing 300s pre-session seed interval, including an invalid prevailing state; never search backward for an older valid quote. No pre-session trade seed or synthetic return endpoint is inferred. Record whether seed coverage is verified. Warm-up starts with actual base endpoints; the first possible five-second return is endpoint six, absent an explicitly stored seed endpoint contract.

**Accepted endpoint-return clarification (2026-09-14):** A five-second return requires finite positive valid prevailing midpoints at both endpoints and no halt or declared source-continuity break across the interval. Intermediate invalid quote states, including brief crossed quotes and invalid/recovered transitions at the same timestamp, do not by themselves invalidate this endpoint return or reset its EW history. An invalid endpoint removes only returns that use it; never substitute an older valid quote. A declared break still forbids a return across it. Quote-valid duration continues to govern exposure measurements. Midpoint-change age retains its stricter continuous-observation semantics because an exact age makes a different claim from an endpoint return. Preserve the quality information necessary for each measurement without imposing a universal uninterrupted-quote gate.

The user subsequently accepted robustness-review items 1–5. Their effects are incorporated in the selected table above: localized semantic exclusions, EW continuity through known bounded gaps, price-independent quote-event age, configurable coverage thresholds with measured exclusion/recovery, and valid locked quotes included in spread exposure. The existing maximum trade reporting age is retained.

Discovery is an eligibility gate, not an estimator reset: pre-discovery accepted data may initialize measurements. Preserve source endpoint and receipt/knowledge timestamp separately. Do not describe a historical bar-close proxy as measured live availability. The accepted contract must define eligibility for each timing basis and expose ex-post halt overlays as retrospective information; unknown receipt timestamps do not establish live reproducibility. No full source receipt should be fabricated to admit historical files.

## 5. Numerical contract to encode

Under accepted D1–D3, for each second and half-life use `lambda = 2**(-1/h)`:

- Return moments: `U2 ← lambda*U2 + I*r²`, `U1 ← lambda*U1 + I*abs(r)`, `W ← lambda*W + I` where `I` indicates supported return. Branch on unsupported input; do not multiply a NaN by zero. `Q=U2/W`, `A=U1/W` when W>0.
- Possible return weight: advance `P ← lambda*P + 1` at every lag-eligible wall-clock endpoint after the first five endpoints of an epoch, regardless of actual quote validity. Coverage is `W/P`. Startup age is separately measured from estimator initialization/reset. A bounded feed gap resets lag continuity, not estimator age or possible-weight history; all gap and lag-recovery slots after initial warm-up advance possible return weight even when no return can be admitted. Exact first eligible endpoint must be tested.
- Exposure families: `U ← lambda*U + numerator`, `D ← lambda*D + supported_seconds`, `P ← lambda*P + 1 second` per grid row. Coverage is `D/P`; estimate is `U/D`. Missing support contributes zero numerator/exposure but still advances possible exposure and decay. Compute each source/side independently.
- Publication of historical summaries requires accepted dependencies/continuity, no active halt, startup, family-specific coverage (90% spread, 80% other EW families), and mathematical domain. There is no blanket requirement that the newest numerical observation be supported. Current measurements retain their own validity. Fully observed no-trade time contributes positive exposure and zero totals. All-zero supported returns give RMS=0 and undefined participation; no epsilon. RMS/spread requires strictly positive available spread.
- Exact p90: queue of H wall-clock slots plus sorted supported ages; discard unavailable ages and lower bounds from the sorted values but retain their slots. For n supported ages, interpolate at `0.9*(n−1)`. H is 60 or 300 for defaults. Update O(H), query O(1), memory O(H); declare a maximum supported alternative H before exposing arbitrary windows.

Phase 0 must settle binary64 rounding/tolerance, underflow/overflow rejection or stable scaling, exact decimal-share accumulation, and checkpoint round trips. Nonfinite output must never be silently serialized. “Exact p90” means an exact order statistic of stored supported age values, not arbitrary-precision EW moments. Test tiny positive returns so underflow cannot silently relabel a nonzero history as mathematically all-zero participation.

## 6. Schema and interface work

The [accepted column/type/null/reason contract](phase_0_schema_review.md) was approved by the user on 2026-09-14. It owns exact field names, types, units, null conventions and reason bit values, superseding earlier naming suggestions. The installed contracts package implements the registry and bounded schema checks. decimal128(38,9) is a lossless supported-input contract enforced by source admission; individual unseen historical members are not thereby certified.

Use proposed new identities `tape_base_1s_v1` and `tape_features_endpoint_ew_v1`; legacy remains `tape_data_product_v1` / `tape_product_compact_v1`. These names are draft identifiers, not existing released schemas. Configuration identities include all D1–D7 choices, timestamp/eligibility rules, source-unit conversion, quantile method, estimator numerical policy, and transitive calculation dependencies.

Phase 0 implementation writes an explicit field-by-field Arrow schema and registry, not an inferred schema from a sample. Required groups:

| Layer | Required fields and types to finalize |
|---|---|
| Shared key | `session_date` canonical date string, `symbol` string, `interval_end_ns` int64 UTC, nonnull and unique/increasing within member |
| Price base | Bid/ask/midpoint TWAP float64 with common int64 ns exposure; endpoint bid/ask float64 and validity; enough duration, transition and epoch information to reconstruct lag support |
| Spread base | Full-spread integral float64 bps-seconds, int64 supported ns, separate endpoint validity context |
| Size base | Per-side normalized endpoint size, shares-seconds integral, int64 supported ns and independent validity; source scale/semantics in metadata |
| Trade base | Eligible count int64, exact share total in an explicitly selected integer/decimal representation, dollar total with declared precision, int64 reliable exposure ns |
| Freshness base | Current ages in seconds; latest-event/observation-origin timestamps in ns; explicit midpoint known/lower-bound/unobservable status and lower bound |
| Quality base | Per-source acceptance and continuity, epoch/reset information, halt overlap and provenance reference, per-second usability; constant discovery/source details in member metadata |
| Feature values | RMS, participation, spread, trades/shares/dollars per second, separate bid/ask sizes, RMS/spread at each EW half-life; current ages and six freshness p90s |
| Feature diagnostics | Per-family usable/possible weight or exposure, coverage, startup age, per-feature reason/validity, separate current-input validity, plus ratio component dependencies |
| Member metadata | Source hashes/admission evidence, selection/discovery clocks, halt/continuity source and knowledge basis, session grid, synthetic/prefix/full status, schema/config/code identities |

Name EW fields with explicit `hl30s`/`hl120s` and p90 fields with `window60s`/`window300s`; never make `30s` ambiguously mean a return lag, rolling window, or half-life. Keep mean absolute return internal. Base endpoint values and derived inspection fields remain accessible without promoting excluded features to headline query dimensions.

Use Arrow null for unpublished values, never stored NaN/Inf. Define versioned reason bits for source unavailable, startup, insufficient coverage, current input unavailable (for current measurements only), halt/reset, undefined zero-movement participation, and nonpositive-spread ratio. Use the accepted schema document’s enumerated bit values and applicability rules; enforce them in the schema registry. Keep discovery/query eligibility distinct from mathematical validity. Do not reuse legacy mask interpretation for new bits. If finite excluded diagnostics are retained, give them separate fields; public null/value and validity cannot contradict each other.

Proposed package boundaries and public calls (names may be refined without changing measurement choices):

- `contracts/`: schemas, defaults, reason registry, configuration validation and semantic identity. No replay, storage-client, query or plotting imports.
- `replay/`: shared decoded event/state contracts and extensible accumulators; `replay.api.build_base_partition(source_pair, member_context, output, *, config, batch_size=4096)`.
- `features/`: pure base-to-feature state machine; `features.api.build_from_base(base_partition, output, *, config, batch_size=4096, checkpoint=None)`. No raw source access in this call.
- Keep existing `features.api.build_partition` and legacy query entry points compatible. Add distinct CLI verbs such as `tape-product base build` and `tape-product features from-base` in later implementation phases.
- Storage owns identity/admission/completion; query owns rule selection and run timing; analysis consumes verified feature releases. Dependency direction is contracts → consumers, with feature calculation depending on the base schema rather than replay implementation.

Phase 0 defines callable signatures, input validation, return manifest types, exception classes, and resume invariants; executable builders follow in Phases 2/3. Do not add placeholder commands that claim to build data.

Restart contract (selected for the first implementation): verified completed-member reuse or reconstruction from session start. `contracts.interfaces.CompletedMember` binds member/input/configuration/code/manifest hashes and row count. `BaseBuilder` and `FeatureBuilder` are structural protocols only. Arbitrary partial replay/EW checkpoints are deferred; no diagnostics row may be used to resume empty estimator state. This supersedes the earlier draft requirement to design and implement all partial checkpoint fields upfront. Detailed timing, transition, numerical, source-admission and manifest obligations are in the [implemented contract documentation](../../docs/contracts/endpoint-ew-v1.md).

Physical sharding, query engine choice and performance tuning remain later measured decisions. Phase 0 specifies logical tables, keys and manifest interfaces without prematurely selecting a serving database.

## 7. Implementation sequence and verification

1. Encode accepted D1–D7, robustness items 1–5 and the activity reporting-age contract into an exact transition table and versioned configuration. Resolve remaining interface/numerical representation details without changing these choices.
2. Add the contract/registry/configuration modules and exact schema tests. Test identities changing whenever a semantic parameter changes; reject unknown fields, invalid units, nonpositive half-lives, malformed clocks, and unsupported reason bits.
3. Write small independent reference examples without importing production estimator reducers. Approve expected values and null/reason masks before Phase 2/3 implementation.
4. Preserve baseline and run bounded compatibility checks. Review schema dependencies and tests before preparing Phase 2's implementation spec.

Required independent examples:

| Fixture | Expected result / invariant |
|---|---|
| Quotes at `t−1ns`, `t`, `t+1ns` | Endpoint t uses only the first; later events affect subsequent rows, with deterministic same-timestamp sequence ordering |
| Constant supported midpoint | RMS 0 after publication gates, participation null, no-change age lower bound excluded from midpoint p90 |
| Constant five-second return magnitude c>0 | RMS=c and participation=1 wherever gates pass |
| One sampled-price jump | Five overlapping return observations can be affected; this is not five independent events |
| Unequal exposure | With lambda=1/2 in a test-only configuration, prior spread integral 10 over 1s and current integral 10 over 0.5s yield `(5+10)/(0.5+0.5)=15` bps, not EW averaging of the per-second ratios |
| Equal-weight return magnitudes 0 and 2 | Q=2, A=1, RMS=sqrt(2), participation=0.5 in the reference moment calculation |
| Ages 0,1,2,3 | Linear p90=2.7; duplicates and unavailable slots evict correctly |
| Invalid quote and recovery between valid endpoints | Return remains usable without a declared break/halt; quote exposure excludes invalid duration and midpoint-change age restarts observation |
| Invalid sampled endpoint | Returns using that endpoint are unavailable; other supported returns and EW history remain intact |
| Bid size unavailable, ask size valid | Per-side support behaves according to accepted D6; no accidental universal complete-case mask |
| Historical p90 with unknown current age | 48 known ages in a mature 60-slot window permits p90 while exact current age is null; 47 does not |
| Spread coverage boundary | Exactly 90% weighted supported exposure passes; immediately below fails; other-family thresholds remain separate |
| Otherwise valid numeric lock | Zero spread numerator, positive spread exposure; zero mean spread still makes RMS/spread undefined |
| Structurally accepted crossed quote | Quote-event age resets, midpoint/spread remain unavailable; trade features unaffected |
| Known bounded feed gap | EW history decays and coverage falls; no return crosses gap; no automatic 300s EW restart on recovery |
| Timely versus late trade | At 1s reporting age eligible if other gates pass; 1s+1ns excluded from count/shares/dollars and eligible-trade freshness; no backdating |
| Reliable no-trade second versus source outage | Zero count with positive exposure versus unavailable; no fabricated inactivity |
| Halt/source break and reporting boundary | Exact accepted transition table, no return across a break; reporting strata alone do not restart state |
| Restart at every small-fixture endpoint | Same values, masks, weights, and identities as uninterrupted execution; changed config/source checkpoint rejected |

Run batches of 1, small uneven sizes, and 4096; exercise event ties across batch boundaries and corruption before completion. Legacy tests must retain all 18 values/masks, support, ordering, discovery and query timing. Numeric reconstruction is separate from file-hash integrity. No historical regeneration or new research finding follows from synthetic success.

## 8. Resource and baseline evidence

Phase 0 is local/offline and sequential. Contract tests use small invented data and fake clients. Use `scripts/measure.py` for owned-worker process-tree RSS monitoring at 50ms with its stricter 768 MiB synthetic ceiling. Do not automatically rerun after a guard stop. General development build targets remain ≤2 GiB RSS with termination before 3 GiB; default projected batches 4096, maximum 25000.

Later replay: O(E) event processing plus O(NF) feature work and O(N·3·sum(H)) sorted-list age-window updates. Memory is bounded by input/output batches, six price endpoints, interval support, EW state, and fixed age histories. No per-event growing arrays, whole-day Pandas materialization, or corpus-sized in-memory membership ledger. Checkpoint/output disk use must be bounded per member and include partial-output overlap; impose actual numerical scratch caps in the measured Phase 2/3 specs.

No external sample was run for this planning task. Before a full external build, later phases must measure the exact production path on named, admitted session-start prefixes, including a dense member and interruption/initialization behavior where available. Record input events, output seconds, elapsed time, sampled process-tree RSS, transferred bytes, output/temporary disk and projections. Synthetic tests cannot establish VPS capacity or corpus throughput.

Baseline HEAD: `d2098f2f66f2c870a09710ce95e18589115a0df0`. The inspected tree has pre-existing tracked changes and an index deletion of AGENTS.md while the local file remains present. This task leaves those changes intact. HEAD alone does not identify the tested tree. The package resolves from this repository under Python 3.13.1; package support is `>=3.13,<3.14`.

Original planning baseline result (before this semantic-review revision): **69 passed**, pytest duration 16.03s; monitored command duration 19.509s; sampled peak process-tree RSS 186,515,456 bytes (177.9 MiB); exit code 0 and no guard stop. Selected suites: `test_direct_frozen_product`, `test_compact_integrity`, `test_product_stages`, `test_tape_cohort_query`, and `test_standalone`. This is a targeted baseline, not the full suite or historical regeneration.

The original tree snapshot and targeted semantics-review JSON/logs were historical verification outputs; they were removed during the 2026-09-14 cleanup. The tracked completion record retains the accepted verification summary.

The bounded baseline run and content-hash snapshot were originally recorded privately. Those temporary files were removed during the 2026-09-14 cleanup. For subsequent implementation, preserve a reviewed code commit containing the tracked planning documents and a concise completion record. Keep any necessary private source receipts outside Git.

## 9. Acceptance checklist

- [x] Product direction for D1–D7, robustness items 1–5 and maximum trade reporting age accepted.
- [x] Column names, types, units, null rules and reason codes accepted; exact decimal-share scale remains subject to source verification.
- [x] Exact transition contract and numerical policies documented; source units/precision supported-input requirements and provider evidence recorded. Every external member still requires its own admission evidence.
- [x] Arrow schemas, reason enums, config/implementation identities, completion-record validation and structural builder interfaces implemented and checked; arbitrary partial checkpoints explicitly deferred.
- [x] Small contract examples cover these cases; production replay/estimator reconstruction and resumed-output equivalence remain explicit Phase 2/3 acceptance work.
- [x] Final full offline suite passed: 245 tests, including 28 new contract cases. Installed-wheel smoke from outside the checkout passed.
- [x] Scoped code baseline and separate ignored planning/evidence snapshot recorded in the completion record.
- [x] Contract/dependency review completed; state transitions, local exclusions, numerical bounds, source admission and restart semantics are explicit. No production numerical engine or external-corpus validity is claimed.

Phase 0 is complete for its definition/interface scope. This does not authorize a full external run or establish production feature correctness. Durable documentation moves to `docs/` alongside the implementation it actually describes; durable plans live in planning/rewrite/ and private operational evidence remains ignored.
