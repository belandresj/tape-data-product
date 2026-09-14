# Raw T/Q → one-second base → default features: implementation specification

Status: design and implementation handoff, 2026-09-14. No production builder, sample acceptance, or full-build readiness is claimed by this document. This is the single execution brief for a Sol agent at medium reasoning. At the user's request, the Phase 2/3 design is supplied together rather than waiting to write all Phase 3 details after Phase 2; the internal base-acceptance checkpoint remains required. Implement Phase 2, accept its base evidence, then implement Phase 3 and prepare a manually launched calculation job. Do not stop after writing another plan. Do not launch or schedule the full calculation job.

## 1. Outcome, authority and execution boundary

Deliver an installed, independently checked pipeline that reads verified local canonical T/Q, persists `tape_base_1s_v1`, and calculates `tape_features_endpoint_ew_v1` plus its support table from base alone. Include source admission, bounded real-sample verification, restart/reuse, resource measurements, and a concrete full-build plan with a manual command. The user will start the full job after the separate transfer finishes.

The product definitions remain [README](README.md), [Phase 0 schema](phase_0_schema_review.md), and [implemented contract](../../docs/contracts/endpoint-ew-v1.md). Read those and the contract modules before implementation. This document supplies engineering choices and tests; it does not authorize weakening source evidence or changing mathematics. If a requirement cannot be represented by the accepted schema, demonstrate the conflict with a minimal fixture and obtain a specific decision instead of silently changing it.

Scope includes both default EW views and all freshness fields, not query/report redesign, performance layout tournaments, public data release, new acquisition, R2 modification, or a scheduler. Preserve all existing legacy CLI semantics and identities. Routine fixes, separate development worktrees, synthetic tests and the bounded local sample below are within implementation scope. The full external calculation requires the measured projection and manual user start.

Design baseline inspected: local/main VM repository `462e2e92c14e7493de4a65cff1ae2bc992910fcc`; active raw-transfer worktree `258607d` on `codex/raw-migration`. Local planning and owner-maintained AGENTS.md have existing edits. Capture the actual implementation baseline and preserve those changes. Do not assume an uncommitted local plan already exists on the VM. Share accepted specification changes through a scoped Git commit and fetch into the development checkout; do not copy source between machines or commit unrelated edits.

### Current source evidence, not an admission result

The running migration manifest summary inspected on 2026-09-14 contains 7,085 T/Q pairs, 43,734,785,941 raw bytes, and 163 dates. Including support objects: 14,306 objects and 47,204,810,169 bytes. Historical research membership is 6,222; 863 transferred members are outside that release. Do not silently substitute either population for the other.

Its evidence-gap counters are: 7,085 missing quote-unit declarations, 7,085 missing trade-precision evidence, 994 missing requested-window records, 994 missing pagination-completion records, 2,066 missing actual extrema, and six non-REST method records. These are inventory findings, not proof every member is irrecoverable. Inspect preserved source records. Precision can be checked during consumption; a units declaration requires evidence of the normalization actually used. Event extrema and file presence cannot establish successful terminal source coverage.

Private VM locators (paths only; do not publish their contents):

- Project SSH alias: `tape-data-product-vps`; follow [VM preflight](../../docs/vps-operations.md).
- Migration control root: `/srv/tape-data-product/control/raw-migration/8e77ed762721e336/`.
- Raw root: `/srv/tape-data-product/raw/r2-canonical-8e77ed762721e336/`.
- Retained sample: `/srv/tape-data-product/raw/phase1-sample/` and its transfer ledger.
- Migration preparation/evidence root: `/srv/tape-data-product/control/raw-migration-prep/`.

Resolve locators against the current migration handoff before use. Read the active SQLite ledger in read-only mode; never take its writer lock, edit its records, or mark its job complete. A copied file is eligible for consideration only after its object has a verified completion record. Full-run preparation does not poll, wait for, or take ownership of the transfer.

## 2. Deliverables and dependency order

Implement these checkpoints in one task, with separate evidence for each:

1. **Admission and independent fixtures:** source adapter/context schema, field mapping below, hand-calculated fixtures, no production-generated expected results.
2. **Base:** streaming raw replay, base writer/verifier, batch/restart tests and a small extensibility example. Independently review base before feature acceptance.
3. **Features:** base-only streaming EW/p90 calculation, support writer/verifier, independent reconstruction and alternate-configuration test.
4. **Installed sample:** build an isolated wheel, test outside checkout, run only the admitted bounded local sample in §10, measure both stages and checks separately.
5. **Manual job preparation:** source/member reconciliation, manifest-bound execution plan, resource projection and manual command. Report whether it is prepared but blocked, sample-verified with projection limitations, or eligible for the user's full-run decision.

Review each phase's implementation and evidence independently as required by the master phase plan. A reviewer may inspect code/fixtures and run synthetic checks; only the main task owns external sample execution. Fix concrete findings and rerun affected checks. Do not claim independence when verification simply calls the production calculator twice.

### Module boundaries and reuse

Add `tape_data_product.replay` for admission, source decoding, quote/trade reducers and base APIs; add `tape_data_product.features.endpoint_ew` for the new feature calculator and its bounded numerical state. Put shared new member-manifest/integrity and run-ledger utilities in a neutral package module. Exact private helper filenames can follow these responsibilities; public interfaces below are fixed.

Reuse contract schemas/configuration, `share_units`/`shares_from_units`, `endpoint_return`, reason helpers and validation. Reuse/adapt streaming hashing and atomic-write patterns from `features/compact_product.py`, and acquisition/storage evidence readers where their actual input contract matches. Do not force historical files through a fabricated modern `pair.json`.

`features/direct_frozen_product.py:events` is a batching/order reference, not the new decoder: it collapses quote state, converts share quantities through floating point and globally rejects unknown codes. `build_market_state.py` supplies the accepted code vocabulary/condition logic and Arrow list handling. Its joint depth validity and legacy lock exclusion do not implement independent bid/ask sizes or the new zero-spread rule. `economic_tape_state_v3.py` is a timing/freshness reference, not a new base schema. Keep legacy behavior unchanged; regression tests must cover intentional new-vs-legacy differences. No sibling imports or dependency on planning files at runtime.

## 3. Public APIs and CLI

Implement the accepted builder protocols:

```python
build_base_partition(source_pair, member_context, output, *, config=DEFAULT_CONFIG,
                     batch_size=4096) -> BuildResult
build_from_base(base_partition, output, *, config=DEFAULT_CONFIG,
                batch_size=4096) -> BuildResult
```

`source_pair` and `member_context` are paths to validated JSON descriptors. `base_partition` is a completed base directory, including its immutable context companion. Output is a new member directory or an exactly matching verified completed member. Coverage/prefix is explicit in context, never inferred from an incomplete file. Validate batch size 1..25,000 before opening input.

Add these installed commands with JSON summaries and nonzero failure exits:

```text
tape-product base admit --inventory PATH --evidence PATH --output DIR
tape-product base build --source-pair PATH --context PATH --output DIR [--config PATH] [--batch-size N]
tape-product base verify --input DIR [--reconstruction --source-pair PATH]
tape-product features build-from-base --base DIR --output DIR [--config PATH] [--batch-size N]
tape-product features verify-from-base --input DIR --base DIR [--reconstruction]
tape-product calculate plan --inventory PATH --admissions DIR --config PATH --limits PATH --output DIR
tape-product calculate run --plan PATH --expected-plan-sha256 HASH
```

Commands are proposed interfaces until implemented; existing `features build` and `features verify` remain legacy. `base admit` inspects metadata/evidence and writes admission descriptors and unresolved findings, with no raw replay, network access or fabricated success. A descriptor distinguishes metadata admission from consumed-row precision/order checks, which remain required during build. `calculate plan` is read-only with respect to raw/derived datasets; it writes planning artifacts only. Neither planning command downloads anything.

`calculate run` uses a disk-backed inventory/ledger and executes a member's base stage followed by its feature stage. It reuses a matching verified base if features need rebuilding. The first release supports one worker, no automatic retries and no partial-state resumption. A source failure records the member and stops; explicit revised plans may exclude members with a preserved denominator/reason. Full completion requires exact expected-member reconciliation, not merely exhaustion of runnable members.

## 4. Source descriptors, context and admission

Use strict versioned JSON with unknown-field rejection, bounded JSON size (1 MiB per member descriptor/context), UTC int64 ns, and canonical member identity. Evidence bodies stay private; descriptors carry hashes and relative references rooted in a declared local source/evidence root. Reject traversal and changed identities. Resolve legitimate raw paths through the configured root, not unrestricted manifest-supplied paths.

`source_pair` fields: version, symbol/session_date, currency (`USD`), source adapter identity, per-stream path/SHA-256/bytes/rows/schema hash, stream source/provenance identity, evidence references, and `SourceUnits` declaration. Preserve original units, conversion multiplier and its evidence. Bind quantity representation/precedence and coverage evidence to the exact object/lineage. A source method may be REST or another verified source method; method name alone is neither admission nor rejection.

`member_context` fields: version/member, output coverage (`session_start_ns`, `end_ns`, `expected_rows`, `kind=full|prefix`), per-stream verified observation intervals and evidence, precise gap intervals/instantaneous break events, halt intervals with stable IDs, unit/evidence references, and selection/discovery provenance. Each historical overlay carries information basis and known-at timestamp when available. Empty halt/break lists must say whether evidence establishes absence or the overlay is merely unavailable. Preserve unavailable knowledge metadata; do not assert measured live delivery. Nominal discovery/receipt mode does not gate measurement construction.

Reject unbounded/untrustworthy timestamp or member identity, conflicting duplicate source keys, unresolved quote units, unsupported required numeric representations, and unsupported requested coverage before member completion. A member lacking source coverage can appear in the admission report but cannot become an accepted complete base with invented observed zeros. A verified prefix may be built only when its required interval is evidenced, even if whole-session coverage is absent. Do not label assumed-empty overlays as verified historical halt absence. Report such a limitation independently of mechanical file completeness and do not call the corpus fully admitted while required context is unresolved.

Admission states are `blocked` (specific evidence/representation failure), `metadata_admitted` (bounded consumption allowed, row checks pending), and `consumption_verified` (all consumed-row/source-stability checks passed for the declared coverage). Only the last can support a completed accepted base. This does not imply verification of records outside an explicit prefix.

| Evidence | Minimum for accepted calculation | Missing-evidence behavior |
|---|---|---|
| Object/member/clock/order | Identity-bound object and member mapping, verified clock meaning; order checked on every consumed row | Block admission/stop consumption; never repair order or infer symbol from arbitrary filename |
| Requested-window/terminal coverage | Successful source completion evidence for each claimed observed interval, including empty results; alternative historical records may satisfy this without a modern receipt format | Block the affected interval/member; event extrema alone cannot establish coverage |
| Quote size units | Bound to original acquisition/normalization lineage, with verified multiplier | Block this full-base builder, which promises displayed-size fields; no default 100-share assumption |
| Trade representation/precision | Evidence of stored representation and lossless check of every admitted eligible quantity/total | Metadata may admit inspection; row failure prevents completion; no need to claim a vendor-wide precision ceiling |
| Pre-session quote seed | Seed only from fully verified source observation sufficient to establish the latest prevailing state within [s−300s,s) | If unavailable, use **no seed** and record `seed_basis=unavailable`; leave price/quote-event origin unknown until a post-start event. This does not block evidenced session-forward measurements. Verified empty seed interval has a distinct `verified_empty` basis; neither creates a synthetic return endpoint |
| Halt context | Identity-bound historical halt source/overlay with explicit covered interval and either accepted intervals or an evidenced empty result | Block accepted calculation for unresolved scope; do not assume no halts. Historical/ex-post knowledge is allowed and labeled. No claim of exchange-wide truth beyond the declared halt source |
| Source continuity | Evidence/declaration of the admitted retrieval's observation intervals and known breaks, distinguishing retrieval coverage from a measured live feed | Block unsupported observation intervals. Terminal historical retrieval evidence can support continuous historical source observation; live receipt logs are not required and must not be implied |
| Selection/discovery | Preserve exact available selection identity/nominal endpoint; no discovery filtering of measurement construction | Missing selection does not block raw measurement values, but blocks claiming reproduction of that historical selected population; full plan identifies its population basis explicitly |
| Actual receipt/known-at timestamps | Required only for claims using measured receipt eligibility | Missing values are annotated and receipt-mode eligibility unavailable; nominal historical calculation is allowed |

This implementation does not add a permissive production mode that silently assumes missing required halt/unit/coverage evidence. If the sample is blocked there, deliver the synthetic-verified code and the precise evidence request. A later user decision to accept a weaker historical context claim must be explicit and versioned, not an implementer's default.

### Decoding rules

- Stream projected Parquet record batches with `use_threads=False`. Quote projection retains prices, both sizes, conditions, indicators, SIP timestamp and sequence; add identity fields required by the admitted adapter. Trade projection retains SIP/participant timestamps, sequence, conditions, correction, price, `decimal_size` and `size`. Metadata/adapter validation determines representation, not dtype coercion after loading.
- Validate strictly increasing `(sip_timestamp, sequence_number)` across batches separately by stream. A tie in SIP with increasing sequence is legal; process every event. Duplicate or decreasing composite keys fail; never sort, deduplicate or skip them silently. No cross-stream merge order is needed.
- Preserve decimal trade text. Use nonnull `decimal_size` when present, otherwise the admitted `size` representation; malformed present decimal text is not a fallback request. Lossless scale-9 check precedes accepting an eligible quantity. Float 0.5 can be exact; float 0.1 is not evidence for decimal text 0.1. Check total overflow. Do not exclude an otherwise eligible trade to make precision fit.
- All conditions on an otherwise eligible trade must belong to the accepted stratum's allowed set. Under the established canonical adapter, a null/empty condition list is regular and null correction means 0; missing required columns are not equivalent to a null field. Null elements/noninteger condition codes are malformed, not an empty list. Quote null/empty lists likewise mean no listed condition/indicator. Validate these assumptions in the adapter and regression fixtures. Unknown condition eligibility localizes to its containing activity second; unknown correction scope fails the member. Trustworthy but known ineligible/late executions do not remove observed exposure. Untrustworthy clocks fail; do not turn them into late trades.
- New quote price validity excludes nonpositive/nonfinite/missing/crossed, one-sided, nonfirm, closed, invalid and unknown validity-affecting codes. Known explicit crossed flags invalidate even numerically uncrossed prices. Unknown indicator meaning is conservatively price/size unknown until an accepted update, unless a versioned mapping proves it is size-only. A trusted structurally decoded quote message still updates quote-event age independently of price/size validity.
- New spread validity additionally rejects contradictory explicit lock flags with unequal prices. Numerically equal positive quotes otherwise valid have supported zero spread. Each size independently requires positive finite normalized own-side shares and valid two-sided prices.
- Maintain fixed counters by classification; bound samples of defects and unknown-code dictionaries, spilling detailed audit evidence to bounded private output if needed. No growing per-event resident arrays.

Trade classification order is structural key/clock trust → correction scope → condition uncertainty → known eligibility/timeliness/numerical validity → exact eligible quantity. Unknown correction always fails, even if other fields would exclude the record. Known correction action records (outside causal payloads 0/7/8) are definitively excluded and need no activity quantity. On causal payloads, unknown/malformed condition membership makes the containing activity second uncertain, including combinations with a known disallowed condition; do not let incidental exclusion short-circuit unknown payload semantics. With fully known conditions, the established allowed-set predicate, nonpositive/nonfinite numeric exclusion and trustworthy report-age cutoff determine known ineligibility. A malformed present quantity that cannot be interpreted, as opposed to a known nonpositive numeric value, makes common activity uncertain. Otherwise eligible finite positive quantities failing scale-9 exactness/overflow fail the member. Count known exclusions and uncertainty separately. Timestamp uncertainty and broad correction ambiguity remain hard failures.

## 5. Base replay algorithm and complete field mapping

For each member, let `s` be 04:00 ET from `session_bounds`. Emit exactly one row for each `[s+i·1s, s+(i+1)·1s)` through the explicit end (at most 20:00 ET). Do not seed a synthetic midpoint endpoint at `s`. Quote seeding considers only the verified preceding 300 seconds, taking the latest prevailing update including invalid state. It seeds quote-event age if structurally trusted; midpoint observation starts at `s`, not before. Trades never seed from before `s`.

Use two bounded generators, one per stream. Each produces one second's accumulators/context; zip them on exact keys and write base batches. Each generator interleaves its events with its source interval boundaries, halt closures and grid boundaries. Integrate the prior state on `[cursor,next_event)` before applying the event. Process all same-time events in sequence order even when interval length is zero. Events at a row's right endpoint are processed in the next row. Context changes at that right endpoint likewise affect the next row; no future outage suppresses the row ending there.

Use integer nanoseconds for durations, convert a segment duration to seconds once for float integrals, and chronological accumulation independent of batch size. Compute midpoint as `bid/2 + ask/2`. Derive midpoint TWAP from the two common-support TWAPs with the same expression. Guard finite arithmetic; no source clipping or denominator epsilon.

| Base fields (literal schema names) | State / finalization / independent case |
|---|---|
| `session_date`, `symbol`, `interval_end_ns` | Admitted member plus exact grid; test event at endpoint and DST dates |
| `bid_twap_usd`, `ask_twap_usd`, `midpoint_twap_usd`, `price_valid_duration_ns`, `price_twap_reason_mask` | Integrate bid/ask over common valid-price ns, divide by seconds; midpoint from half-sum; unequal-duration fixture Q1 |
| `bid_end_usd`, `ask_end_usd`, `price_end_reason_mask` | Current strictly prior valid two-sided state, no older-good fallback; endpoint/invalid-tie fixtures |
| `spread_integral_bps_seconds`, `spread_valid_duration_ns`, `spread_integral_reason_mask` | Integral of `10000*(ask-bid)/(bid/2+ask/2)` over spread-valid segments; Q1 and lock contradiction |
| `bid_size_integral_shares_seconds`, `bid_size_valid_duration_ns`, `bid_size_integral_reason_mask` | Integral of independently valid normalized bid size; Q1, missing ask size, verified lot conversion |
| `ask_size_integral_shares_seconds`, `ask_size_valid_duration_ns`, `ask_size_integral_reason_mask` | Same for ask; Q1 and independently missing bid size |
| `bid_size_end_shares`, `ask_size_end_shares`, each side's `_size_end_reason_mask` | Current own-side validity; no common depth mask |
| `trade_count_1s`, `share_volume_1s`, `dollar_volume_1s_usd`, `activity_valid_duration_ns`, `activity_reason_mask` | Count, scaled-int exact shares, float sum of price×shares within known observation; T1 and known-zero/unknown-eligibility fixtures |
| `trade_age_seconds`, `trade_age_reason_mask` | Endpoint minus last definitely eligible trade SIP; late reports do not refresh; unknown eligibility clears origin before later eligible events can seed it |
| `quote_age_seconds`, `quote_age_reason_mask` | Endpoint minus latest structurally accepted quote SIP; invalid price can have known quote age |
| `midpoint_change_age_seconds`, `midpoint_change_age_reason_mask` | Endpoint minus last observed midpoint change within continuously valid state; first seed/recovery is not a change |
| `midpoint_age_status`, `midpoint_observation_start_ns`, `midpoint_age_lower_bound_seconds` | Unobservable: all associated values null. Seed/no observed change: status 1, origin set, exact age null, lower bound `t-origin`. Observed change: status 2, retain continuous observation origin, exact age known, lower bound null |
| `quote_source_status`, `trade_source_status` | Endpoint observation trust, independently per stream; accepted source does not imply valid numeric quote or observed eligible trade |
| `quote_observed_duration_ns`, `trade_observed_duration_ns` | Union of verified observable ns in this second, independent of semantic validity; gap-overlap fixture |
| `quote_continuity_id`, `trade_continuity_id` | Start at 0; increment on each affected declared break onset and halt entry, never on ordinary invalidity/batch/discovery; IDs never reused |
| `quote_continuity_break_in_second`, `trade_continuity_break_in_second` | True for affected declared break onset/closed-halt entry within the second; detailed immutable context preserves multiple boundaries |
| `halt_active`, `halt_id` | Any positive overlap with accepted halt closes entire second. Canonicalize overlapping/touching halt closures into deterministic union intervals and retain constituent IDs/provenance in context; row ID references that union |

### Interruptions and publication masks

Translate halt intervals to whole-second closures first; ignore events in closed seconds for both accumulation and recovery seeding. On first fully open second require new post-closure events. Clear both origins on halt entry; startup clocks restart only after closure. Source observed durations may describe actual observation, but all semantic exposure is zero on closed seconds.

A declared feed gap clears only its stream's current state/origins and increments continuity at onset; recovery does not increment it again. Quote gaps break lag support even when both lag endpoints are numerically equal. Integrate known before/after-gap exposure within a second. A gap ending inside a second can produce an accepted endpoint and partial base exposure. Gap intervals are half-open; an instantaneous declared break has no missing duration but clears affected origins/continuity. Process context transitions before source events at the same timestamp so a recovery-time event may seed the new segment and an onset-time event cannot seed an outage.

For unknown trade eligibility, zero the entire containing second's common activity support and null all three totals, including otherwise known contributions. Preserve structural observed duration. Clear exact age at the unknown event; a later definitely eligible event in that same second can restore current trade age. Ordinary invalid quotes clear only midpoint origin/comparison; quote age survives. Even a zero-duration invalid quote followed by recovery destroys midpoint-change continuity.

Generate masks from final publication state, not accumulated defect bits. Positive-supported base integrals/totals have mask 0 even if another part of that second is invalid/outage. Zero support means null numerator(s) and `NO_SUPPORTED_DATA`, plus applicable cause(s). Current invalid endpoints use `INVALID_CURRENT_VALUE`; absent qualifying origins use `NO_OBSERVED_EVENT`; no-change midpoint state uses `MIDPOINT_AGE_LOWER_BOUND_ONLY`. Source and halt reasons apply according to their own scope. No STARTUP/LOW_COVERAGE bits on base values. Use exact contract reason bits; do not copy legacy masks.

The following mask decision order is normative (no blanket union of all observed defects):

1. `source_bit(status)` is 0 for ACCEPTED, SOURCE_UNVERIFIED for UNVERIFIED, SOURCE_UNAVAILABLE for UNAVAILABLE. Status concerns the strictly prior endpoint. Halt closure overrides feature/value publication, not that separately stored source-trust status.
2. For a base integral/activity tuple: if supported duration >0, mask=0. Otherwise start with NO_SUPPORTED_DATA; add HALT if closed, and the source's endpoint source bit if nonzero. Outside halt, add INVALID_CURRENT_VALUE when an observed invalid/uncertain payload left the family without support; add CONTINUITY_BREAK when a declared interruption cleared a necessary state and no qualifying recovery state has yet seeded it. Do not add invalidity for mere initial absence of an event. Preserve bounded internal flags distinguishing these causes.
3. For current prices/sizes: on halt use HALT plus source bit; on unaccepted endpoint use source bit; otherwise valid value gives mask=0. If no valid state, use INVALID_CURRENT_VALUE, plus CONTINUITY_BREAK only while the necessary state remains unseeded after a declared break. Bad own-side size invalidates that side only. A valid recovered state does not retain historical break bits.
4. For current exact ages: on halt use HALT plus source bit; on unaccepted endpoint use source bit. Otherwise a known exact age gives mask=0. A missing trade/quote origin gives NO_OBSERVED_EVENT, plus CONTINUITY_BREAK only while awaiting a post-break qualifying event. Unobservable midpoint gives INVALID_CURRENT_VALUE, plus an applicable unseeded CONTINUITY_BREAK. Continuously valid no-change midpoint gives MIDPOINT_AGE_LOWER_BOUND_ONLY alone (status 1), not ZERO_RETURN_VARIATION or a fabricated exact age.
5. EW history masks are independently ORed source bit, HALT, STARTUP when elapsed<startup, and NO_SUPPORTED_DATA when usable state is mathematically zero; if positive and mature but below threshold, use LOW_COVERAGE. No LOW_COVERAGE during startup. Halt rows have reset elapsed/usable/possible=0, hence HALT|STARTUP|NO_SUPPORTED_DATA plus source bit. Direct CONTINUITY_BREAK/INVALID_CURRENT_VALUE do not leak into EW/p90 history masks.
6. Age-p90 uses the same source/halt/startup/empty/low logic with elapsed slots, required H, supported count and threshold ceil(fraction*H). Zero count gets NO_SUPPORTED_DATA, not LOW_COVERAGE. Only when mask=0 compute/publish the percentile. Derived-feature zero masks apply only after all component history masks are zero.

An instantaneous break at a second's left boundary clears origins before events at that boundary. That full observed second may be the first new age-window slot; a break strictly inside a second discards its pre-break age window and that partial post-break slot. A gap ending exactly on a left boundary likewise permits this full recovery second to count. These rules must use context event time; a generic `break_in_second=True` alone is insufficient.

## 6. Base-only default feature algorithm

Read ordered base batches and the verified context companion only. Verify exact schema/keys, coverage and context consistency first/while streaming. Never open raw T/Q, R2, selection databases or sibling checkouts. Context must preserve boundaries needed for age-window recovery; endpoint IDs alone cannot reconstruct a gap that both begins and ends within one second.

Maintain a six-endpoint ring, per-stream startup clock, per-view scaled accumulators, and fixed age windows. Do not materialize a member-sized dataframe. At each open second: apply context/reset decisions; advance clocks; decay accumulators; admit current contributions; append lag/age slots; evaluate publication; write feature/support rows with identical keys.

### Returns and EW construction

For endpoints `t-5s` and `t`, use `endpoint_return` with valid midpoints, unchanged quote continuity and no intervening halt/break. Ordinary intermediate invalidity is not a lag veto. The first possible return after session/halt reset is row six. Known feed gaps do not restart that possible-return schedule, including recovery slots without usable lag endpoints.

For each half-life `h`, `lambda=2**(-1/h)`:

```text
U1 <- lambda*U1 + |r|        (only add for supported return)
U2 <- lambda*U2 + r²         (same support as U1)
W  <- lambda*W  + 1          (same support)
Wp <- lambda*Wp + 1          (every possible return slot, including missing)
RMS = sqrt(U2/W)
participation = U1²/(W*U2)
```

Zero supported history is unavailable. Supported all-zero returns give RMS 0 and undefined participation. Maintain `ever_positive_return` since the last session/halt reset; missing time or floating underflow cannot clear it. Return lag ring clears at a quote break, but return EW state and startup retain/decay. Do not reset return possible weight there.

For spread/bid size/ask size: decay the integral sum and usable seconds; add base integral and `valid_duration_ns/1e9` only when supported. For activity: common usable seconds plus three separate decayed totals, converting exact per-second shares to float only at this feature step. All possible exposure denominators add one second per open wall row, including declared source gaps. Never average per-second ratios with unequal exposure. Current ratios divide current admissible estimates, with no further smoothing.

Use the `history_reasons` decision algorithm for EW startup/coverage/source/halt gating, implemented internally with scaled comparisons as detailed below; the current helper's float signature cannot represent every retained scaled weight. Quote-derived features use quote status/clock; activity uses trade status/clock. Defaults: startup 60/300, spread coverage .9, other EW coverage .8. No epsilon around thresholds. In ordinary numeric invalidity an older history may still publish; an active source outage suppresses that source's historical features. Ratio mask is the OR of RMS/spread dependency history masks; add ZERO_SPREAD only when both component histories are admissible and spread is zero. Participation adds ZERO_RETURN_VARIATION only for an admissible, supported mathematically all-zero history. Emit the literal registry fields and support schema, not a second handwritten name list.

### Scaled arithmetic design

Implement a nonnegative `ScaledSum` represented by zero or mantissa `m` in `[0.5,1)` and integer exponent `e`, meaning `m*2**e`. Normalize using `frexp`; decay by multiplying mantissa by lambda and renormalizing; add by exponent alignment then normalize. Squared-return contributions are formed from `frexp(abs(r))`, squaring its mantissa and doubling its exponent, never by first computing an underflowed `r*r`. Additions smaller than binary64 precision may round away normally, but a positive retained state must not turn to zero solely because its exponent is small.

Compute `sqrt(U2/W)` by mantissa division plus exponent difference, split odd exponents before square root and final `ldexp`. Compute `U1²/(W*U2)` by combined mantissa products/divisions and exponent arithmetic; never square a tiny materialized mean. Use the same pattern for other ratios. Use scaled sums for all nonnegative EW numerators/usable/possible denominators. Export support weights as float64 diagnostics, which are not restart checkpoints. Track mathematical positive-history separately from a diagnostic weight rounded to zero.

Coverage compares normalized scaled `usable` against scaled `minimum*possible`; emptiness tests the scaled zero state, not its float projection. Implement a private scaled-aware history gate with exactly the decision order above and prove equivalence to existing `history_reasons` for representable inputs. Do not change the public helper merely to materialize tiny weights. If positive usable weight underflows the float diagnostic, a mature history has LOW_COVERAGE rather than NO_SUPPORTED_DATA, plus any source/halt reasons; never ZERO_RETURN_VARIATION merely from underflow. Record a bounded per-family diagnostic-underflow count in validation metadata so a float diagnostic zero is not misread as an estimator reset. Both states are unavailable; masks retain the distinction. The existing schema permits this (support floats are not sufficient restart state). Include a distinct h=1 fixture with one supported return followed by >1,100 unsupported slots, separately from the supported-zero-return stress fixture.

Keep compatible denominator accumulation order identical so fully supported usable and possible weights agree exactly. For partial exposure, assert the mathematical ordering and test float boundary behavior. Do not relax coverage or clip values as a workaround. If a representable-domain invariant fails due to arithmetic, fix the arithmetic and add its independent regression case. Treat unexpected output overflow as failure before completion.

To prevent independently rounded raw moments from producing participation slightly above 1, additionally retain a nonnegative scaled central sum `C = sum(w*(abs(r)-A)^2)` with the same return support/decay. This is internal numerical state, not a new measurement. On missing slots decay C only. On a supported magnitude x, let W0 be the already decayed previous usable weight and mu0 the previous U1/W mean (unchanged by decay). For nonempty old state, update `C = lambda*C_previous + [W0/(W0+1)]*(x-mu0)^2`; first supported observation sets C=0. Form the squared difference and small weight factor using scaled operations; do not underflow their product by prematurely materializing it. Evaluate differences through aligned scaled values when necessary.

Publish participation as `K/(K+C)` where `K=U1²/W`, all scaled; common-exponent numerator/denominator evaluation guarantees 0<=P<=1 without clipping. This is algebraically `U1²/(W*U2)`. RMS remains sqrt(U2/W) from the required squared raw moment. Independently verify both `U2/W ≈ (U1/W)²+C/W` and agreement of published participation with the high-precision direct-moment formula. Keep the direct scaled-moment calculation as a validation check, not a reason to clamp. Add constant and near-constant magnitudes, alternating signs with equal magnitude, and widely separated magnitudes to the oracle tests. The all-zero and ever-positive rules remain unchanged.

### Exact freshness p90s

For each age and H in `config.age_windows_seconds` (defaults 60/300), keep a deque of at most the most recent H elapsed wall slots (value or unavailable) plus a sorted multiset of supported values. Use `bisect` insert/remove with duplicates retained; interpolation index `p=.9*(n-1)`, linear between floor/ceil values. Work O(H) per update, memory O(H). Startup requires H slots since applicable reset; coverage requires `ceil(config.age_min_coverage*H)` supported exact ages (default fraction .8). Lower bounds never enter the sorted list. A current unavailable age may coexist with a published historical p90.

Session/halt clears all windows. Quote gap clears quote/midpoint windows, trade gap clears trade window; windows do not accrue startup slots during the relevant gap. Begin again at first fully observed recovery second, determined from precise context, not simply a valid endpoint. Ordinary invalidity/unknown local trade eligibility adds unsupported slots without resetting window maturity. Different source gap resets remain independent. During halt emit zero elapsed/sample counts and unavailable features; EW sums/startup are reset.

Support table: return usable/possible weights; spread/activity/bid-size/ask-size usable/possible seconds per view; quote/trade EW startup seconds; exact-age elapsed slots and sample counts per window. Clamp elapsed-slot counters to H as specified, never clamp coverage or numerical feature values. Features, support and base keys must match one-for-one.

## 7. Files, identities and restart

Initial correctness layout is per-member Parquet under separate base and feature dataset roots, partition directories `session_date=DATE/symbol=SYMBOL`. Use ZSTD level 3, projected output batches of 4,096 rows and one batch per row group initially. This is a bounded build format, not acceptance of the final corpus serving layout. Preserve logical schema metadata. Do not add packing, native database copies or row-group buffering beyond bounds as an implementation prerequisite.

Base directory: `base.parquet`, canonical `context.json`, `manifest.json`. Feature directory: `features.parquet`, `support.parquet`, `manifest.json`; binds base manifest and all companions. Manifests follow the envelope in the contract: coverage, source/evidence/config/schema hashes, outputs and validation evidence. Include full source revision, installed wheel SHA-256 and separate implementation digests: replay/base dependencies only for base, feature dependencies only for features; the full run binds both. Cover all relevant runtime modules/resources, not only `contracts.implementation_identity()`. A changed feature-only module must not invalidate base reuse; a changed replay dependency must. Revision/wheel hashes remain provenance, not automatic base incompatibility when its scoped implementation and semantics match.

Base semantics do not change with EW half-life, although the current shared contract hash includes feature configuration. Record a base-compatibility descriptor (base schema, raw measurement/timing/population/unit policy and scoped replay/base implementation identity) in addition to the full original contract hash. `build_from_base` may vary view half-lives/startup/coverage/age windows when that descriptor matches; it must reject changed raw measurement policy such as trade reporting-age cap. Verify this explicitly. Feature completion binds its feature implementation and the immutable consumed base manifest. Do not force a raw rebuild just because feature-only configuration or implementation changed, or ignore a truly incompatible base.

Write into a new attempt directory with exclusive member writer lock, flush files, verify keys/schemas/counts/hashes/context and source stability, then atomically commit the complete manifest/directory. Completion is the last action. Outputs cannot be referenced as complete until all companions are verified. Bound manifests to 1 MiB; ledger stores member records on disk. Source hash checks can use streaming I/O and must be included in measurements.

Reuse requires matching member, coverage, all inputs/control, semantic/implementation identities and verification of every output companion. Changed/corrupt completed output fails rather than overwriting. Incomplete members restart from session start using a distinct attempt. No serialized partial EW state, automatic partial recovery, or reading features/support as a numerical checkpoint. Limit retained attempts by stopping before scratch is exhausted, not deleting unrelated retained work. Any cleanup command must target only an explicitly owned uncommitted attempt and be separate from full-run authorization.

## 8. Independent numerical fixtures and test matrix

Commit tiny raw fixtures and literal expected base rows, including every field/mask/context column. Use synthetic member names and no real market rows. Oracle modules must not import production reducers, scaled arithmetic, return helpers or mask decision helpers; shared schema enums/constants are allowed. For synthetic EW expectations use explicit weighted sums with high-precision Decimal arithmetic (including exp/log/sqrt where needed), not the production recurrence. Verification tolerances: floats rtol=1e-10/atol=1e-12; common-support TWAP identity rtol=1e-12/atol=0; exact keys, durations, counts, decimals, masks and identities. Same-order production results across batch sizes compare exactly after decoding Parquet (physical file bytes may differ).

Named required cases:

| Case | Independent expected behavior |
|---|---|
| Q1 unequal exposure | Quotes at s: bid 99, ask 101, sizes 10/20; at s+.25s: 100/102, sizes 30/40. First row: bid TWAP 99.75, ask 101.75, midpoint 100.75; spread integral `.25*200 + .75*(20000/101)` bps·s; bid/ask size integrals 25/35 shares·s; all durations 1e9; endpoint 100/102, 30/40; quote and midpoint-change ages .75s; observation origin s |
| Q1 endpoint event | A quote at s+1s does not change Q1 first row; it updates the second row. Two updates at identical SIP with increasing sequence both process; last is prevailing |
| Zero-duration invalidity | At one SIP: crossed update then restored identical midpoint. Exact midpoint-change origin clears; recovery is lower-bound-only despite zero lost duration; quote age updates |
| Size independence/units | Invalid ask size leaves valid bid size and price; verified 100-share lot factor multiplies once, declared shares factor 1; missing evidence rejects admission |
| Lock variants | Numeric 100/100 gives midpoint 100, spread integral 0 and positive duration; explicit lock flag on 99/101 retains otherwise valid midpoint but zero supported spread duration; crossed/nonfirm invalidates price |
| T1 exact totals | Two timely eligible executions, price 100 × decimal shares `0.1`, price 102 × `0.2`, yield count 2, shares exactly `0.300000000`, dollars 30.4 within float tolerance; last eligible SIP determines age |
| Reporting cutoff | Report ages 0 and exactly 1e9ns eligible; -1ns and 1e9+1ns known ineligible; known action corrections excluded; unknown correction fails |
| Stratum/population | Condition 12 eligibility changes exactly at 09:30/16:00 SIP boundaries, with no quote/EW/age reset; discovery likewise no reset |
| Known zero vs uncertainty | Reliable no-trade second emits three zero totals with full duration; unknown condition anywhere nulls that second's activity tuple/exposure, but a later eligible trade may establish current age |
| Gap/exposure | Gap [.25,.75) in a second produces .5s structural observation; quotes require new recovery update; a gap ending exactly at right edge still affects that row's endpoint; one ending inside can yield accepted endpoint and partial integral |
| Lag validity | Same valid lag endpoints with an intermediate ordinary invalid quote give supported return; a declared quote break invalidates it; unrelated trade gap does not |
| Halt | Any subsecond halt overlap closes that row, resets all state, ignores its events; first fully open row requires new events and restart; multiple adjacent closure intervals have deterministic IDs |
| Seeding/end/DST | Prior quote within verified 300s can seed; latest invalid pre-session quote is not skipped; no earlier trade origin/04:00 return endpoint; row six first possible return; final 20:00 endpoint emitted; summer/winter UTC boundaries tested |
| EW hand sum | With h=1 in a tiny private arithmetic test, two supported returns 1 and 3 give W=1.5, U1=3.5, U2=9.5, Q=19/3, RMS=sqrt(19/3), participation=49/57; next unsupported slot decays usable sums and adds possible weight only |
| Exposure weighting | Two contributions with h=1: integral/exposure (1,.1), then (20,1), yield 20.5/1.05, not the EW average of ratios 10 and 20 |
| Startup/coverage | Fast rows 59/60 and slow 299/300; after maturity exact threshold and immediately below for .9/.8; initial no-support distinct from low coverage; source outages suppress otherwise mature histories without resetting EW clocks |
| Zero/scale | All-zero return history: RMS 0, participation ZERO_RETURN_VARIATION. One nonzero return followed by enough decay to underflow naive squared sums retains correct scaled behavior and never becomes mathematically all-zero; use h=1 to fit within a 57,600-row session |
| P90/maturity | Known values [0,1,2,3] interpolate to 2.7 in helper; duplicate deletion, unsupported slots, 48/60 and 240/300 coverage, full-window startup, and current-invalid/historical-valid case; lower bounds excluded |
| Invariance/restart | Batch sizes 1, 7, 4096 and 25000; splits at endpoint/gap/halt/tied SIP; interrupted writer leaves no complete output; restart yields identical logical rows; changed input/config/corrupt companion prevents reuse |
| Feature-only rebuild | Alternate h=45/startup=90 plus age window 30/coverage .9 built from same base while raw access is forbidden; feature-only implementation identity change preserves base reuse; raw cap change rejects base compatibility; correct new config identity |

Add deterministic generated small cases for random event spacings, duplicates in age windows, long quiet intervals and source defects. Compare to an independently implemented interval-intersection base oracle and explicit-history feature oracle. Keep random seeds fixed and cases bounded. Demonstrate adding one private quote accumulator through the replay extension point on synthetic data without altering public base schema; it must not require replacing the source event loop.

Fault-injection acceptance includes source identity change during read, malformed descriptor, path traversal, precision overflow, unsupported code, disk/RSS/time guard, companion key mismatch, duplicate member, interrupted manifest commit and concurrent member writer. Do not add tests whose only assertion is that the implementation calls its own helper.

## 9. Resource contract and installed verification

Complexity: raw replay O(E+N), features O(N·V + N·A·H), where V<=8, A=3 and H<=300 per configured window; oracle cost separately bounded by tiny fixtures/sample length. Resident state is at most one raw batch per stream, one output batch, fixed source/event lookahead, six endpoints, bounded EW/age state and bounded metadata. No full-day raw tables, corpus lists, global sort, or growing event histories. Parquet metadata/row-group decode allocations count toward RSS even when batch output is small.

Develop on a separate VM checkout/environment. Before tests, inspect active jobs, CPU/memory/disk, reserve remaining transfer bytes and preserve the active transfer's installation. Use one test/calculation process initially, Arrow/BLAS threads=1, at most two CPU cores for this job. Concurrent transfer is not permission for broad CPU or memory use.

Synthetic verification initial guards: sampled process-tree RSS stop 1 GiB, cgroup MemoryMax 1536 MiB, MemorySwapMax=0, TasksMax=64, CPUQuota=200%, runtime cap 15 minutes per monitored command, scratch cap 2 GiB. Sample guards in §10 are similarly explicit. Monitor at 50ms; stop the whole descendant tree, preserve logs and incomplete work; no automatic restart after a guard trip. If ceilings cannot be installed, report and establish an equally effective guard before external sample execution.

Build an isolated wheel from the reviewed commit using Python 3.13 and the verified Linux dependency lock. Install into a new release environment, record wheel/revision/dependency identities, and run from outside the checkout. Do not switch `/opt/tape-data-product/current` or the active transfer environment. Run legacy regression suite plus new synthetic end-to-end tests, manifest verification and alternate-feature rebuild under raw/network denial. New fixture/calculation runtime imports must resolve to the installed package. Verify the production CLI dispatch, not just internal functions.

## 10. Bounded local T/Q sample and external reconstruction

The authorized implementation sample is exactly KDP and NVDA on 2026-09-02 from the retained four Phase 1 objects (recorded aggregate size 114,278,636 bytes; reverify identities). No GET, no replacement download, no additional dates/members. Output only the first 720 seconds from 04:00 ET (1,440 rows total), with quote seeding limited to the preceding verified 300s or the explicit no-seed basis in §4. This is a session-start prefix, not a full symbol-day. No sample execution occurs while writing this specification; the sample belongs to the later implementation task.

Before decoding: freeze local sample manifest/identities, footer schemas, units/quantity representation evidence, requested coverage and terminal source records, and available halt/continuity information. Record admissible and unresolved dependencies individually. If this sample is not admissible, finish synthetic/installed code verification and produce a specific admission blocker; do not fabricate evidence, label the sample synthetic, or substitute another member without a bounded revised scope.

Per sample invocation: one worker, batch 4096, threads=1, CPUQuota=200%, sampled RSS stop 1 GiB, cgroup 1536 MiB, swap 0, runtime cap 600s, output+scratch cap 2 GiB, at most 2,000,000 decoded raw rows across both members/streams and at most 1 GiB counted input read bytes including identity/reconstruction I/O. Stop before crossing a guard. Each explicit acceptance attempt records cumulative bytes/time; reruns require a stated reason and refreshed concurrent-resource checks, not a loop that silently resets limits. Do not skip source identity checks to fit the limit.

Prefix reader uses ordered timestamps and projected batches; stop after enough information to finalize the prefix, not after decoding a full day. Account for seed scan, lookahead batch and Parquet page/row-group reads. A timestamp predicate alone is not a byte budget. Instrument the actual file read path (including hash reads) so read limits and measurement apply to physical reads requested by the process. If row-group decoding cannot stay inside guards, stop and propose a bounded alternative; do not expand automatically.

Disk preflight during transfer must subtract its outstanding reserved bytes and largest in-flight partial from free space, then reserve this attempt's output/scratch while retaining the migration's 80 GiB post-transfer reserve. Do not merely check current `df` free bytes. After transfer, retain that reserve for the sample unless explicitly revised by the owner. Inspect the migration's ledger/accounting read-only; never adjust its budgets.

For real base reconstruction, separately stream the same bounded prefix through the independent interval-intersection/reference decoder with bounded state. It must not use production accumulators or cached production-decoded semantic values. Check all base fields and masks, not just hashes; fixture tests alone establish edge coverage that may be absent in real data. For real features, reconstruct from the 720-row base prefixes using explicit weighted histories and independently sorted age windows; those tiny fixed-size histories are permitted in the test verifier, not production. Audit reference numerical tolerance against the high-precision synthetic oracle. Include verification I/O/CPU/RSS separately from build-only measurements.

Record per member: decoded/read rows and bytes, raw hash overhead, base/feature rows and output bytes, elapsed/CPU time, sampled peak process-tree RSS, disk high-water, source exclusions/defects, available values and reasons by feature, plus differences versus independent reconstruction. A quiet 04:00 prefix may prove startup/zero handling but not busy-market throughput or every feature's available behavior. Do not choose a different slice merely to obtain favorable features.

## 11. Manual full-job preparation and readiness

Write a private immutable plan and a concise tracked acceptance/runbook summary. The plan binds exact expected members/population and inventory hash, migration identity/completion dependency, admission descriptors/evidence, source raw-root, base/features roots, release/config/implementation/wheel hashes, budgets, sample evidence and disk-backed ledger location. It contains no credentials. Use distinct output release IDs; never reuse the legacy dataset identity.

Plan validation reports transferred vs verified vs admitted vs unresolved members and bytes. Default proposed population is all 7,085 migration T/Q members, separately tagging historical 6,222 membership; this is a proposed scope, not acceptance or authorization. Missing provenance does not disappear from the denominator. Source-incomplete and source-rejected members remain explicit. Do not auto-launch on transfer completion or install an automation/systemd timer.

Project runtime using separately measured raw event/byte processing, dense base output, feature seconds/s, hash/reconstruction overhead, and per-member overhead. Project disk for retained raw + base + features + support + control + largest attempts + future feature-version overlap + scratch + filesystem reserve. At 7,085 full 57,600-row members there are 408,096,000 rows; dense one-second output can exceed raw size. Each additional 100 compressed bytes/row costs ~40.8 GB. Do not infer compression from schema width or extrapolate a quiet prefix as a reliable corpus bound.

Provide measured ranges and assumptions; if the two prefixes cannot bound busy-member resources or full-session compression, mark the full-run projection insufficient and propose the smallest additional exact-member/full-session measurement for user approval. Do not execute that extension automatically. This does not block delivering the functioning calculator and manual runner.

`calculate run` must fail preflight, without starting calculation, unless: selected transfer completion and hashes reconcile; source/context admission meets the declared output coverage; wheel/config/plan identities match; outputs do not conflict; current resource/disk budget fits; plan contains accepted representative measurement references. Refresh transient capacity and active-job checks at manual launch. A changed plan requires its new explicit expected hash. No waiting or polling for missing raw inputs; return actionable pending counts/reasons.

The delivered manual command names the absolute isolated-release `tape-product`, the exact private plan path and its hash, with the measured systemd/resource wrapper. It must be a real tested interface, not a placeholder command presented as ready. During this implementation task test it on a synthetic tiny inventory only, including transfer-incomplete and identity-mismatch preflight failures. The user starts the external full plan later; preparing a command is not its execution.

## 12. Completion evidence and handoff

Update implemented docs and separate Phase 2/3 completion records to state exactly what passes: code/synthetic correctness, installed isolation, real-sample admission/reconstruction, measurements, and manual-plan readiness. Never mark an external criterion passed because a synthetic substitute passes. Keep detailed rows, source records, logs and manifests in ignored/private directories.

Final report leads with whether base + both default views work, whether the real sample matched independent calculations, and whether full launch is ready or which specific evidence/budget blocks it. Include tests' meaning, observed resources, proposed corpus ranges, and the manual launch artifact. No promise that local reads make calculation faster than transfer until measured: network staging is removed, but quote decode, state updates, dense outputs and verification can dominate.

### Copyable Sol task instruction

> Implement `planning/rewrite/feature_pipeline_implementation.md` end to end on the project VM using Sol at medium reasoning. Follow its Phase 2→3 checkpoints, preserve the active transfer and legacy product, independently review calculations, verify the installed wheel with synthetic fixtures and only the specified admitted local KDP/NVDA prefix sample, then prepare a manifest-bound manual full-build plan and command. Do not schedule, wait for, or launch the corpus job. Resolve routine engineering issues autonomously; surface demonstrated contract conflicts, unavailable source evidence or insufficient representative resource evidence precisely. Finish the code and all independent work possible even if external sample admission or full-run readiness remains blocked.

### Design review record

An independent read-only agent reviewed the contracts and this specification on 2026-09-14. Incorporated findings include explicit source/control admission policy, no-seed handling, deterministic masks and same-time transitions, scaled-aware coverage underflow, stable participation arithmetic, configurable p90s, separate base/feature implementation identities, bounded sample limits and manual-only launch gating. Relative document links and hand-calculated fixture arithmetic were checked. This is design review, not completed executable fixtures, Sol implementation, real-data testing or Phase 2/3 acceptance.
