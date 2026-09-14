# Phase 0 completion record

**Complete for contract/schema/interface scope, 2026-09-14.** Phase 1 VPS preparation is user-owned and can proceed independently. The new production replay and EW feature calculator are not implemented by this phase.

## What is complete

- Accepted column names, types, units, 12 reason bits, 48-field base schema, 51-field feature schema (keys + 24 values + masks), 37-field support schema, and 27-measurement query registry exist in the installed `tape_data_product.contracts` package.
- Immutable bounded configuration and separate semantic/schema/implementation identities; strict unknown-field and invalid-value validation.
- Exact transition/timing/population contract: source gaps retain decayed EW state but break returns/event origins; halts reset; ordinary quote defects remain local; numeric locks contribute zero spread; quote age is price-independent; maximum eligible trade reporting age remains 1s inclusive.
- Exact scale-9 quantity/total checks independent of Decimal context, plus required identity-bound size-unit declarations. Current provider documentation and source representation evidence are recorded. No universal vendor precision ceiling or unseen historical member admission is claimed.
- Bounded record-batch validation, structural builder protocols, and verified completed-member identity checks. First production restart reconstructs incomplete members from session start; arbitrary partial checkpoints are explicitly deferred rather than left as an implicit implementation choice.
- Durable documentation: `docs/contracts/endpoint-ew-v1.md`. The product README/phase plan/schema review in this planning directory record acceptance without becoming runtime dependencies.

## Verification

Final full offline suite: **245 passed in 24.20s**. Monitored command elapsed **24.844s**, sampled peak process-tree RSS **220,512,256 bytes (210.3 MiB)**, 50ms sampling, no guard stop. Includes 28 contract test cases and the existing suite. Review found and fixed current-source/status contradictions and malformed completion identities before this final run.

Wheel built without dependency acquisition and loaded from `/private/tmp` through a separate target installation. All three schemas and 27 query fields were available, exact decimal admission worked, and package contents contained no private plans/AGENTS.md. Its semantic and implementation identities match the final source tree. This proves contract packaging, not a new production data release. See `phase_0_full_suite_final.json`, corresponding `.log`, and `phase_0_wheel_smoke.json`.

Independent tiny numerical examples cover unequal exposure, participation, linear p90 and EW coverage decay. Policy tests cover clock/lag boundaries, late reports, local defects and reset distinctions. These are contract checks; actual replay, new-feature numerical reconstruction, arbitrary batch/resume equivalence and external resource measurement remain Phase 2/3 obligations. No historical market-data rebuild, VPS connection, R2 acquisition/upload or report regeneration occurred.

## Preserved baseline

Reviewed scoped Git commit: `0efec5de7ffd21d9ddec640cb67a928d3d2a3806` (`Define endpoint EW schemas and Phase 0 contracts`). Only the twelve new contract/source/test/documentation files were committed. Pre-existing staged AGENTS.md deletion and other user edits remain outside that commit. The docs index also has the new navigation entry in the existing modified working file.

`phase_0_code_baseline.tar.gz` preserves the committed tree. `phase_0_plans_snapshot.tar.gz` preserves the ignored plans and concise evidence separately. `phase_0_snapshot_manifest.json` records file/archive checksums and current Git status. Do not force-add these private artifacts to Git or the installed package. Existing user changes are not represented as this phase's implementation work.

Contract semantic identity: `bf4d8c1211bf096d9b0ab3b2c0d62f3858ff1da738b42aa8c3ed4cba67020bd1`. Source-unit evidence: `phase_0_source_evidence.json`. Per-member actual units/precision, observation coverage and knowledge-time evidence must be verified when real source members are admitted; unsupported precision needs an explicit schema revision, never silently rounded values or skipped eligible trades.

## Next implementation handoff

Phase 1: install the same repository/package under supported Python 3.13; prepare persistent data directories and measured small transfers. No full migration follows from Phase 0 acceptance.

Phase 2: inspect the integrated code, write the bounded raw-to-base replay spec against the installed contract, then implement the shared event decoder/accumulators, versioned manifests and member completion. Preserve condition/population constants; classify local unknown records as defined; verify actual source units/quantities. Use the transition table and accepted schema, not legacy output fields as the new architecture.

Phase 3: implement exponent-scaled EW moments, exposure-weighted averages, exact bounded age p90s and reconstruction from base prefixes. The contract document specifies startup/coverage/zero rules and numerical tolerances. Source and reporting-clock boundaries must not become a universal complete-case gate. Add tests against independent values before external builds.

Before any full external run, obtain the required confirmation after representative measurements. Routine implementation need not reopen accepted product decisions.
