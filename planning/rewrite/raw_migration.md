# Raw migration checkpoint specification

Status: ready for execution preparation, 2026-09-14; no corpus transfer accepted. This is an operational checkpoint between completed Phase 1 and Phase 2, not a new numbered phase. It implements the early-transfer path in [storage and migration](storage_and_migration.md).

## Objective and scope

Leave the VM with a verified persistent working copy of the explicitly selected canonical raw trades/quotes and available supporting records, plus enough evidence to start Phase 2 without rediscovering the machine or source inventory. Preserve R2 and historical derived releases. No new acquisition, raw conversion, feature calculation, publication, unrelated infrastructure change or deletion of retained data is in scope.

The prior inventory (~43.7 GB and 7,085 paired symbol-dates) is a planning snapshot, not an executable manifest. Default proposed transport population is all paired members in the refreshed canonical T/Q inventory; reconcile this separately against historical report membership. Report unpaired objects and ambiguities rather than silently including, dropping or treating them as accepted research members. Missing semantic provenance need not prevent byte-preserving transport, but must prevent unsupported production-admission claims.

Read repository guidance, product direction, phase plan, Phase 0 contracts/schema review, Phase 1 specification/completion, and storage/migration plan first. Record the actual starting revision and local edits. Do not overwrite pre-existing changes or alter owner-maintained AGENTS.md. Use the R2 storage skill for R2 work. Use the installed package and public interfaces; older repositories may supply reference records but never runtime imports.

## Checkpoint A: make the transfer executable and reviewable

Connection correction, 2026-09-14: use local SSH alias `tape-data-product-vps`, verified against the successful Phase 1 deployment; see `docs/vps-operations.md`. The earlier migration attempt used unrelated `capstone-db-hil`, so its timeouts are not evidence of this VM being down. Read-only verification of the correct host found the runtime link, all four sample Parquet paths totaling 114,278,636 bytes, and 203,923,947,520 free filesystem bytes. This verifies reachability/file presence, not fresh sample hashes or full migration acceptance. Refresh capacity and active jobs at execution time.

1. Inspect VM reachability, current runtime/deployment identity, active jobs, directory permissions and free bytes. Locate existing access configuration without exposing secrets. Reverify the retained Phase 1 sample or record exactly why it is unavailable. Do not reconfigure authentication/firewalls merely to perform a transfer.
2. Refresh R2 inventory and available object identities with read-only operations. Reconcile stream pairs, dates, selected-member records, source units, discovery/coverage evidence and halt/continuity records. Identify which canonical root/identity is authoritative; surface unresolved alternatives.
3. Write private immutable transfer inputs under ignored local_docs/ or private/, and a sanitized tracked execution record. Include exact members/objects/bytes, manifest digest, destination, reuse candidates, identity verification, expected missing evidence and excluded prefixes. ETags alone must not be presented as SHA-256. Missing trusted content hashes require an explicit alternative integrity method and its limitations before transfer.
4. Implement or adapt a general bounded transport entry point using installed storage primitives. The Phase 1 harness admits exactly four objects; do not bypass that guard or silently broaden its historical contract. Preserve its tests. Keep the new runner reproducible and independent of sibling repositories. Stream inventory/ledger processing or use a disk-backed catalog; no growing corpus-sized resident ledger.
5. Validate new transport behavior with fake clients: verified-file reuse without GET; identity change; corruption; interruption; persisted download budget across restart; free-space refusal; completion only after exact reconciliation. Verify bounded Parquet decoding and timestamp checks without whole-day materialization. Preserve per-object status and reasons.
6. Before any corpus operation, measure the exact new runner on at most the already scoped four Phase 1 objects, reusing verified copies. If a GET/retransfer is necessary and not already explicitly authorized in the task, include that bounded action in the concrete approval scope. Report observed timing/RSS and extrapolation limitations; do not infer new-runner performance solely from the old harness.

Prepare the transfer budget in bytes: current free capacity, already verified/reusable bytes, remaining raw bytes, control bytes, largest partial/retry overlap, retained evidence and an 80 GiB post-transfer reserve. Use one transfer worker; set explicit process-tree RSS stop, cgroup ceiling, swap policy, chunk/batch size, runtime cap and persisted cumulative response-body cap. Start from Phase 1 limits where applicable, but derive total duration/download caps from the selected manifest and measured path. Do not increase concurrency to meet an unsupported ETA.

Checkpoint A output must state whether the exact scope fits. A shortfall requires a concrete smaller scope or capacity option; do not silently reduce the reserve or remove existing data. Detailed keys, hosts, credentials and row data stay private.

## Checkpoint B: transfer and verify

Present the concrete scope and measurements, then obtain explicit bulk-transfer confirmation unless the current task already explicitly approves that exact measured manifest and limits. A request to prepare the job/spec or the old four-file sample approval is not corpus authorization. Remain in the same task after approval and finish the work; do not turn the approval checkpoint into a separate implementation phase.

Transfer directly R2 to VM with one writer/lock and persistent accounting. Validate object identity before and after copy, hash/byte counts, bounded decoding, row counts where trustworthy, and event ordering. Write completion atomically only for verified objects. Reuse verified inputs on restart; changed objects must fail or be separately versioned, never silently replace the chosen source identity. Replacement of the runner's own uncommitted partial must be explicitly included in its execution scope. Never delete R2 or retained unrelated files.

Check reserve and runtime/download caps throughout execution. Stop cleanly on violations, preserving verified work and an incomplete ledger. Report failures without automatic unbounded retries. Reconcile expected, reused, newly verified, failed and excluded objects/members and bytes; zero failures may not be inferred from a successful process exit.

## Acceptance and handoff

Create `planning/rewrite/raw_migration_completion.md` only when recording real results; distinguish completed, partial and blocked criteria explicitly. Record sanitized:

- Source selection and manifest digest, exact counts/bytes, exclusions and unresolved source evidence.
- Runner/package/revision identities and meaningful fake-client/installed verification results.
- Current VM data-root layout, free space, measured transfer time/RSS/disk high-water, reuse and restart evidence.
- Integrity guarantees and their limits; transport completion is not source admission or feature correctness.
- An accessible private manifest/ledger locator for the next job, with no credentials or detailed keys tracked.
- A proposed bounded Phase 2 sample selected from locally verified inputs, with known schema/units/coverage issues. The original KDP/NVDA pair remains the starting candidate, not a declaration of admission.

Update the phase plan links/status accurately. Do not reopen completed Phases 0/1 or mark Phase 2 complete. Final handoff must supply a self-contained prompt to execute Phase 2 checkpoint A in [phase_2.md](phase_2.md). If migration cannot complete, give the exact blocker and state whether Phase 2 synthetic work can proceed independently. Do not claim transfer readiness when credentials, identity or capacity are unresolved.
