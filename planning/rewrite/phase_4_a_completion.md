# Phase 4 checkpoint A reader handoff

**Reviewed reader handoff completed 2026-09-15.** This record accepts the shared reference/registry/selection interface for B and C on the fixed pilot. It does not implement or accept B's predicates/exports, C's distributions, D's report rehearsal, a full-universe reference, or Phase 4 as a whole.

## Baseline and installed artifact

The VM branch `codex/phase4-a` started from the completed producer revision `ea2e16128225a91ceb6003fcb3f6ef80985ba8e0` plus the scoped Phase 4 specification baseline `52b5b3502c9567b120a92316f47a8642a718e11c`. The implementation commit is `e3ca68fa85cca4b5690fa8865bfe853cec28e3db`; the installed source baseline including the delivery-priority clarification is `b3d467dad943d82aca6c861d4e3a3de9199f6841`.

The isolated wheel is `/opt/tape-data-product/releases/phase4-a-b3d467d/wheel/tape_data_product-0.1.0-py3-none-any.whl`, SHA-256 `f88fe930bc91dc5057ac7fbe5a019a01ea4f8078439b7cde1a3c4644ed705c8e`. It was installed non-editably into a separate Python 3.13.15 environment. The installed module resolved from that environment's `site-packages`, outside the development checkout.

## Pilot identity and lineage

The versioned reference is `/srv/tape-data-product/control/phase4-a-52b5b35-20260915/pilot-reference-v1-deterministic`, identity `4a410bd8321cfe0466dc7ecd02c8a5762a679c320d64376c24cea076465ae832`.

Selection used only admitted raw-event counts: one member from each monthly rank quartile for March–August, ordered by SHA-256 of `phase4-pilot-v1|session_date|symbol`, preferring unseen symbols and recording fallback only when necessary. The result has 24 unique symbols and no repeat fallback. It binds the accepted 5,208-member plan/population/completion identities, producer revision/wheel and scoped implementation identities, configuration/schema identities, member manifests, companion hashes and source/selection context. It points to existing partitions and copies no Parquet data.

Repeated construction returned the same reference identity. Construction accounted for 20,055,676 metadata bytes and 316,610,419 selected companion/manifest bytes, completed in 9.39 seconds, and used 202,432 KiB peak RSS. Each table has 1,382,400 rows (24 × 57,600).

## Interfaces handed to B and C

The public package surface is:

- `EndpointSelection(population_timing_mode, sessions=..., members=..., dates=..., endpoint_start_ns=..., endpoint_stop_ns=...)`
- `build_endpoint_reference(...)`
- `open_endpoint_reference(path, *, expected_identity, data_roots)`
- `describe_endpoint_fields(config)`
- `iter_endpoint_batches(handle, *, fields, selection, include_support=False, include_run_boundaries=False, batch_size=4096)`

The additive CLI is `tape-product endpoint-data {pilot,verify,fields,inspect}`. The implemented behavior and a consumer example are in [Endpoint/EW reference reader](../../docs/endpoint-data.md).

## Correctness and review evidence

The independent read-only review initially rejected the handoff for a hash-to-open replacement window and incomplete pilot coverage validation. Both were resolved. Hashes now come from one pinned inode, structured metadata is decoded from the same stable bytes, Parquet is checked before and after opening, and all consumed paths remain under the handle snapshot. A fault-injection regression replaces a valid Parquet file after hashing but during open and proves no result is accepted. Pilot open independently requires all ordered March–August × strata 0–3 cells and exact date-derived full-session bounds with 57,600 rows; a self-consistent prefix/full substitution is rejected.

The review also found that nominal discovery accepted a bare timestamp, direct evidence retained whole member tables, and source-specific/registry tests were too shallow. Nominal mode now requires provenance; receipt mode requires both clock provenances. Literal interruption tests distinguish quote-only from trade-only continuity. All 27 registry mappings are checked. The direct checker now reads only row groups intersecting first/middle/last eight-row intervals and retains exactly 24 rows per member.

The complete suite passed **345 tests in 83.22 seconds** with one-thread library settings. The affected installed workflow then passed from the isolated wheel with both the raw root and development checkout blocked by an audit guard:

| Check | Result |
|---|---:|
| Reference validation | 316,720,293 bytes; 1.250 s |
| Full pilot, batch 4096 | 1,382,400 rows; 76.906 s |
| Full pilot, batch 7 | identical output; 56.938 s |
| Selection segments | 72 starts and 72 ends |
| Fixed-interval direct comparison | 576 rows, 99 columns; passed in 0.564 s |
| Direct projected scan | 216 row groups; 48,665,308 compressed bytes |
| Ten-second four-field projection | 0.473 s |
| Full-member four-field projection | 0.549 s |
| Peak sampled process-tree RSS | 404,316,160 bytes |
| Installed run wall time | 137.04 s |

One full all-field scan represents 293,600,826 compressed projected bytes; one four-field member scan represents 2,938,512 bytes. The successful installed acceptance invocation therefore accounted for 958,464,277 logical validation/projected bytes. Observable filesystem input was 140,352 512-byte blocks on a warm shared cache; it is not labeled a cold-disk measurement. Output was 24 blocks plus small JSON evidence. The invocation ran with one worker/library thread, 200% CPU quota, a 2 GiB memory-high/guard stop, 3 GiB hard limit, zero swap, 15-minute runtime and 64-task cap. It stayed below all limits and produced no spill.

Installed CLI checks separately returned 27 registry entries, verified the same 24 members/1,382,400 rows, and inspected an explicit ten-row selection with exact values/masks/support/continuity. Legacy behavior remained covered by the complete suite. These comparisons verify identity, layout, alignment and projection; they are not an independent reconstruction of feature mathematics.

The final independent read-only review found no material blocker to the clarified reader handoff after these corrections. No reviewer launched data jobs or wrote code. Operational catalogs, detailed rows, scripts and logs remain outside Git.

## Limitations and next boundary

- The pilot's contexts contain historical membership but no usable nominal or receipt discovery clocks. `historical_membership` is accurately retrospective; discovery-dependent requests fail preflight for every affected member.
- The handle performs full consumed-file hash validation once per read session and has no durable trust cache. Narrow time selections currently stream the selected member companions before filtering. Both are deliberate correctness-first behavior; optimization is deferred.
- Parquet paths are snapshot-checked around opening and the resulting open file is used for the scan. This closes the demonstrated persistent replacement race on the trusted immutable roots; defense against an adversarial swap-and-restore inside the small open interval is deferred hardening.
- Query I/O is reported as projected compressed bytes and the operating system's warm-cache input counter, not as a single physical-device byte counter. The two quantities are kept distinct.
- The reference covers the 24-member development pilot, not the full 5,208-member universe. Creating and measuring a full-universe reference remains separately bounded work and is not authorized by this handoff.
- Full-history batch-size equality and fixed-interval direct reads validate the reader, not raw-to-feature numerical reconstruction or predictive/executable edge.

B's first observation query must open this exact reference with the exact identity and explicit roots, derive physical dependencies through `describe_endpoint_fields`, pass an explicit `EndpointSelection`, and consume `iter_endpoint_batches`. It must keep selected/eligible/unavailable/matching/nonmatching accounting separate. Strict-run work requests `include_run_boundaries=True`; feature predicates and exports remain B's ownership. C uses the same handle/registry/selection interface for unconditional marginal/joint aggregation. Neither consumer may hard-code the pilot into generic query logic.
