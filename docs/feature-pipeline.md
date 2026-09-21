# One-second base and endpoint/EW builders

The installed CLI now has immutable per-member builders for the new rewrite identities:

```text
tape-product base admit --inventory INVENTORY.json --evidence EVIDENCE.json --output ADMISSIONS
tape-product base build --source-pair PAIR.json --context CONTEXT.json --output BASE_MEMBER
tape-product base verify --input BASE_MEMBER
tape-product features build-from-base --base BASE_MEMBER --output FEATURE_MEMBER
tape-product features verify-from-base --input FEATURE_MEMBER --base BASE_MEMBER
```

The base builder consumes only locally present canonical Parquet files described by a strict, hash-bound source pair and context. It does not access R2. A fresh build checks each complete source hash before replay, freezes its device/inode/size/mtime/ctime identity, validates strictly increasing `(sip_timestamp, sequence_number)` keys while consuming projected batches, and refuses publication if any frozen identity changes. Generated batches are semantically validated before writing; the finished file is hashed once and its Parquet schema and row count are checked before atomic publication. A completed base contains `base.parquet`, `context.json`, and an atomically published `manifest.json`.

The feature builder reads only a completed base and its context companion. On a fresh build it verifies the base hashes and metadata once, freezes all companion identities, and performs base semantic/key validation in the same streaming scan used for calculation. Generated feature/support batches are validated before writing; each finished file is hashed once and checked for schema, count, and subsequent mutation before atomic publication. The public standalone verifier remains a separate strict full read for completed-member reuse and explicit audits. The builder emits `features.parquet`, `support.parquet`, and a manifest bound to the base-manifest hash. Default outputs are the 30s/120s half-life EW views and 60s/300s exact-age p90 views. Alternative feature settings can reuse a compatible base; changing the raw trade reporting-age policy cannot.

Completion means integrity and schema verification passed. Independent reconstruction is a separate validation claim and is recorded separately; the CLI does not relabel integrity checking as reconstruction. The V2 path uses `features build-from-base` and `features verify-from-base`.

## Admission boundary

The normal `tape_source_pair_v1` admission path requires identity-bound terminal coverage, verified quote-size units, a declared trade-quantity representation, and accepted halt/continuity context. A separate historical `tape_source_pair_v2` path permits the explicitly recorded status `unverified_missing_original_vendor_pagination_receipts`, with `terminal_complete=false`. This exception preserves the historical completeness limitation; it does not prove complete vendor retrieval. Both paths retain the remaining identity, units, context, and numerical checks. Other missing required evidence blocks admission rather than becoming synthetic zero activity or an assumed empty halt overlay. A prefix context must declare exact start, end, and row count.

`tape-product calculate plan` writes an immutable, hash-addressed plan. `calculate run` fails before calculation if transfer completion, admission, measurements, paths, or identities are unresolved. It has no automatic retry, scheduling, download, or R2 mutation behavior.
