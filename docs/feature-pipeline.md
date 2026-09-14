# One-second base and endpoint/EW builders

The installed CLI now has immutable per-member builders for the new rewrite identities:

```text
tape-product base admit --inventory INVENTORY.json --evidence EVIDENCE.json --output ADMISSIONS
tape-product base build --source-pair PAIR.json --context CONTEXT.json --output BASE_MEMBER
tape-product base verify --input BASE_MEMBER
tape-product features build-from-base --base BASE_MEMBER --output FEATURE_MEMBER
tape-product features verify-from-base --input FEATURE_MEMBER --base BASE_MEMBER
```

The base builder consumes only locally present canonical Parquet files described by a strict, hash-bound source pair and context. It does not access R2. It checks the complete file identities before and after replay, validates strictly increasing `(sip_timestamp, sequence_number)` keys while consuming projected batches, and emits one row per declared second. A completed base contains `base.parquet`, `context.json`, and an atomically published `manifest.json`.

The feature builder reads only a completed base and its context companion. It emits `features.parquet`, `support.parquet`, and a manifest bound to the base-manifest hash. Default outputs are the 30s/120s half-life EW views and 60s/300s exact-age p90 views. Alternative feature settings can reuse a compatible base; changing the raw trade reporting-age policy cannot.

Completion means integrity and schema verification passed. Independent reconstruction is a separate validation claim and is recorded separately; the CLI does not relabel integrity checking as reconstruction. Existing `features build` and `features verify` commands retain the legacy compact-product meaning.

## Admission boundary

Accepted calculation requires identity-bound terminal coverage, verified quote-size units, a declared trade-quantity representation, and accepted halt/continuity context. Missing evidence produces a blocked admission, not synthetic zero activity or an assumed empty halt overlay. A prefix context must declare exact start, end, and row count.

`tape-product calculate plan` writes an immutable, hash-addressed plan. `calculate run` fails before calculation if transfer completion, admission, measurements, paths, or identities are unresolved. It has no automatic retry, scheduling, download, or R2 mutation behavior.
