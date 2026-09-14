# Compact layout and validation

`tape_data_product_v1` defines the mathematics; `tape_product_compact_v1` defines storage.

| Artifact | Physical columns | Contents |
|---|---:|---|
| `features.parquet` | 45 | Three keys, eighteen Float64 feature values, eighteen Int64 reason masks, six context fields |
| `support.parquet` | 63 | Three keys and sixty reconstruction/quality fields |
| `manifest.json` | — | Input/calculation identities, discovery metadata, schemas, SHA-256/length/rows, completion and validation evidence |

Authoritative field types, nullability and reason bits are in [compact_product_schema.py](../../../src/tape_data_product/features/compact_product_schema.py); units and feature definitions are in [all_feature_month_schema.py](../../../src/tape_data_product/features/all_feature_month_schema.py).

Schema hashes without Arrow metadata:

- Features: `0ee68c7b58a53262af4eaa9252ac54ba6f1a80a919ca78543d8a1943298ee3e0`.
- Support: `fe9b4307fef42d58932432201c4e0702e54af8c50bf9fa8e4b1e27645bd68f34`.

The masks encode undefined (1), unaccepted source (2), immature (4), insufficient support (8), active halt (16), carried history (32), nonpositive spread (64), and zero-total movement (128). Unknown bits are rejected. Primary retrieval requires mask zero for every requested feature and post-discovery eligibility. Finite diagnostic values can be ineligible. No universal eighteen-feature complete-case gate is implied.

Generation uses a quote cursor and trade cursor, bounded input/output batches and fixed rolling state with H≤300. Time is O(T+Q+N·F+N·H); resident working objects are O(B·input width + output batch·output width + H·F), plus Arrow/compression buffers. Inputs default to 4,096 rows and cannot exceed 25,000. Direct output buffers cannot exceed 12,288 rows. Independent reconstruction costs O(N log H). The query scans projected batches of 4,096 rows with fixed state and bounded output buffers; per-date member summaries grow with routed symbols, not raw event count. Results can grow with fragmentation, so disk quotas remain necessary.

`compact_product.write_partition` writes a completion marker only after validation. Default integrity checks establish structure/identity, not independent numerical reconstruction. `audit_complete` explicitly checks numerical reconstruction and leaves the original evidence unchanged. The synthetic demo requests reconstruction on its bounded fixture; this does not upgrade historical integrity-only partitions.
