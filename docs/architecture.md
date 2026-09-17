# Architecture

The installed `tape_data_product` package implements a bounded, identity-checked pipeline from canonical trade/quote inputs to one-second base measurements, endpoint/EW features, queryable release catalogs, and report artifacts.

```text
reference + minute screen
  -> canonical trade/quote pairs
  -> one-second base replay
  -> endpoint/EW feature and support tables
  -> verified full-population reference
  -> projected Arrow or scoped DuckDB queries
  -> reconciled report aggregates and figures
```

| Package/module | Responsibility |
|---|---|
| `acquisition` | Provider requests, reference membership, minute screening, and canonical T/Q normalization |
| `storage` | Local inventories, immutable object identities, verification, and explicit R2 transfer |
| `contracts` | Endpoint/EW schemas, registry, configuration, masks, timing, and transition rules |
| `replay` | Admission and streaming raw T/Q replay into `tape_base_1s_v1` |
| `features.endpoint_ew` | Base-only construction of feature and support companions |
| `calculate` / `calculate_runtime` | Immutable plans, per-member execution, restart/reuse, and resource accounting |
| `query` | Reference construction, verified projected reads, DuckDB catalogs, predicates, runs, and exports |
| `analysis` | Population accounting, ECDFs, joint distributions, worked examples, and offline rendering |
| `cli` | The installed `tape-product` command surface |

## Storage and identity

Raw, base, feature, and support tables are immutable Parquet artifacts with canonical JSON manifests. Manifests bind member identity, source/control evidence, configuration, implementation identity, byte hashes, schemas, row counts, and validation evidence. Base compatibility is separated from feature configuration so alternative feature views can reuse compatible one-second measurements without replaying raw T/Q.

The accepted report population is represented by a verified reference rather than hard-coded paths. Readers require an expected reference identity and explicit trusted data roots. They recheck manifests and consumed companion hashes before exposing rows. Machine-specific roots, private member catalogs, and credentials are not package defaults.

## Measurement and query semantics

Each row ending at `t` summarizes `[t-1s,t)`; events exactly at `t` enter the following row. Quote and trade continuity are independent. Missing or semantically unknown observations remain unavailable rather than becoming zero activity. Halts, source gaps, startup history, coverage thresholds, and current-value validity are explicit in the base, feature, and support companions.

The database exposes 27 queryable measurements with their individual validity masks. Projecting an unrelated display field does not change predicate eligibility. Scoped DuckDB sessions require explicit date bounds and verify only the selected members. Large results stream to immutable Parquet exports instead of being materialized as unbounded data frames.

## Bounded execution

Raw replay is linear in input events plus emitted seconds. Endpoint/EW calculation is linear in base rows and configured views, with fixed estimator and age-window state. Readers process one member at a time in bounded Arrow batches. Joint histograms retain fixed bin arrays; exact ECDFs use bounded DuckDB memory and disk-backed sorting. Corpus builds are controlled by immutable plans and explicit worker, memory, time, scratch, and disk-reserve limits.

Integrity, independent numerical reconstruction, and empirical evidence are different claims. Hash/schema/grid verification proves that the intended artifacts were consumed. Independent reconstruction checks calculation behavior. Neither establishes predictive power, execution quality, or trading expectancy.
