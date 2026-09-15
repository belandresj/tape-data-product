# Feature batch optimization checkpoint

**Status:** complete development checkpoint on 2026-09-15. Installed-release and real-data acceptance remain deferred.

The base-to-default-feature calculator now reads validated Parquet batches through primitive Arrow/NumPy columns and explicit validity arrays instead of materializing row dictionaries. It writes bounded 4,096-row feature/support column buffers directly as Arrow batches. Schema positions, output mappings, thresholds, and interruption lookup are constructed once. Mathematically identical possible-exposure histories are shared across spread, activity, and bid/ask-size families; the three activity numerators also share one usable-exposure history. Estimator state, reset behavior, schemas, compression, validation, and public build interfaces are unchanged.

The retained test-only row calculator is the differential baseline, not a second runtime backend. The focused comparison uses changing quote/activity inputs, ordinary unavailable endpoints, independent quote/trade gaps, a halt, all default views, a three-view alternate configuration, all configured age-window shapes, and input batches of 1, 7, 4,096, and 25,000 rows. It requires exact keys, nulls, masks, counts, and batch invariance, with the existing `rtol=1e-10`, `atol=1e-12` only for floating results. Existing independent explicit-history expectations remain in force.

On a deterministic 14,400-row synthetic base with maturing 30s/120s views, changing prices/sizes/activity, and periodic unavailable quote endpoints, three isolated single-thread runs of the complete `_calculate` path (Parquet read, feature/support state, validation, and ZSTD-3 Parquet write) measured:

| Calculator | Median time | Median throughput | Median peak RSS |
|---|---:|---:|---:|
| Row baseline | 3.355 s | 4,292 rows/s | 216.0 MiB |
| Column/batch implementation | 1.971 s | 7,306 rows/s | 171.3 MiB |

This is a **1.70x complete-calculation speedup** and a **44.6 MiB (20.7%) peak-RSS reduction** on the synthetic workload. Fixture creation was outside timing; each timed repeat ran in a fresh process with one worker and Arrow/BLAS computational threads limited to one. The profile was separate from reported timing.

The remaining measured bottleneck is native Python estimator work: scaled-sum add/normalize/decay and history-mask evaluation dominate the optimized profile. Output construction, columnar validation, and Parquet writing were about 0.18 seconds in the profiled 14,400-row run. No compiler/native dependency or alternate production backend was added.

All 82 focused endpoint/pipeline/contract/columnar tests passed, followed by the single final full regression run: **304 passed in 56.32 seconds**. No wheel was built, no installed release was changed, and no raw or real symbol-day data was read or rebuilt.
