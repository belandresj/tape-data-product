# Raw replay optimization checkpoint

**Status:** complete development checkpoint on 2026-09-15. Installed-release, real-data, and concurrency acceptance remain deferred.

Starting from `c09d5ba43bcd7f49342c28d660a66b7fad20b654`, the raw T/Q replay now decodes projected Parquet batches into bounded primitive column buffers instead of converting every column to Python objects and allocating a dictionary per event. Fixed policy sets, column positions, observation/gap interval indexes, and break metadata are constructed once. Quote and trade replay remain separate and ordered by their original `(sip_timestamp, sequence_number)` keys.

Trade quantities are still admitted through the lossless scale-9 parser. Per-second totals now add the already-admitted integer units directly with the same decimal128 maximum check, avoiding an integer-to-Decimal-to-integer round trip for every eligible trade. Final shares remain exact `decimal128(38, 9)` values. Public interfaces, schemas, metadata, compression, validation, hashing, atomic completion, and source/evidence mutation checks are unchanged. Because `builder.py` is included in the scoped base implementation identity, old outputs do not silently reuse the optimized runtime identity.

## Correctness evidence

The focused replay/endpoint/contract/columnar suite passed **86 tests**. Added representation tests compare the optimized column decoder with the retained row/dictionary reference for nulls, fractional and fallback quantities, tied timestamps with increasing sequence numbers, and batch sizes 1, 2, and 4,096. Literal tests cover indexed interval boundaries and exact decimal128 maximum/overflow behavior. Existing independent endpoint expectations continue to cover eligibility/reporting cutoff, quote validity and locks, gap/halt transitions, seeding, source mutation, batch invariance, restart/reuse, and exact fractional totals.

On the benchmark fixture, every logical value matched a fresh baseline artifact across all **600 rows and 48 base fields** in each of five paired repetitions. The final full regression suite passed once after production changes: **308 passed in 56.08 seconds**, with 367 MiB peak process-tree RSS.

## Complete fresh-build benchmark

The deterministic input contains 600,000 changing quote events and 300,000 trades over 600 seconds. Trades include scale-9 fractional quantities plus known ineligible conditions, corrections, and late reports; quotes include known invalid/nonfirm and explicit-lock conditions. Fixture generation is outside timing. Every run used a fresh output, batch size 4,096, ZSTD level 3, one worker, and one Arrow/BLAS computational thread. The timed public `build_base_partition` path includes descriptor/evidence loading, full source identity/schema checks, projected decoding, replay, output construction and columnar validation, ZSTD Parquet writing, output hashing, and pre-commit source/control stability checks.

Five paired runs totaled 125.01 timed seconds:

| Calculator | Median wall time | Throughput | Median peak RSS |
|---|---:|---:|---:|
| Baseline `c09d5ba` | 17.084 s | 52,681 events/s | 158.1 MiB |
| Optimized | 7.899 s | 113,933 events/s | 160.6 MiB |

This is a **2.16x complete-build speedup**. Median peak RSS increased by **2.5 MiB (1.6%)**, reflecting the bounded native-value buffers; measured peak remained far below the 2 GiB task ceiling. Each output contained 600 rows and a 45,696-byte base Parquet file.

The separate diagnostic profile deliberately inflated wall time and is used only for attribution. The remaining cost is Python per-event work: bounded event iteration/decoding, quote classification/state updates, trade classification including exact decimal parsing, and time integration. In that profile, event iteration had 3.86 s self time, quote application 2.50 s, exact `share_units` parsing 1.19 s, trade classification 1.03 s, and integration 0.86 s. Source hashing was 0.032 s cumulative, columnar validation 0.008 s, and Parquet writing 0.003 s; output construction was immaterial for this event-dense, 600-row case. A materially larger next gain would require reducing Python calls in quote replay/decoding or moving that kernel to native code, which is outside this checkpoint.

No raw corpus was read, no real symbol-day was built, and no wheel, installed release, multiprocessing run, R2 operation, or corpus projection was performed.
