# Feature readiness checkpoint — 2026-09-14 Pacific

The transfer, bounded KDP/NVDA acceptance, and a three-member full-session benchmark are complete, but the full feature build is not ready for execution. The original [full-session benchmark](full_session_feature_benchmark.md) projected about 106.7 one-worker build hours. The subsequent [fresh-build validation optimization](fresh_build_validation_optimization.md) reduced the same three-member projection to about 64.2 hours, or 8.0 hours under hypothetical perfect eight-worker scaling; the four-to-five-hour objective still requires calculation-loop optimization and measured multi-worker scaling. Mean compressed output remains 99.7 GB under the original sample. No full-corpus job was started. Historical vendor retrieval completeness is recorded as unverified under the owner's accepted policy.

## Implemented performance improvement

Revision `d746f6ce0a2de49ec1999a25a3d041038fd4a9ef` removes unused reason-name decoding during value validation, reuses per-batch schema/registry/member checks, and converts bounded Arrow columns to Python lists once per batch. It retains all validation passes, exact integer/Decimal values, null/domain checks, feature definitions and output schemas. Auxiliary validator memory is now bounded by batch rows times field count (normal batch 4,096; hard maximum 25,000).

All 273 tests passed in 46.75 seconds; the monitored process tree peaked at 257,339,392 bytes RSS. Independent code review found no blocking correctness issue. The new isolated installed wheel was exercised outside the development checkout. Its changed modules match the committed source; wheel SHA-256 is `cf73dd3ab24c6e47b7d9f84b219ea16a4703e22e7cc4c847d9a8aee948162af5`. The existing release and full calculation plan were not changed.

## Measured synthetic throughput

Each run computes four independent 3,600-second invented members through the installed public base and feature builders: 14,400 rows per table. The fixture contains two trades and two quotes followed by quiet history. Wall time includes task/process setup. Worker tests use an isolated benchmark harness; the production calculation runner still supports one worker.

| Version | Workers | Wall seconds | Aggregate rows/s |
|---|---:|---:|---:|
| Original 59d3abc | 1 | 65.981 | 218 |
| Original 59d3abc | 2 | 34.815 | 414 |
| Original 59d3abc | 4 | 18.494 | 779 |
| Optimized d746f6c | 1 | 12.386 | 1,163 |
| Optimized d746f6c | 2 | 7.820 | 1,841 |
| Optimized d746f6c | 4 | 5.096 | 2,826 |

The single-worker improvement is 5.33x. All 72 base/features/support Parquet comparisons across the six configurations and four tasks were byte-identical. Optimized four-worker process-tree RSS peaked at 848,281,600 bytes; no resource guard fired.

The planned 7,085 members contain 408,096,000 one-second rows per table. Five hours requires 22,672 aggregate rows/s (four hours: 28,340). Straight row-count extrapolation of the optimized four-worker synthetic result is about 40.1 hours, **not a production ETA**. Short quiet fixtures omit representative event replay, member-size variation, full-day history behavior and production-runner overhead. They establish a bottleneck and improvement, not accepted throughput. Concurrency alone is not demonstrated sufficient for the five-hour target.

Synthetic output totals 792,420 bytes per 3,600-row member across the three tables (220.12 bytes/row). Naive extrapolation is 89.83 GB. Available derived-data headroom at preflight was approximately 63.48 GB after the 80 GiB free-space reserve and 2 GiB scratch allocation. Full-day compression and real-data measurements are required before treating either figure as a storage forecast.

## Real-source reconciliation

The immutable transfer completion record reports 14,306 objects and 47,204,810,169 bytes: 14,302 verified transfers plus four reused sample objects. The canonical catalog contains 7,085 paired members. Transport identity does not establish complete historical vendor retrieval.

Read-only inspection of the retained September 2 KDP/NVDA files, limited to the authorized first 720 seconds and quote seeds, decoded 20,480 rows including batch lookahead and read 231,145,784 bytes including identity checks. All 7,387 sample trade quantities were representable at scale 9. No inspected composite-order failure or file-hash change occurred. These were source diagnostics, not feature calculations or independent feature reconstruction.

Four read-only R2 HEAD checks contain row counts, hashes and window labels, but no terminal pagination receipts. Reference upload code hardcodes those labels; reference download code follows pagination, but code alone cannot establish that a particular retrieval completed. Original download completion receipts bound to the retained objects have not been recovered. No replacement download was performed.

The accepted follow-up does not change that finding. Missing original pagination receipts are non-blocking for calculation of this existing historical corpus, but version 2 descriptors retain `terminal_complete=false` and record retrieval completeness as `unverified_missing_original_vendor_pagination_receipts`. File identity, schema, ordering, units, precision, halt context and numerical validation remain blocking checks.

Recovered historical job-list/run-manifest hashes establish that KDP and NVDA were among the 245 members evaluated in the September 2 external halt bundle. The original Nasdaq acquisition succeeded and contained neither symbol. This supports empty historical external-only halt overlays for these two members; it does not validate inferred feed gaps or real-time availability. Small original evidence records were preserved privately, without introducing a legacy runtime dependency.

## Bounded real-data result

The installed base and default-feature builders processed exactly KDP and NVDA on 2026-09-02 for 720 seconds per member. The four build invocations took 4.435 seconds in total; peak sampled process-tree RSS was 177,831,936 bytes. The complete base/feature/support directories occupy 618,011 bytes. Independent reconstruction checked all 48 base, 51 feature and 37 support fields on every row. Base values matched exactly; the largest feature difference was `1.8190e-11`, and the largest support difference was `1.4211e-13`.

The calculation preflight now parses the actual raw-migration completion record and reconciles its manifest hash, 14,306 objects, 47,204,810,169 bytes, 14,302 verified objects/47,090,531,533 bytes, and four reused objects/114,278,636 bytes against the immutable manifest. Focused tests cover successful reconciliation plus manifest, total, state and incomplete-record rejection.

## Remaining steps before a manual full run

1. Move the remaining per-row feature state and per-event replay loops out of interpreted Python, then repeat the same bounded full-session workload. Redundant fresh-build validation passes are already removed; feature calculation and busy-member raw replay are now the measured targets.
2. Resolve corpus admission evidence, including halt context for 863 members outside the retained historical release and per-member consumption checks. Missing original pagination receipts remain non-blocking but unverified under the accepted policy.
3. Implement and measure production multi-worker disjoint-member ownership, atomic outputs and ledger/restart behavior across 1/2/4/6/8 workers. Ideal eight-worker scaling of the current optimized build is about 8.0 hours and remains unmeasured.
4. Only after runtime, storage, admission, and concurrency evidence pass should a new immutable plan be tied to the selected installed wheel and an explicit readiness decision. Obtain the full-run confirmation required by AGENTS.md. The old 59d3abc plan remains blocked and must not be edited in place.

## Private evidence and reproduction

Earlier VM control records remain under `/srv/tape-data-product/control/feature-readiness-20260915/`. The bounded real-data descriptors, admission report, measurements and independent comparison are under `/srv/tape-data-product/control/feature-readiness-real-dc54eeb/`; outputs are under `/srv/tape-data-product/scratch/feature-readiness-real-dc54eeb/`. Installed releases remain isolated and `/opt/tape-data-product/current` was not changed.

Reproduction entry points are `scripts/readiness/benchmark_synthetic.py`, `inspect_sample.py` and `profile_synthetic.py`. Synthetic fixtures are explicitly supplied from this repository's tests. None of these benchmark results is an accepted real-data measurement, and this checkpoint does not complete Phase 2 or Phase 3 acceptance.
