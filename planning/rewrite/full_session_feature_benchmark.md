# Bounded full-session feature benchmark — 2026-09-14 Pacific

Revision `d1d6f112fbe640f55790cfd29134f120f0bae270` and its isolated installed wheel successfully built and verified three full-session one-second base/default-feature members. The one-worker corpus target is not operationally feasible in four to five hours: the measured model projects about **32.2 hours of base replay plus 74.5 hours of feature calculation**, before a separate 31.7-hour verification pass. Mean measured compressed output projects to **99.7 GB**, while only **64.4 GB** was available above the required 80 GiB free-space reserve. No corpus job was launched.

This benchmark establishes measurement correctness and representative resource behavior for three existing local members. Historical vendor retrieval completeness remains unverified under `historical_tq_completeness_unverified_v1`; original pagination claims in retained metadata were not promoted to independently verified receipts.

## Release, sample, and guards

The installed package was used outside the development checkout at `/opt/tape-data-product/releases/feature-readiness-d1d6f11/venv`. Wheel SHA-256: `e3811afaf60073bbaf85af9bc83dc4e6021fd01a8d3504a994d1acac1279886b`. Installed package files matched the source tree at the revision above; `/opt/tape-data-product/current` and existing releases were not changed.

The local transfer manifest contains 7,085 complete T/Q pairs. Members were ordered by combined trade-plus-quote row count, with `(rows, date, symbol)` as the deterministic tie break. Nearest-rank positions `ceil(q*N)` selected the median and p90; the final ordered member selected the maximum.

| Role | Member | Rank | Trades | Quotes | Combined rows | Raw bytes |
|---|---|---:|---:|---:|---:|---:|
| Median | KPTI 2026-07-31 | 3,543 / 7,085 | 89,849 | 43,662 | 133,511 | 3,081,192 |
| p90 | GLE 2026-05-07 | 6,377 / 7,085 | 468,191 | 213,666 | 681,857 | 15,813,529 |
| Maximum | SPCX 2026-06-12 | 7,085 / 7,085 | 8,571,830 | 2,240,370 | 10,812,200 | 236,181,340 |

All three symbols are distinct. Local file sizes, footer row counts, schemas, and row-group SIP extrema matched the transfer manifest. Retained historical membership bound the exact objects and accepted empty halt overlays. Every consumed trade quantity was checked losslessly at decimal scale 9, and composite SIP/sequence order and source stability passed during replay. The reader decoded the entire listed row population for each member because footer maximum SIP times were before the configured session end. It also decoded permitted pre-session quote rows, but did not apply them as state seeds: `seed_basis=unavailable` was retained because original seed-interval completion receipts were not independently verified.

The shared hard limits were three members, one worker, one computational thread, batch size 4,096, two hours from the first metadata audit, 4 GiB process-tree RSS, 8 GiB combined owned output/scratch, and at least 80 GiB free disk. Each command ran in a cgroup with `MemoryMax=4G`, `MemorySwapMax=0`, `CPUQuota=100%`, and `TasksMax=64`. A 50 ms process-tree monitor stopped its descendants on deadline, RSS, owned-byte, or free-disk breach. No guard fired. The complete task used about 26 minutes of the shared deadline; minimum observed free disk was 150.31 GB (140.01 GiB).

## Measured results

Each output table has 57,600 rows. `Read chars` is process-level logical read accounting and therefore includes source hashing, projected Parquet decoding, evidence reads, and output validation; raw compressed input bytes are reported separately above.

| Member | Raw→base | Base→features | Installed verification | Read chars, base | Peak RSS, base/features | Peak attempt bytes, base/features |
|---|---:|---:|---:|---:|---:|---:|
| KPTI | 12.264 s | 38.376 s | 15.800 s | 30,287,706 | 194.1 / 235.4 MB | 1.95 / 11.33 MB |
| GLE | 27.184 s | 38.697 s | 16.252 s | 70,748,019 | 197.0 / 236.1 MB | 3.95 / 13.90 MB |
| SPCX | 287.067 s | 36.520 s | 16.528 s | 662,515,722 | 201.5 / 234.6 MB | 3.08 / 8.03 MB |

Raw replay scales with event volume: SPCX contains 81 times the median member's raw rows and took 23 times as long to build base. Feature calculation is instead dominated by the fixed 57,600-row history and stayed between 36.5 and 38.7 seconds. The maximum sampled process-tree RSS for a production stage was 236.1 MB. Independent sampled verification peaked at 816.8 MB; this larger verifier deliberately holds completed tables while calculating explicit histories and is not the production streaming path.

| Member | Base bytes | Feature bytes | Support bytes | Metadata bytes |
|---|---:|---:|---:|---:|
| KPTI | 1,939,148 | 9,912,253 | 1,411,877 | 10,237 |
| GLE | 3,939,740 | 10,329,774 | 3,569,164 | 10,217 |
| SPCX | 3,069,218 | 5,515,572 | 2,514,425 | 10,245 |

All support diagnostics were present on all 57,600 rows. Activity-rate features had 57,541 valid fast rows and 57,301 valid slow rows for every member, exactly excluding the 59/299 startup rows. KPTI was otherwise broadly available; its main additional loss was midpoint-change-age history (57,099 fast and 56,571 slow valid rows). GLE's fast RMS had 57,508 valid rows and fast spread 57,475, with short low-coverage intervals. SPCX had only about 29.2–29.5 thousand valid quote-derived fast/slow rows because its raw quotes begin well after 04:00 and later quote states also lacked support; trade activity remained available after startup. Across the sample, unavailability reasons were exactly `STARTUP`, `NO_SUPPORTED_DATA`, and `LOW_COVERAGE`. No halt or source-status reason was fabricated.

Installed base and feature integrity/semantic validators passed every member. An independent streaming raw integrator and explicit-history feature calculator checked 10 rows at session startup, 10 beginning at 10:00 ET, and 10 beginning at 19:50 ET for each member. It compared all 48 base, 51 feature, and 37 support fields at those 90 row positions. Base differences were zero; maximum absolute feature differences were `2.27e-11` (KPTI), `1.31e-10` (GLE), and `1.05e-9` (SPCX), within `rtol=2e-10, atol=1e-9`; maximum support difference was `2.45e-12`. This is sampled independent reconstruction, not full-session independent verification.

A completed KPTI rerun returned through the reuse path in 19.5 seconds including source/output validation; all companion hashes and mtimes were unchanged, proving no recomputation. In a 57,600-row synthetic fixture, forced termination of base and feature writers left no complete target. Subsequent invocations restarted from session start in distinct attempts and produced base, feature, and support tables with 57,600 rows that passed installed verification.

## Corpus projection and admission audit

The corpus has 1,967,991,110 raw events and 408,096,000 one-second rows per output table. A least-squares model over the three measured sessions gives `9.24 seconds/member + 25.70 microseconds/raw event` for base replay, or 32.23 hours corpus-wide. Pairwise models give 31.86–33.10 hours, a narrow interpolation range but not a statistical confidence interval. Feature runtime projects from the per-member mean to 74.52 hours. The resulting one-worker build estimate is **106.74 hours**; a separate installed verification pass adds about **31.74 hours**, for **138.48 hours** total acceptance work.

Hypothetical perfect two- and four-worker scaling would reduce build-only time to 53.4 and 26.7 hours. These figures are unmeasured and production multiprocessing remains intentionally unimplemented. They are far above the four-to-five-hour target. The measured feature rate is about 1,521 one-second rows/s; five hours requires 22,672 rows/s and four hours 28,340 rows/s. The next optimization should target the base-to-feature pass—especially the exact age-window maintenance and repeated validation work—because it contributes about 70% of projected build time. After that, profile replay's per-event Python decoding/state-update cost. Concurrency cannot close the measured gap by itself.

Mean sample compression projects to 21.13 GB base, 60.83 GB features, 17.70 GB support, and 0.073 GB metadata: **99.74 GB total**. The member-observed low/high envelope is 62.89–126.46 GB and reflects real variation in validity/compressibility, not a probabilistic interval. Current headroom above the 80 GiB reserve is 64.41 GB. Even the low envelope leaves insufficient space for the required 8 GiB scratch allowance; the mean projection exceeds headroom by 35.33 GB. A corpus build therefore fails the current disk contract independently of runtime.

The metadata-only audit opened every local Parquet footer without decoding the corpus: no file, byte-count, row-count, schema, or SIP-statistics failures were found across 14,170 objects. Remaining admission work is explicit:

- 863 members lie outside the retained 6,222-member historical release and lack its identity-bound selection/halt-overlay evidence; halt context is blocking until separately established.
- Composite ordering, scale-9 trade precision, and source stability are consumption-time checks for all unbuilt members. Only the three benchmark members have passed them for the new pipeline.
- Original object metadata lacked quote-unit declarations and trade-precision declarations for all 7,085 members. The benchmark used per-object identity-bound declarations under the documented post-November-2025 share-unit policy and then checked every consumed trade. Equivalent descriptors/checks have not been generated corpus-wide.
- The transport audit reported 994 objects without requested-window metadata, 994 without pagination-complete metadata, 2,066 without original extrema, and six non-REST method records. Missing pagination receipts are non-blocking only under the accepted historical exception and remain labeled unverified. Footer extrema exist for every local object but do not prove vendor retrieval completeness. The six non-REST records require evidence review rather than method-name rejection.

The evidence supports the production calculations on these three members, but not a full-run readiness claim. The next decision is whether to optimize the feature pass and physical layout first or provision materially more storage; both are required before producing a new immutable corpus plan. No downloads, R2 writes, schedule, multiprocessing implementation, or full-corpus execution occurred.

Finalized measurement JSON and availability counters are preserved under `/srv/tape-data-product/control/full-session-benchmark-d1d6f11/evidence/`; bounded output attempts remain under `/srv/tape-data-product/scratch/full-session-benchmark-d1d6f11/`, and guard configuration plus private verifier code are under the private control directory.
