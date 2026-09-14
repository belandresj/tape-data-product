# Storage and migration implementation plan

Status: planning update, 2026-09-14. User priority: corpus scans first; efficient feature rebuilds during development and future research second. No transfer, storage benchmark, or new calculator is claimed complete by this document. Code inspection baseline: `462e2e92c14e7493de4a65cff1ae2bc992910fcc`, with existing local planning edits preserved. This supplements the phase plan. Execute the next operational job using [raw migration](raw_migration.md), then start [Phase 2 checkpoint A](phase_2.md). The settings below remain benchmark candidates except where the execution spec makes a conservative initial guard explicit.

## 1. Working design

Use immutable canonical raw files, a persisted one-second base, and materialized default features. Keep the accepted logical schemas and exact numerical types. Raw acquisition and raw transfer are distinct from calculation. Feature configuration changes read base from session start and do not read raw T/Q; adding a measurement absent from base can still require raw processing.

Start with Parquet and the already installed DuckDB query engine as the benchmark baseline, not a claim that their physical layout is optimal. Avoid introducing a database service or duplicating the entire feature release into a native database before measuring a need.

| Layer | Initial physical candidate | Reason |
|---|---|---|
| Raw | Preserve canonical object bytes and source identities in local files | No conversion cost or new raw representation; traceability to R2 |
| Base | Date groups, ordered by session date, symbol, interval end; complete member histories contiguous | Sequential feature rebuilds with bounded state |
| Features | Date-grouped, moderately sized multi-member Parquet shards, ordered by session date, symbol, interval end | Reduce file overhead for corpus scans while retaining sequential histories |
| Support | Separate logical support table, matching keys and release membership | Ordinary predicates should not read all diagnostics |
| Catalog | Small local metadata catalog and immutable release manifests | Select exact files, members, schemas and configurations without scanning all raw data |

Build/retry unit is a symbol-session. Serving file need not be a symbol-session. Write verified member outputs to bounded staging and compact completed members into deterministic shards. Record member-to-shard mapping and key ranges. Commit manifests only after output verification; queries read one completed release identity, never a mixture of old and new shards. Incomplete members restart from session start under the accepted V1 contract. File boundaries must not reset estimator state.

Compaction should append already ordered member streams in deterministic key order, avoiding a whole-corpus sort. Keep staging, packing overlap and retained versions in the budget. Cleanup of previously retained data is a separate explicitly scoped operation. Do not retain both every member staging file and every packed file indefinitely by default.

Feature queries may need current ages from base. Benchmark projecting just keys and the three current ages through a validated join versus a reproducible serving projection containing those ages with features. A serving projection must retain the logical schema provenance and validate equality; it must not silently redefine the feature schema. Benchmark support joins separately when requested.

Do not partition by feature value or threshold: research predicates vary and the extra layouts create rebuild costs. Date filtering can prune date partitions; an all-date arbitrary-feature scan may still read most row groups. The baseline optimization is reading only needed columns, reducing file overhead and avoiding raw calculation during queries. Run queries must retain nonmatching/invalid boundaries or use exact timestamp adjacency so filtering does not bridge gaps.

## 2. Raw migration checkpoint, before the full calculation build

First perform read-only preparation:

1. Verify VM reachability, installed package/revision, free filesystem bytes, data paths and active jobs. Reverify the retained four-file Phase 1 sample. Previous completion records are not a current machine inventory.
2. Refresh canonical R2 key/byte inventory and available identities. Reconcile paired T/Q objects, selected research membership and associated discovery, coverage, halt/continuity, units and source records. Keep detailed keys and private records outside Git.
3. Separate transportable objects from members admissible for production calculation. Missing semantic evidence must remain explicit; an exact file copy does not prove source completeness.
4. Produce a concrete transfer manifest: member/date scope, object count, exact bytes, identity-verification method, missing records, destination, resumability, runtime/download caps and resource limits. Preserve R2 and exclude old derived releases unless separately needed.
5. Present the raw-only budget and obtain explicit bulk-transfer confirmation. Phase 1 sample permission does not authorize the corpus. Begin with one transfer worker and the proven recovery/reuse model; scaling requires measurement.

Early raw staging does not require completed Phase 2/3 calculators. It does require a conservative storage envelope. Proposed initial policy: leave at least 80 GiB free after raw copy and worst-case transfer partials. This is a planning reserve for derived work, not evidence the full product fits; refresh against actual filesystem capacity. If this cannot be met, propose a smaller selected scope or capacity change before copying. Do not delete existing data to meet it.

Use current exact free bytes, remaining raw bytes (excluding verified reusable copies), additional control bytes and worst-case partial/retry overlap. Check disk before each object; interruption must not exceed a persisted cumulative download cap. Resume cannot silently reset budgets. Credentials and private manifests remain outside Git. Report transfer duration as an estimate based on measured end-to-end transport, not advertised bandwidth.

## 3. Bounded layout and workload experiment

Implement after independently verified base/feature builders exist. First use synthetic rows conforming to the accepted schemas to validate layout and query correctness. Use the retained real sample for bounded production-path measurements, then propose an explicitly bounded representative extension if necessary. Two members on one date cannot establish corpus compression or scan performance.

Select representative members using source size/activity metadata rather than desired feature outcomes: quiet, median, and quote-heavy sessions, with multiple dates. State exact members, bytes, prefix length and limits before external processing. Session-start prefixes preserve initialization; they do not establish full-session compression. Add bounded full sessions when measuring that uncertainty. Synthetic scale tests establish mechanics, not real compression or market results.

Compare a small number of candidates sequentially:

1. Per-member Parquet as the simplest baseline.
2. Date-grouped packed Parquet, sorted by date/symbol/time. Initial target 128 MiB compressed files; allow a short final file and avoid filling it by crossing unrelated release identities. Compare daily grouping with monthly grouping only if daily groups produce many small files.
3. For the better grouping, compare 64K versus 128K row groups and ZSTD level 1 versus 3. These are benchmark candidates, not selected production settings. Input/output batches remain 4,096 rows, maximum 25,000; row-group buffering is separate and must be measured and capped explicitly.

Only test native DuckDB feature materialization if the best Parquet candidate misses the scan targets or measured scans show a plausible substantial benefit. Include ingestion time, duplicate bytes and rebuild replacement cost in that comparison.

| Workload | Fixed benchmark behavior | Main measurements |
|---|---|---|
| Narrow corpus scan | Project keys, RMS/spread, trade rate and required validity; evaluate RMS/spread > 2 and trades/s >= 10; aggregate counts by symbol/date | First-run/repeat latency, bytes read, CPU, RSS; exact counts |
| Broad corpus scan | Multiple movement, spread, size and freshness fields; RTH and full-session variants | Decode/scan throughput; correct availability denominators |
| Strict matching runs | Same predicates, return contiguous intervals with gap/invalid boundaries preserved | End-to-end latency including ordering/reduction; exact intervals |
| Descriptive report | Marginal histogram counts and selected joint bins, with fixed bins across candidates | Full scan and aggregation cost; exact totals |
| Feature rebuild | Recompute both default EW views and age p90s, then one alternative half-life, sequentially from base session start | Seconds/s, read/write bytes, CPU, process-tree RSS, peak disk; zero raw reads |
| Focused inspection | One symbol-session and a short time range | Latency and unnecessary bytes read |

Freeze exact query text, column projections, parameters and expected results before comparing layouts. No layout may gain speed by discarding coverage or changing population. Validate logical key uniqueness, schema/types, counts, null/reason masks, numerical outputs and run intervals across every candidate. Repacking and batch/restart boundaries must preserve results.

Use one job at a time initially. Proposed VM benchmark limits: one calculation worker, two query threads, 4 GiB process-tree RSS stop, 2 GiB query-engine memory limit, 8 GiB scratch cap, and 20 GiB filesystem reserve. Configure an additional cgroup cap and monitor RSS independently; engine memory settings do not bound all allocations. No swaps or unrelated workload should silently contaminate measurements. Retain stricter existing development-machine limits locally.

Record package versions, threads, host/job activity, filesystem bytes, input/output bytes and output rows. Run one first-read and three repeated trials in rotated candidate order; report median and range. Call a run cold-cache only when cache state has actually been controlled and documented; reopening a connection does not clear the OS cache. Do not evict system caches during unrelated jobs.

Proposed full-corpus engineering targets, not performance claims: narrow aggregate scan <=10 s first-read and <=3 s repeated; broad scan/run/report workload <=30 s first-read; one-symbol inspection <=1 s repeated. Validate feasibility from representative measurements before promising these targets. Select the simplest layout meeting them. If none meets them, report measured bottlenecks and revised options. Among scan-competitive layouts (within 15% on the primary workloads), prefer lower rebuild cost and disk usage. Reject a layout causing >20% rebuild slowdown versus the best bounded baseline unless a quantified scan improvement justifies the tradeoff.

## 4. Full-build budget and completion evidence

For scale only: 7,085 full 16-hour symbol-sessions would contain 408,096,000 one-second rows. This is a conditional arithmetic scenario, not accepted membership or a forecast; actual session lengths and admission differ. At that row count, each 100 compressed bytes per row costs about 40.8 GB per retained representation. Do not assume dense one-second output is smaller than raw for quiet symbols.

Project raw, base, features, support, optional serving copies, metadata/reports, largest in-flight member, packing overlap, feature-version replacement, scratch and growth separately. Use per-stratum observed bytes/row and rows/session, with conservative ranges. Include the old and new feature releases coexisting during a rebuild; base need not be duplicated when only a feature configuration changes. No compressed size estimate should be based on schema width alone.

Before the full external calculation run, present measured raw-to-base and base-to-feature throughput, process-tree RSS, disk high-water, selected layout/query results, corpus membership reconciliation and the complete capacity estimate. Obtain the existing full-run confirmation. Early raw transfer does not waive this checkpoint.

Immediate deliverable is the read-only VM/inventory report and reviewable raw-transfer scope. Phase 2 owns base writer correctness and bounded raw-to-base measurements; Phase 3 owns rebuild measurements; Phase 4 owns scan correctness and layout comparison; Phase 5 owns corpus build and actual full-scale validation. Update this document with measured choices instead of presenting candidates as implemented behavior.

## References

Existing package schemas: `src/tape_data_product/contracts/schemas.py`; current dependencies already include DuckDB and PyArrow. Physical tuning rationale: [DuckDB Parquet tips](https://duckdb.org/docs/current/data/parquet/tips), [Parquet projection/filter pushdown](https://duckdb.org/docs/stable/data/parquet/overview), and [file-format performance](https://duckdb.org/docs/current/guides/performance/file_formats). Documentation informs benchmark candidates; production behavior must be checked against the installed version.
