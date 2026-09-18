# Existing EW half-life selection comparison

This bounded descriptive study compares the existing 30-second and 120-second
EW feature screens. It does not regenerate features, optimize thresholds, add
the report's active-tape gate, or estimate trading performance.

The runner opens the identity-bound query catalog through
`open_tape_database`, writes one ordered disposable projection containing only
keys, reporting session, and the twelve required feature fields, and reuses
that projection for all results. It preserves unavailable rows and physical
grid gaps as period breaks. Period construction uses the existing structured
episode reducer with an explicit 30-second off delay, 600-second minimum, and
0.80 occupancy. The detailed projection is deleted after successful reduction.

## Pilot command

On the configured project VM, with `QUERY_RELEASE`, `QUERY_CATALOG`,
`QUERY_CATALOG_IDENTITY`, `BASE_ROOT`, and `FEATURE_ROOT` loaded from the
private environment:

```bash
$QUERY_RELEASE/bin/python scripts/research/compare_half_life_selection.py \
  --start-date 2026-06-01 \
  --end-date 2026-06-05 \
  --output /srv/tape-data-product/private/half-life-comparison/2026-06-01_2026-06-05
```

The output path must be new. It receives `summary.md`, paired CSV/JSON tables,
retained-period boundaries, the exact SQL/configuration, and run metadata.
Detailed member outputs belong only in an ignored/private directory.
Run this inside the existing one-worker resource-controlled VM job: one library
thread, at most 200% CPU, a 2 GiB process-tree RSS stop, 3 GiB hard memory/no
swap, and a 300-second wall limit for the workload. The runner itself uses a
single DuckDB thread, no spill, a 4 GiB output/scratch cap, and a 20 GiB
free-disk reserve. If a limit is reached, inspect the partial failure and revise
the measured plan; do not increase the limits or expand membership implicitly.

## Later expanded command — do not run without approval

The same runner accepts other inclusive bounds only with the explicit scope
override:

```bash
$QUERY_RELEASE/bin/python scripts/research/compare_half_life_selection.py \
  --start-date START_DATE \
  --end-date END_DATE \
  --output PRIVATE_OUTPUT_DIRECTORY \
  --allow-expanded-scope
```

The override is a mechanical guard, not authorization. A larger/full-release
run still requires an approved member/read/runtime/storage budget.
