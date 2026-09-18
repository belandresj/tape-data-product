# Existing EW half-life selection comparison

This bounded descriptive study compares the existing 30-second and 120-second
EW feature screens. It does not regenerate features, optimize thresholds, add
the report's active-tape gate, or estimate trading performance.

The runner opens the identity-bound query catalog through
`open_tape_database` and processes each represented date independently. Each
date is read once into a bounded projection containing only keys, reporting
session, and the twelve required feature fields. Conditions are evaluated once
and all 17 baseline/removal episode variants share the same ordering and
availability segmentation. It preserves unavailable rows and physical grid
gaps as period breaks. Period construction retains the existing reducer's
30-second off delay, 600-second minimum, and 0.80 occupancy semantics. The
bounded projection is deleted after the date is reduced.

Completed dates are saved under the output's in-progress directory with source,
settings, implementation, and artifact identities. A restart re-verifies the
date's product inputs and reuses only a matching intact checkpoint. Final
statistics are merged exactly across dates: stock membership uses set union,
overlap uses pooled seconds, and failure combinations are ranked only after all
date histograms are summed.

## Pilot command

On the configured project VM, with `QUERY_RELEASE`, `QUERY_CATALOG`,
`QUERY_CATALOG_IDENTITY`, `BASE_ROOT`, and `FEATURE_ROOT` loaded from the
private environment:

```bash
$QUERY_RELEASE/bin/python scripts/research/compare_half_life_selection.py \
  --start-date 2026-06-01 \
  --end-date 2026-06-05 \
  --output /srv/tape-data-product/private/half-life-comparison/2026-06-01_2026-06-05-optimized \
  --memory-limit 4GiB --threads 4 \
  --temp-directory /srv/tape-data-product/scratch/half-life-comparison
```

The output path must be new. It receives `summary.md`, paired CSV/JSON tables,
retained-period boundaries, the exact SQL/configuration, and run metadata.
Detailed member outputs belong only in an ignored/private directory. Use one
runner process and select DuckDB threads, memory, and bounded spill from measured
VM headroom. The command has no arbitrary wall-clock cutoff. The defaults are
four DuckDB threads, a 4 GiB memory limit, an 8 GiB temporary-directory limit,
and a 20 GiB free-disk reserve. These are execution settings, not changes to
the measurement definitions or permission to run the full release.

## Later expanded command — do not run without approval

The same runner accepts other inclusive bounds only with the explicit scope
override:

```bash
$QUERY_RELEASE/bin/python scripts/research/compare_half_life_selection.py \
  --start-date START_DATE \
  --end-date END_DATE \
  --output PRIVATE_OUTPUT_DIRECTORY \
  --memory-limit 4GiB --threads 4 \
  --temp-directory PRIVATE_BOUNDED_SCRATCH_DIRECTORY \
  --allow-expanded-scope
```

The override is a mechanical guard, not authorization. A larger/full-release
run still requires an approved member/read/runtime/storage budget.
