# V2 report artifacts

This directory contains the public, reviewable artifacts supporting the endpoint/EW report published as the repository root `README.md`.

## Contents

- [`report_v2_final.md`](report_v2_final.md): preserved final client-facing report before its root-relative link adjustment.
- `assets/`: rendered publication figures in PNG and SVG form.
- `data/`: sanitized aggregates, summaries, and artifact identities supporting the published figures.

Private member catalogs, member-level contribution tables, detailed market-data rows, absolute VM paths, execution logs, and credentials are intentionally excluded.

Reusable calculation and rendering code lives in [`src/tape_data_product/analysis`](../../src/tape_data_product/analysis). Reference construction and query access live in [`src/tape_data_product/query`](../../src/tape_data_product/query). The report directory contains publication artifacts, not a second query or calculation implementation.

## Lineage highlights

The five-date structured-tape query returned 189,162 matching endpoints from 286 represented symbol-days. The saved reducer retained periods lasting at least ten minutes with at least 80% matching occupancy while allowing interruptions shorter than 30 seconds. Its publication-safe aggregate is [`data/strong_tape_episode_population.json`](data/strong_tape_episode_population.json).

The GPUS/CAST comparison is rendered by [`gpus_cast_comparison.py`](../../src/tape_data_product/analysis/gpus_cast_comparison.py) from identity-checked base partitions and [`data/gpus_cast_comparison_numerical_source.json`](data/gpus_cast_comparison_numerical_source.json). The fixed endpoints and rebasing values are saved so the illustration cannot drift silently.

`daily_completed_members` is produced by [`endpoint_population.py`](../../src/tape_data_product/analysis/endpoint_population.py) from verified inventory metadata. `daily_active_tape_hours` is produced from reconciled member gate accounting. Both publish date-level aggregates without disclosing symbol membership.

The full-release joint-distribution analysis used reference identity `a1fab9c41bb51122ad49f9976f79b125a5542d2c2d4c73c4c2b0a23684d787ea`, scanned all 5,208 members and 299,980,800 represented seconds, and saved pooled/session and activity-conditioned accounting. Tracked figures disclose their population and validity denominators; detailed member contributions remain private.
