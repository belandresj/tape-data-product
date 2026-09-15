# Phase 4 checkpoint C — first real-data joint preview

**Status: first C preview delivered on 2026-09-15; checkpoint C and Phase 4 remain incomplete.** This record covers the requested six pooled joint histograms on the fixed 24-member checkpoint-A pilot. Session ECDFs, activity-conditioned pages, full-population execution, and final report integration are subsequent work.

## Scope and identity

The analysis branch is `codex/phase4-c-preview`. It starts from A's final handoff `96d74da8d37b8f6956db16405fc03bc621a453d9`, incorporates the report-first Phase 4 specification as a scoped Git commit, and adds the preview implementation at `d3662968bb33990b30063e5000009e2ace7edbc6`. The isolated wheel SHA-256 is `1e28b95ba625e9b508257141ae8cc09813ff422c1b40ae55ae9282c1c8117ed5`; the package resolved from a separate non-editable Python 3.13 environment.

Input was exactly the A reference at `/srv/tape-data-product/control/phase4-a-52b5b35-20260915/pilot-reference-v1-deterministic`, identity `4a410bd8321cfe0466dc7ecd02c8a5762a679c320d64376c24cea076465ae832`, and its existing endpoint/EW base and feature roots. The analysis used explicit `historical_membership` selection and all complete premarket, RTH, and after-hours sessions: 24 symbol-days and 1,382,400 represented one-second rows. It did not rebuild, resample, expand, copy, or filter members.

## Implemented calculation

One verified A-reader handle projected only the eight RMS, spread, participation, and eligible-trade-rate fields for the 30s and 120s half-lives, including their registry-defined masks and keys. Optional support and run-boundary columns were disabled. One 4,096-row-batch scan accumulated all six pair-specific histograms and member contributions. No activity gate, ratio threshold, all-feature complete-case rule, row-dictionary materialization, or repeated panel scan was used.

The fixed configuration has 50 bins per displayed axis. Positive RMS, spread, and rate use logarithmic axes; participation uses linear `[0,1]`. Exact zero, positive underflow, displayed bins, and overflow are retained in the numerical grid. Each rendered cell is the percentage of that panel's pair-valid observations. Corresponding 30s/120s panels share axes and color normalization. The RMS–spread panels include RMS/spread reference lines at 0.1, 1, and 10 where visible.

The packaged literal checks cover exact lower/internal/final bin boundaries, valid zeros, unavailable values, pair-specific validity, off-axis values, and equality across one versus two batch divisions. Seven focused tests passed from the isolated installed wheel in 1.41 seconds. A's acceptance suite and digest comparisons were not rerun.

## Real output and accounting

The immutable VM output is `/srv/tape-data-product/reports/phase4-c-first-preview-d366296`; a user-inspection copy is in ignored `local_docs/phase4-c-first-preview-d366296`. It contains the six-panel PNG, long-form Parquet histogram counts and percentages, JSON/CSV coverage, per-member Parquet contributions, explicit bin and selection configuration, run metadata, and payload hashes.

| Panel | Pair-valid | Unavailable | Plotted | Zero/tail/off-axis | Largest member share |
|---|---:|---:|---:|---:|---:|
| RMS vs spread, 30s | 1,371,914 | 10,486 | 1,201,265 (87.56%) | 170,649 | 4.19% |
| RMS vs spread, 120s | 1,363,722 | 18,678 | 1,293,277 (94.83%) | 70,445 | 4.20% |
| RMS vs participation, 30s | 1,348,844 | 33,556 | 1,210,120 (89.72%) | 138,724 | 4.27% |
| RMS vs participation, 120s | 1,344,666 | 37,734 | 1,313,957 (97.72%) | 30,709 | 4.26% |
| RMS vs trade rate, 30s | 1,373,502 | 8,898 | 1,078,724 (78.54%) | 294,778 | 4.19% |
| RMS vs trade rate, 120s | 1,368,149 | 14,251 | 1,155,976 (84.49%) | 212,173 | 4.19% |

All 24 members contribute to every panel. The largest contributor is `2026-03-19/ALM`, but its 4.19%–4.27% shares are close to the equal-duration baseline of 4.17%; no panel total is concentrated in a single member. The lower plotted fractions are disclosed rather than renormalized away. For example, 30s RMS has 10.10% positive mass below the 0.01 bps display floor and 1.79% exact zero; 30s trade rate has 17.79% positive mass below 0.01 trades/s and 1.42% exact zero. Spread has no pair-valid zero mass; about 2.2% lies above the 3,000 bps display ceiling at both half-lives. The saved extended-bin grid retains all of these observations.

The validity partitions, extended histogram totals, plotted cells, and member contribution totals reconcile exactly for all panels. Seven payload artifacts were independently rehashed after transfer. The rendered figure was inspected at original resolution: axes, titles, reference lines, captions, color scales, and footnote are legible; no panel or annotation is clipped.

## Measured execution

The single real invocation ran with one worker/library thread, a 300-second wall limit, 200% CPU quota, 2 GiB sampled process-tree RSS stop, 3 GiB hard memory ceiling, no swap, and the required disk reserve. It completed 360 batches with:

| Measurement | Result |
|---|---:|
| File verification | 1.216 s; 316,720,293 bytes |
| Scan and six-histogram aggregation | 24.037 s |
| Rendering from saved tables | 1.881 s |
| Total wall time | 27.198 s |
| Peak sampled process-tree RSS | 249,163,776 bytes |
| Owned output | 585,108 bytes |
| Spill | 0 bytes |

The shared OS cache was uncontrolled and had prior checkpoint-A reads, so this is not labeled cold. Process filesystem input blocks were zero during the measured invocation, consistent with cache-resident reads; the logical verification-byte count is reported separately.

Reproducible invocation, using the isolated installed build and the same resource wrapper:

```sh
python -m tape_data_product.analysis.endpoint_joint_preview \
  --reference /srv/tape-data-product/control/phase4-a-52b5b35-20260915/pilot-reference-v1-deterministic \
  --reference-identity 4a410bd8321cfe0466dc7ecd02c8a5762a679c320d64376c24cea076465ae832 \
  --base-root /srv/tape-data-product/derived/endpoint-ew-v1-ea2e161-20260915/base \
  --features-root /srv/tape-data-product/derived/endpoint-ew-v1-ea2e161-20260915/features \
  --output NEW_EMPTY_OUTPUT_DIRECTORY \
  --cache-state 'describe the actual cache state'
```

## Descriptive finding and limits

The RMS–spread panels are broad and multimodal, with most visible mass below the RMS/spread=1 line rather than a single tight proportional ridge. The 120s view is more concentrated than the 30s view. RMS–participation mass is strongest at low participation, with secondary islands; this is mechanically related to the common return moments and is not evidence of independent dimensions. RMS–trade-rate shows the clearest upward-sloping visible bands, especially at 120s, but the multiple bands and quiet/off-axis mass are consistent with member/session composition as well as any within-member association. No correlation, causal, predictive, or executable-edge claim follows from these pooled heatmaps.

This is a retrospectively selected stratified development sample, not a frequency estimate for the 5,208-member corpus or the wider market. Observations are dependent because five-second returns overlap and all plotted features are exponentially smoothed. Full-population execution, ECDFs, session splits, equal-member sensitivity, and activity-conditioned figures remain unperformed.
