# Reproduce the final eight-condition V2 comparison

The root report's 577 fast and 854 slow retained periods come from
`analyze_half_life_dollar_throughput_streaming.py` with a fixed dollar-throughput
floor of **10,000 USD/s**, on the common fast/slow availability population.
This is a V2 analysis of stored endpoint/EW features; it does not rebuild raw
trades/quotes, optimize thresholds, or simulate trading.

## Source and inputs

The runner, helper, projection, and tests were recovered from study revision
`2e05ce86f6cd3726cb4a2d102932454a4b72f8df`. The runner and its calculation
sources were compared byte for byte with the scripts in the installed study
release. [source_identity.json](source_identity.json) records those file hashes
and the original study wheel identity. This recovery does not change the
published data or attribute the original execution to a newer commit.

Install the current package in a separate Python 3.13 environment and run the
scripts from this repository. See [dataset construction](../../../docs/dataset-build.md)
and [historical input requirements](../../../docs/data-access.md). Define:

- `STUDY_PYTHON`: the absolute path to that environment's Python executable.
- `QUERY_CATALOG` and `QUERY_CATALOG_IDENTITY`: the verified endpoint catalog and its identity.
- `BASE_ROOT` and `FEATURE_ROOT`: trusted roots of the matching completed V2 partitions.
- `BASELINE_STUDY`: a new output directory for the seven-condition baseline.
- `STUDY_OUTPUT`, `STUDY_CHECKPOINTS`, `STUDY_SCRATCH`, `BASELINE_SCRATCH`: separate private paths.
- `STUDY_MEASUREMENT`: a new private resource-record filename.

The historical study used catalog identity
`4fe3df9b375af2dd1981d0464c06a5e99714f4b0f9eaad5c3466a76a31276dd5`
and endpoint release revision `ea2e16128225a91ceb6003fcb3f6ef80985ba8e0`.
The [public summary](../../../reports/report_v2/data/half_life_dollar_10k_summary.json)
records the full identities. Private source paths and credentials are not defaults.

## 1. Produce the matching seven-condition baseline

The final runner verifies its unconstrained results against this baseline;
it needs `matching_endpoint_overlap.json`, `retained_periods.json`, and
`config.json` from the baseline directory. An identity-verified saved baseline
with the required scope can be reused. The five-date example alone cannot
reconcile the full 122-date study.

```sh
"$STUDY_PYTHON" scripts/research/compare_half_life_selection.py \
  --start-date 2026-03-09 --end-date 2026-08-31 \
  --output "$BASELINE_STUDY" --allow-expanded-scope \
  --threads 4 --memory-limit 4GiB \
  --temp-directory "$BASELINE_SCRATCH" --max-temp-directory-size 8GiB
```

## 2. Apply the final dollar constraint and reconstruct periods

```sh
"$STUDY_PYTHON" scripts/research/monitor_bounded_command.py \
  --measurement "$STUDY_MEASUREMENT" \
  --watch "$STUDY_OUTPUT" --watch "$STUDY_CHECKPOINTS" --watch "$STUDY_SCRATCH" \
  --rss-limit-bytes 8589934592 --owned-byte-limit 12884901888 \
  -- "$STUDY_PYTHON" scripts/research/analyze_half_life_dollar_throughput_streaming.py \
  --start-date 2026-03-09 --end-date 2026-08-31 \
  --baseline-study "$BASELINE_STUDY" \
  --output "$STUDY_OUTPUT" --checkpoint-root "$STUDY_CHECKPOINTS" \
  --temp-directory "$STUDY_SCRATCH" --dollar-floor 10000 \
  --threads 4 --memory-limit 4GiB --max-temp-directory-size 8GiB --batch-size 4096
```

Export the catalog/root variables listed above before either command, or pass
`--catalog`, `--identity`, `--base-root`, and `--feature-root` explicitly.
The commands describe full historical reproduction, not an automatic installation
step. Measure a bounded sample and establish the run's resource budget before
executing them. The runner requires a 20 GiB free-disk reserve. The monitor owns
only its child process group and the explicitly watched output/scratch paths.
Watch the new output's parent as well if accounting for its temporary
`.in-progress` directory is required; that parent must be dedicated to this run.

Each date is reduced separately. Identity-bound checkpoints retain compact
aggregates, periods, and narrow dollar-value records; temporary wide projections
are removed after verification. An identical checkpoint can be reused with a
new final output path. `--finalize-only` requires all selected checkpoints to
exist and pass their identity checks. Finalization still reads the catalog,
baseline, and saved checkpoint artifacts.

## Outputs and verification

The final directory contains the constrained membership, matching/period overlap,
retained periods, dollar-gate accounting, saved SQL/configuration, and run metadata.
`baseline_reproduction.json` must report exact original endpoint and period
reconciliation. Compare the constrained totals and overlap arithmetic with the
public summary; hashes of a newly produced run need not equal historical hashes
because execution metadata and installed identities can differ.

The helper `analyze_half_life_dollar_throughput.py` also has an earlier pilot CLI,
and `pilot_config.json` records that pilot's **25,000 USD/s** setting. Neither is
the final report invocation. The streaming runner above requires exactly 10,000.
`benchmark_config.json` records the original three-date resource sample.

The restored tests cover threshold boundaries, common availability, missing
versus below-floor observations, period segmentation, deterministic date merges,
and checkpoint mutation detection. CI runs these with the baseline reducer tests.
No historical corpus is bundled or rerun by those tests.
