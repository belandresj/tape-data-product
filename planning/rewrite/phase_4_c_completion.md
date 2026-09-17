# Phase 4 checkpoint C — first real-data joint preview

**Status: expanded C preview and second-size scaling check delivered on 2026-09-15; checkpoint C and Phase 4 remain incomplete.** This record covers the six pooled joint histograms, activity/session partitions, and a bounded 120-member scaling sample. Full-population execution, session ECDFs, and final report integration remain subsequent work.

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

This is a retrospectively selected stratified development sample, not a frequency estimate for the 5,208-member corpus or the wider market. Observations are dependent because five-second returns overlap and all plotted features are exponentially smoothed. Full-population execution, ECDFs and equal-member sensitivity remain unperformed; the expanded activity/session follow-up is recorded below.

## Expanded activity pilot, 2026-09-15

The follow-up implementation at `fbdb613c6bd7b79176a06b27be5fb0ad3f7b192a` adds the requested activity-conditioned test run without changing or overwriting the unconditional preview. Its isolated wheel SHA-256 is `340d1541fdf01478421a96c230b9d0e8a882aa4a8a33d0e9abb5488a2e1a0154`. Twelve focused installed-wheel tests passed in 1.46 seconds, covering inclusive gate boundaries, all rate-band boundaries, unavailable gate inputs, pair-specific feature validity, session/pooled reconciliation, and batch-division invariance.

The expanded scan projects the original eight fields plus trade-age p90 at window60s and window300s. It applies the explicitly labeled legacy-like gates `trade_rate_hl30s >= 1/s and trade_age_p90_window60s <= 2s` and `trade_rate_hl120s >= 1/s and trade_age_p90_window300s <= 2s`. It saves unconditional and gated histograms by pooled/premarket/RTH/after-hours session, plus exact-zero, positive-below-one, 1–<10, 10–<30, 30–<100 and >=100 trades/s partitions where applicable. The output contains 208 panel partitions, 579,184 extended histogram cells and 4,992 per-member contribution rows.

The fixed pilot retains 188,369 endpoints at the fast gate (13.63% of represented time; 13.84% of gate-valid time) and 170,576 at the slow gate (12.34% represented; 12.59% gate-valid). Represented-time retention is 7.66%/7.04% premarket, 24.73%/22.22% RTH and 3.79%/3.58% after hours for fast/slow. The gate-passing spread-panel band counts are:

| Band | Fast 30s/60s gate | Slow 120s/300s gate |
|---|---:|---:|
| 1–<10 trades/s | 110,543 | 92,458 |
| 10–<30 trades/s | 62,554 | 64,173 |
| 30–<100 trades/s | 14,368 | 13,637 |
| >=100 trades/s | 887 | 308 |

The detailed plots reproduce the old report's main qualitative activity structure: higher activity bands move toward larger RMS, and RMS–spread becomes a clearer positive diagonal. Participation continues to overlap broadly within and across activity bands. The >=100 trades/s band is retained and reconciled but is too sparse in this pilot for interpretation.

The gated pilot is much more member-concentrated than the unconditional preview. `2026-04-22/NOW` contributes 17.4% of fast gated pair-valid observations and 19.2% of slow observations; the top five members contribute 59.7% and 63.7%. These plots validate mechanics and motivate the larger population run; they are not estimates of corpus frequencies.

The one-pass real invocation processed 1,382,400 rows in 360 batches. File verification took 1.523 seconds, scan/expanded aggregation 18.604 seconds, rendering three pages from saved tables 6.811 seconds, and total wall time 29.670 seconds. Peak sampled process-tree RSS was 756,797,440 bytes, output was 1,796,809 bytes and spill was zero. The cache was uncontrolled/warm and the result is not a cold benchmark. The VM artifacts are `/srv/tape-data-product/reports/phase4-c-expanded-pilot-fbdb613`; the ignored inspection copy is `local_docs/phase4-c-expanded-pilot-fbdb613`.

All validity partitions, extended histogram totals, activity-band partitions, session-to-pooled sums, member contributions, gate/member accounting and payload hashes reconcile. The three figures were visually checked at original resolution; axes, logarithmic color scales, reference lines, labels and footnotes are legible and unclipped. This accepts the expanded 24-member pilot mechanics only; the larger scaling evidence follows.

## 120-member scaling check, 2026-09-15

The bounded scaling reference contains exactly five feature-blind selections from each event-count rank quartile in each of the six represented months: 120 symbol-days and 6,912,000 one-second rows. Selection is deterministic (`month_event_rank_quartiles_five_per_cell_v1`, seed `phase4-scaling-v1`) and does not inspect feature values. Reference identity is `43d4de38f3d6e5d623e85b705a1c327ea7e830a1d5f83eac8a2bff02691abe35`. Constructing and fully validating the reference took 43.191 seconds and consumed 1,558,069,782 validation bytes plus 20,055,676 metadata bytes.

The generalized expanded runner at `0b1c1d8bdd865246ef5170e50b01ced404be24f3` processed the reference once, projecting the same ten fields and producing the same 208 numerical panel partitions. Its isolated wheel SHA-256 is `6ac1657a8573dec8859092d5606cd0165129694152f591cea2f080c58b5de21e`. The presentation-only follow-up at `d696cf1076200075fb6f5871053d7bf594b2fb26` makes titles population-aware and adds saved-table re-rendering; its isolated wheel SHA-256 is `292f8b4c62473a402182469468f8c5299284c59bf16f10a4619623c3db1af6b6`. Twenty-eight focused tests passed from that installed wheel in 1.73 seconds.

| Measurement | 24 members | 120 members | Scaling |
|---|---:|---:|---:|
| Represented rows | 1,382,400 | 6,912,000 | 5.00x |
| Scan and expanded aggregation | 18.604 s | 92.535 s | 4.974x |
| File verification | 1.523 s | 5.408 s | 3.55x |
| Figure rendering | 6.811 s | 6.773 s | effectively flat |
| Peak sampled process-tree RSS | 756,797,440 B | 763,707,392 B | 1.009x |
| Spill | 0 B | 0 B | unchanged |

The 120-member run completed in 107.401 seconds total under one worker/library thread, a 300-second wall limit, a 2 GiB sampled RSS stop, a 3 GiB hard ceiling, and no swap. The cache was uncontrolled and mixed/warm after reference construction, so the verification timing is not a cold-cache claim. All 579,184 histogram cells, 24,960 member-contribution rows, validity partitions, activity bands, session-to-pooled sums, gate accounting, and artifact hashes reconcile exactly.

The larger sample materially reduces composition noise. The fast and slow gates retain 781,985 (11.31%) and 694,727 (10.05%) represented observations. The largest member contributes about 5.0% of fast and 5.45% of slow gated pair-valid observations, versus 17.4% and 19.2% in the 24-member pilot; top-five concentration falls from 59.7%/63.7% to approximately 19.6%/21.3%. The activity-conditioned RMS/spread diagonal and the upward activity shift remain visible and are substantially smoother. This supports producing the full-universe descriptive plots, while still not establishing predictive or executable edge.

The measured row-linear projection from 120 to all 5,208 members is 66.9 minutes for aggregation and 3.9 minutes for verification, followed by roughly seven seconds of rendering. Because the verification measurement was warm and operational overhead is not perfectly linear, budget **75–100 minutes after the full reference exists**. Building and validating that full reference projects to another 31 minutes centrally; budget **about two hours end to end, with a 2.5-hour operational window** for the first full run. Memory should remain below 1 GiB on the observed flat scaling curve, so the existing 2 GiB stop and 3 GiB hard ceiling remain adequate. These are extrapolations, not measured full-corpus timings.

Numerical outputs are durable. Re-rendering the three figures from saved histogram and coverage tables took 6.682 seconds and did not rescan feature data, so legend, title, color, and layout iterations do not require rerunning the long aggregation. The VM result is `/srv/tape-data-product/reports/phase4-c-expanded-scaling120-0b1c1d8`; the ignored inspection copy is `local_docs/phase4-c-expanded-scaling120-0b1c1d8`. The full-universe run was not started by this scaling check.
