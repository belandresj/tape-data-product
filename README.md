# Tape Data Product

Direction-neutral U.S. equity tape measurements and causal interval retrieval. Turn SIP trades and NBBO quotes into interpretable movement, concentration, friction, activity and freshness measurements; retrieve the stock-time intervals meeting an explicit researcher-defined query.

This repository contains the implemented compact 60s/300s product and its causal cohort query. It is a curated product checkout with synthetic examples and a completed five-date descriptive pilot. It is not a predictive strategy, validated trading model, full project archive, or redistribution of market data. Public release still requires a licensing and presentation decision.

## Quickstart

Python 3.11+ on macOS or Linux. Run from the checkout root:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -B -m pytest -q
.venv/bin/python -B examples/synthetic_demo.py
```

The demo generates **721 invented quotes and 8,640 invented trades over 720 seconds**, computes the eighteen rolling features and their support fields, independently reconstructs the numerical output, and applies the [selected six-condition query](config/tape_cohort_300s_ms3_p050_selected_v1.json). It writes an ignored `output/synthetic-demo/` directory, including a summary and temporary synthetic Parquet files. Choose a fresh `--output` directory when rerunning; existing results are never overwritten. No credentials or network access are needed after dependency installation.

This is a source-checkout application: installation prepares dependencies and project metadata; run the documented scripts from the checkout. It does not yet expose an installed Python library or wheel-distributed CLI. The [verification report](handoff/VERIFICATION.md) records the actual controlled environment and resource measurements, including the limits of that verification.

## Product and timing

Each row ending at `t` measures `[t−1s,t)` on the SIP information clock. Events stamped exactly `t` enter the next row. The full grid is 57,600 seconds per symbol-day, 04:00–20:00 America/New_York. Features continue across reporting-session boundaries. The final 20:00 endpoint describes the last source interval; it cannot initiate new query membership.

Nine measurements use trailing 60s and 300s histories: mean absolute five-second midpoint movement, movement participation, duration-weighted quoted spread, trade rate, dollar rate, trade-age p90, quote-age p90, midpoint-change-age p90, and mean movement/spread. Movement uses overlapping five-second log changes in one-second duration-weighted midpoints. Participation is `(sum d)^2 / (n * sum d^2)`; it measures concentration, ignoring sign and order. Zero-total movement leaves mean movement at zero and participation undefined.

Query eligibility requires post-discovery status and zero reason masks for the requested features. Five consecutive strict passes confirm entry at the fifth endpoint, without backdating. Relaxed continuation conditions and five consecutive failures determine economic exit; unavailability, halt and continuity boundaries take effect immediately. Membership is causal at endpoint resolution; completed duration is known only afterward. Historical halt overlays and incomplete vendor received-at information limit claims of live reproducibility.

## Architecture

```text
Authorized SIP trades + NBBO quotes
  -> market-state decoder + bounded direct feature calculator
  -> compact partition: features + support + immutable completion manifest
  -> verified feature-only cache + projected reader
  -> causal cohort state machine
  -> intervals, strict runs, coverage, daily supply and concentration
```

The [feature contract](docs/tape_data_product/README.md), [physical schema](docs/compact-layout.md) and [query contract](docs/query-contract.md) govern the maintained product. Older RTH V1 and episode-local 5m/10m/20m MU/X/Q methodologies are separate contracts. Retained upstream modules/specifications support decoding, numerical regression and provenance; they do not promote older feature families or clustering workflows into this product.

Existing numbered source directories preserve working imports and file identities. `src/03_features/direct_frozen_product.py` calculates the compact measurements. `src/04_research/run_tape_cohort_query.py` handles planning and supervised retrieval; `verify_tape_cohort_query.py` checks persisted outputs. Details are in the [scope and dependency map](handoff/SCOPE.md).

## Evidence and limitations

In the [five-date pilot](reports/pilot.md), adding participation entry ≥0.50 and continuation ≥0.40 retained **9,760 / 12,177 active stock-seconds (80.2%)** of the baseline and yielded 23 windows. Two of five dates had no matches. These are descriptive stock-time observations, not independent signals or evidence of guaranteed daily supply. The pilot influenced threshold selection and is development evidence.

No entry/exit execution model, costs, latency, adverse excursion, profitability or predictive expectancy has been tested here. Overlapping histories, unequal symbol/date contributions, selection bias and historical annotations constrain interpretation. Quoted spread is not realized execution cost; turnover is not executable capacity.

[Real-data access and reproduction](docs/data-access.md) require authorized private inputs. No full external-data acceptance run was performed for this repository. No six-month cohort completion is claimed. See [migration and preservation decisions](handoff/MIGRATION.md) before replacing the original workspace.

## Rights

No license grant has been added. Existing source notices are preserved; no root license file was found in the source checkout. A public/open-source license requires an explicit owner decision. Raw data, cached market data, detailed vendor-derived feature rows and existing empirical chart exports are excluded; redistribution rights are not assumed. Retained pilot tables are aggregate research summaries and still require review before a public release.
