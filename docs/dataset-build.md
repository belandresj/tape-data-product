# Dataset build and reproduction

This repository contains the compact feature-calculation core and a runnable synthetic example. It does **not yet contain a complete workflow to rebuild the March 9–August 31, 2026 dataset and all report figures from vendor inputs**. The published report summarizes completed research results imported from the original analysis; that is different from having reproduced them in this checkout.

## Build stages and current coverage

| Stage | Required inputs and output | What is present here |
|---|---|---|
| 1. Reference universe and screening | Daily eligible common stocks/ADRs and minute bars → selected symbol-date pairs and first qualifying two-minute endpoint | Screen described in the root report. The daily universe acquisition and minute-screen runners are not included. |
| 2. Trade/quote acquisition | Selected pairs → complete normalized trade/quote Parquet pairs, coverage and identity records | Storage/identity helpers are included. The vendor tick-acquisition and full normalization workflow is not included. |
| 3. Build inputs | Canonical pairs, screening/discovery records, halt/continuity context → accepted member inventory | The inventory bridge reads existing selection databases. It does not create the original screening inventory. Halt-related helpers are present, but a documented fresh-input preparation workflow is missing. |
| 4. Feature calculation | Accepted member inputs → `features.parquet`, `support.parquet`, `manifest.json` | Implemented direct calculator, compact writer, supervised runner, integrity verification, and explicit numerical reconstruction are included. |
| 5. Release assembly | Completed publication records and accepted calculation identities → immutable release membership | Capture/freeze helpers are included. Historical private controls and accepted-calculation lists must be supplied separately. |
| 6. Report analysis | Accepted release → activity-filtered distributions, population counts, illustrative comparisons, and figures | Published aggregate counts, figures, and their checksums are included. The report aggregation/rendering and example-figure generation scripts are not included. |

The source folders therefore contain important working components, but the missing acquisition and report stages cannot be replaced by changing file names or adding a wrapper command.

## What can be run today without private data

Follow the [developer guide](developer-guide.md) to install dependencies, then run the [synthetic example](../examples/synthetic_demo.py) with a fresh output directory:

```sh
.venv/bin/python -B examples/synthetic_demo.py --output output/build-example
```

It generates 721 invented quotes and 8,640 invented trades over 720 seconds, calculates the compact features, verifies their structure, independently reconstructs their numerical values, and exercises the existing query reader/state machine. It is a bounded demonstration of the calculation path, not a reconstruction of the acquisition screen or historical report. For monitored execution and recorded resource evidence, see [verification](verification/README.md).

## Feature calculation from prepared real inputs

The included API path is:

```text
normalized quotes.parquet + trades.parquet
  + symbol/date + verified discovery + explicit halt/continuity context
  → direct_frozen_product.product_pairs
  → compact_product.write_partition
  → features.parquet + support.parquet + manifest.json
  → verify_complete (integrity) / audit_complete (numerical reconstruction)
```

The [direct runner](../src/04_research/run_direct_frozen_product.py) exposes `freeze`, `run`, `report`, `reconcile`, and `audit` operations. `freeze` consumes prepared `selection.sqlite` files; it does not screen minute bars. `run` requires an accepted member inventory and run configuration. Its `report` operation summarizes execution receipts; it does not produce the distribution figures in the root README. Use `--help` and the [inventory bridge](../src/04_research/compact_product_inventory.py) to inspect the actual interface; no complete owner-ready input recipe is currently bundled.

Historical feature retrieval is documented separately in [data access](data-access.md). It begins with already calculated features and is not a substitute for rebuilding them from trades and quotes. The existing cohort planner is restricted to the recorded historical release.

## Work required for complete reproduction

1. Bring in the exact maintained reference-universe, minute-screen, tick-acquisition, and normalization paths with their dependencies and smallest meaningful fixtures. Preserve the acquisition criteria and discovery timing.
2. Specify the public input/configuration schemas for canonical pairs, coverage, discovery, halts, and accepted build inventories. Supply synthetic examples; keep credentials and private catalogs outside Git.
3. Provide one supported build entry point that prepares those inputs and invokes the existing bounded calculator. Declare which steps require vendor or R2 access and which artifacts each step produces.
4. Bring in the report aggregation/rendering paths and the rules used to select and render the illustrative examples. Bind report outputs to a complete accepted release and their analysis parameters.
5. Validate each stage with bounded fixtures and representative production-path measurements before any complete external-data run. A new implementation may change provenance identities even when numerical definitions are preserved; historical plans must not be resumed under new code.

This is the next reproducibility task, not a claim that these stages are already integrated. A sensible acceptance sequence is a bounded synthetic workflow, a measured representative source prefix, a separately confirmed full symbol-day, and only then a broader dataset rebuild.

## Resource constraints

The existing calculator’s documented time bound is O(T + Q + N·F + N·H), with T/Q source events, N output seconds, F features, and H≤300. Resident state is bounded by projected input/output batches and rolling histories, plus Arrow/compression buffers; independent reconstruction costs O(N log H). Details are in the [compact layout](compact-layout.md).

Any integration specification must also bound acquisition, inventory, aggregation, rendering, and intermediate storage; those absent stages have not been resource-accepted here. Use projected input batches ≤25,000 rows (default 4,096), single-process work where practical, live process-tree RSS observation, a normal target ≤2 GiB, and a stop before 3 GiB. Report measured rows, elapsed time, peak RSS, and projected full-run resources, then obtain explicit confirmation before a full external-data acceptance run. Neither the historical report nor passing synthetic tests waives that checkpoint.
