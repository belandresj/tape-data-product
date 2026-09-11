# Standalone verification

The compact product and causal cohort query run locally without the original checkout, private data or credentials on the tested paths. This is **ready for review and private GitHub publication**, not a completed public portfolio release or a full project backup.

| Check | Result and scope |
|---|---|
| Installation | Editable project metadata installed successfully with `pip install --no-deps --no-build-isolation -e '.[dev]'` in a new venv using system site packages. No global packages/settings were changed. |
| Controlled environment | Python 3.13 on macOS; exact directly relevant versions in `requirements-verified.txt`. This reused installed dependencies to avoid a large download beside ongoing work. A pristine dependency install and other operating systems were not tested. |
| Tests | **166 passed**, including direct/reference feature parity, persisted reconstruction and corruption rejection, masks, zero/null/maturity/halt behavior, query timing, cache/retry/resume fixtures, schema/query hashes and standalone import identity. Fixtures use invented data and fake storage clients. |
| Test resources | 14.70s pytest time, 15.345s monitored wall time; **197,738,496 bytes (188.58 MiB)** peak sampled process-tree RSS. Single test process; no resource stop. |
| Synthetic production API demo | **720 feature rows from 721 quotes and 8,640 trades**, 421 available / 299 unavailable endpoints, one right-censored window with 417 active seconds. Feature generation, persisted compact numerical reconstruction, projected reader and six-condition state machine passed. |
| Demo resources | 1.304s monitored wall time; **126,730,240 bytes (120.86 MiB)** peak sampled process-tree RSS. No resource stop. |
| Offline/source independence | Tests and final demo ran with audit guards rejecting network connections and reads from sibling source repositories. Imported product modules resolved inside the new checkout. |
| CLIs | Feature-generation and query help commands run; independent worked state oracle returns the prescribed six-second window. |
| Historical query identity | All four generated pilot configs exactly match their recorded A–D query hashes. Selected C is `edf8ce6e063a68184ff223efe810e3d670ef775c09df33c2e72fe679bf481054`. Both physical schema hashes match the frozen contract. |
| Historical evidence review | Twenty saved pilot completion manifests inspected; all complete. Their saved verified window/active-time counts reconcile with retained aggregate summaries. No feature replay or fresh remote hash audit was performed. |
| Source preservation | 407 small source/control/document hashes checked. Implementation/configuration and Git HEAD/index/config/status remain unchanged. One excluded document, `docs/tape_data_product/report_revised_draft.md`, changed concurrently; this task performed no writes there. See `source-preservation.json`. No data-cache-wide content audit was attempted. |
| Publication contents | Exact staged bytes reviewed for credential patterns, machine-specific paths, excluded data types, >512 KiB files and missing local Markdown targets. Private source credential values were compared in memory without disclosure. `.env.example` contains names with empty values only. |

The resource sampler observes the worker and descendants every 50 ms and stops at 768 MiB for these curation checks. These are measured synthetic bounds, not an acceptance forecast for a real symbol-day or corpus. No full external-data run, real-data installation acceptance, overnight job launch, R2 transfer, R2 mutation or remote Git write occurred.

No raw/vendor feature data, database files, empirical chart binaries, logs, source catalogs, credentials or previous Git history are proposed for Git. Secret scanning is heuristic defense in depth; the exact file inventory remains available for owner review. Existing source notices are retained and no license grant has been invented.

## Reproduce the bounded checks

From the checkout root, after installing dependencies:

```sh
.venv/bin/python -B scripts/measure.py handoff/tests-resources.json .venv/bin/python -B -m pytest -q
.venv/bin/python -B scripts/measure.py handoff/demo-resources.json \
  .venv/bin/python -B scripts/offline_run.py examples/synthetic_demo.py \
  --output output/new-synthetic-check
.venv/bin/python -B scripts/pilot_configs.py --output output/new-pilot-configs
.venv/bin/python -B src/04_research/verify_tape_cohort_query.py --oracle
.venv/bin/python -B scripts/review_contents.py
```

Use fresh output paths. `review_contents.py` examines the Git index; stage the intended files before running it. Scripts write only their explicitly chosen local outputs. Resource reports record the command and measured values; ignored raw logs remain local.
