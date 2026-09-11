# Authorized real-data access and reproduction

The synthetic demo requires no account. Historical reproduction requires permission to use the vendor data and access to the private object store; GitHub access alone is insufficient. This repository does not supply the vendor data, private catalogs, credentials or a redistribution license.

## Inputs to obtain from the data owner

Request the immutable release controls for identity
`c9abea6e3faea62e2471e8b8b883494c612cc4d48b4e44e82d9a7b47dd9aae50`:

- `manifest.json`, `accepted.jsonl`, `reconciliation.jsonl`, `capture_exclusions.jsonl`, and any relative captured-manifest files referenced by those controls, preserving their layout and hashes.
- The exact accepted calculation-identity allowlist, including the recorded OCG recovery identity. Do not relax it to accept an arbitrary calculation with the same version name.
- Read permission for the exact feature and completion-manifest objects referenced by the controls. Ordinary cohort queries download features and manifests, not raw trades/quotes or support files.

These are the private source artifacts recorded under `research/report_r2/streaming_20260910/release/` and its adjacent `accepted_calculations.json` in the original workspace. Those are provenance locators, not runtime paths in this checkout. Ask the owner to provide a verified copy under `private/release/` and `private/accepted_calculations.json`, or specify another authorized location. Do not invent a reduced catalog and claim it has the original release identity.

The historical release reports 6,222 symbol-days on 122 acquired dates, March 9–August 31, 2026. This is selection/feature availability, not completion of the six-month cohort query. Curation inspected saved pilot evidence; it did not rehash this full remote release.

## Credentials

Create a local `.env` using the four variable names in [.env.example](../.env.example). The loader reads this checkout's file through `dotenv_values`; it does not automatically use another checkout or merely exported shell variables. Supply the authorized bucket, endpoint, access key and secret privately. Never commit this file. Read-only credentials are sufficient for cohort retrieval. Existing storage helpers also expose publication/deletion operations, which require separate explicit scope.

## Plan, measure, then execute

From the checkout root, after placing authorized inputs:

```sh
mkdir -p private/cache private/cohort
.venv/bin/python -B src/04_research/run_tape_cohort_query.py plan \
  --release private/release \
  --calculations private/accepted_calculations.json \
  --query config/tape_cohort_300s_ms3_p050_selected_v1.json \
  --date-from 2026-06-18 --date-to 2026-06-18 \
  --cache private/cache --output private/cohort
.venv/bin/python -B src/04_research/run_tape_cohort_query.py checkpoint \
  --plan private/cohort/run_plan.json
```

The planner deliberately rejects every other release identity. General support for new releases requires a reviewed release-contract change; it is not implied by accepting a Parquet path. Planning can also select OCG as a reconstruction/provenance checkpoint outside the requested date. Inspect the generated plan before external reads.

The checkpoint uses the production reader on bounded prefixes. Review measured process-tree RSS, elapsed time, output/scratch bytes and resource projection before approving any full symbol-day or larger run. The shipped example does not authorize a real-data run. Preserve a 3 GiB disk floor; query settings use one worker, 4,096-row batches, a 1 GiB RSS stop, 1 GiB cache, 512 MiB scratch allowance and 1 GiB results allowance. Do not automatically resume after a resource stop. A synthetic run is not a full-data resource acceptance checkpoint.

Only after an explicit approval is recorded in a newly generated plan:

```sh
.venv/bin/python -B src/04_research/run_tape_cohort_query.py run --plan private/cohort/run_plan.json
.venv/bin/python -B src/04_research/verify_tape_cohort_query.py --run private/cohort/RUN_ID
```

Replace `RUN_ID` with the returned run directory. Inspect completion, expected membership, missing dates and final resource evidence as well as artifact verification. Do not set an approval field merely to bypass the checkpoint. Old source-workspace plans bind different code identities and paths; do not copy an approval or resume them here.

To reproduce all pilot comparisons, generate the configs with `scripts/pilot_configs.py`, then repeat planning/checkpoint/approval/run for A–D on 2026-03-13, 2026-04-08, 2026-06-18, 2026-07-20 and 2026-08-04. Compare each query hash with [pilot provenance](../reports/pilot-provenance.json). No full-market scan or threshold retuning is required to reproduce that historical design.

## Rebuilding features

See [dataset build and reproduction](dataset-build.md) for the complete stage map. The instructions above query already calculated features; they do not recreate the acquisition universe or historical report.

The direct calculator accepts normalized quote/trade Parquet paths, day, symbol, discovery metadata and explicit halt context. The [demo](../examples/synthetic_demo.py) shows the complete local API: `direct_frozen_product.product_pairs` → `compact_product.write_partition` → `verify_complete` / `audit_complete` → projected query reader. Use the same schemas and source-event semantics for authorized real inputs. The retained `run_direct_frozen_product.py` orchestrator additionally needs frozen selection inventories and measured, approved run configuration; these private inventories are not supplied.

Canonical raw storage uses `tq/session_date=YYYY-MM-DD/symbol=SYMBOL/{trades,quotes}.parquet`. The [R2 interface](../src/01_data/r2_tq_storage.py) verifies SHA-256, bytes and row counts. Publication is immutable; existing keys must match exactly and are never overwritten. Raw JSONL exports are not canonical Parquet and require separate coverage auditing and normalization. Avoid downloading entire corpora; use bounded projected reads and private staging. This curation made no R2 requests or mutations.
