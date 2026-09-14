# VPS operation and bounded transfer checks

The product has been installed and checked on Ubuntu 26.04 x86_64 with isolated Python 3.13.15. The Phase 1 environment and small R2 transfer are complete; production one-second replay and EW features remain future implementation. See the [Phase 1 contract](../planning/rewrite/phase_1.md) and [measured results](../planning/rewrite/phase_1_completion.md).

## Environment layout

Keep repository/releases and the virtual environment under `/opt/tape-data-product`; persistent raw/base/feature datasets, control records, reports and bounded scratch under `/srv/tape-data-product`. Keep secrets under `/etc/tape-data-product`, outside both Git and package artifacts. Administrative login and a non-login `tape` runtime account are separate. A shared group permits controlled access without making datasets or credentials world-readable.

Use Python 3.13, as required by pyproject.toml, independently of the OS-default interpreter. Install a built wheel, record its SHA-256 and source revision, and capture the resolved Linux dependency closure. The checked direct pins in requirements-verified.txt describe a prior environment; they are not a full transitive lock. A deployment must carry its own verified lock and wheel.

The [verified Linux lock](../config/vps-python313-linux.lock) now preserves the Phase 1 runtime/test dependency closure with hashes. Install it with `python -m pip install --require-hashes -r config/vps-python313-linux.lock`, then install the built wheel with `--no-deps`. The checkpoint CI builds with setuptools 80.9.0 and uses this same lock. It is a Python 3.13 Linux baseline, not a claim of verification on every platform.

Run the [offline demonstration](dataset-build.md) outside the source checkout. On a fresh machine, initialize Matplotlib's font cache before enabling the offline audit guard:

```sh
python -c 'import matplotlib.font_manager'
```

Use the same `MPLCONFIGDIR` and interpreter for initialization and the guarded demo. The guard continues to block network/subprocess access and reads of specified source checkouts; initialization does not relax it.

## Bounded historical transport harness

[scripts/phase1/transfer_sample.py](../scripts/phase1/transfer_sample.py) is an operational proof for exactly four approved KDP/NVDA objects on 2026-09-02. It imports the installed storage adapter and verifies remote/local identities. It deliberately does not produce the newer acquisition receipts consumed by `tape-product storage stage`.

Provide a private manifest containing `objects`, with each record specifying `symbol`, `stream`, `key`, `bytes`, `sha256` and `rows`. Inspect and freeze HEAD identities before any transfer. Credentials are loaded from `/etc/tape-data-product/r2.env` using the installed adapter's `R2_BUCKET`, `R2_ENDPOINT_URL`, `R2_REGION`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY` fields. Use a dedicated object-read-only credential; never put values in command arguments.

Run the harness only inside a resource-controlled job with the scope and limits in the Phase 1 contract. `--manifest` selects the private identity file; `--root` selects the sample output directory. `--interrupt` deliberately exits during NVDA quotes after 8 MiB. Repeat without `--interrupt` to verify/reuse completed objects and restart the incomplete one. A completed rerun should perform metadata/local validation with no GET requests. The exclusive root lock prevents concurrent writers; the durable byte reservation prevents restarted attempts from exceeding the cumulative response-body budget.

The reference invocation is:

```sh
python scripts/phase1/transfer_sample.py \
  --manifest PRIVATE_MANIFEST.json --root SAMPLE_OUTPUT_DIRECTORY
```

This command alone does not impose an OS memory limit: use the existing process-tree monitor and a systemd job with MemoryMax=768M, MemorySwapMax=0, TasksMax=64, CPUQuota=200%, and RuntimeMaxSec=600. The harness itself enforces one sequential worker, a 256 MiB response-body reservation budget, a 512 MiB sample-local byte cap, and a 20 GiB disk reserve. These are sample limits, not a general corpus configuration.

The tested administrative runner needed explicit `tape` group selection because its persistent systemd user manager predated group changes. Verify the actual job identity and cgroup settings rather than assuming a new SSH session updates an existing user manager. Do not broaden filesystem permissions to compensate.

## Source admission and future builds

Hash/row verification proves that the intended objects arrived and decode correctly. Production replay must separately establish source eligibility, terminal coverage, discovery/knowledge clocks, continuity, and exact units/precision. Preserve unavailable evidence explicitly; do not synthesize receipts from filenames or first/last event times.

The measured sample does not authorize full migration. Measure the exact raw-to-base and base-to-feature paths before setting corpus storage/concurrency budgets or projecting full builds. Raw retention, derived outputs, rebuild overlap and spill all compete for the same disk.
