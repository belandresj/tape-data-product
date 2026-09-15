# VPS operation and bounded transfer checks

The product has been installed and checked on Ubuntu 26.04 x86_64 with isolated Python 3.13.15. The Phase 1 environment and small R2 transfer are complete; production one-second replay and EW features remain future implementation. See the [Phase 1 contract](../planning/rewrite/phase_1.md) and [measured results](../planning/rewrite/phase_1_completion.md).

## Environment layout

Use the project-specific local SSH alias `tape-data-product-vps` (endpoint and login are in the operator's `~/.ssh/config`, outside Git). It was verified against the Phase 1 project host on 2026-09-14. Do not select another available SSH alias merely because it exists: `capstone-db-hil` is not this project's Phase 1 endpoint. Before diagnosing a VM outage, reconcile the resolved SSH destination with the last successful project deployment and verify the project runtime/data paths. The migration task's three reported timeouts on that date targeted the unrelated alias; a subsequent read-only connection to the project host succeeded.

Keep repository/releases and the virtual environment under `/opt/tape-data-product`; persistent raw/base/feature datasets, control records, reports and bounded scratch under `/srv/tape-data-product`. Keep secrets under `/etc/tape-data-product`, outside both Git and package artifacts. Administrative login and a non-login `tape` runtime account are separate. A shared group permits controlled access without making datasets or credentials world-readable.

### Required connection preflight for every VM job

1. Resolve `ssh -G tape-data-product-vps` and inspect hostname, user, port and identity selection locally. Keep endpoint/key details out of tracked logs. An absent alias may resolve as a literal DNS name; that is not a verified destination. If configuration is missing, recover the last successful project connection from private operational records or the Phase 1 task history. Do not substitute another alias from `~/.ssh/config`.
2. Use noninteractive public-key authentication and `StrictHostKeyChecking=yes`; do not bypass a host-key mismatch. On first connection for the job, verify hostname, login identity, the Git origin of `/opt/tape-data-product/repository`, and the project runtime/data paths using read-only commands. Compare against the recorded project deployment before allowing writes. An unexpected identity or missing project paths requires reconciliation, not automatic provisioning on that host.
3. If connection fails, state the actual failure stage: local name/configuration resolution, TCP connection, SSH handshake/host key, authentication, or remote command. A TCP timeout does not establish that the VM is stopped. Recheck the intended destination against prior successful evidence before retrying or recommending provider/firewall changes.
4. Every handoff must name the project alias and this document and preserve the private connection locator. Sanitizing or deleting operational logs must not remove the only usable connection mapping. A successful SSH login alone is insufficient evidence that this is the correct project server.

Example read-only identity check after resolving the alias:

```sh
ssh tape-data-product-vps 'hostname; id -un; git -C /opt/tape-data-product/repository remote get-url origin; readlink -f /opt/tape-data-product/current; test -d /srv/tape-data-product && echo project-data-root-present'
```

The Git origin must identify this `tape-data-product` repository, allowing equivalent SSH/HTTPS URL forms. Reconcile an intentional host migration or path change with its deployment record; never silently assume the old layout applies to a different machine.

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

## Endpoint/EW calculation runner

The candidate release is installed beside, and never switched over, `/opt/tape-data-product/current`. Its exact source revision, wheel SHA-256, scoped implementation identities, executable, 7,085-member inventory hash, and calculation-plan hash are recorded in the private plan under `/srv/tape-data-product/control/feature-calculation-plan-final/`.

The manual invocation is a fail-closed `systemd-run --user --wait --collect --pipe` service with `WorkingDirectory=/tmp`, `MemoryMax=1536M`, `MemorySwapMax=0`, `TasksMax=64`, `CPUQuota=200%`, and `RuntimeMaxSec=21600`. Because the persistent user manager predates group changes, invoke the absolute release executable through `/usr/bin/sg tape -c`, passing `calculate run --plan ABSOLUTE_PLAN --expected-plan-sha256 EXACT_HASH`.

The current plan must exit before calculation: transfer reconciliation, source admission, and representative measurement references are incomplete. Do not schedule or launch it as a corpus job. Once those blockers are resolved, generate a new immutable plan and use its new exact expected hash; never edit an accepted plan in place.
