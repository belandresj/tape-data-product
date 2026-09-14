# Phase 1 — VPS environment and bounded transfer

**Status: completed for environment and transport scope, 2026-09-14.** See the [completion record](phase_1_completion.md) for measurements and limitations. Production replay admission, full-corpus migration and feature builds are not part of this acceptance.

## Scope and baseline

Prepare the selected 8-vCPU, 24-GB RAM, 200-GB SSD Ubuntu VPS; install the repository package under supported Python 3.13; establish key-based administrative access and a separate runtime account; and prove a small direct R2 transfer with identity verification and restart recovery.

The measured package baseline is `0efec5de7ffd21d9ddec640cb67a928d3d2a3806`, containing the Phase 0 contracts and legacy installed product. The measured wheel SHA-256 is `3ffa3fd15cd4f3a6c1a7a79851853f22a1857b42dbe23cefe0f425722caae2c2`. The transport harness is separately versioned in [scripts/phase1/transfer_sample.py](../../scripts/phase1/transfer_sample.py). Its measured SHA-256 is `12bfae7aada03156f524d7f64e17ad05c6568ad83183c70ba86f391064c655f8`. Documentation/packaging changes in the subsequent repository checkpoint do not retroactively change these measurement identities.

This phase does not change any Phase 0 equation, schema, eligibility population or timing rule. Legacy demo results remain legacy outputs. No Massive API acquisition, R2 mutation, corpus migration or public market-data release follows from completion.

## Environment and access contract

- Install CPython 3.13 in an isolated location; never replace Ubuntu's system interpreter. The executed environment used Python 3.13.15 on Ubuntu 26.04 x86_64 and uv 0.12.13. Preserve the resolved Linux dependency closure, wheel hash and deployment revision with private operational evidence.
- Use key-based SSH and verify a second connection before retaining access changes. Keep a rollback path during changes. The executed setup allows TCP 22 for IPv4/IPv6, denies other unsolicited incoming traffic by default, allows outgoing traffic, and disables password/keyboard-interactive SSH authentication. Local sudo authentication remains available.
- Separate administrative login from the non-login `tape` account. Keep code/runtime under `/opt/tape-data-product`, data/control/reports under `/srv/tape-data-product`, and restricted configuration under `/etc/tape-data-product`.
- Use a dedicated object-read-only R2 credential scoped to the selected bucket. Store it as root-owned, tape-group-readable mode 0640 outside Git. Never put credential values into commands, logs, manifests or documentation. Runtime and tests must use installed package imports rather than another checkout.
- Initialize Matplotlib's font cache before the strict offline guard; its first Linux font discovery may invoke a subprocess. This setup operation is separate from the guarded demo.

## Transfer input and output

The private JSON manifest has an `objects` array of exactly four records, each containing `symbol`, `stream`, `key`, `bytes`, `sha256`, and `rows`. The harness deliberately admits only KDP/NVDA trades/quotes for 2026-09-02, using canonical keys derived by the installed storage adapter. It is a bounded acceptance harness, not a general corpus staging API. Required manifest identities were frozen by HEAD before GET and reconciled with the prior inventory.

Outputs beneath an explicitly named sample root are per-symbol trade/quote Parquet files, uncommitted `.partial` files, a transfer ledger, a process lock, and a final completion record. Completion means verified transport for this exact sample. It must not be read as a current acquisition `pair.json`, a coverage receipt, or an admitted base/feature member.

Historical selected-member files were located and their identities recorded privately. Object metadata declares provider, acquisition method, row counts and session windows. These records do not substitute for missing terminal acquisition, complete reference-population, discovery-clock, halt/continuity, or per-member size-unit evidence. Phase 2 must reconcile those dependencies before admitting real members.

## Algorithm and resource bounds

1. Take an exclusive sample-root process lock. Freeze the complete input records in a durable ledger and reject identity changes on restart.
2. Recheck remote identity. Verify any completed local object against expected SHA-256, bytes and rows; reuse it only on agreement.
3. Discard only the harness's uncommitted partial for the object being retried. Reserve the whole expected body length durably before GET. Failed/interrupted attempts continue to consume that reservation; disable hidden GET retries.
4. Stream in at most 1 MiB chunks, checking byte/disk limits and recording progress. Validate response size, hash, Parquet row count, full decoding in 4096-row batches and nondecreasing SIP timestamps. Check remote identity again before renaming a newly verified object into place.
5. Write completion only after all four files verify. A restart with verified inputs must not download them again.

The deliberately injected interruption exits the worker after 8 MiB of NVDA quotes. This tests process termination and restart, not arbitrary power-loss/filesystem durability. Numerical features and event eligibility are not calculated here.

Fixed four-object membership bounds the inventory and ledger. Work is linear in bytes decoded/hashed, plus per-row ordering checks; memory holds a transfer chunk, a 4096-row Arrow batch and fixed metadata. It does not materialize full raw days in Pandas. The sample-local directory is fixed in size and monitored per chunk.

| Limit | Enforced setting |
|---|---:|
| Transfer workers | 1 |
| Cumulative reserved response-body bytes, including retries | 256 MiB |
| Sample and partial logical-byte cap | 512 MiB |
| Filesystem free-space reserve | 20 GiB |
| Systemd aggregate memory ceiling | 768 MiB |
| Swap | 0 |
| Tasks | 64 |
| CPU quota | 200% |
| Per-invocation runtime | 600 seconds |
| Process-tree RSS polling | 50 ms |

The response-body budget excludes TLS/HTTP overhead and TCP retransmissions. Disk enforcement is an application budget for the fixed sample, not a host-wide quota against unrelated writers. Future replay/feature workers need their own measured scratch/spill limits. Eight vCPUs do not authorize eight simultaneous data workers.

## Verification and acceptance

The [focused tests](../../tests/test_phase1_transfer.py) cover interruption/reuse, corruption, changed remote identity, cumulative download limits and free-space refusal. Fake clients deliberately inject the failure paths; fixture disk capacity is explicit. The real sample proves actual permissions, transport, verification and process restart on the VPS.

Acceptance requires all of the following, with evidence distinguished by type:

- Actual machine/runtime and access configuration recorded.
- Installed offline demonstration outside the checkout, including execution as the restricted runtime account.
- Named sample and provenance classification, verified local objects and independently recorded remote identities.
- Deliberate incomplete state without completion, successful restart, and a no-new-GET reuse run.
- Measured time, memory, temporary/final disk and response-body accounting within the agreed caps.
- Preserved code, harness and dependency identities; clean repository checkout available on the VPS.

These criteria were met within the limitations in the completion record. The next operational job is the separately specified [raw migration checkpoint](raw_migration.md), followed by [Phase 2 checkpoint A](phase_2.md). Preserve this phase's historical four-object acceptance boundary. Record/synchronize the actual code and planning revision needed by the next job without overwriting unrelated edits; deployment does not imply publication. Full external runs still require new representative measurements and explicit scope.
