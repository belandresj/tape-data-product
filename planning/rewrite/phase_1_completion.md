# Phase 1 completion record

**Completed for server environment and bounded transport scope, 2026-09-14.** This is not acceptance of new replay/features, full source coverage, or corpus capacity. The [Phase 1 specification](phase_1.md) defines the boundary.

## Accepted evidence

Ubuntu 26.04 x86_64 provided 8 CPUs, approximately 22 GiB usable RAM and 191 GiB initially available disk. Python 3.13.15 was installed separately from system Python 3.14.4. A wheel at package revision `0efec5d` was installed non-editably with a captured hash-pinned Linux dependency closure. A clean Git checkout of the same revision was transferred using a Git bundle while GitHub lagged behind; this was deployment, not publication.

Key-based administrative SSH was verified after password login was disabled. UFW was enabled with default-deny incoming and SSH permitted over IPv4/IPv6. The non-login `tape` runtime account and separate application/data/configuration directories were verified. The installed offline demo ran as `tape` with systemd restrictions: 17.651 seconds, 366010368 bytes sampled summed process-tree RSS, no guard stop. The separately reported cgroup memory peak used different accounting and is not substituted for RSS.

Selected Linux regression suites passed **114 tests** in 21.99 seconds (22.765 seconds monitored), at 197353472 bytes peak RSS. This included Phase 0 endpoint contracts and legacy acquisition/storage/query coverage; it was not the full suite. The repository checkpoint subsequently runs the integrated suite separately.

The approved September 2 KDP/NVDA sample contains four Parquet objects and **4799552 rows**. All files passed expected SHA-256, byte length, row count, full bounded decoding and SIP ordering checks. A worker exited after 8 MiB of NVDA quotes, leaving three verified files, a partial, and no completion marker. Restart reused the three completed files and downloaded the remaining object. A final reuse run issued no new GET requests.

| Measurement | Result |
|---|---:|
| Retained raw bytes | 114278636 |
| Received payload including interrupted attempt | 122667244 |
| Conservatively reserved body bytes | 152137911 |
| GET attempts | 5 |
| Initial/interrupted run | 6.387 seconds |
| Resume | 5.060 seconds |
| Complete-file reuse check | 4.461 seconds |
| Maximum sampled process-tree RSS | 162414592 bytes (154.89 MiB) |
| Largest partial file | 71082844 bytes |
| Sample-directory logical-byte high-water | 114282602 bytes |
| Free filesystem bytes after completion | 203948478464 |

No resource stop occurred. Received payload divided by initial-plus-resume monitored time is approximately 10.72 MB/s, including interpreter startup, metadata reads and validation but excluding time between invocations. This is an end-to-end sample rate, not isolated network bandwidth or a reliable full-corpus projection. Protocol overhead was not measured.

Five focused fake-client tests independently exercised recovery/reuse, corruption rejection, identity mismatch, the cumulative download cap and free-space refusal. The real request worker ran as `admin` with explicit `tape` group access because the persistent systemd user manager had stale supplementary groups. The separate restricted demo established execution as the `tape` UID; network access under that UID was not measured separately.

## Limitations and Phase 2 obligations

- Remote hashes and declared session windows establish object identity and declared coverage, not terminal source completeness or information-clock provenance. Historical selection records are not automatically equivalent to current screen receipts. Reconcile actual member admission before production replay.
- The transport harness is separately tracked, uses installed package verification primitives, and is limited to the approved sample. The installed public staging CLI was not extended. No Massive acquisition, R2 mutation, raw-data publication or full-corpus migration occurred.
- Process restart was tested; arbitrary power-loss durability was not. Replay restart remains the Phase 0 completed-member/session-start contract.
- Local sample caps are not quotas against unrelated writers. Phase 2/3 must measure their own production output, memory and spill footprint before a full run.
- Preserve private logs, catalogs, source receipts and credentials outside Git. Track these concise findings and the reproducible interfaces, not detailed market rows or administrative transcripts.

The measured transport script SHA-256 is `12bfae7aada03156f524d7f64e17ad05c6568ad83183c70ba86f391064c655f8`; measured wheel identity is recorded in the specification. Private per-run JSON/logs and input identities remain with the operator. The repository checkpoint finalizes tracked planning, implemented documentation and packaging while preserving legacy report attribution.

## Repository checkpoint verification

After promoting the harness/tests and reconciling documentation, the complete local Python 3.13 suite passed **250 tests in 26.45 seconds** (27.216 seconds monitored), with 202735616 bytes sampled peak process-tree RSS and no guard stop. The staged-content audit found no findings. A fresh isolated Linux environment installed the checkpoint wheel and passed the guarded offline demo in **19.051 seconds**, with 364392448 bytes peak process-tree RSS. The wheel SHA-256 was `e58f9346ad762684cee08bd268e8f92a7617a152e0f902b7349592335d1de519`. This verifies the installed package outside both source checkouts; it does not rerun market-data acquisition or replace the earlier transport measurements.

The tested Linux dependency closure is tracked with hashes in [config/vps-python313-linux.lock](../../config/vps-python313-linux.lock). Package artifact inspection confirmed private evidence, credentials and instruction/planning files are excluded; the source distribution includes the operational harness and implemented documentation. GitHub CI execution is reported separately from these local/VPS checks.
