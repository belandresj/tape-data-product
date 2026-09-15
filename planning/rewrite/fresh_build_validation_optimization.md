# Fresh-build validation optimization — 2026-09-14 Pacific

Revision `af7b7f435941120f09e9cd1d46052f8980101039` removes redundant full-file verification from fresh base and feature builds without changing feature definitions, schemas, output values, completed-member reuse, or the public standalone verifiers. Wheel SHA-256: `1eb98bcd666342082a05e12e56d919c640e3064078da9589a34919d9979629a6`.

A fresh base build now hashes each raw source once before replay, validates consumed event order and generated batches during replay, detects concurrent source mutation from frozen filesystem identities, hashes the completed output once, checks its Parquet schema/count, and publishes atomically. A fresh feature build verifies base hashes/metadata once, folds base semantic/key validation into the calculation scan, validates generated feature/support batches, hashes each completed output once, checks schema/count and mutation, and publishes atomically. Explicit verification and completed-output reuse still perform strict full reads and reject corrupt companions.

The complete source suite passed 282 tests in 43.71 seconds. Added fault tests prove that source/control mutation prevents publication and that fresh construction does not call the standalone output verifiers. The isolated installed wheel ran outside the checkout under one-core/one-thread and 4 GiB memory guards on the same three previously authorized full-session members.

| Member | Baseline base | Optimized base | Change | Baseline features | Optimized features | Change |
|---|---:|---:|---:|---:|---:|---:|
| KPTI 2026-07-31 | 12.264 s | 8.45 s | -31.1% | 38.376 s | 21.44 s | -44.1% |
| GLE 2026-05-07 | 27.184 s | 21.17 s | -22.1% | 38.697 s | 21.49 s | -44.5% |
| SPCX 2026-06-12 | 287.067 s | 257.49 s | -10.3% | 36.520 s | 19.62 s | -46.3% |

All nine optimized Parquet payloads (base, features, and support for three members) were byte-identical to the baseline. Strict installed feature/base verification passed all three and took 10.70–10.87 seconds per member. The earlier verification measurements were 15.75–16.48 seconds, but cache state was not controlled across the two runs, so that difference is not claimed as a validator speedup.

Fitting the original event-count model to the optimized base measurements projects 23.19 one-worker hours for corpus base replay. Mean optimized feature time projects 41.03 hours, for 64.22 build hours total versus 106.74 hours before this change. Perfect eight-worker scaling would be 8.03 hours; this is arithmetic, not a measured concurrency result. The remaining four-to-five-hour gap is no longer redundant verification: it is primarily the interpreted Python feature-state loop and busy-member raw event replay. No full-corpus run, R2 operation, scheduled job, feature-definition change, or active-release switch occurred.
