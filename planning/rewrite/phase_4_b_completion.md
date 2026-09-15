# Phase 4 checkpoint B — independent-core completion record

Status: independently testable core complete; A integration and checkpoint B
acceptance pending, 2026-09-15.

## Result and boundary

The checkpoint B core now validates canonical conjunctions, evaluates eligibility
from exactly the predicate fields, reduces strict matching runs across arbitrary
input batches, reconciles member and symbol-date accounting, and writes immutable
schema-bound Parquet/JSON query artifacts. Strict-run state is constant-size and
production export accounting flushes one member at a time to SQLite.

This is not checkpoint B acceptance. Checkpoint A had no reviewed implementation
commit when this record was written. Its worktree remained uncommitted on
`52b5b3502c9567b120a92316f47a8642a718e11c`, so B did not consume its reader,
selection implementation, registry mapping, CLI adapter, or changing files. The
provisional A pilot reference identity observed read-only was
`7a7069322e652721209b8072f7d3c836f86a6ff620200bc100a1c17a4a8c480d`; B has not
queried or accepted that reference. Integration must pin A's eventual reviewed
commit and reverify the reference identity rather than relying on this observation.

## Source and identities

- Authorized implementation baseline:
  `52b5b3502c9567b120a92316f47a8642a718e11c`.
- Independently testable B implementation revision:
  `5807bcba6e91cb336223b01f39e41cca002491b9`.
- Endpoint/EW contract identity:
  `bf4d8c1211bf096d9b0ab3b2c0d62f3858ff1da738b42aa8c3ed4cba67020bd1`.
- B module implementation identity:
  `53fd61d4eddfdce5c040b39d0e87eef1be7ec245c272386b96685ce83f7f8df9`.
- Isolated wheel SHA-256:
  `278818a5e2db4ce0d49bb603c0c8c6a083b3e52df075e94d68f90e5f5206599d`.
- Illustrative synthetic predicate identity for
  `midpoint_rms_5s_to_spread_hl30s > 2`:
  `ef2bb1ca0cc5a8fd47d5e70b11ceff73ebdb81b5b663b70e7b7a5048ebba9631`.
- Synthetic query identity binding historical-membership selection, the
  illustrative predicate, contract, B implementation, and output schema:
  `bc7284ac58083a880eba1ecbfa70a05dac22b2d80c94c8e30091ad9325e09c2f`.
- That synthetic matching-observation Arrow schema identity:
  `a2c491a3592e07cb3a9cff78068b7e5c17999a3d6154b6b7c5b41643fd281eeb`.

The synthetic reference label in that query identity is
`synthetic-phase4-b-reference-v1`; it is deliberately not a real release claim.

## Verified behavior

Fourteen independent focused tests use literal expected rows, runs, counts, and
hashes. They verify:

- `>`, `>=`, `<`, `<=`, equality and range boundary behavior, including zero;
- rejection of unknown fields, nonfinite/oversized thresholds, malformed ranges,
  and contradictory intersections;
- canonical identities for equivalent reordered conjunctions;
- unavailable predicate inputs versus eligible nonmatches, while invalid unrelated
  display fields do not alter matching;
- inclusion of display values and their reason masks in matching-observation output;
- false and unavailable middle rows, input-batch boundaries, member boundaries,
  selection censoring, halts, and quote/trade-specific continuity changes;
- run endpoints, `[first_endpoint-1s,last_endpoint)` represented coverage,
  represented duration, endpoint elapsed time, closure reasons and censoring;
- no-eligible versus valid-zero-match members and a hand-counted two-symbol
  contribution example;
- schema-correct empty Parquet output and nonempty reopen/value/file-hash checks;
- exact reconciliation of matching observations, strict runs, SQLite member totals,
  Parquet member totals and the complete receipt.

The isolated wheel was installed into a fresh Python 3.13 environment outside the
checkout using the verified Linux lock. The same 14 tests passed there in 0.51s.
This proves the core is included in the built artifact and does not depend on the
development checkout. It does not prove A integration or real-pilot correctness.

## Resource evidence

A worst-fragmentation synthetic reducer scan alternated match/nonmatch for 100,000
selected endpoints, creating 50,000 one-row runs. It completed in 1.27s wall time
at 20,044 KiB peak RSS, with zero file input and no swap. The reducer retained only
the current run, previous endpoint metadata and a scalar run count. Production
accounting holds only the current member during scanning and persists totals in
SQLite; final Parquet materialization is bounded by the declared reference member
count (24 for the pilot, 5,208 for the accepted parent population).

No raw replay, feature rebuild, pilot scan, external transfer, upload, deletion,
publication, or full-corpus analysis was run by B.

## Independent review

A bounded read-only reviewer inspected the committed core. The review found and B
resolved: unbounded retention of emitted runs; noncanonical predicate ordering;
missing display reason masks; permissive continuity flags; query/schema identity
gaps; missing matching-observation reconciliation; and possible disagreement
between SQLite and Parquet accounting. The reviewer also identified the remaining
A integration and acceptance work below. Review did not launch a data job.

## Pending integration and acceptance

After A publishes a reviewed commit, checkpoint B still must:

1. Integrate that exact commit through Git and record it as the dependency revision.
2. Derive field values, masks, units, Arrow schemas, selected segments and required
   source-continuity columns from A's public reader/registry interfaces. Do not accept
   caller-invented schemas in the installed query path.
3. Map A's segment-start/end evidence to member/session/selection boundary reasons
   and censoring, then add integration tests without duplicating A's selection logic.
4. Add the installed query API and additive CLI while preserving legacy commands.
5. Run the bounded illustrative query on A's fixed 24-member pilot, write a
   reproducible export, compare exported rows with direct source projections, and
   reconcile every member and row denominator. A positive real match is not needed.
6. Measure first-use validation separately from query scanning and verify the
   four-predicate pilot target and resource limits.
7. Build and verify a new isolated integration wheel, rerun independent review, and
   update this record and implemented documentation with the accepted A revision,
   pilot/query identities, performance, accounting and remaining limitations.

Until those steps pass, the precise integration dependency is A's reviewed shared
reader/reference/selection commit, which did not yet exist at record time.
