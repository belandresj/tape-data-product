# SQL query product checkpoint completion

**Status: ready for owner verification.** Checkpoint 1 is implemented and
tested on the project VM. This is not owner acceptance, an installed query
release, checkpoint 2, or full-corpus query evidence.

## Checkpoint 1 evidence

The VM branch `codex/section5-checkpoint1` uses accepted reader baseline
`96d74da8d37b8f6956db16405fc03bc621a453d9`. The tested implementation source
is `f2d738ac4ef4047a5cbc81f62275df196fdea2d0`; the completion-document commit
follows it. No branch was pushed and no query wheel was installed. Development
used a separate checkout and Python 3.13 environment.

The package now builds identity-bound `endpoint_query_catalog_v1` catalogs and
opens inclusive date/member-scoped DuckDB handles. Public tables are
`features`, `current_ages`, `members` and `feature_catalog`. The first contains
the 24 stored feature measurements/masks. `current_ages` contains the other
three registry measurements/masks and joins on the exact three member keys.
The 27-row catalog names the owning table and unit. This layout is intentional:
DuckDB 1.5.5 retained the base Parquet scan for an unused keyed left join and
for the equivalent scalar-subquery view, while the implemented feature-only
plan has exactly one feature-Parquet scan and no base/support scan.

The full suite passed **348 tests in 101.61 seconds**. New fixtures cover strict
and inclusive threshold boundaries, valid zero versus unavailable, exact
date/member scoping, all-session default behavior, all 27 catalog mappings,
keyed age access, feature-only projection, missing/changed inputs, catalog
identity mismatch and mutation during an open session.

The bounded real check used accepted pilot member `2026-03-11/FBGL`, all
57,600 represented seconds and catalog identity
`4d55752c3c52b86abfa3704313afacc2c2986feb69b7972bd99c3803f914fde0`.
Session counts were 19,800 premarket, 23,400 RTH and 14,400 after-hours. For
`midpoint_rms_5s_bps_hl30s > 10`, 56,771 seconds were available, 829 were
unavailable and 17,511 matched. A direct `ParquetFile` read returned the same
match count, and five returned keys, values and zero masks agreed exactly.

The successful measured invocation took 2.35 seconds total: 1.21 seconds to
open/verify the 24-member source reference, 0.78 seconds to validate/build the
one-member catalog, 0.089 seconds to open the scoped database, 0.015 seconds
for the threshold count and 0.017 seconds for session counts. Sampled
process-tree peak RSS was 215,683,072 bytes; selected base/feature companions
were 9,904,534 bytes and DuckDB spill was zero. It ran with one thread, 256 MiB
DuckDB memory, a 2 GiB sampled stop, 3 GiB cgroup ceiling, no swap, 300-second
wall limit, 1 GiB spill cap, 4 GiB owned-output ceiling and 20 GiB disk reserve.
An initial comparison invocation exited before producing evidence because the
generic Arrow dataset reader inferred a conflicting Hive partition type; the
direct checker was corrected to open the exact Parquet file, and the bounded
successful invocation followed. No resource limit fired and no dataset was
modified.

The source release remains producer revision
`ea2e16128225a91ceb6003fcb3f6ef80985ba8e0`, producer wheel SHA-256
`a506c2ad2fd7aabeb6d214a876f6741f303d8bc713fb34a4b6dbbc83d85cafe9`,
and accepted pilot reference identity
`4a410bd8321cfe0466dc7ecd02c8a5762a679c320d64376c24cea076465ae832`.
The query catalog format is arbitrary-member, but this checkpoint constructed
only the required one-member pilot catalog. Historical-membership selection is
retrospective; no strict-run engine, support scan, CLI, installed query release,
full-universe catalog or corpus performance claim is included.

## Exact next checkpoint scope

After explicit owner acceptance, checkpoint 2 installs the query package in an
isolated VM release; adds a small CLI for field discovery and SQL files with
explicit date bounds; returns bounded Arrow batches or a small-result DataFrame;
documents SSH/notebook use and a reusable SQL example; preserves the same table
and field names; caps previews and streams larger Parquet exports with saved SQL,
scope, source identity and row count. It then compares CLI and Python outside
the checkout, tests an empty date and valid zero-match query, confirms date
pruning, records setup/query time and memory, runs relevant existing tests, and
proposes five consecutive represented dates centered on the median date with
projected rows, input bytes, runtime, output and an explicit job budget. No
checkpoint 2 work has started.
