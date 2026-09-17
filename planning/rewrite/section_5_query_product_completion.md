# SQL query product checkpoint completion

**Checkpoint 1 status: owner accepted 2026-09-16.** The owner explicitly
accepted the checkpoint in the task immediately preceding checkpoint 2. The
evidence below remains the accepted checkpoint-1 record; later sections record
checkpoint-2 work separately.

**Checkpoint 2 status: owner accepted 2026-09-16.** The owner explicitly
accepted the installed SQL/Python workflow, the proposed five-date population,
and its stated resource budget before authorizing checkpoint 3. No branch was
pushed, and no installed current release or persistent dataset was replaced.

**Checkpoint 3 status: ready for owner verification.** The accepted five-date
screen completed without threshold changes, an added activity/duration gate, or
session filtering. The result and bounded execution evidence are recorded below.

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

## Checkpoint 1 handoff scope (historical)

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
checkpoint 2 work had started when checkpoint 1 was handed off.

## Checkpoint 2 evidence

The isolated installed release is
`/opt/tape-data-product/releases/query-cp2-4f94d53/venv`; it was built
non-editably from implementation commit
`4f94d536792167839e772c0a715ef7c94be504ac`. Its wheel is
`/opt/tape-data-product/releases/query-cp2-4f94d53/wheel/tape_data_product-0.1.0-py3-none-any.whl`,
SHA-256 `07ca04aeec9634954f8de5e66dfd2d31d2f291a372d737f52840c91077baaff5`.
The installed query modules match the committed source byte-for-byte and import
from this release's `site-packages`, outside the development checkout. The later
source commit `7365605a6feb731db9c9d9956c93b1c1492585cc` adds only the final
date-pruning test.

The CLI adds `endpoint-data query-fields` and `endpoint-data sql` without
changing existing commands. Both require inclusive start/end trading dates,
catalog identity and local base/feature roots. SQL comes from a file; terminal
previews default to 20 and are capped at 100 rows. `--export` streams to a new
Parquet directory and records the exact SQL, selected members/date bounds,
all-session scope, source release/catalog identities, result hash/bytes and row
count. Python returns a `TapeQueryResult`: `arrow_batches()` defaults to 4,096
rows and caps batches at 25,000; `df()` refuses more than 10,000 rows by default.

Outside the checkout, the CLI and Python returned the same first five ordered
FBGL observations for `midpoint_rms_5s_bps_hl30s > 10`, beginning at
`2026-03-11 08:06:05+00:00` with `30.38570716025295` bps. The CLI measured
0.0970 seconds setup, 0.0215 seconds query, 0.71 seconds process wall time and
228,776 KiB peak RSS. The Python run measured 0.2788 seconds setup, 0.0217
seconds query, 0.70 seconds wall and 229,312 KiB peak RSS. Field discovery
returned all 27 registry fields with the same `features`/`current_ages` table
names used by Python. The installed valid zero-match query returned zero rows;
an unrepresented date failed with exit 2 and `scope contains no completed
members`. The installed session count remained 19,800 premarket, 23,400 RTH and
14,400 after-hours. A test removed an unrelated date's feature file before
opening the selected date; the selected query still succeeded, proving file
selection happens before file verification/scanning.

The installed streaming export produced 17,511 rows in 173,782 bytes, with
0.0928 seconds setup and 0.0327 seconds query time; its recorded row count and
Parquet metadata agree. A conservative full-row projection using the exact
checkpoint-3 output columns wrote all 57,600 FBGL rows in 1,875,199 bytes
(32.56 bytes/row), with 0.0966 seconds setup, 0.1124 seconds query, 0.86 seconds
wall time and 254,548 KiB peak RSS. These are bounded warm-cache measurements,
not a corpus-performance claim.

Focused endpoint verification passed 26 tests. The complete suite passed
**352 tests in 88.11 seconds** (89.12 seconds process wall), with 446,520 KiB
peak RSS and exit 0. The long suite ran as monitored PID 355063; evidence is in
ignored VM control storage under
`/srv/tape-data-product/control/section5-checkpoint2-20260916/evidence/`.

## Proposed checkpoint 3 population and budget

The accepted 5,208-member completed-file metadata at
`/srv/tape-data-product/control/phase4-c-full-5208-f427278/` has 122 represented
trading dates from 2026-03-09 through 2026-08-31. The median represented date is
2026-06-03 (zero-based sorted index 60). Before inspecting any query result, the
default rule selected the two preceding and two following represented dates:

- **2026-06-01 (59):** AIIO, ANY, BATL, BNRG, BURU, CRDO, CRE, CTNT, CXAI, CXDO, DBGI, DEVS, DOMH, DXF, DXST, EEIQ, ELAB, EQ, FFAI, FJET, FLNC, FOFO, FRGT, GDC, GFAI, GNS, GNTA, HCWC, HPE, HUBC, IMCC, KIDZ, LIQT, LIXT, LSTA, MNTS, MTEK, NAMM, OCG, OCS, PRFX, PURR, QTEX, RCAT, SBEV, SDOT, SMMT, SOAR, SUUN, TDIC, TIC, UMAC, VSA, WALD, WOK, WTO, ZCMD, ZJYL, ZNB.
- **2026-06-02 (59):** ABTS, AIFF, AIIO, AIM, AMZE, ANY, BJDX, CRDO, CTNT, DBGI, DEVS, DLXY, EVGN, GME, GMEX, GNTA, GPUS, GRRR, GTLB, GXAI, HIVE, HUBC, ICCM, KLXE, LAC, LASE, LOBO, LRHC, MEHA, MNTS, MRVL, MVIS, NAMM, PANW, PMI, QTEX, QUCY, RDGT, RDW, RKTO, RUBI, SBFM, SOAR, SPCE, STAK, TGHL, TOPS, UFG, UMAC, VMAR, VRAX, WCT, WOK, XOS, YMAT, YYGH, ZCMD, ZJYL, ZNB.
- **2026-06-03 (52):** ACCL, AI, AMZE, ANY, ATPC, AVGO, BMGL, BNRG, CHSN, CISS, CLDI, CRDO, CRWD, CXAI, DBGI, DEVS, FEED, FNGR, FOXX, GRRR, HIVE, HKIT, HUBC, JZ, LASE, NAMI, NCT, NVTS, PMI, QTEX, RNAZ, RUBI, SBEV, SELX, SINT, SOAR, SPCE, STAK, SUGP, TGL, TLYS, TOPS, TURB, TWAV, VMAR, VRAX, WOK, XOS, YMAT, YYGH, ZCMD, ZENA.
- **2026-06-04 (57):** AADX, AI, AIB, AIM, ALOY, ASTC, AUUD, BBCP, BGMS, BJDX, BURU, CLIK, CURV, CXAI, DEVS, EDHL, FOXX, GLXG, GRAN, GSIT, HCAT, HKIT, HUBC, IOTR, IQST, KEEL, LASE, LFS, LGPS, LRE, LULU, MOBX, MRLN, NEXR, PL, PMI, QMCO, QNT, QTEX, RBRK, RMSG, ROLR, RUM, RYDE, SBEV, SCAG, SHPH, SMTK, SNGX, SPRC, TE, TPET, VIVK, WOK, XOS, YYGH, ZCMD.
- **2026-06-05 (59):** ACCL, AEHL, AGRZ, ALP, BBCP, BCDA, BGMS, BMGL, BRR, CMND, CRDO, CREG, CXAI, DEVS, DGXX, DLXY, DRCT, DSS, EDBL, ELOG, ELPW, FOXX, GLXG, GMHS, GRAN, HKIT, HLP, IOT, LASE, LGPS, LHSW, LRHC, MASK, MBIO, MDIA, MPU, MRLN, MRVL, MTVA, NEXR, NIVF, NOTV, OMH, POET, PTLE, QNT, RGNT, RMSG, RYET, SHPH, SMTK, SNBR, SOAR, VEEE, VERU, VVOS, WCT, YXT, YYGH.

The exact proposed population is 286 completed members and 16,473,600
represented seconds. Its selected base Parquet is 578,576,478 bytes, feature
Parquet is 2,682,626,483 bytes and bound member manifests are 2,733,107 bytes:
3,263,936,068 bytes total before query projection. A conservative one-pass
verification plus full-feature-file scan envelope is 5,946,562,551 bytes. The
linear warm projection from the full-row small test is about 28 seconds setup
plus 32 seconds query. The output row count is unknowable before the screen and
must remain zero through 16,473,600; using the measured 32.56 bytes/row gives a
conservative all-rows Parquet ceiling of 536,306,914 bytes.

Checkpoint 3 has a 300-second hard wall limit, one process/thread, 2 GiB sampled
RSS stop and 3 GiB hard ceiling, DuckDB 256 MiB memory, at most 1 GiB spill,
4 GiB owned output/scratch, 8 GiB total read budget including verification, and
a 20 GiB free-disk reserve. It begins by deriving a five-date catalog from the
already accepted completed-member metadata without scanning unrelated dates;
selected members are then identity/hash/schema checked by the existing query
opening path. No acquisition, replay, feature rebuild, upload or publication is
authorized.

## Checkpoint 3 evidence

The catalog was derived from the accepted 5,208-member metadata without
opening unrelated Parquet. It contains exactly the accepted 286 members and
16,473,600 rows for 2026-06-01 through 2026-06-05. Its identity is
`aac38b2970a2eb1ccdaf50dd735a221031c2c5cf41e4e6b42cd0acb68d35f5b4`;
the parent full-reference identity is
`a1fab9c41bb51122ad49f9976f79b125a5542d2c2d4c73c4c2b0a23684d787ea`.
The source producer remains revision
`ea2e16128225a91ceb6003fcb3f6ef80985ba8e0` and wheel SHA-256
`a506c2ad2fd7aabeb6d214a876f6741f303d8bc713fb34a4b6dbbc83d85cafe9`.

The exact seven-condition query returned **189,162 matching one-second
observations** from 15,995,023 eligible seconds (1.183%) and 16,473,600
represented seconds (1.148%). The other 478,577 seconds lacked at least one
required valid input. Matches occurred in 177 of 286 symbol-days; 109 members
had valid zero matches. No full member had zero eligible inputs. One member
session, 2026-06-04 QNT premarket, had no eligible inputs and is retained as
such in the summary.

| Date | Represented | Eligible | Unavailable | Matches | Matching symbols |
|---|---:|---:|---:|---:|---:|
| 2026-06-01 | 3,398,400 | 3,308,281 | 90,119 | 40,807 | 30 |
| 2026-06-02 | 3,398,400 | 3,347,717 | 50,683 | 45,878 | 43 |
| 2026-06-03 | 2,995,200 | 2,928,147 | 67,053 | 32,817 | 35 |
| 2026-06-04 | 3,283,200 | 3,173,511 | 109,689 | 26,231 | 31 |
| 2026-06-05 | 3,398,400 | 3,237,367 | 161,033 | 43,429 | 38 |

Premarket contributed 98,322 matches, RTH 67,823 and after-hours 23,017.
These are adjacent, overlapping-history observations, not independent signals.
The largest symbol-day contribution was 2026-06-02 PMI with 13,652 matches
(7.217%); the ten largest symbol-days contributed about 38.7%, so the result is
not a single-name artifact but is materially concentrated.

Every exported row was re-evaluated against all seven predicates with zero
failures. Exported and summarized match counts agree exactly; represented rows
equal the selected population; eligible plus unavailable equals represented;
and seven deterministic observations spanning the result matched the stored
feature Parquet values exactly. Focused endpoint regression verification passed
**26 tests in 2.25 seconds**.

The source projection scan took 25.9325 seconds, match export 1.0534 seconds,
and member/session aggregation 1.1000 seconds. Input identity/schema checking
took approximately 11.87 seconds; the heavy unit ran 39.93 seconds wall. Peak
sampled process-tree RSS was 628,957,184 bytes. The systemd cgroup peak,
including file cache, was 1,985,327,104 bytes. DuckDB spill was zero, peak owned
output/scratch was 678,465,500 bytes, minimum free disk was 61,575,503,872
bytes, verified input was 3,265,171,897 bytes, profiled DuckDB reads were
55,620,729 bytes, and the conservative verification/read envelope was
4,068,835,565 bytes. Every accepted budget remained satisfied.

The exact SQL is stored at
`/srv/tape-data-product/control/section5-checkpoint3-20260916/run/query.sql`.
The 189,162-row match export is 9,179,683 bytes with SHA-256
`86e3402849dd37241aaa1d467d63df2911fad0e263caa82438c71ce57a52284b`.
The 858-row member/date/session summary is 8,797 bytes with SHA-256
`5e7d898c4942b35fe4b4bfc7f52ab1c8adf73f5929b26ba2b0529f951bc25ee5`.
The private result manifest and profiles are under
`/srv/tape-data-product/control/section5-checkpoint3-20260916/`.

The monitor initially stopped an attempt because it used cgroup memory, which
includes file cache, instead of the specified process-tree RSS. That stop
occurred before query output. The corrected monitor retained the 2 GiB RSS and
3 GiB hard limits. The heavy unit later produced all final Parquet artifacts but
exited during Python conversion of timezone-aware summary timestamps because
the isolated release does not include optional `pytz`. A bounded 3.43-second
finalizer cast summary timestamps to strings and verified the existing outputs;
the source query was not rerun. Failed-attempt and corrected-monitor evidence
are preserved beside the final profiles.

The historical tape-characterization strict cohort is not the same screen. It
used legacy 300-second mean-absolute movement, a 0.50 participation floor, an
inclusive 100 bps spread ceiling, a $5,000/s dollar-rate floor, a movement to
spread floor of 3, and five-pass/five-failure membership. This checkpoint uses
new endpoint-RMS/EW definitions, the exact strict/inclusive boundaries in the
section-five plan, freshness p90 conditions, no dollar-rate floor, and no
duration state machine. The result is descriptive tape-state retrieval, not
predictive evidence or executable expectancy. No plots, profitability work,
wider-corpus run, push, publication, or installed-release change was performed.
