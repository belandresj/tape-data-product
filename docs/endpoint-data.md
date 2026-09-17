# Endpoint/EW reference reader

The installed `tape_data_product.query` package reads the accepted one-second base and endpoint/EW feature partitions through an identity-bound reference. It does not recalculate feature histories or read raw T/Q.

## Python interface

```python
from tape_data_product.query import (
    EndpointSelection,
    describe_endpoint_fields,
    iter_endpoint_batches,
    open_endpoint_reference,
)

handle = open_endpoint_reference(
    reference_path,
    expected_identity=reference_identity,
    data_roots={"base": base_root, "features": feature_root},
)
selection = EndpointSelection(
    "historical_membership",
    sessions=("rth",),
    members=("2026-03-11/FBGL",),
    endpoint_start_ns=None,
    endpoint_stop_ns=None,
)
batches = iter_endpoint_batches(
    handle,
    fields=("midpoint_rms_5s_bps_hl30s",),
    selection=selection,
    include_support=True,
    include_run_boundaries=True,
)
```

`describe_endpoint_fields(config)` is the registry handoff for all 27 queryable measurements. Each entry identifies the physical table, exact value and reason-mask columns, unit, family, source, half-life or window, support dependencies and inspection dependencies. B and C must use this mapping rather than reconstructing names.

`open_endpoint_reference(...)` requires an expected reference identity and explicit trusted base/feature roots. It verifies reference/member identities, every consumed manifest and companion hash, exact schemas and declared rows. Pilot references must contain the ordered March–August × rank-stratum cells and each member must declare the exact full-session 57,600-row coverage. Hashing uses a pinned inode; JSON is decoded from the same stable bytes; Parquet paths are checked against the verified inode before and after opening. The handle retains snapshots and rejects subsequent mutations.

`EndpointSelection` requires an explicit population timing mode. `historical_membership` is retrospective. `nominal_post_discovery` requires a validated nominal timestamp and provenance; `receipt_post_discovery` requires nominal and receipt timestamps and both provenance records. Unsupported discovery modes fail during member preflight. Session ranges are date-aware and endpoint bounds are UTC nanoseconds with `endpoint_start_ns <= t < endpoint_stop_ns`.

`iter_endpoint_batches(...)` accepts one or more registry field names and yields exact Arrow types, keys, values, masks, selection/segment flags, optional support/inspection columns and optional source-specific halt/continuity columns. B should request predicate dependencies separately and set `include_run_boundaries=True` only when strict-run logic needs continuity. C can read unconditional fields without adding feature predicates.

## Scoped DuckDB query access

`build_endpoint_query_catalog(handle, output, members=...)` derives a distinct
`endpoint_query_catalog_v1` from a verified endpoint reference. Unlike the
deterministic pilot reference, the catalog format has no month/stratum shape:
it binds an explicit arbitrary set of completed member files. Construction
scans every included member once through the accepted 27-field projected
reader, preserving its key, value and reason-mask validation instead of adding
a second validity implementation.

`open_tape_database(...)` selects completed files by inclusive trading-date
bounds and exact `YYYY-MM-DD/SYMBOL` member keys before it registers any
Parquet scan. It returns a context-managed in-memory DuckDB handle with these
researcher-facing tables:

- `features`: exact keys, UTC `endpoint_time`, reporting `session`, and the 24
  stored endpoint/EW feature values with their reason masks.
- `current_ages`: the three unsmoothed base-age measurements and masks, with
  the same exact keys. Join to `features` explicitly with
  `(session_date, symbol, interval_end_ns)` when a query needs them.
- `members`: the selected completed members, coverage and release identity.
- `feature_catalog`: all 27 registry measurements, units, masks and owning
  table (`features` or `current_ages`).

This split is required for projected I/O. DuckDB 1.5.5 retains the base side of
an otherwise-unused keyed left join because Parquet does not provide an
enforceable uniqueness constraint. Keeping current ages separate makes a
feature-only `features` query scan only `features.parquet`; `support.parquet`
is not registered. The catalog construction already proves exact key
alignment, and any query combining ages uses the explicit three-key join.

All represented sessions are present by default. Invalid values remain SQL
`NULL` with their nonzero mask; a valid zero remains numeric zero, so ordinary
numeric predicates distinguish unavailable data from a valid zero-match
result without extra filtering. The query boundary reads stored values and
does not recalculate histories.

```python
from tape_data_product.query import open_tape_database

with open_tape_database(
    query_catalog,
    expected_identity=query_catalog_identity,
    data_roots={"base": base_root, "features": feature_root},
    start_date="2026-03-11",
    end_date="2026-03-11",
    members=("2026-03-11/FBGL",),
) as db:
    rows = db.sql("""
        SELECT symbol, session_date, session, endpoint_time,
               midpoint_rms_5s_bps_hl30s
        FROM features
        WHERE midpoint_rms_5s_bps_hl30s > 10
        ORDER BY endpoint_time
        LIMIT 5
    """).fetchall()
```

The default DuckDB configuration uses one thread, a 256 MiB memory limit and
no disk spill. Passing an explicit bounded `temp_directory` enables at most
1 GiB of spill after enforcing the 20 GiB free-disk reserve.

`db.sql(...)` returns a `TapeQueryResult`. Its `arrow_batches(batch_size=4096)`
method streams batches capped at 25,000 rows. `df()` is convenient for small
results and refuses to materialize more than 10,000 rows by default; pass a
smaller explicit `max_rows` where appropriate. `db.export_parquet(sql, output)`
streams a larger result to a new immutable directory containing
`results.parquet`, the exact `query.sql`, selected `scope.json`, and a manifest
with source identity, row count, hashes, bytes, and query time.

## Command line

The additive command group preserves existing command meanings:

```text
tape-product endpoint-data pilot
tape-product endpoint-data full
tape-product endpoint-data verify
tape-product endpoint-data fields
tape-product endpoint-data inspect
tape-product endpoint-data query-fields
tape-product endpoint-data sql
```

`inspect` requires a saved `endpoint_selection_v1` document with explicit members and caps displayed rows at 1,000. `verify` performs first-use reference and consumed-file verification. Reuse one verified in-process handle for multiple bounded projections; there is no durable verification cache.

## Trusted VM access

Run queries through SSH or a notebook whose kernel uses the isolated installed
release on the VM. Do not expose DuckDB or a notebook directly to the public
network. If a browser notebook is useful, bind it to VM loopback and forward
that port through SSH; the notebook and CLI must use the same installed Python
environment, catalog identity, and local derived-data roots.

For command-line discovery, `query-fields` reads the selected catalog's actual
`feature_catalog`; both date bounds are mandatory even though the catalog table
itself has 27 fixed rows. SQL execution accepts one saved file and also requires
inclusive trading-date bounds. Terminal output defaults to 20 rows and can
never exceed 100. Add `--export NEW_DIRECTORY` to stream the complete result to
Parquet instead of printing rows. Sessions remain unrestricted unless the SQL
contains an explicit `session` predicate.

```bash
QUERY_RELEASE=/opt/tape-data-product/releases/<query-release>/venv
$QUERY_RELEASE/bin/tape-product endpoint-data sql \
  --catalog /srv/tape-data-product/control/<query-catalog> \
  --identity <query-catalog-identity> \
  --base-root /srv/tape-data-product/derived/<release>/base \
  --feature-root /srv/tape-data-product/derived/<release>/features \
  --start-date 2026-03-11 --end-date 2026-03-11 \
  --sql-file examples/endpoint-rms-threshold.sql --preview-limit 20
```

The Python interface uses the identical tables and names:

```python
with open_tape_database(
    catalog,
    expected_identity=catalog_identity,
    data_roots={"base": base_root, "features": feature_root},
    start_date="2026-03-11",
    end_date="2026-03-11",
) as db:
    result = db.sql(
        "SELECT symbol, session_date, endpoint_time "
        "FROM features ORDER BY endpoint_time LIMIT 10"
    )
    print(result.df())
```

## Release boundary

The installed reader supports both deterministic pilot references and the accepted full-population reference. The V2 report’s full reference contains 5,208 members and 299,980,800 represented one-second rows. Historical-membership mode is available for that release. Discovery-dependent modes require usable discovery timestamps in the source contexts and fail clearly when those timestamps are unavailable; they never fall back silently.
