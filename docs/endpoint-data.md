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

## Command line

The additive command group preserves existing command meanings:

```text
tape-product endpoint-data pilot
tape-product endpoint-data verify
tape-product endpoint-data fields
tape-product endpoint-data inspect
```

`inspect` requires a saved `endpoint_selection_v1` document with explicit members and caps displayed rows at 1,000. `verify` performs first-use reference and consumed-file verification. Reuse one verified in-process handle for multiple bounded projections; there is no durable verification cache.

## Current release boundary

The accepted 24-member pilot supports `historical_membership`. Its source contexts do not contain usable discovery timestamps, so both discovery-dependent modes fail clearly rather than falling back. Narrow endpoint selections currently stream the selected member's physical companions while filtering output; row-group pruning is deferred because it is not required for the fixed pilot handoff. A full-universe reference has not been constructed or authorized.
