"""Single-feature exact ECDF from a frozen compact snapshot.

Read O(N) projected batches; external sort O(N log N); retained disk O(N).
RAM: one 4096-row batch, bounded reducer arrays, 256 MB DuckDB, one canvas.
The parent observes tree RSS and enforces a 600s deadline. No source writes.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

FEATURE = "movement_mean_5s_bps_60s"
SESSIONS = ("premarket", "rth", "after_hours", "pooled")


def exact_table(
    connection,
    paths,
    output,
    batch_size=4096,
    field=None,
    input_reader=None,
    output_writer=None,
):
    """Vectorized reduction of sorted batches; ties crossing batches are merged."""
    if type(batch_size) is not int or not 1 <= batch_size <= 25000:
        raise ValueError("batch_size must be 1..25000")
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    if field is not None:
        from tape_data_product.features import compact_product_schema as S

        if field not in S.FEATURES:
            raise ValueError("unknown feature")
    column = "value" if field is None else '"' + field + '"'
    if input_reader is not None:
        connection.register("report_input", input_reader)
        reader = connection.execute(
            "SELECT value, session FROM report_input WHERE value IS NOT NULL ORDER BY value"
        )
    else:
        reader = connection.execute(
            f"SELECT {column} AS value, session FROM read_parquet(?) WHERE {column} IS NOT NULL ORDER BY value",
            [[str(p) for p in paths]],
        )
    method = getattr(reader, "to_arrow_reader", None)
    reader = method(batch_size) if method else reader.fetch_record_batch(batch_size)
    schema = pa.schema(
        [("value", pa.float64())]
        + [(s + "_count", pa.int64()) for s in SESSIONS]
        + [(s + "_cumulative", pa.int64()) for s in SESSIONS]
    )
    totals = np.zeros(4, dtype=np.int64)
    pending_value = None
    pending_counts = np.zeros(4, dtype=np.int64)
    distinct = 0
    # Coalesce tiny pending-value emits into bounded row groups. Otherwise every
    # sorted input batch can create an extra one-row footer entry at corpus scale.
    pending_batches = []
    buffered_rows = 0
    output_batch_rows = 25000
    with (
        output_writer(schema)
        if output_writer
        else pq.ParquetWriter(output, schema, compression="zstd")
    ) as writer:

        def flush():
            nonlocal buffered_rows
            if pending_batches:
                writer.write_table(
                    pa.Table.from_batches(pending_batches, schema=schema),
                    row_group_size=output_batch_rows,
                )
                pending_batches.clear()
                buffered_rows = 0

        def emit(values, counts):
            nonlocal distinct, buffered_rows
            if not len(values):
                return
            cumulative = np.cumsum(counts, axis=0) + totals
            payload = {"value": values}
            payload.update({s + "_count": counts[:, i] for i, s in enumerate(SESSIONS)})
            payload.update(
                {s + "_cumulative": cumulative[:, i] for i, s in enumerate(SESSIONS)}
            )
            batch = pa.RecordBatch.from_pydict(payload, schema=schema)
            offset = 0
            while offset < batch.num_rows:
                size = min(output_batch_rows - buffered_rows, batch.num_rows - offset)
                pending_batches.append(batch.slice(offset, size))
                buffered_rows += size
                offset += size
                if buffered_rows == output_batch_rows:
                    flush()
            totals[:] = cumulative[-1]
            distinct += len(values)

        for batch in reader:
            values = batch["value"].to_numpy()
            sessions = batch["session"].to_numpy()
            if not len(values):
                continue
            if (
                not np.isfinite(values).all()
                or (values < 0).any()
                or ((sessions < 0) | (sessions > 2)).any()
            ):
                raise ValueError("invalid eligible value/session")
            starts = np.r_[0, np.flatnonzero(values[1:] != values[:-1]) + 1]
            unique = values[starts]
            counts = np.zeros((len(starts), 4), dtype=np.int64)
            for s in range(3):
                counts[:, s] = np.add.reduceat((sessions == s).astype(np.int64), starts)
            counts[:, 3] = counts[:, :3].sum(axis=1)
            if pending_value is not None:
                if unique[0] == pending_value:
                    counts[0] += pending_counts
                else:
                    emit(np.array([pending_value]), pending_counts[None, :])
            emit(unique[:-1], counts[:-1])
            pending_value, pending_counts = float(unique[-1]), counts[-1].copy()
        if pending_value is not None:
            emit(np.array([pending_value]), pending_counts[None, :])
        flush()
    return dict(totals=dict(zip(SESSIONS, map(int, totals))), distinct_values=distinct)
