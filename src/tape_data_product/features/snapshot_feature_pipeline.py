"""Versioned one-second publication over the existing bounded V3 stream.

No changes to frozen V1 or episode-local MU/X/Q. Output batches <=1,024 rows;
raw batches <=25,000 rows. Rolling state is at most 300 observations per field.
"""

from __future__ import annotations
from collections import Counter
import json
import math
from pathlib import Path
import sys
import time
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
from tape_data_product.features import economic_tape_state_v3 as V
from tape_data_product.features.tape_snapshot_inventory import digest

VERSION = "tape_snapshot_features_v1"


def identity():
    return dict(
        version=VERSION,
        upstream=V.contract_identity(),
        publisher_sha256=V.sha256(__file__),
        horizons_seconds=[60, 300],
        cadence_seconds=1,
        age_statistic="linear p90 of valid endpoint ages, milliseconds",
        historical_overlay="not live-reproducible without point-in-time halt status",
        ratio="same-horizon midpoint_movement_bps_per_30s / quoted_spread_bps; positive denominator",
        nbbo_state_change="inherited price/size/exchange/validity signature, not price/size changes only",
    )


def enrich(row, primitive):
    row = dict(row)
    row["interval_end"] = row["interval_end_ns"]
    row["market_data_endpoint_ns"] = row["interval_end_ns"]
    row["live_reproducible"] = False
    for key in (
        "trade_count",
        "dollars",
        "quote_age",
        "trade_age",
        "raw_messages",
        "changes",
        "price_changes",
        "size_changes",
        "quote_source_file_accepted",
        "trade_source_file_accepted",
    ):
        row[f"primitive_{key}"] = primitive[key]
    for h in V.HORIZONS:
        a, b = (
            row[f"midpoint_movement_bps_per_30s_{h}s"],
            row[f"quoted_spread_bps_{h}s"],
        )
        ratio = a / b if math.isfinite(a) and math.isfinite(b) and b > 0 else None
        row[f"movement_to_spread_{h}s"] = ratio
        row[f"movement_to_spread_valid_{h}s"] = bool(
            ratio is not None
            and row[f"movement_support_valid_{h}s"]
            and row[f"quoted_spread_valid_{h}s"]
            and not row["halt_interval_active"]
        )
    return {
        k: None if isinstance(v, float) and not math.isfinite(v) else v
        for k, v in row.items()
    }


def schema():
    fields = list(V.output_schema())
    fields += [
        pa.field("interval_end", pa.timestamp("ns", "UTC")),
        pa.field("market_data_endpoint_ns", pa.int64()),
        pa.field("live_reproducible", pa.bool_()),
    ]
    for key in (
        "trade_count",
        "dollars",
        "quote_age",
        "trade_age",
        "raw_messages",
        "changes",
        "price_changes",
        "size_changes",
    ):
        fields.append(pa.field(f"primitive_{key}", pa.float64()))
    for key in ("quote_source_file_accepted", "trade_source_file_accepted"):
        fields.append(pa.field(f"primitive_{key}", pa.bool_()))
    for h in V.HORIZONS:
        fields += [
            pa.field(f"movement_to_spread_{h}s", pa.float64()),
            pa.field(f"movement_to_spread_valid_{h}s", pa.bool_()),
        ]
    return pa.schema(fields)


def write(
    quotes,
    trades,
    day,
    symbol,
    output,
    provenance,
    *,
    halts=(),
    seconds=57600,
    batch_size=25000,
):
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".partial")
    contract = identity()
    meta = dict(
        contract=contract,
        config_hash=digest(contract),
        provenance=provenance,
        sample=seconds != 57600,
        expected_rows=seconds,
    )
    out_schema = schema().with_metadata(
        {b"tape_snapshot": json.dumps(meta, sort_keys=True).encode()}
    )
    model, buffer, counts, stats = V.FeatureStream(), [], Counter(), Counter()
    started = time.monotonic()
    try:
        with pq.ParquetWriter(temporary, out_schema, compression="zstd") as writer:
            for p in V.primitives(
                quotes,
                trades,
                day,
                symbol,
                halts=halts,
                seconds=seconds,
                batch_size=batch_size,
                stats=stats,
            ):
                row = enrich(model.push(p), p)
                counts["rows"] += 1
                counts["halt_rows"] += bool(row["halt_interval_active"])
                counts["mature_300s_rows"] += bool(row["state_mature_300s"])
                for f in V.FEATURES:
                    counts["null:" + f] += row[f] is None
                buffer.append(row)
                if len(buffer) == 1024:
                    writer.write_table(pa.Table.from_pylist(buffer, schema=out_schema))
                    buffer.clear()
            if buffer:
                writer.write_table(pa.Table.from_pylist(buffer, schema=out_schema))
        if counts["rows"] != seconds:
            raise ValueError("publication row count mismatch")
        temporary.replace(output)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return dict(
        **meta,
        counts=dict(counts),
        raw_stats=dict(stats),
        elapsed_seconds=time.monotonic() - started,
        sha256=V.sha256(output),
        bytes=output.stat().st_size,
    )


def verify(path, day, symbol, *, expected_rows=57600):
    """Check persisted clocks, types, identities, finite values and ratio arithmetic."""
    pf = pq.ParquetFile(path)
    if pf.metadata.num_rows != expected_rows:
        raise ValueError("wrong feature row count")
    import numpy as np

    start, _ = V.session_bounds(day)
    offset = 0
    for batch in pf.iter_batches(batch_size=1024, use_threads=False):
        rows = batch.to_pydict()
        n = batch.num_rows
        expected = start + np.arange(offset + 1, offset + n + 1, dtype=np.int64) * V.NS
        if not np.array_equal(rows["interval_end_ns"], expected):
            raise ValueError("noncanonical output clock")
        if set(rows["symbol"]) != {symbol} or set(rows["session_date"]) != {day}:
            raise ValueError("wrong output source key")
        for field in batch.schema:
            if pa.types.is_floating(field.type):
                if any(
                    x is not None and not math.isfinite(x) for x in rows[field.name]
                ):
                    raise ValueError("nonfinite persisted feature")
        for h in V.HORIZONS:
            for a, b, r in zip(
                rows[f"midpoint_movement_bps_per_30s_{h}s"],
                rows[f"quoted_spread_bps_{h}s"],
                rows[f"movement_to_spread_{h}s"],
            ):
                expected_ratio = (
                    a / b if a is not None and b is not None and b > 0 else None
                )
                if r != expected_ratio:
                    raise ValueError("same-horizon ratio mismatch")
        offset += n
    return dict(rows_verified=offset, sha256=V.sha256(path))


def distributions(path):
    """Mergeable fixed log-bin counts, with zeros/nulls/quality kept explicit.

    This is a descriptive publication check, not independent signal counts or
    a fitted ranking. Bins run 1e-6..1e12 in each feature's published unit;
    underflow and overflow are counted, never clipped out of population totals.
    """
    import numpy as np

    fields = list(V.FEATURES) + [f"movement_to_spread_{h}s" for h in V.HORIZONS]
    edges = np.logspace(-6, 12, 73)
    groups = {}
    columns = [
        "session_date",
        "interval_end_ns",
        "halt_interval_active",
        "state_mature_300s",
        *fields,
    ]
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=1024, columns=columns, use_threads=False
    ):
        table = pa.Table.from_batches([batch])
        day = table["session_date"][0].as_py()
        start, _ = V.session_bounds(day)
        seconds = (table["interval_end_ns"].to_numpy() - start - 1) // V.NS
        sessions = {
            "premarket": seconds < 19800,
            "rth": (seconds >= 19800) & (seconds < 43200),
            "after_hours": seconds >= 43200,
        }
        halt = table["halt_interval_active"].to_numpy()
        mature = table["state_mature_300s"].to_numpy()
        for session, mask in sessions.items():
            if not mask.any():
                continue
            for field in fields:
                key = session + "|" + field
                item = groups.setdefault(
                    key,
                    dict(
                        rows=0,
                        nulls=0,
                        zeros=0,
                        negative=0,
                        underflow=0,
                        overflow=0,
                        positive_histogram=[0] * 72,
                        minimum=None,
                        maximum=None,
                        halt_rows=0,
                        mature_300s_rows=0,
                    ),
                )
                values = table[field].to_numpy()[mask]
                finite = values[np.isfinite(values)]
                item["rows"] += int(mask.sum())
                item["halt_rows"] += int(halt[mask].sum())
                item["mature_300s_rows"] += int(mature[mask].sum())
                item["nulls"] += int(len(values) - len(finite))
                if not len(finite):
                    continue
                lo = float(finite.min())
                hi = float(finite.max())
                item["minimum"] = (
                    lo if item["minimum"] is None else min(lo, item["minimum"])
                )
                item["maximum"] = (
                    hi if item["maximum"] is None else max(hi, item["maximum"])
                )
                item["zeros"] += int((finite == 0).sum())
                item["negative"] += int((finite < 0).sum())
                positive = finite[finite > 0]
                item["underflow"] += int((positive < edges[0]).sum())
                item["overflow"] += int((positive >= edges[-1]).sum())
                counts = np.histogram(
                    positive[(positive >= edges[0]) & (positive < edges[-1])],
                    bins=edges,
                )[0]
                item["positive_histogram"] = [
                    a + int(b) for a, b in zip(item["positive_histogram"], counts)
                ]
    return dict(
        version="tape_feature_distribution_v1",
        weighting="one count per published second; dependent observations",
        eligibility="all published rows; quality and maturity counts are separate, not economic gates",
        positive_bin_edges=edges.tolist(),
        groups=groups,
    )
