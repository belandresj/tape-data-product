"""Batch product assembly and summaries; independent verifier remains unchanged.

O(BF) allocations, B<=4096 by default and <=25000 hard ceiling. Never accumulates
partition arrays. The row assembly and Summary classes remain regression oracles.
"""

from collections import Counter
from itertools import zip_longest
import math
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from tape_data_product.features import all_feature_month_core as C
from tape_data_product.features import all_feature_month_verify as V


def array(column):
    return column.to_numpy(zero_copy_only=False)


def numeric_equal(a, b, label):
    if not np.all(np.isclose(a, b, rtol=1e-10, atol=1e-12, equal_nan=True)):
        raise ValueError(label + " reconstruction mismatch")


def assemble(base, extension, discovery):
    """Vectorized equivalent of C.assemble; nulls and gates retain exact semantics."""
    n = base.num_rows
    b = {k: base[k] for k in base.schema.names}
    r = {k: b[k] for k in C.CONTEXT}
    r.update(
        {f"{x}_{h}s": b[f"{x}_{h}s"] for h in C.HORIZONS for x in C.BASE_DIAGNOSTICS}
    )
    r.update({k: extension[k] for k in extension.schema.names})
    cache = {}

    def a(k):
        if k not in cache:
            cache[k] = array(r[k]) if isinstance(r[k], pa.Array) else r[k]
        return cache[k]

    def put(k, value):
        r[k] = value
        cache[k] = value

    active = a("halt_interval_active")
    qa = a("primitive_quote_source_file_accepted")
    ta = a("primitive_trade_source_file_accepted")
    count = array(b["primitive_trade_count"])
    dollars = array(b["primitive_dollars"])
    if not np.all(
        np.isfinite(count) & (count >= 0) & (count < 2**63) & (count == np.floor(count))
    ) or not np.all(np.isfinite(dollars) & (dollars >= 0)):
        raise ValueError("invalid transaction primitive")
    put("trade_count_1s", count.astype(np.int64))
    put("dollar_volume_1s", dollars)
    for kind, accepted in (("trade", ta), ("quote", qa)):
        put(
            f"{kind}_age_end_seconds",
            np.where(
                ~active & accepted, array(b[f"primitive_{kind}_age"]) / 1000, np.nan
            ),
        )
    for key, source in (
        ("first_discovery_endpoint_ns", "endpoint_ns"),
        ("discovery_received_at_ns", "received_at_ns"),
        ("discovery_provenance_hash", "provenance_hash"),
    ):
        r[key] = pa.array(
            [discovery.get(source)] * n, type=C.explicit_schema([key], {}).field(0).type
        )
    r["discovery_timing_basis"] = pa.array(
        [discovery.get("timing_basis", "unavailable")] * n
    )
    t = a("interval_end_ns")
    day = b["session_date"][0].as_py()
    post = (
        t >= discovery["endpoint_ns"]
        if discovery.get("verified") and discovery.get("endpoint_ns") is not None
        else np.zeros(n, dtype=bool)
    )
    put("post_discovery_eligible", post)
    left = (t - C.session_start(day)) // C.NS - 1
    r["session_segment"] = pa.array(
        np.where(
            left < 19800, "premarket", np.where(left < 43200, "rth", "after_hours")
        )
    )
    for h in C.HORIZONS:
        for target, (source, scale, unit, family) in C.MAPPINGS.items():
            v = array(b[f"{source}_{h}s"])
            put(
                f"{target}_{h}s",
                (
                    v / 6
                    if target == "movement_mean_5s_bps"
                    else v / 1000 if "age_p90" in target else v
                ),
            )
        for name, family in C.FAMILIES:
            f = f"{name}_{h}s"
            value = a(f)
            source = ta if family in ("activity", "trade_age") else qa
            mature = (
                a(f"midpoint_change_age_mature_{h}s")
                if family == "mid_age"
                else a(f"state_mature_{h}s")
            )
            if family in ("trade_age", "quote_age"):
                support = a(f"{family}_observation_count_{h}s") >= math.ceil(0.8 * h)
            else:
                support = a(f"{C.SUPPORT_STEMS[family]}_{h}s")
            reason = (
                (~np.isfinite(value)).astype(np.int64)
                | ((~source).astype(np.int64) * 2)
                | ((~mature).astype(np.int64) * 4)
                | ((~support).astype(np.int64) * 8)
                | (active.astype(np.int64) * 16)
            )
            if name == "movement_participation":
                reason |= a(f"movement_zero_total_{h}s").astype(np.int64) * 128
            put(f + "_reason_mask", reason)
            put(f + "_analysis_valid", reason == 0)
        spread = a(f"quoted_spread_mean_bps_{h}s")
        m = a(f"movement_mean_5s_bps_{h}s")
        f = f"movement_mean_to_spread_{h}s"
        value = np.full(n, np.nan)
        np.divide(
            m,
            spread,
            out=value,
            where=np.isfinite(m) & np.isfinite(spread) & (spread > 0),
        )
        put(f, value)
        reason = a(f"movement_mean_5s_bps_{h}s_reason_mask") | a(
            f"quoted_spread_mean_bps_{h}s_reason_mask"
        )
        reason |= ((~np.isfinite(spread)) | (spread <= 0)).astype(np.int64) * 64
        reason |= (~np.isfinite(value)).astype(np.int64)
        put(f + "_reason_mask", reason)
        put(f + "_analysis_valid", reason == 0)
        numeric_equal(value, array(b[f"movement_to_spread_{h}s"]) / 6, "mean ratio")
        for f in C.FEATURES_BY_HORIZON[h]:
            carried = (
                ~a(f"state_fully_post_halt_{h}s")
                if not f.startswith("midpoint_change_age")
                else np.zeros(n, dtype=bool)
            )
            put(
                f + "_reason_mask",
                a(f + "_reason_mask") | (carried.astype(np.int64) * 32),
            )
            put(f + "_eda_eligible", a(f + "_analysis_valid") & ~carried & post)
    schema = C.explicit_schema(C.final_columns(), {})
    values = []
    for field in schema:
        v = r[field.name]
        if isinstance(v, pa.Array):
            values.append(v.cast(field.type))
        else:
            values.append(
                pa.array(
                    v,
                    type=field.type,
                    mask=~np.isfinite(v) if pa.types.is_floating(field.type) else None,
                )
            )
    return pa.RecordBatch.from_arrays(values, schema=schema)


def projected(path, columns, batch_size, limit=None):
    pf = pq.ParquetFile(path)
    if set(columns) - set(pf.schema_arrow.names):
        raise ValueError("missing projected columns")
    remaining = pf.metadata.num_rows if limit is None else limit
    for batch in pf.iter_batches(
        batch_size=batch_size, columns=columns, use_threads=False
    ):
        take = min(remaining, batch.num_rows)
        batch = batch.slice(0, take)
        for field, column in zip(batch.schema, batch.columns):
            if pa.types.is_floating(field.type):
                v = array(column)
                if np.any(np.isinf(v)):
                    raise ValueError("nonfinite persisted value: " + field.name)
                # Distinguish stored NaN from Arrow null.
                if np.any(np.isnan(v) & ~array(column.is_null())):
                    raise ValueError("nonfinite persisted value: " + field.name)
        yield batch
        remaining -= take
        if remaining == 0:
            break
    if remaining:
        raise ValueError("base prefix incomplete")


def local_batches(
    catalog, base, extension, *, columns=None, batch_size=4096, check=lambda: None
):
    from tape_data_product.features import july_r2_product as J

    if not 1 <= batch_size <= 25000:
        raise ValueError("batch size must be 1..25000")
    J.validate_catalog(catalog)
    selected = C.final_columns() if columns is None else list(columns)
    if len(set(selected)) != len(selected) or set(selected) - set(C.final_columns()):
        raise ValueError("unknown/duplicate columns")
    for path, expected in ((base, catalog["base"]), (extension, catalog["extension"])):
        J.OLD.local_identity(path, expected)
    schema = pq.ParquetFile(extension).schema_arrow
    if schema.names != C.extension_columns() or not schema.remove_metadata().equals(
        C.explicit_schema(C.extension_columns(), {}).remove_metadata()
    ):
        raise ValueError("extension schema mismatch")
    ident = catalog["identity"]
    start = C.session_start(ident["day"])
    n = 0
    for a, b in zip_longest(
        projected(base, C.BASE_COLUMNS, batch_size, ident["seconds"]),
        projected(extension, C.extension_columns(), batch_size),
    ):
        check()
        if a is None or b is None or a.num_rows != b.num_rows:
            raise ValueError("incomplete base/extension grid")
        clock = start + np.arange(n + 1, n + a.num_rows + 1, dtype=np.int64) * C.NS
        for batch in (a, b):
            if (
                not np.array_equal(array(batch["interval_end_ns"]), clock)
                or not np.all(array(batch["symbol"]) == ident["symbol"])
                or not np.all(array(batch["session_date"]) == ident["day"])
            ):
                raise ValueError("duplicate, gap, or incomplete base/extension grid")
        n += a.num_rows
        yield assemble(a, b, catalog["discovery"]).select(selected)
    if n != ident["seconds"]:
        raise ValueError("incomplete partition grid")


class Summary:
    """Same population, reason counts and bin edges as the scalar oracle."""

    def __init__(self):
        self.groups = {}

    def push(self, batch):
        segment = array(batch["session_segment"])
        post = array(batch["post_discovery_eligible"])
        for session in np.unique(segment):
            selection = segment == session
            for f in C.FEATURES:
                key = (str(session), f)
                if key not in self.groups:
                    ax = V.axis(f)
                    self.groups[key] = dict(
                        rows=0,
                        valid=0,
                        nulls=0,
                        zeros=0,
                        post=0,
                        eda=0,
                        support=0,
                        reason=Counter({k: 0 for k in C.REASONS}),
                        histogram=np.zeros(ax["bins"], dtype=np.int64),
                        axis=ax,
                        minimum=None,
                        maximum=None,
                    )
                g = self.groups[key]
                v = array(batch[f])[selection]
                reason = array(batch[f + "_reason_mask"])[selection]
                valid = array(batch[f + "_analysis_valid"])[selection]
                eligible = array(batch[f + "_eda_eligible"])[selection]
                g["rows"] += len(v)
                g["valid"] += int(valid.sum())
                g["nulls"] += int(np.isnan(v).sum())
                g["zeros"] += int((v == 0).sum())
                g["post"] += int(post[selection].sum())
                g["eda"] += int(eligible.sum())
                g["support"] += int(((reason & 8) == 0).sum())
                for name, bit in C.REASONS.items():
                    g["reason"][name] += int(((reason & bit) != 0).sum())
                values = v[eligible]
                if not len(values):
                    continue
                if not np.all(np.isfinite(values)) or np.any(values < 0):
                    raise ValueError("invalid eligible histogram value")
                low = float(values.min())
                high = float(values.max())
                g["minimum"] = low if g["minimum"] is None else min(g["minimum"], low)
                g["maximum"] = high if g["maximum"] is None else max(g["maximum"], high)
                if f.startswith("movement_participation_"):
                    if high > 1:
                        raise ValueError("participation histogram bounds")
                    indices = np.minimum(
                        199,
                        np.searchsorted(g["axis"]["edges"], values, side="right") - 1,
                    )
                else:
                    indices = np.where(
                        values == 0,
                        0,
                        np.where(
                            values < 1e-6,
                            1,
                            np.where(
                                values >= 1e12,
                                74,
                                1
                                + np.searchsorted(
                                    g["axis"]["edges"], values, side="right"
                                ),
                            ),
                        ),
                    )
                g["histogram"] += np.bincount(indices, minlength=g["axis"]["bins"])

    def result(self):
        result = []
        for (session, f), g in sorted(self.groups.items()):
            if int(g["histogram"].sum()) != g["eda"]:
                raise ValueError("histogram eligible mass mismatch")
            result.append(
                dict(
                    session_segment=session,
                    feature=f,
                    **(g | {"histogram": g["histogram"].tolist()}),
                )
            )
        return result


def verify(batches, seconds):
    reference = V.Reference()
    summary = Summary()
    n = 0
    for batch in batches:
        # Keep the separately implemented reference mathematics and checks.
        for offset in range(0, batch.num_rows, 512):
            for row in batch.slice(offset, 512).to_pylist():
                reference.push(row)
        summary.push(batch)
        n += batch.num_rows
    if n != seconds:
        raise ValueError("verification coverage mismatch")
    return dict(
        rows_verified=n,
        reconstructed_fields=list(C.FEATURES),
        coverage=summary.result(),
    )
