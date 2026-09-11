"""Raw, projected, bounded compact-feature reader for cohort state processing."""
from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import all_feature_month_core as CORE
import compact_product_schema as S
from tape_cohort_config import normalize_config

MAX_BATCH = 25_000
MAX_CHUNK = 16 * 1024**2
MAX_PROJECTED_ROW_GROUP = 64 * 1024**2
KNOWN_MASK = sum(S.REASONS.values())
CONTEXT = ("continuity_segment_id", "halt_interval_active", "post_discovery_eligible",
           "primitive_quote_source_file_accepted", "primitive_trade_source_file_accepted")


@dataclass
class ValidationStats:
    rows: int = 0
    batches: int = 0
    projected_compressed_bytes: int = 0
    validation_scope: str = "full"
    prefix_offset: int = 0
    prefix_count: int | None = None


def projected_columns(config, include_midpoint=False):
    config = normalize_config(config); features = [c["feature"] for c in config["conditions"]]
    return [*S.KEYS, *features, *(f + "_reason_mask" for f in features), *CONTEXT,
            *(["midpoint"] if include_midpoint else [])]


def _requires(feature):
    if feature.startswith(("quoted_spread_", "quote_age_", "midpoint_change_age_")):
        return ("quote",)
    if feature.startswith(("trade_rate_", "dollar_rate_", "trade_age_")):
        return ("trade",)
    return ("quote", "trade") if feature.startswith("movement_mean_to_spread_") else ("quote",)


def _validate_batch(batch, entry, metadata, features, offset):
    n = batch.num_rows
    arrays = {name: batch[name].to_numpy(zero_copy_only=False) for name in batch.schema.names}
    expected_end = CORE.session_start(entry["session_date"]) + (np.arange(offset, offset+n, dtype=np.int64)+1)*CORE.NS
    if not np.array_equal(arrays["interval_end_ns"], expected_end): raise ValueError("timestamp grid mismatch")
    if not np.all(arrays["session_date"] == entry["session_date"]): raise ValueError("session_date key mismatch")
    if not np.all(arrays["symbol"] == entry["symbol"]): raise ValueError("symbol key mismatch")
    discovery = metadata.get("discovery", {})
    endpoint = discovery.get("first_discovery_endpoint_ns")
    expected_post = expected_end >= endpoint if metadata.get("discovery_verified") and endpoint is not None else np.zeros(n, bool)
    if not np.array_equal(arrays["post_discovery_eligible"], expected_post): raise ValueError("post-discovery mask mismatch")
    for name in (*CONTEXT,):
        if batch[name].null_count: raise ValueError("null common context")
    for feature in features:
        masks = arrays[feature + "_reason_mask"]
        if batch[feature + "_reason_mask"].null_count or masks.dtype.kind not in "iu" or np.any(masks < 0) or np.any(masks & ~KNOWN_MASK):
            raise ValueError("invalid reason mask: " + feature)
        nulls = batch[feature].is_null().to_numpy(zero_copy_only=False)
        values = arrays[feature]
        if np.any(~nulls & ~np.isfinite(values)): raise ValueError("stored nonfinite feature: " + feature)
        zero = masks == 0
        if np.any(zero & (nulls | (values < 0))): raise ValueError("zero-mask value contradiction: " + feature)
        if feature.startswith("movement_participation_") and np.any(zero & (values > 1)):
            raise ValueError("participation outside [0,1]")
        for dependency in _requires(feature):
            accepted = arrays[f"primitive_{dependency}_source_file_accepted"]
            if np.any(zero & ~accepted): raise ValueError("zero mask contradicts required source")
        if np.any(zero & arrays["halt_interval_active"]): raise ValueError("zero mask contradicts active halt")
    for horizon in ("60s", "300s"):
        m, spread, ratio = f"movement_mean_5s_bps_{horizon}", f"quoted_spread_mean_bps_{horizon}", f"movement_mean_to_spread_{horizon}"
        if {m, spread, ratio} <= set(features):
            valid = (arrays[m+"_reason_mask"] == 0) & (arrays[spread+"_reason_mask"] == 0) & (arrays[ratio+"_reason_mask"] == 0)
            if np.any(valid & ~np.isclose(arrays[ratio], arrays[m]/arrays[spread], rtol=1e-10, atol=1e-12)):
                raise ValueError("stored movement/spread ratio contradiction")


def inspect_physical(path, entry, columns):
    pf = pq.ParquetFile(path)
    if pf.metadata.num_rows != entry["expected_rows"]: raise ValueError("Parquet row count mismatch")
    if not pf.schema_arrow.equals(S.FEATURE_SCHEMA, check_metadata=False): raise ValueError("physical feature schema mismatch")
    for rg in range(pf.metadata.num_row_groups):
        group = pf.metadata.row_group(rg)
        if group.num_rows > MAX_BATCH: raise ValueError("unbounded Parquet row group")
        projected = 0
        for i in range(group.num_columns):
            chunk = group.column(i)
            if chunk.path_in_schema in columns:
                if chunk.total_uncompressed_size > MAX_CHUNK: raise ValueError("unbounded projected column chunk")
                projected += chunk.total_uncompressed_size
        if projected > MAX_PROJECTED_ROW_GROUP: raise ValueError("unbounded projected row group")
    return pf


def iter_query_batches(path, member, metadata, config, batch_size=4096, include_midpoint=False,
                       *, max_rows=None, stats=None):
    """Yield raw RecordBatches; max_rows is checkpoint-only prefix selection."""
    if type(batch_size) is not int or not 1 <= batch_size <= MAX_BATCH: raise ValueError("batch_size must be 1..25000")
    if max_rows is not None and (type(max_rows) is not int or not 1 <= max_rows <= member["expected_rows"]):
        raise ValueError("invalid checkpoint prefix")
    config = normalize_config(config); features = [c["feature"] for c in config["conditions"]]
    columns = projected_columns(config, include_midpoint); stats = stats or ValidationStats()
    stats.validation_scope = "prefix" if max_rows is not None else "full"; stats.prefix_count = max_rows
    pf = inspect_physical(Path(path), member, columns); offset = 0
    for batch in pf.iter_batches(batch_size=batch_size, columns=columns, use_threads=False):
        if max_rows is not None and offset >= max_rows: break
        if max_rows is not None and offset + batch.num_rows > max_rows:
            batch = batch.slice(0, max_rows-offset)
        for name in columns:
            if batch.schema.field(name) != S.FEATURE_SCHEMA.field(name): raise ValueError("projected schema mismatch: " + name)
        _validate_batch(batch, member, metadata, features, offset)
        stats.rows += batch.num_rows; stats.batches += 1; offset += batch.num_rows
        yield batch
    expected = max_rows if max_rows is not None else member["expected_rows"]
    if offset != expected: raise ValueError("incomplete projected grid")
