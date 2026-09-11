"""Typed bounded cohort outputs, exact duration counts, and streamed verification."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

MAX_BUFFER_ROWS = 1024
MAX_PART_ROWS = 25_000
MAX_PART_BYTES = 32 * 1024**2

WINDOW_SCHEMA = pa.schema([
    pa.field("window_id", pa.string(), False), pa.field("partition_identity", pa.string(), False),
    pa.field("session_date", pa.string(), False), pa.field("symbol", pa.string(), False),
    pa.field("window_ordinal", pa.int64(), False), pa.field("continuity_segment_id", pa.int64(), False),
    pa.field("candidate_start_endpoint_ns", pa.int64(), False), pa.field("entry_confirmed_at_ns", pa.int64(), False),
    pa.field("last_member_endpoint_ns", pa.int64(), False), pa.field("exit_effective_at_ns", pa.int64(), False),
    pa.field("exit_trigger_endpoint_ns", pa.int64(), True), pa.field("exit_confirmed_at_ns", pa.int64(), True),
    pa.field("entry_confirmation_count", pa.int64(), False), pa.field("exit_confirmation_count", pa.int64(), False),
    pa.field("member_endpoint_count", pa.int64(), False), pa.field("active_seconds", pa.int64(), False),
    pa.field("strict_pass_count", pa.int64(), False), pa.field("continuation_pass_count", pa.int64(), False),
    pa.field("pending_exit_count", pa.int64(), False), pa.field("entry_reason", pa.string(), False),
    pa.field("exit_reason", pa.string(), False), pa.field("boundary_causes_json", pa.string(), False),
    pa.field("trigger_failed_features", pa.int64(), False), pa.field("exit_failed_features", pa.int64(), False),
    pa.field("exit_unavailable_features", pa.int64(), False), pa.field("exit_reason_union_mask", pa.int64(), False),
    pa.field("exit_field_reason_masks_json", pa.string(), False), pa.field("left_censored", pa.bool_(), False),
    pa.field("right_censored", pa.bool_(), False), pa.field("censor_causes_json", pa.string(), False),
    pa.field("premarket_active_seconds", pa.int64(), False), pa.field("rth_active_seconds", pa.int64(), False),
    pa.field("after_hours_active_seconds", pa.int64(), False), pa.field("crosses_session_boundary", pa.bool_(), False)])
WINDOW_FEATURE_SCHEMA = pa.schema([pa.field("window_id", pa.string(), False), pa.field("feature", pa.string(), False),
    pa.field("count", pa.int64(), False), pa.field("first", pa.float64(), False), pa.field("last", pa.float64(), False),
    pa.field("minimum", pa.float64(), False), pa.field("maximum", pa.float64(), False), pa.field("mean", pa.float64(), False)])
STRICT_SCHEMA = pa.schema([pa.field("strict_run_id", pa.string(), False), pa.field("partition_identity", pa.string(), False),
    pa.field("session_date", pa.string(), False), pa.field("symbol", pa.string(), False),
    pa.field("continuity_segment_id", pa.int64(), False), pa.field("first_endpoint_ns", pa.int64(), False),
    pa.field("last_endpoint_ns", pa.int64(), False), pa.field("endpoint_count", pa.int64(), False),
    pa.field("preceding_cause", pa.string(), False), pa.field("following_cause", pa.string(), False),
    pa.field("left_censored", pa.bool_(), False), pa.field("right_censored", pa.bool_(), False)])
DURATION_SCHEMA = pa.schema([pa.field("duration_seconds", pa.int64(), False), pa.field("window_count", pa.int64(), False)])


def schema_hash(schema): return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
OUTPUT_SCHEMA_HASHES = {"windows": schema_hash(WINDOW_SCHEMA), "window_features": schema_hash(WINDOW_FEATURE_SCHEMA),
                        "strict_runs": schema_hash(STRICT_SCHEMA), "durations": schema_hash(DURATION_SCHEMA)}


def file_identity(path):
    h = hashlib.sha256(); size = 0
    with Path(path).open("rb") as stream:
        while block := stream.read(1024**2): h.update(block); size += len(block)
    return {"sha256": h.hexdigest(), "size_bytes": size}


def atomic_json(path, value):
    path = Path(path)
    fd,name = tempfile.mkstemp(prefix="."+path.name+".", suffix=".partial", dir=path.parent)
    temp=Path(name)
    try:
        with os.fdopen(fd,"w") as stream:
            json.dump(value, stream, sort_keys=True, separators=(",", ":"), allow_nan=False)
            stream.flush(); os.fsync(stream.fileno())
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


class PartWriter:
    def __init__(self, root, schema, *, buffer_rows=MAX_BUFFER_ROWS, part_rows=MAX_PART_ROWS):
        if not 1 <= buffer_rows <= MAX_BUFFER_ROWS or not 1 <= part_rows <= MAX_PART_ROWS: raise ValueError("writer bound")
        self.root = Path(root); self.root.mkdir(parents=True, exist_ok=True); self.schema = schema
        self.buffer_rows = buffer_rows; self.part_rows = part_rows; self.buffer = []; self.writer = None
        self.part = self.part_count = self.total_rows = 0; self.parts = []; self.path = None
    def add(self, row):
        self.buffer.append(row)
        if len(self.buffer) >= self.buffer_rows: self.flush()
    def _open(self):
        self.path = self.root / f"part-{self.part:05d}.parquet"
        self.writer = pq.ParquetWriter(self.path, self.schema, compression="zstd", use_dictionary=True)
        self.part_count = 0
    def flush(self):
        if not self.buffer: return
        if self.writer is None: self._open()
        table = pa.Table.from_pylist(self.buffer, schema=self.schema)
        self.writer.write_table(table, row_group_size=min(len(self.buffer), MAX_BUFFER_ROWS))
        count = len(self.buffer); self.total_rows += count; self.part_count += count; self.buffer.clear()
        if self.part_count >= self.part_rows or table.nbytes >= MAX_PART_BYTES:
            self._close_part()
    def _close_part(self):
        if self.writer is None: return
        self.writer.close(); self.parts.append({"path": self.path.name, "rows": self.part_count, **file_identity(self.path)})
        self.writer = None; self.part += 1
    def close(self): self.flush(); self._close_part(); return list(self.parts)


class DurationHistogram:
    def __init__(self): self.counts = np.zeros(57601, dtype=np.uint64)
    def add(self, duration, count=1):
        if type(duration) is not int or not 1 <= duration <= 57600 or type(count) is not int or count < 0: raise ValueError("duration/count domain")
        if np.iinfo(np.uint64).max - self.counts[duration] < count: raise OverflowError("duration count overflow")
        self.counts[duration] += count
    def quantile(self, p):
        n = int(self.counts.sum(dtype=np.uint64))
        if n == 0: return None
        h = (n-1)*p; lo, hi = int(np.floor(h)), int(np.ceil(h)); cumulative = np.cumsum(self.counts, dtype=np.uint64)
        a = int(np.searchsorted(cumulative, lo+1)); b = int(np.searchsorted(cumulative, hi+1))
        return float(a + (h-lo)*(b-a))
    def write(self, path):
        indexes = np.flatnonzero(self.counts)
        table = pa.Table.from_arrays([pa.array(indexes, pa.int64()), pa.array(self.counts[indexes].astype(np.int64), pa.int64())], schema=DURATION_SCHEMA)
        pq.write_table(table, path, compression="zstd", row_group_size=1024)


class DateSinks:
    """Only bounded row buffers and a fixed histogram are retained."""
    def __init__(self, root, *, buffer_rows=1024, part_rows=25000):
        root = Path(root); self.root = root
        self.windows_writer = PartWriter(root/"windows", WINDOW_SCHEMA, buffer_rows=buffer_rows, part_rows=part_rows)
        self.features_writer = PartWriter(root/"window_features", WINDOW_FEATURE_SCHEMA, buffer_rows=buffer_rows, part_rows=part_rows)
        self.strict_writer = PartWriter(root/"strict_runs", STRICT_SCHEMA, buffer_rows=buffer_rows, part_rows=part_rows)
        self.duration = DurationHistogram(); self.windows_count = self.active_seconds = 0
    def windows(self, row):
        self.windows_writer.add(row); self.duration.add(row["active_seconds"])
        self.windows_count += 1; self.active_seconds += row["active_seconds"]
    def window_features(self, row): self.features_writer.add(row)
    def strict_runs(self, row): self.strict_writer.add(row)
    def close(self):
        parts = {"windows": self.windows_writer.close(), "window_features": self.features_writer.close(),
                 "strict_runs": self.strict_writer.close()}
        duration_path = self.root/"duration_counts.parquet"; self.duration.write(duration_path)
        return {"parts": parts, "duration_counts": {"path": duration_path.name, **file_identity(duration_path)},
                "window_count": self.windows_count, "active_seconds": self.active_seconds,
                "median_duration": self.duration.quantile(.5), "p90_duration": self.duration.quantile(.9)}


def iter_parts(root, records, schema, batch_size=1024):
    for record in records:
        path = Path(root)/record["path"]
        if file_identity(path) != {k: record[k] for k in ("sha256", "size_bytes")}: raise ValueError("result part identity mismatch")
        pf = pq.ParquetFile(path)
        if not pf.schema_arrow.equals(schema, check_metadata=False) or pf.metadata.num_rows != record["rows"]: raise ValueError("result part schema/count mismatch")
        yield from pf.iter_batches(batch_size=batch_size, use_threads=False)


def verify_date(root, manifest, expected_features):
    root = Path(root); windows = features = strict = active = strict_endpoints = 0; ids = set(); previous = {}
    for batch in iter_parts(root/"windows", manifest["outputs"]["parts"]["windows"], WINDOW_SCHEMA):
        for row in batch.to_pylist():
            if row["window_id"] in ids: raise ValueError("duplicate window id")
            ids.add(row["window_id"]); windows += 1; active += row["active_seconds"]
            if row["active_seconds"] != row["member_endpoint_count"] or row["last_member_endpoint_ns"] != row["exit_effective_at_ns"]-1_000_000_000: raise ValueError("window invariant")
            if row["member_endpoint_count"] != row["continuation_pass_count"]+row["pending_exit_count"]: raise ValueError("window count invariant")
            key = row["partition_identity"]
            if key in previous and row["entry_confirmed_at_ns"] < previous[key]: raise ValueError("overlapping windows")
            previous[key] = row["exit_effective_at_ns"]
    for batch in iter_parts(root/"window_features", manifest["outputs"]["parts"]["window_features"], WINDOW_FEATURE_SCHEMA): features += batch.num_rows
    for batch in iter_parts(root/"strict_runs", manifest["outputs"]["parts"]["strict_runs"], STRICT_SCHEMA):
        strict += batch.num_rows; strict_endpoints += sum(batch.column("endpoint_count").to_pylist())
    if features != windows*expected_features or windows != manifest["outputs"]["window_count"] or active != manifest["outputs"]["active_seconds"]:
        raise ValueError("date output reconciliation")
    member_strict = sum(m["observed_strict"] for m in manifest["members"])
    if strict_endpoints != member_strict or strict != sum(m["strict_run_count"] for m in manifest["members"]): raise ValueError("strict-run reconciliation")
    return {"windows": windows, "window_features": features, "strict_runs": strict,
            "active_seconds": active, "strict_endpoints": strict_endpoints}
