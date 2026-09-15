"""Bounded immutable writers for endpoint query artifacts."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .endpoint_query_core import query_identity


MAX_BUFFER_ROWS = 4096


def schema_hash(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()

STRICT_RUN_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), False),
        pa.field("session_date", pa.string(), False),
        pa.field("symbol", pa.string(), False),
        pa.field("selection_segment_id", pa.string(), False),
        pa.field("first_endpoint_ns", pa.int64(), False),
        pa.field("last_endpoint_ns", pa.int64(), False),
        pa.field("match_count", pa.int64(), False),
        pa.field("represented_start_ns", pa.int64(), False),
        pa.field("represented_end_ns", pa.int64(), False),
        pa.field("represented_duration_seconds", pa.int64(), False),
        pa.field("endpoint_elapsed_seconds", pa.int64(), False),
        pa.field("preceding_reason", pa.string(), False),
        pa.field("closure_reason", pa.string(), False),
        pa.field("left_censored", pa.bool_(), False),
        pa.field("right_censored", pa.bool_(), False),
        pa.field("source_continuity_json", pa.string(), False),
    ]
)

MEMBER_ACCOUNTING_SCHEMA = pa.schema(
    [
        pa.field("session_date", pa.string(), False),
        pa.field("symbol", pa.string(), False),
        pa.field("selected", pa.int64(), False),
        pa.field("eligible", pa.int64(), False),
        pa.field("matching", pa.int64(), False),
        pa.field("nonmatching", pa.int64(), False),
        pa.field("unavailable", pa.int64(), False),
        pa.field("match_fraction", pa.float64(), True),
        pa.field("eligibility_class", pa.string(), False),
    ]
)

CONTRIBUTION_SCHEMA = pa.schema(
    [
        pa.field("session_date", pa.string(), False),
        pa.field("symbol", pa.string(), False),
        pa.field("matching", pa.int64(), False),
        pa.field("matching_share", pa.float64(), True),
    ]
)


def _identity(path: Path) -> dict:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
            size += len(block)
    return {"path": path.name, "sha256": digest.hexdigest(), "bytes": size}


def _atomic_json(path: Path, value: object) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".partial", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, sort_keys=True, separators=(",", ":"), allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


class StreamingParquetWriter:
    """Writes an explicit schema even when no rows are emitted."""

    def __init__(self, path: Path, schema: pa.Schema, *, buffer_rows: int = 4096):
        if not 1 <= buffer_rows <= MAX_BUFFER_ROWS:
            raise ValueError("buffer_rows must be in 1..4096")
        self.path = Path(path)
        self.schema = schema
        self.buffer_rows = buffer_rows
        self.buffer: list[dict] = []
        self.writer = pq.ParquetWriter(self.path, schema, compression="zstd")
        self.rows = 0

    def add(self, row: Mapping[str, object]) -> None:
        self.buffer.append(dict(row))
        if len(self.buffer) >= self.buffer_rows:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        table = pa.Table.from_pylist(self.buffer, schema=self.schema)
        self.writer.write_table(table, row_group_size=min(len(table), MAX_BUFFER_ROWS))
        self.rows += len(table)
        self.buffer.clear()

    def close(self) -> dict:
        self.flush()
        self.writer.close()
        return {
            **_identity(self.path),
            "rows": self.rows,
            "schema_sha256": schema_hash(self.schema),
        }


class EndpointQueryExport:
    def __init__(
        self,
        root: Path,
        *,
        observation_schema: pa.Schema,
        descriptor: Mapping[str, object],
        identity: str,
        buffer_rows: int = 4096,
    ):
        self.root = Path(root)
        if identity != query_identity(descriptor):
            raise ValueError("query identity does not match descriptor")
        expected_names = ["session_date", "symbol", "interval_end_ns"]
        for field in descriptor.get("field_projection", ()):
            if not isinstance(field, Mapping) or set(field) != {"name", "reason_mask"}:
                raise ValueError("query descriptor has malformed field projection")
            expected_names.extend((field["name"], field["reason_mask"]))
        expected_names = list(dict.fromkeys(expected_names))
        if observation_schema.names != expected_names:
            raise ValueError("observation schema does not match query projection")
        if schema_hash(observation_schema) != descriptor.get("observation_schema_sha256"):
            raise ValueError("observation schema identity does not match query descriptor")
        if self.root.exists():
            raise FileExistsError("query output is immutable")
        self.root.mkdir(parents=True)
        self.descriptor = dict(descriptor)
        self.identity = identity
        self.observations = StreamingParquetWriter(
            self.root / "matching_observations.parquet", observation_schema,
            buffer_rows=buffer_rows,
        )
        self.runs = StreamingParquetWriter(
            self.root / "strict_runs.parquet", STRICT_RUN_SCHEMA,
            buffer_rows=buffer_rows,
        )

    def add_observation(self, row: Mapping[str, object]) -> None:
        self.observations.add(row)

    def add_run(self, row: Mapping[str, object]) -> None:
        self.runs.add(row)

    def finish(self, summary: Mapping[str, object]) -> dict:
        if summary.get("query_identity") != self.identity:
            raise ValueError("summary query identity mismatch")
        observations = self.observations.close()
        runs = self.runs.close()
        if runs["rows"] != summary.get("strict_run_count"):
            raise ValueError("strict-run output count mismatch")
        members_path = self.root / "member_accounting.parquet"
        contributions_path = self.root / "symbol_date_contributions.parquet"
        pq.write_table(
            pa.Table.from_pylist(summary["members"], schema=MEMBER_ACCOUNTING_SCHEMA),
            members_path,
            compression="zstd",
        )
        pq.write_table(
            pa.Table.from_pylist(summary["contributions"], schema=CONTRIBUTION_SCHEMA),
            contributions_path,
            compression="zstd",
        )
        receipt = {
            "version": "endpoint_query_export_v1",
            "query_identity": self.identity,
            "query": self.descriptor,
            "summary": {
                key: value
                for key, value in summary.items()
                if key not in ("members", "contributions")
            },
            "artifacts": {
                "matching_observations": observations,
                "strict_runs": runs,
                "member_accounting": {
                    **_identity(members_path),
                    "rows": len(summary["members"]),
                    "schema_sha256": schema_hash(MEMBER_ACCOUNTING_SCHEMA),
                },
                "symbol_date_contributions": {
                    **_identity(contributions_path),
                    "rows": len(summary["contributions"]),
                    "schema_sha256": schema_hash(CONTRIBUTION_SCHEMA),
                },
            },
            "complete": True,
        }
        _atomic_json(self.root / "query.json", self.descriptor)
        _atomic_json(self.root / "receipt.json", receipt)
        return receipt
