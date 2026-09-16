"""Scoped DuckDB access to completed endpoint/EW Parquet members."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
import os
from pathlib import Path
import shutil
import tempfile
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ..contracts.config import ContractError, FeatureConfig
from ..contracts.schemas import BASE_SCHEMA, feature_schema, schema_hash
from ..integrity import safe_relative, sha256_file, write_atomic_json
from .endpoint_catalog import read_endpoint_query_catalog
from .endpoint_reader import describe_endpoint_fields
from .endpoint_release import (
    _hash_checked,
    _open_parquet_checked,
    _read_json_stable,
    _snapshot,
    _within,
)
from .endpoint_selection import session_ranges


KEYS = ("session_date", "symbol", "interval_end_ns")
DEFAULT_ARROW_BATCH_SIZE = 4096
MAX_ARROW_BATCH_SIZE = 25_000
DEFAULT_DATAFRAME_ROWS = 10_000
BASE_AGES = (
    ("trade_age_seconds", "trade_age_reason_mask"),
    ("quote_age_seconds", "quote_age_reason_mask"),
    ("midpoint_change_age_seconds", "midpoint_change_age_reason_mask"),
)


def _parse_date(value, name):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ContractError(f"{name} must be an ISO trading-date string")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ContractError(f"{name} must be an ISO trading-date string") from error


def _scope_members(records, start_date, end_date, members):
    start = _parse_date(start_date, "start_date")
    end = _parse_date(end_date, "end_date")
    if start is not None and end is not None and start > end:
        raise ContractError("date bounds are reversed")
    available = {record["member"] for record in records}
    if members is not None:
        if (
            not isinstance(members, (tuple, list))
            or not members
            or len(set(members)) != len(members)
            or any(not isinstance(member, str) or not member for member in members)
        ):
            raise ContractError("members must be a nonempty unique sequence")
        missing = sorted(set(members) - available)
        if missing:
            raise ContractError(f"scope requests absent completed members: {missing}")
        wanted = set(members)
    else:
        wanted = available
    selected = []
    for record in records:
        trading_date = date.fromisoformat(record["session_date"])
        if record["member"] not in wanted:
            continue
        if start is not None and trading_date < start:
            continue
        if end is not None and trading_date > end:
            continue
        selected.append(record)
    if not selected:
        raise ContractError("scope contains no completed members")
    return tuple(selected)


def _output_record(manifest, name):
    outputs = manifest.get("outputs")
    if not isinstance(outputs, list):
        raise ContractError("completed member outputs are malformed")
    matches = [record for record in outputs if isinstance(record, dict) and record.get("path") == name]
    if len(matches) != 1:
        raise ContractError(f"completed member output is missing or duplicated: {name}")
    return matches[0]


def _verify_selected_files(catalog, selected, config, roots, snapshots):
    release = catalog["release"]
    feature_paths = []
    base_paths = []
    validation_bytes = 0
    for record in selected:
        base_partition = _within(roots["base"], record["base_path"])
        feature_partition = _within(roots["features"], record["feature_path"])
        manifests = {}
        for table, partition, expected in (
            ("base", base_partition, record["base_manifest"]),
            ("features", feature_partition, record["feature_manifest"]),
        ):
            manifest_path = partition / "manifest.json"
            size, snapshot = _hash_checked(
                manifest_path, expected["sha256"], expected["bytes"]
            )
            validation_bytes += size
            snapshots[str(manifest_path)] = snapshot
            current, decoded_snapshot, _, _ = _read_json_stable(
                manifest_path, expected["sha256"], expected["bytes"]
            )
            if decoded_snapshot != snapshot:
                raise ContractError("member manifest changed between hash and decode")
            manifests[table] = current
        expected_member = {
            "session_date": record["session_date"],
            "symbol": record["symbol"],
        }
        base_manifest = manifests["base"]
        feature_manifest = manifests["features"]
        if (
            base_manifest.get("manifest_version") != "tape_member_manifest_v1"
            or feature_manifest.get("manifest_version") != "tape_member_manifest_v1"
            or base_manifest.get("complete") is not True
            or feature_manifest.get("complete") is not True
            or base_manifest.get("validation", {}).get("integrity") != "passed"
            or feature_manifest.get("validation", {}).get("integrity") != "passed"
            or base_manifest.get("member") != expected_member
            or feature_manifest.get("member") != expected_member
            or base_manifest.get("coverage") != record["coverage"]
            or feature_manifest.get("coverage") != record["coverage"]
            or FeatureConfig.from_dict(base_manifest.get("contract_config")) != config
            or FeatureConfig.from_dict(feature_manifest.get("contract_config")) != config
        ):
            raise ContractError("completed member manifest binding mismatch")
        if (
            base_manifest.get("implementation_identity", {}).get("sha256")
            != release.get("base_implementation_identity")
            or feature_manifest.get("implementation_identity", {}).get("sha256")
            != release.get("feature_implementation_identity")
            or feature_manifest.get("inputs", {}).get("base_manifest_sha256")
            != record["base_manifest"]["sha256"]
            or record["feature_manifest"].get("consumed_base_manifest_sha256")
            != record["base_manifest"]["sha256"]
        ):
            raise ContractError("completed member producer/base binding mismatch")

        base_output = _output_record(base_manifest, "base.parquet")
        feature_output = _output_record(feature_manifest, "features.parquet")
        if (
            base_output != record["base_manifest"]["outputs"].get("base.parquet")
            or feature_output
            != record["feature_manifest"]["outputs"].get("features.parquet")
        ):
            raise ContractError("completed member output metadata mismatch")
        for table, partition, output, expected_schema, paths in (
            ("base", base_partition, base_output, BASE_SCHEMA, base_paths),
            (
                "features",
                feature_partition,
                feature_output,
                feature_schema(config),
                feature_paths,
            ),
        ):
            path = partition / safe_relative(output["path"])
            size, snapshot = _hash_checked(path, output["sha256"], output["bytes"])
            validation_bytes += size
            snapshots[str(path)] = snapshot
            parquet = _open_parquet_checked(path, snapshot)
            if (
                output["schema_sha256"] != schema_hash(expected_schema)
                or output["rows"] != record["coverage"]["expected_rows"]
                or parquet.metadata.num_rows != output["rows"]
                or not parquet.schema_arrow.equals(expected_schema, check_metadata=True)
            ):
                raise ContractError(f"completed {table} companion schema/count mismatch")
            paths.append(str(path))
    for path, snapshot in snapshots.items():
        if _snapshot(Path(path)) != snapshot:
            raise ContractError("query input changed during database open")
    return tuple(feature_paths), tuple(base_paths), validation_bytes


def _member_table(selected, catalog_identity, release):
    rows = []
    for record in selected:
        ranges = dict(
            zip(
                ("premarket", "rth", "after_hours"),
                session_ranges(
                    record["session_date"],
                    ("premarket", "rth", "after_hours"),
                ),
            )
        )
        rows.append(
            {
                "member": record["member"],
                "session_date": record["session_date"],
                "symbol": record["symbol"],
                "expected_rows": record["coverage"]["expected_rows"],
                "session_start_ns": record["coverage"]["session_start_ns"],
                "session_end_ns": record["coverage"]["end_ns"],
                "premarket_end_ns": ranges["premarket"][1],
                "rth_end_ns": ranges["rth"][1],
                "catalog_identity": catalog_identity,
                "release_source_revision": release.get("source_revision"),
                "release_wheel_sha256": release.get("wheel_sha256"),
            }
        )
    return pa.Table.from_pylist(rows)


def _catalog_table(config):
    rows = []
    for descriptor in describe_endpoint_fields(config):
        rows.append(
            {
                "name": descriptor["name"],
                "table_name": (
                    "features" if descriptor["table"] == "features" else "current_ages"
                ),
                "physical_table": descriptor["table"],
                "value_column": descriptor["value_column"],
                "reason_mask": descriptor["reason_mask"],
                "unit": descriptor["unit"],
                "family": descriptor["family"],
                "half_life_seconds": descriptor["half_life_seconds"],
                "window_seconds": descriptor["window_seconds"],
                "sources": json.dumps(descriptor["sources"], separators=(",", ":")),
            }
        )
    return pa.Table.from_pylist(rows)


def _quote_identifier(name):
    return '"' + name.replace('"', '""') + '"'


def _configure_views(connection, feature_paths, base_paths, members_table, catalog_table, config):
    connection.register("_members_arrow", members_table)
    connection.execute("CREATE TEMP TABLE members AS SELECT * FROM _members_arrow")
    connection.unregister("_members_arrow")
    connection.register("_catalog_arrow", catalog_table)
    connection.execute("CREATE TEMP TABLE feature_catalog AS SELECT * FROM _catalog_arrow")
    connection.unregister("_catalog_arrow")

    connection.from_parquet(
        list(feature_paths),
        hive_partitioning=False,
        union_by_name=False,
    ).create_view("_endpoint_feature_data", replace=True)
    base_columns = list(KEYS)
    for value, mask in BASE_AGES:
        base_columns.extend((value, mask))
    base_projection = ", ".join(_quote_identifier(name) for name in base_columns)
    connection.from_parquet(
        list(base_paths),
        hive_partitioning=False,
        union_by_name=False,
    ).project(base_projection).create_view("_endpoint_age_data", replace=True)

    feature_descriptors = [
        row for row in describe_endpoint_fields(config) if row["table"] == "features"
    ]
    feature_columns = []
    for descriptor in feature_descriptors:
        feature_columns.extend((descriptor["value_column"], descriptor["reason_mask"]))
    feature_sql = ",\n               ".join(
        f"f.{_quote_identifier(name)}" for name in feature_columns
    )
    connection.execute(
        f"""
        CREATE TEMP VIEW features AS
        SELECT f.session_date,
               f.symbol,
               f.interval_end_ns,
               make_timestamp_ns(f.interval_end_ns) AT TIME ZONE 'UTC' AS endpoint_time,
               CASE
                   WHEN f.interval_end_ns <= m.premarket_end_ns THEN 'premarket'
                   WHEN f.interval_end_ns <= m.rth_end_ns THEN 'rth'
                   ELSE 'after_hours'
               END AS session,
               {feature_sql}
        FROM _endpoint_feature_data AS f
        JOIN members AS m
          ON m.session_date = f.session_date AND m.symbol = f.symbol
        """
    )
    age_columns = []
    for value, mask in BASE_AGES:
        age_columns.extend((value, mask))
    age_sql = ",\n               ".join(
        f"b.{_quote_identifier(name)}" for name in age_columns
    )
    connection.execute(
        f"""
        CREATE TEMP VIEW current_ages AS
        SELECT b.session_date,
               b.symbol,
               b.interval_end_ns,
               make_timestamp_ns(b.interval_end_ns) AT TIME ZONE 'UTC' AS endpoint_time,
               CASE
                   WHEN b.interval_end_ns <= m.premarket_end_ns THEN 'premarket'
                   WHEN b.interval_end_ns <= m.rth_end_ns THEN 'rth'
                   ELSE 'after_hours'
               END AS session,
               {age_sql}
        FROM _endpoint_age_data AS b
        JOIN members AS m
          ON m.session_date = b.session_date AND m.symbol = b.symbol
        """
    )


def _bounded_positive(value, name, maximum):
    if type(value) is not int or not 1 <= value <= maximum:
        raise ContractError(f"{name} must be in 1..{maximum}")
    return value


class TapeQueryResult:
    """DuckDB relation with bounded materialization and streaming Arrow access."""

    def __init__(self, relation):
        self._relation = relation

    def arrow_batches(self, batch_size=DEFAULT_ARROW_BATCH_SIZE):
        """Yield record batches without materializing the complete result."""
        _bounded_positive(batch_size, "Arrow batch size", MAX_ARROW_BATCH_SIZE)
        yield from self._relation.to_arrow_reader(batch_size=batch_size)

    def df(self, max_rows=DEFAULT_DATAFRAME_ROWS):
        """Convert a small result to pandas, failing instead of over-materializing."""
        _bounded_positive(max_rows, "DataFrame row limit", MAX_ARROW_BATCH_SIZE)
        frame = self._relation.limit(max_rows + 1).df()
        if len(frame) > max_rows:
            raise ContractError(
                f"query exceeds the {max_rows}-row DataFrame limit; "
                "use arrow_batches() or a streamed Parquet export"
            )
        return frame

    def preview(self, limit=20):
        """Return a capped JSON-compatible preview without counting the full result."""
        _bounded_positive(limit, "preview limit", 100)
        table = self._relation.limit(limit + 1).to_arrow_table()
        truncated = table.num_rows > limit
        if truncated:
            table = table.slice(0, limit)
        return {
            "columns": table.column_names,
            "displayed_rows": table.num_rows,
            "truncated": truncated,
            "rows": table.to_pylist(),
        }

    def __getattr__(self, name):
        # Preserve existing relation methods such as fetchone/fetchall.
        return getattr(self._relation, name)


@dataclass
class TapeDatabase:
    """Context-managed DuckDB connection plus immutable scope metadata."""

    connection: duckdb.DuckDBPyConnection
    catalog_identity: str
    selected_members: tuple[str, ...]
    validation_seconds: float
    validation_bytes: int
    start_date: str | None
    end_date: str | None
    release_source_revision: str | None
    release_wheel_sha256: str | None
    _input_snapshots: dict[str, tuple[int, int, int, int, int]]
    _temporary_directory: tempfile.TemporaryDirectory | None = None

    def _check_inputs(self):
        for path, snapshot in self._input_snapshots.items():
            if _snapshot(Path(path)) != snapshot:
                raise ContractError("verified query input changed during database session")

    def sql(self, query, params=None):
        if not isinstance(query, str) or not query.strip():
            raise ContractError("SQL query must be a nonempty string")
        self._check_inputs()
        return TapeQueryResult(self.connection.sql(query, params=params))

    def export_parquet(self, query, output):
        """Stream a query to an immutable Parquet export with reproducibility data."""
        return _export_parquet(self, query, output)

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        if self._temporary_directory is not None:
            self._temporary_directory.cleanup()
            self._temporary_directory = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def _write_new_text(path, text):
    with Path(path).open("x", encoding="utf-8") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())


def _sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def _export_parquet(database, query, output):
    if not isinstance(query, str) or not query.strip():
        raise ContractError("SQL query must be a nonempty string")
    normalized = query.strip()
    encoded = (normalized + "\n").encode()
    if len(encoded) > 1 << 20:
        raise ContractError("SQL file exceeds 1 MiB")
    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(output.parent).free < 20 * 1024**3:
        raise ContractError("export destination violates 20 GiB free-disk reserve")
    attempt = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    started = time.perf_counter()
    try:
        database._check_inputs()
        sql_path = attempt / "query.sql"
        scope_path = attempt / "scope.json"
        parquet_path = attempt / "results.parquet"
        _write_new_text(sql_path, encoded.decode())
        scope = {
            "start_date": database.start_date,
            "end_date": database.end_date,
            "members": list(database.selected_members),
            "sessions": "all_represented",
            "source_identity": {
                "query_catalog_identity": database.catalog_identity,
                "release_source_revision": database.release_source_revision,
                "release_wheel_sha256": database.release_wheel_sha256,
            },
        }
        write_atomic_json(scope_path, scope)
        executable = normalized[:-1].rstrip() if normalized.endswith(";") else normalized
        database.connection.execute(
            f"COPY ({executable}) TO {_sql_literal(parquet_path)} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        database._check_inputs()
        parquet = pq.ParquetFile(parquet_path)
        sql_sha, sql_bytes = sha256_file(sql_path)
        scope_sha, scope_bytes = sha256_file(scope_path)
        result_sha, result_bytes = sha256_file(parquet_path)
        manifest = {
            "version": "endpoint_query_export_v1",
            "source_identity": scope["source_identity"],
            "scope": {"path": "scope.json", "sha256": scope_sha, "bytes": scope_bytes},
            "sql": {"path": "query.sql", "sha256": sql_sha, "bytes": sql_bytes},
            "result": {
                "path": "results.parquet",
                "sha256": result_sha,
                "bytes": result_bytes,
                "rows": parquet.metadata.num_rows,
            },
            "query_seconds": time.perf_counter() - started,
        }
        write_atomic_json(attempt / "manifest.json", manifest)
        os.rename(attempt, output)
    except Exception:
        shutil.rmtree(attempt, ignore_errors=True)
        raise
    return {**manifest, "output": str(output)}


def open_tape_database(
    catalog,
    *,
    data_roots,
    expected_identity=None,
    start_date=None,
    end_date=None,
    members=None,
    memory_limit="256MiB",
    temp_directory=None,
):
    """Open a DuckDB handle over explicit completed date/member files.

    Dates are inclusive trading-date bounds. All represented sessions are exposed;
    a session filter exists only when the researcher's SQL states one.
    """
    started = time.perf_counter()
    if not isinstance(data_roots, dict) or set(data_roots) != {"base", "features"}:
        raise ContractError("data_roots must explicitly provide base and features")
    roots = {name: Path(path).resolve() for name, path in data_roots.items()}
    manifest, records, config, snapshots, validation_bytes = read_endpoint_query_catalog(
        catalog, expected_identity=expected_identity
    )
    selected = _scope_members(records, start_date, end_date, members)
    feature_paths, base_paths, companion_bytes = _verify_selected_files(
        manifest, selected, config, roots, snapshots
    )
    validation_bytes += companion_bytes
    temporary = None
    if temp_directory is None:
        temporary = tempfile.TemporaryDirectory(prefix="tape-duckdb-")
        temp_directory = temporary.name
        max_temp_directory_size = "0B"
    else:
        temp_directory = str(Path(temp_directory).resolve())
        Path(temp_directory).mkdir(parents=True, exist_ok=True)
        max_temp_directory_size = "1GiB"
        if shutil.disk_usage(temp_directory).free < 20 * 1024**3:
            raise ContractError("DuckDB temp directory violates 20 GiB free-disk reserve")
    connection = None
    try:
        connection = duckdb.connect(
            database=":memory:",
            config={
                "threads": "1",
                "memory_limit": memory_limit,
                "temp_directory": temp_directory,
                "max_temp_directory_size": max_temp_directory_size,
            },
        )
        _configure_views(
            connection,
            feature_paths,
            base_paths,
            _member_table(selected, manifest["catalog_identity"], manifest["release"]),
            _catalog_table(config),
            config,
        )
    except Exception:
        if connection is not None:
            connection.close()
        if temporary is not None:
            temporary.cleanup()
        raise
    return TapeDatabase(
        connection=connection,
        catalog_identity=manifest["catalog_identity"],
        selected_members=tuple(record["member"] for record in selected),
        validation_seconds=time.perf_counter() - started,
        validation_bytes=validation_bytes,
        start_date=start_date,
        end_date=end_date,
        release_source_revision=manifest["release"].get("source_revision"),
        release_wheel_sha256=manifest["release"].get("wheel_sha256"),
        _input_snapshots=snapshots,
        _temporary_directory=temporary,
    )
