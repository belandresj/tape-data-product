"""Bounded physical output with integrity checks and explicit reconstruction audit.

O(N F + N log H) time; O(4096 F + 12288 F + H F) memory, H <= 300.
Only verification/reuse reconstructs public rows; direct generation supplies pairs.
Local completion is atomic and immutable. Remote publication is explicitly invoked.
"""

from collections import Counter
from itertools import zip_longest
from pathlib import Path
import hashlib
import json
import math
import os
import time
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from tape_data_product.features import all_feature_month_core as core
from tape_data_product.features import all_feature_month_verify as reconstruction
from tape_data_product.features import compact_product_schema as product_schema

BUILDER_VERSION = "direct_frozen_product_v1"
VALIDATION_MODES = ("integrity", "reconstruction")
VALIDATION_POLICY_VERSION = "compact_validation_v2"
# Aliases retained for independent historical regression consumers.
C, R, S = core, reconstruction, product_schema


def digest(value):
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".partial")
    with temporary.open("w") as f:
        json.dump(value, f, sort_keys=True, indent=2, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def file_identity(path, check=lambda: None):
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        while block := f.read(1024 * 1024):
            h.update(block)
            size += len(block)
            check()
    return dict(sha256=h.hexdigest(), size_bytes=size)


def calculation_identity():
    from tape_data_product.stages import implementation_identity

    return dict(
        version=BUILDER_VERSION,
        midpoint="exact_binary64_integer_nanoseconds_v1",
        implementation=implementation_identity(),
    )


def require_compatible(provenance, accepted):
    calc = provenance.get("calculation")
    if (
        not calc
        or calc.get("midpoint") != "exact_binary64_integer_nanoseconds_v1"
        or calc not in accepted
    ):
        raise ValueError("unknown or incompatible corrected calculation identity")


def split_rows(rows):
    """Require all existing fields; never silently discard inconsistent metadata."""
    for row in rows:
        for f in product_schema.FEATURES:
            valid, eligible = product_schema.eligibility(
                row[f + "_reason_mask"], row["post_discovery_eligible"]
            )
            if (
                row[f + "_analysis_valid"] != valid
                or row[f + "_eda_eligible"] != eligible
            ):
                raise ValueError("inconsistent redundant eligibility in reuse source")
        offset = (
            row["interval_end_ns"] - core.session_start(row["session_date"])
        ) // core.NS - 1
        segment = (
            "premarket"
            if offset < 19800
            else "rth" if offset < 43200 else "after_hours"
        )
        if row["session_segment"] != segment:
            raise ValueError("inconsistent session segment in reuse source")
        yield (
            {k: row[k] for k in product_schema.FEATURE_COLUMNS},
            {k: row[k] for k in product_schema.SUPPORT_COLUMNS},
            {k: row[k] for k in product_schema.DISCOVERY_FIELDS},
        )


def joined_rows(directory, metadata, check=lambda: None):
    directory = Path(directory)
    a = core.rows(directory / "features.parquet", check=check, validate_finite=True)
    b = core.rows(directory / "support.parquet", check=check, validate_finite=True)
    for feature, support in zip_longest(a, b):
        if feature is None or support is None:
            raise ValueError("feature/support length mismatch")
        if tuple(feature[k] for k in product_schema.KEYS) != tuple(
            support[k] for k in product_schema.KEYS
        ):
            raise ValueError("feature/support key mismatch")
        row = support | feature | metadata["discovery"]
        for f in product_schema.FEATURES:
            row[f + "_analysis_valid"], row[f + "_eda_eligible"] = product_schema.eligibility(
                row[f + "_reason_mask"], row["post_discovery_eligible"]
            )
        offset = (
            row["interval_end_ns"] - core.session_start(row["session_date"])
        ) // core.NS - 1
        row["session_segment"] = (
            "premarket"
            if offset < 19800
            else "rth" if offset < 43200 else "after_hours"
        )
        yield row


def verify(directory, metadata, check=lambda: None):
    directory = Path(directory)
    for name, schema in [
        ("features", product_schema.FEATURE_SCHEMA),
        ("support", product_schema.SUPPORT_SCHEMA),
    ]:
        pf = pq.ParquetFile(directory / (name + ".parquet"))
        if not pf.schema_arrow.remove_metadata().equals(schema):
            raise ValueError(name + " explicit schema mismatch")
    reference = reconstruction.Reference(strict_values=True)
    counts = Counter()
    n = 0
    start = core.session_start(metadata["session_date"])
    for row in joined_rows(directory, metadata, check):
        n += 1
        if tuple(row[k] for k in product_schema.KEYS) != (
            metadata["session_date"],
            metadata["symbol"],
            start + n * core.NS,
        ):
            raise ValueError("noncanonical, missing or duplicate grid key")
        for schema in (product_schema.FEATURE_SCHEMA, product_schema.SUPPORT_SCHEMA):
            for field in schema:
                if not field.nullable and row[field.name] is None:
                    raise ValueError("null required field: " + field.name)
        endpoint = metadata["discovery"]["first_discovery_endpoint_ns"]
        expected = bool(
            metadata["discovery_verified"]
            and endpoint is not None
            and row["interval_end_ns"] >= endpoint
        )
        if row["post_discovery_eligible"] != expected:
            raise ValueError("discovery eligibility mismatch")
        reference.push(row)
        for f in product_schema.FEATURES:
            counts[f + "|null"] += row[f] is None
            counts[f + "|reason=" + str(row[f + "_reason_mask"])] += 1
        if n % 256 == 0:
            check()
    if n != metadata["expected_rows"]:
        raise ValueError("wrong compact row count")
    return dict(
        rows_verified=n, counts=dict(counts), reconstructed_fields=list(product_schema.FEATURES)
    )


def _all_true(values):
    return pc.all(pc.fill_null(values, True)).as_py() is not False


def _validate_columns(batch):
    for field, column in zip(batch.schema, batch.columns):
        if not field.nullable and column.null_count:
            raise ValueError("null required field: " + field.name)
        if pa.types.is_floating(field.type) and not _all_true(pc.is_finite(column)):
            raise ValueError("nonfinite value: " + field.name)
        if field.name.endswith("_reason_mask"):
            if not _all_true(
                pc.and_(pc.greater_equal(column, 0), pc.less_equal(column, 255))
            ):
                raise ValueError("unknown reason mask: " + field.name)
        if field.name in product_schema.FEATURES or field.name in (
            "trade_count_1s",
            "dollar_volume_1s",
            "quoted_spread_integral_bps_seconds",
        ):
            if not _all_true(pc.greater_equal(column, 0)):
                raise ValueError("negative value: " + field.name)
        if field.name in (
            "midpoint_valid_duration_ns",
            "quoted_spread_valid_duration_ns",
        ):
            if not _all_true(
                pc.and_(pc.greater_equal(column, 0), pc.less_equal(column, core.NS))
            ):
                raise ValueError("invalid support duration: " + field.name)


def _typed_array(values, field):
    # Infer first, then cast safely: explicit pa.array(..., int64) may truncate
    # fractional floats. Only compatible numeric representations may convert.
    array = pa.array(values)
    if not array.type.equals(field.type):
        numeric = lambda t: pa.types.is_integer(t) or pa.types.is_floating(t)
        if not pa.types.is_null(array.type) and not (
            numeric(array.type) and numeric(field.type)
        ):
            raise ValueError("wrong scalar type: " + field.name)
        array = pc.cast(array, field.type, safe=True)
    return array


def verify_integrity(directory, metadata, check=lambda: None):
    """Bounded Arrow checks of persisted structure; no rolling reconstruction."""
    counts = Counter()
    expected_rows = metadata["expected_rows"]
    start = core.session_start(metadata["session_date"])
    for name, schema in (
        ("features", product_schema.FEATURE_SCHEMA),
        ("support", product_schema.SUPPORT_SCHEMA),
    ):
        pf = pq.ParquetFile(Path(directory) / (name + ".parquet"))
        if not pf.schema_arrow.remove_metadata().equals(schema):
            raise ValueError(name + " explicit schema mismatch")
        if pf.metadata.num_rows != expected_rows:
            raise ValueError("wrong compact row count")
        offset = 0
        for batch in pf.iter_batches(batch_size=4096, use_threads=False):
            check()
            _validate_columns(batch)
            endpoints = pa.array(
                range(
                    start + (offset + 1) * core.NS,
                    start + (offset + batch.num_rows + 1) * core.NS,
                    core.NS,
                ),
                type=pa.int64(),
            )
            if not _all_true(pc.equal(batch.column("interval_end_ns"), endpoints)):
                raise ValueError("noncanonical, missing or duplicate grid key")
            for key in ("session_date", "symbol"):
                if not _all_true(pc.equal(batch.column(key), metadata[key])):
                    raise ValueError("partition key mismatch: " + key)
            if name == "features":
                endpoint = metadata["discovery"]["first_discovery_endpoint_ns"]
                expected = (
                    pc.greater_equal(endpoints, endpoint)
                    if metadata["discovery_verified"] and endpoint is not None
                    else pa.repeat(False, batch.num_rows)
                )
                if not _all_true(
                    pc.equal(batch.column("post_discovery_eligible"), expected)
                ):
                    raise ValueError("discovery eligibility mismatch")
                for feature in product_schema.FEATURES:
                    counts[feature + "|null"] += batch.column(feature).null_count
                    for item in pc.value_counts(
                        batch.column(feature + "_reason_mask")
                    ).to_pylist():
                        counts[feature + "|reason=" + str(item["values"])] += item[
                            "counts"
                        ]
            offset += batch.num_rows
        if offset != expected_rows:
            raise ValueError("wrong compact row count")
    return dict(
        rows_verified=expected_rows,
        rows_integrity_checked=expected_rows,
        counts=dict(counts),
        mode="integrity",
        independent_reconstruction="not_run",
        reconstructed_fields=[],
    )


def validation_evidence(directory, metadata, check=lambda: None):
    policy = metadata.get("validation_policy")
    if policy is None:
        return verify(directory, metadata, check)  # Historical exhaustive contract.
    if (
        policy.get("version") != VALIDATION_POLICY_VERSION
        or policy.get("mode") not in VALIDATION_MODES
    ):
        raise ValueError("unknown validation policy")
    if policy["mode"] == "integrity":
        return verify_integrity(directory, metadata, check)
    evidence = verify(directory, metadata, check)
    return dict(
        evidence,
        rows_integrity_checked=evidence["rows_verified"],
        mode="reconstruction",
        independent_reconstruction="passed",
    )


def audit_complete(directory, check=lambda: None):
    """Explicit diagnostic; never rewrites a partition's immutable evidence."""
    manifest = verify_complete(directory, check=check)
    evidence = verify(directory, manifest["metadata"], check)
    return dict(
        state="passed",
        partition_identity=manifest["partition_identity"],
        mode="reconstruction",
        independent_reconstruction="passed",
        **evidence,
    )


def write_partition(
    directory,
    pairs,
    metadata,
    *,
    output_rows=12288,
    check=lambda: None,
    validation_mode="integrity",
):
    """Write private attempt; manifest is the only completion marker.

    Failed/interrupted directories are deliberately retained for diagnosis.
    Caller must supply immutable input, calculation, overlay and discovery IDs.
    """
    if validation_mode not in VALIDATION_MODES:
        raise ValueError("unknown validation mode")
    if not 1 <= output_rows <= 12288:
        raise ValueError("invalid output buffer bound")
    if not 1 <= metadata["expected_rows"] <= 57600:
        raise ValueError("invalid row count")
    for key in (
        "inputs",
        "calculation",
        "overlay",
        "discovery_verified",
        "session_date",
        "symbol",
    ):
        if key not in metadata:
            raise ValueError("missing partition identity: " + key)
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    schemas = (product_schema.FEATURE_SCHEMA, product_schema.SUPPORT_SCHEMA)
    buffers = [{k: [] for k in schema.names} for schema in schemas]
    fields = [tuple(schema) for schema in schemas]
    names = [frozenset(schema.names) for schema in schemas]
    destinations = [tuple(columns.items()) for columns in buffers]
    discovery = None
    count = 0
    generation_seconds = 0.0
    source = iter(pairs)

    def timed_pairs():
        nonlocal generation_seconds
        while True:
            tick = time.perf_counter()
            try:
                pair = next(source)
            except StopIteration:
                generation_seconds += time.perf_counter() - tick
                return
            generation_seconds += time.perf_counter() - tick
            yield pair

    def flush(writers):
        for writer, schema, columns, frozen_fields in zip(
            writers, schemas, buffers, fields
        ):
            arrays = []
            for field in frozen_fields:
                arrays.append(_typed_array(columns[field.name], field))
                columns[field.name].clear()
            batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
            _validate_columns(batch)
            writer.write_batch(batch, row_group_size=output_rows)
            del batch, arrays
        check()

    with (
        pq.ParquetWriter(
            directory / "features.parquet", schemas[0], compression="zstd"
        ) as fw,
        pq.ParquetWriter(
            directory / "support.parquet", schemas[1], compression="zstd"
        ) as sw,
    ):
        for features, support, d in timed_pairs():
            if discovery is None:
                discovery = dict(d)
            if d != discovery:
                raise ValueError("nonconstant discovery metadata")
            for row, expected, columns in zip((features, support), names, destinations):
                if row.keys() != expected:
                    raise ValueError("physical column membership mismatch")
                for name, values in columns:
                    values.append(row[name])
            count += 1
            if count % output_rows == 0:
                flush((fw, sw))
            if count % 256 == 0:
                check()
        if count % output_rows:
            flush((fw, sw))
    if not count:
        raise ValueError("empty partition")
    meta = dict(
        metadata,
        discovery=discovery,
        layout_version=product_schema.LAYOUT_VERSION,
        feature_contract=product_schema.FEATURE_CONTRACT,
        builder_version=BUILDER_VERSION,
        validation_policy=dict(version=VALIDATION_POLICY_VERSION, mode=validation_mode),
    )
    calculation_write_seconds = time.monotonic() - started
    verification_started = time.monotonic()
    evidence = validation_evidence(directory, meta, check)
    verification_seconds = time.monotonic() - verification_started
    hash_started = time.monotonic()
    objects = {}
    for name in ("features", "support"):
        path = directory / (name + ".parquet")
        objects[name] = file_identity(path, check) | dict(
            rows=count,
            file=path.name,
            schema_hash=product_schema.schema_hash(pq.ParquetFile(path).schema_arrow),
        )
    manifest = dict(
        metadata=meta,
        objects=objects,
        validation=evidence,
        elapsed_seconds=time.monotonic() - started,
        phase_seconds=dict(
            calculate_write=calculation_write_seconds,
            generation=generation_seconds,
            write=calculation_write_seconds - generation_seconds,
            verify=verification_seconds,
            hash=time.monotonic() - hash_started,
        ),
    )
    manifest["partition_identity"] = digest(meta)
    atomic_json(directory / "manifest.json", manifest)
    return manifest


def verify_complete(directory, expected_metadata=None, check=lambda: None):
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    meta = manifest["metadata"]
    if (
        meta.get("layout_version") != product_schema.LAYOUT_VERSION
        or meta.get("feature_contract") != product_schema.FEATURE_CONTRACT
    ):
        raise ValueError("unsupported compact contract")
    if digest(meta) != manifest["partition_identity"]:
        raise ValueError("partition identity mismatch")
    if expected_metadata and any(
        meta.get(k) != v for k, v in expected_metadata.items()
    ):
        raise ValueError("completion dependencies mismatch")
    for name in ("features", "support"):
        obj = manifest["objects"][name]
        if obj["file"] != name + ".parquet":
            raise ValueError("unexpected object path")
        actual = file_identity(directory / obj["file"], check)
        if any(actual[k] != obj[k] for k in actual):
            raise ValueError("persisted object identity mismatch")
    evidence = validation_evidence(directory, meta, check)
    if evidence != manifest["validation"]:
        raise ValueError("completion validation evidence mismatch")
    return manifest


def legacy_calculation_identity():
    """The exact corrected base/extension reference accepted for reuse."""
    from tape_data_product.features import snapshot_feature_pipeline as F

    return dict(
        version="corrected_snapshot_extension_reference_v1",
        midpoint="exact_binary64_integer_nanoseconds_v1",
        snapshot=F.identity(),
        extension_files={
            name: file_identity(Path(__file__).parent / name)["sha256"]
            for name in (
                "all_feature_month_core.py",
                "all_feature_month_moments.py",
                "all_feature_month_schema.py",
            )
        },
    )


def completed_rows(directory, *, allow_partial=False, check=lambda: None):
    """Public local reader: completion marker required, then bounded public rows."""
    manifest = verify_complete(directory, check=check)
    if manifest["metadata"]["expected_rows"] != 57600 and not allow_partial:
        raise ValueError("partial partition requires explicit allow_partial")
    yield from joined_rows(directory, manifest["metadata"], check)
