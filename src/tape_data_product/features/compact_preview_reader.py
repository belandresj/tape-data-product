"""Projected compact preview, separate from acceptance reconstruction.

O(object bytes + N*F) time, O(B*F + Parquet row-group decode buffers + 1 MiB)
RAM; B defaults 4096, <=25000. One private feature object on disk at a time.
No support object requests and no feature calculation. Whole-object streaming
is deliberate: minimize request latency; only requested columns are decoded.
"""

import hashlib
import json
from pathlib import Path
import tempfile
import time
import numpy as np
import pyarrow.parquet as pq
from tape_data_product.features import compact_product as P
from tape_data_product.features import compact_product_schema as S
from tape_data_product.features import all_feature_month_core as C
from tape_data_product.query import report_release_inventory as REPORT


def columns_for(features):
    if (
        not features
        or len(set(features)) != len(features)
        or set(features) - set(S.FEATURES)
    ):
        raise ValueError("select unique compact numerical features")
    return [
        *S.KEYS,
        "post_discovery_eligible",
        *features,
        *(f + "_reason_mask" for f in features),
    ]


def stats_new():
    return dict(
        bytes_transferred=0,
        http_requests=0,
        head_requests=0,
        get_requests=0,
        whole_feature_bytes=0,
        projected_chunk_bytes=0,
        marker_bytes=0,
        decode_seconds=0.0,
        validation_seconds=0.0,
        transfer_seconds=0.0,
        scratch_peak_bytes=0,
        rows=0,
        mode="whole-feature-transfer/projected-decode",
    )


def get_response(client, bucket, obj, stats):
    """Full GET binds metadata and bytes directly; a separate HEAD adds no proof.

    The streamed whole-object SHA detects changed/truncated bodies, including
    an object changing after receipt freeze. No range read is being trusted.
    """
    stats["http_requests"] += 1
    stats["get_requests"] += 1
    response = client.get_object(Bucket=bucket, Key=obj["object_key"])
    meta = response.get("Metadata", {})
    if (
        not response.get("ETag")
        or response.get("ContentLength") != obj["size_bytes"]
        or meta.get("sha256") != obj["sha256"]
        or (obj["rows"] is not None and meta.get("rows") != str(obj["rows"]))
    ):
        response["Body"].close()
        raise ValueError("remote object response identity mismatch")
    return response


def validate_manifest(manifest, entry, accepted):
    compatibility = REPORT.manifest_compatibility(manifest, entry, accepted)
    # Downstream callers can retain this distinction through manifest_sink;
    # this reader never upgrades integrity evidence to reconstruction.
    manifest["report_compatibility"] = compatibility
    return manifest["metadata"]


def fetch_manifest(client, entry, accepted, stats):
    start = time.perf_counter()
    obj = entry["objects"]["manifest"]
    if obj["size_bytes"] > 256 * 1024:
        raise ValueError("completion exceeds bound")
    response = get_response(client, entry["bucket"], obj, stats)
    try:
        raw = response["Body"].read(obj["size_bytes"] + 1)
    finally:
        response["Body"].close()
    stats["bytes_transferred"] += len(raw)
    stats["marker_bytes"] += len(raw)
    if (
        len(raw) != obj["size_bytes"]
        or hashlib.sha256(raw).hexdigest() != obj["sha256"]
    ):
        raise ValueError("completion marker SHA/length mismatch")
    stats["transfer_seconds"] += time.perf_counter() - start
    start = time.perf_counter()
    manifest = json.loads(raw)
    validate_manifest(manifest, entry, accepted)
    stats["validation_seconds"] += time.perf_counter() - start
    return manifest


def validate_batch(batch, features, entry, meta, offset, include_diagnostics=False):
    """Return batch-only EDA arrays; finite excluded diagnostics become unavailable."""
    n = batch.num_rows
    arrays = {}
    for name in columns_for(features):
        field = S.FEATURE_SCHEMA.field(name)
        if batch.schema.field(name) != field:
            raise ValueError("projected schema type/nullability mismatch: " + name)
        if not field.nullable and batch[name].null_count:
            raise ValueError("null required field: " + name)
        arrays[name] = batch[name].to_numpy(zero_copy_only=False)
    positions = np.arange(offset, offset + n, dtype=np.int64)
    ends = C.session_start(entry["session_date"]) + (positions + 1) * C.NS
    for name, expected in [
        ("interval_end_ns", ends),
        ("session_date", entry["session_date"]),
        ("symbol", entry["symbol"]),
    ]:
        if not np.all(arrays[name] == expected):
            raise ValueError("timestamp grid/key mismatch: " + name)
    post = arrays["post_discovery_eligible"]
    endpoint = meta["discovery"]["first_discovery_endpoint_ns"]
    expected_post = (
        ends >= endpoint
        if meta["discovery_verified"] and endpoint is not None
        else np.zeros(n, bool)
    )
    if not np.array_equal(post, expected_post):
        raise ValueError("post-discovery mask mismatch")
    values = {}
    known = sum(S.REASONS.values())
    for f in features:
        mask, value = arrays[f + "_reason_mask"], arrays[f]
        if mask.dtype.kind not in "iu" or np.any(mask < 0) or np.any(mask & ~known):
            raise ValueError("noninteger/negative/unknown reason bits: " + f)
        eligible = (mask == 0) & post
        # Parquet nulls map to NaN, but stored NaNs/infinities are illegal even when excluded.
        if np.any(
            ~np.isfinite(value) & ~batch[f].is_null().to_numpy(zero_copy_only=False)
        ):
            raise ValueError("stored nonfinite value: " + f)
        if np.any(eligible & ~np.isfinite(value)) or np.any(value[eligible] < 0):
            raise ValueError("invalid eligible feature domain: " + f)
        if f.startswith("movement_participation_") and np.any(
            (value[eligible] < 0) | (value[eligible] > 1)
        ):
            raise ValueError("participation outside [0,1]")
        values[f] = np.where(eligible, value, np.nan)
    sessions = np.where(positions < 19800, 0, np.where(positions < 43200, 1, 2))
    if include_diagnostics:
        return values, post, sessions, {f: arrays[f + "_reason_mask"] for f in features}
    return values, post, sessions


def local_batches(
    path, entry, meta, features, stats, batch_size=4096, include_diagnostics=False
):
    if type(batch_size) is not int or not 1 <= batch_size <= 25000:
        raise ValueError("batch_size must be 1..25000")
    cols = columns_for(features)
    offset = 0
    with pq.ParquetFile(path) as pf:
        if pf.metadata.num_rows != entry["expected_rows"]:
            raise ValueError("Parquet row count mismatch")
        for name in cols:
            if pf.schema_arrow.field(name) != S.FEATURE_SCHEMA.field(name):
                raise ValueError("projected schema mismatch: " + name)
        for rg in range(pf.metadata.num_row_groups):
            group = pf.metadata.row_group(rg)
            # Reject unbounded row groups before decoding; production groups <=12288.
            if group.num_rows > 25000:
                raise ValueError("unbounded Parquet row group")
            for i in range(group.num_columns):
                chunk = group.column(i)
                if chunk.path_in_schema in cols:
                    stats["projected_chunk_bytes"] += chunk.total_compressed_size
                    if chunk.total_uncompressed_size > 16 * 1024**2:
                        raise ValueError("unbounded projected column chunk")
        it = pf.iter_batches(batch_size=batch_size, columns=cols, use_threads=False)
        while True:
            start = time.perf_counter()
            try:
                batch = next(it)
            except StopIteration:
                stats["decode_seconds"] += time.perf_counter() - start
                break
            stats["decode_seconds"] += time.perf_counter() - start
            start = time.perf_counter()
            result = validate_batch(
                batch, features, entry, meta, offset, include_diagnostics
            )
            stats["validation_seconds"] += time.perf_counter() - start
            offset += batch.num_rows
            stats["rows"] += batch.num_rows
            yield result
            del batch, result
    if offset != entry["expected_rows"]:
        raise ValueError("incomplete timestamp grid")


def batches(
    client,
    entry,
    accepted,
    features,
    scratch,
    stats,
    batch_size=4096,
    manifest_sink=None,
    include_diagnostics=False,
):
    cols = columns_for(features)
    manifest = fetch_manifest(client, entry, accepted, stats)
    if manifest_sink is not None:
        manifest_sink.update(manifest)
    obj = entry["objects"]["features"]
    scratch = Path(scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    if obj["size_bytes"] > 128 * 1024**2:
        raise ValueError("feature scratch exceeds 128 MiB bound")
    # The file is created by this reader. Failed reads retain it for diagnosis.
    with tempfile.NamedTemporaryFile(
        prefix="preview-feature-", suffix=".parquet", dir=scratch, delete=False
    ) as out:
        path = Path(out.name)
        start = time.perf_counter()
        response = get_response(client, entry["bucket"], obj, stats)
        sha = hashlib.sha256()
        size = 0
        try:
            while block := response["Body"].read(1024**2):
                size += len(block)
                stats["bytes_transferred"] += len(block)
                if size > obj["size_bytes"]:
                    raise ValueError("feature body exceeds declared length")
                sha.update(block)
                out.write(block)
        finally:
            response["Body"].close()
        stats["whole_feature_bytes"] += size
        stats["scratch_peak_bytes"] = max(stats["scratch_peak_bytes"], size)
        if size != obj["size_bytes"] or sha.hexdigest() != obj["sha256"]:
            raise ValueError("feature whole-object SHA/length mismatch")
        stats["transfer_seconds"] += time.perf_counter() - start
    yield from local_batches(
        path,
        entry,
        manifest["metadata"],
        features,
        stats,
        batch_size,
        **({"include_diagnostics": True} if include_diagnostics else {}),
    )
    path.unlink()  # only this reader-owned, successfully processed file
