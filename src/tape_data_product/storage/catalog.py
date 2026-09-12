"""Canonical pair inventories and immutable, streamed local/R2 transfers."""

from pathlib import Path
import json
import sqlite3
import hashlib
import shutil
import tempfile
import pyarrow.parquet as pq
from tape_data_product.acquisition.common import (
    bounds,
    digest,
    dump,
    records,
    validate_symbol,
    BATCH_ROWS,
)
from tape_data_product.acquisition.vendor import SCHEMAS


def object_key(day, symbol, stream):
    bounds(day)
    validate_symbol(symbol)
    if stream not in SCHEMAS:
        raise ValueError("Expected trades or quotes")
    return f"tq/session_date={day}/symbol={symbol}/{stream}.parquet"


def verify_pair(pair, *, allow_synthetic=False):
    """Verify bytes, schema, rows, SIP ordering and declared complete coverage."""
    pair = Path(pair)
    manifest = json.loads((pair / "pair.json").read_text())
    day, symbol = manifest["session_date"], manifest["symbol"]
    if manifest.get("version") != "canonical_tq_pair_v1" or set(
        manifest["streams"]
    ) != set(SCHEMAS):
        raise ValueError("Incomplete or unsupported pair")
    if manifest.get("synthetic") and not allow_synthetic:
        raise ValueError("Synthetic pair cannot be treated as canonical market data")
    for stream, receipt in manifest["streams"].items():
        path = pair / f"{stream}.parquet"
        if (
            receipt["session_date"] != day
            or receipt["symbol"] != symbol
            or receipt["stream"] != stream
        ):
            raise ValueError("Pair receipt membership mismatch")
        if receipt["sha256"] != digest(path) or receipt["bytes"] != path.stat().st_size:
            raise ValueError("Pair byte identity mismatch")
        if receipt.get("synthetic", False) != manifest.get("synthetic", False):
            raise ValueError("Pair synthetic status mismatch")
        expected = bounds(day, stream)
        actual = receipt["start_ns"], receipt["end_ns"]
        if actual != expected and not (
            manifest.get("synthetic")
            and expected[0] <= actual[0] < actual[1] <= expected[1]
        ):
            raise ValueError("Canonical coverage mismatch")
        if (
            not receipt.get("pagination_complete")
            or receipt.get("source_provider") != "massive"
            or receipt.get("source_method") != "rest"
        ):
            raise ValueError("Missing complete acquisition provenance")
        parquet = pq.ParquetFile(path)
        if (
            parquet.schema_arrow != SCHEMAS[stream]
            or parquet.metadata.num_rows != receipt["rows"]
        ):
            raise ValueError("Pair schema/row identity mismatch")
        previous = minimum = None
        count = 0
        for batch in parquet.iter_batches(
            columns=["sip_timestamp"], batch_size=BATCH_ROWS, use_threads=False
        ):
            for ts in batch.column(0).to_pylist():
                if (
                    ts is None
                    or not actual[0] <= ts < actual[1]
                    or (previous is not None and ts < previous)
                ):
                    raise ValueError("Invalid pair timestamp ordering/coverage")
                if minimum is None:
                    minimum = ts
                previous = ts
                count += 1
        if (minimum, previous) != (
            receipt["minimum_sip_ns"],
            receipt["maximum_sip_ns"],
        ) or count != receipt["rows"]:
            raise ValueError("Receipt timestamp/row evidence mismatch")
    return manifest


def build_inventory(selection, pair_root, output):
    """Reconcile every selected member; missing pairs cannot shrink membership."""
    from tape_data_product.stages import verify_stage

    selection, pair_root, output = Path(selection), Path(pair_root), Path(output)
    verify_stage(selection.parent)
    with tempfile.TemporaryDirectory(prefix="inventory-", dir=output.parent) as scratch:
        con = sqlite3.connect(Path(scratch) / "members.sqlite")
        con.execute("PRAGMA cache_size=-1024")
        con.execute(
            "CREATE TABLE members(day TEXT,symbol TEXT,PRIMARY KEY(day,symbol)) WITHOUT ROWID"
        )
        try:
            with output.open("x") as sink:
                for row in records(selection):
                    day, symbol = row["session_date"], row["symbol"]
                    con.execute("INSERT INTO members VALUES (?,?)", (day, symbol))
                    pair = pair_root / Path(object_key(day, symbol, "trades")).parent
                    manifest = verify_pair(
                        pair, allow_synthetic=row.get("synthetic", False)
                    )
                    if manifest["selection_sha256"] != digest(selection):
                        raise ValueError("Pair selection identity mismatch")
                    result = {
                        **row,
                        "trades_path": str((pair / "trades.parquet").resolve()),
                        "quotes_path": str((pair / "quotes.parquet").resolve()),
                        "pair_manifest_path": str((pair / "pair.json").resolve()),
                        "pair_manifest_sha256": digest(pair / "pair.json"),
                    }
                    sink.write(json.dumps(result) + "\n")
        finally:
            con.close()
    return output


def metadata(receipt):
    return {
        "sha256": receipt["sha256"],
        "rows": str(receipt["rows"]),
        "bytes": str(receipt["bytes"]),
        "source-provider": receipt["source_provider"],
        "source-method": receipt["source_method"],
        "requested-start-ns": str(receipt["start_ns"]),
        "requested-end-ns": str(receipt["end_ns"]),
        "pagination-complete": "true",
        "session-date": receipt["session_date"],
        "symbol": receipt["symbol"],
        "minimum-sip-ns": str(receipt["minimum_sip_ns"]),
        "maximum-sip-ns": str(receipt["maximum_sip_ns"]),
    }


def _head_matches(head, receipt):
    if head["ContentLength"] != receipt["bytes"] or any(
        head.get("Metadata", {}).get(k) != v for k, v in metadata(receipt).items()
    ):
        raise ValueError("Remote object identity or coverage conflict")


def _error_code(exc):
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def _verify_remote_body(client, bucket, key, receipt):
    """HEAD provenance is necessary but never substitutes for actual body SHA."""
    body = client.get_object(Bucket=bucket, Key=key)["Body"]
    observed = hashlib.sha256()
    size = 0
    try:
        while chunk := body.read(min(1024 * 1024, receipt["bytes"] - size + 1)):
            size += len(chunk)
            if size > receipt["bytes"]:
                raise ValueError("Remote body exceeds expected byte length")
            observed.update(chunk)
    finally:
        body.close()
    if size != receipt["bytes"] or observed.hexdigest() != receipt["sha256"]:
        raise ValueError("Remote body byte identity mismatch")


def publish_pair(client, bucket, pair):
    """Explicit conditional PUT only; validate both streams before any mutation.

    Existing metadata/byte identities must agree. A concurrent writer is handled
    by IfNoneMatch, never an overwrite. Partial pair publication remains visible
    if a remote operation fails and may be retried after inspection.
    """
    manifest = verify_pair(pair)
    pair = Path(pair)
    statuses = {}
    # Check both existing objects before publishing either missing member.
    for stream, receipt in manifest["streams"].items():
        key = object_key(manifest["session_date"], manifest["symbol"], stream)
        try:
            head = client.head_object(Bucket=bucket, Key=key)
        except Exception as exc:
            if _error_code(exc) not in {"404", "NotFound", "NoSuchKey"}:
                raise
            statuses[stream] = "missing"
        else:
            _head_matches(head, receipt)
            _verify_remote_body(client, bucket, key, receipt)
            statuses[stream] = "existing_identical"
    for stream, receipt in manifest["streams"].items():
        key = object_key(manifest["session_date"], manifest["symbol"], stream)
        if statuses[stream] == "missing":
            try:
                with (pair / f"{stream}.parquet").open("rb") as body:
                    client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=body,
                        ContentLength=receipt["bytes"],
                        Metadata=metadata(receipt),
                        ContentType="application/vnd.apache.parquet",
                        IfNoneMatch="*",
                    )
                statuses[stream] = "uploaded_verified"
            except Exception as exc:
                if _error_code(exc) not in {"412", "PreconditionFailed"}:
                    raise
                statuses[stream] = "existing_identical"
        _head_matches(client.head_object(Bucket=bucket, Key=key), receipt)
        _verify_remote_body(client, bucket, key, receipt)
    return statuses


def stage_pair(client, bucket, manifest_path, output, *, max_bytes=512 * 1024 * 1024):
    """Stage only explicitly named pair, one 1 MiB buffer, exact body SHA audit."""
    manifest_path, output = Path(manifest_path), Path(output)
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("synthetic") or set(manifest.get("streams", {})) != set(SCHEMAS):
        raise ValueError("Expected a complete canonical pair manifest")
    if sum(r["bytes"] for r in manifest["streams"].values()) > max_bytes:
        raise ValueError("Requested pair exceeds explicit staging byte budget")
    ancestor = output.parent
    while not ancestor.exists():
        ancestor = ancestor.parent
    needed = sum(r["bytes"] for r in manifest["streams"].values())
    if shutil.disk_usage(ancestor).free - needed < 3 * 1024**3:
        raise ValueError("Staging would breach the 3 GiB free-disk reserve")
    output.mkdir(parents=True, exist_ok=False)
    for stream, receipt in manifest["streams"].items():
        key = object_key(manifest["session_date"], manifest["symbol"], stream)
        before = client.head_object(Bucket=bucket, Key=key)
        _head_matches(before, receipt)
        response = client.get_object(Bucket=bucket, Key=key)
        body, size = response["Body"], 0
        try:
            with (output / f"{stream}.parquet").open("xb") as sink:
                while chunk := body.read(min(1024 * 1024, receipt["bytes"] - size + 1)):
                    size += len(chunk)
                    if size > receipt["bytes"]:
                        raise ValueError("Remote body exceeds expected byte length")
                    sink.write(chunk)
        finally:
            body.close()
        if (
            size != receipt["bytes"]
            or digest(output / f"{stream}.parquet") != receipt["sha256"]
        ):
            raise ValueError("Remote body byte identity mismatch")
        _head_matches(client.head_object(Bucket=bucket, Key=key), receipt)
    dump(output / "pair.json", manifest)
    verify_pair(output)
    return output
