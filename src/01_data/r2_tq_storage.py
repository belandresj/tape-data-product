#!/usr/bin/env python3
"""Verified R2 staging and immutable publication for symbol-day artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

import boto3
import pyarrow.parquet as pq
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import dotenv_values


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"
DEFAULT_SESSION_ROOT = (
    PROJECT_ROOT / "data/minute_broad_activity_rth_strict_v2_tq/sessions"
)
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SYMBOL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
STREAMS = ("trades", "quotes")
HASH_CHUNK_BYTES = 8 * 1024 * 1024
MULTIPART_THRESHOLD_BYTES = 64 * 1024 * 1024
MULTIPART_CHUNK_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class R2Settings:
    bucket: str
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    region: str = "auto"


@dataclass(frozen=True)
class ObjectIdentity:
    object_key: str
    size_bytes: int
    sha256: str
    rows: int | None


@dataclass(frozen=True)
class StagedSymbolDay:
    session_date: str
    symbol: str
    directory: Path
    trades: Path
    quotes: Path
    source_objects: tuple[ObjectIdentity, ...]


def _positive_workers(value: str) -> int:
    workers = int(value)
    if workers < 1:
        raise argparse.ArgumentTypeError("worker count must be positive")
    return workers


def _validate_date_symbol(session_date: str, symbol: str) -> tuple[str, str]:
    if not DATE_RE.fullmatch(session_date):
        raise ValueError(f"invalid session date: {session_date}")
    symbol = symbol.upper()
    if not SYMBOL_RE.fullmatch(symbol):
        raise ValueError(f"unsafe symbol: {symbol}")
    return session_date, symbol


def tq_object_key(session_date: str, symbol: str, stream: str) -> str:
    session_date, symbol = _validate_date_symbol(session_date, symbol)
    if stream not in STREAMS:
        raise ValueError(f"unknown T/Q stream: {stream}")
    return f"tq/session_date={session_date}/symbol={symbol}/{stream}.parquet"


def load_r2_settings(env_file: Path = DEFAULT_ENV_FILE) -> R2Settings:
    values = {
        key: (value or "").strip()
        for key, value in dotenv_values(env_file.expanduser()).items()
    }
    required = (
        "R2_BUCKET",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_ENDPOINT_URL",
    )
    missing = [key for key in required if not values.get(key)]
    if missing:
        raise ValueError(f"missing R2 configuration fields: {', '.join(missing)}")
    return R2Settings(
        bucket=values["R2_BUCKET"],
        endpoint_url=values["R2_ENDPOINT_URL"].rstrip("/"),
        access_key_id=values["R2_ACCESS_KEY_ID"],
        secret_access_key=values["R2_SECRET_ACCESS_KEY"],
        region=values.get("R2_REGION") or "auto",
    )


def build_client(settings: R2Settings) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key_id,
        aws_secret_access_key=settings.secret_access_key,
        region_name=settings.region,
        config=Config(
            signature_version="s3v4",
            connect_timeout=15,
            read_timeout=120,
            retries={"max_attempts": 8, "mode": "standard"},
        ),
    )


def transfer_config(workers: int) -> TransferConfig:
    if workers < 1:
        raise ValueError("transfer worker count must be positive")
    return TransferConfig(
        multipart_threshold=MULTIPART_THRESHOLD_BYTES,
        multipart_chunksize=MULTIPART_CHUNK_BYTES,
        max_concurrency=workers,
        use_threads=workers > 1,
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(HASH_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _head_or_none(client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return None
        raise


def object_identity_from_head(key: str, head: Mapping[str, Any]) -> ObjectIdentity:
    metadata = {str(k).lower(): str(v) for k, v in head.get("Metadata", {}).items()}
    digest = metadata.get("sha256", "").lower()
    if not SHA256_RE.fullmatch(digest):
        raise RuntimeError(f"R2 object lacks valid sha256 metadata: {key}")
    try:
        size_bytes = int(head["ContentLength"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"R2 object lacks valid byte length: {key}") from exc
    if size_bytes < 0:
        raise RuntimeError(f"R2 object has negative byte length: {key}")
    rows_value = metadata.get("rows")
    rows: int | None = None
    if rows_value is not None:
        try:
            rows = int(rows_value)
        except ValueError as exc:
            raise RuntimeError(f"R2 object has invalid row metadata: {key}") from exc
        if rows < 0:
            raise RuntimeError(f"R2 object has negative row metadata: {key}")
    return ObjectIdentity(key, size_bytes, digest, rows)


def remote_identity(client: Any, bucket: str, key: str) -> ObjectIdentity:
    head = _head_or_none(client, bucket, key)
    if head is None:
        raise FileNotFoundError(f"R2 object missing: {key}")
    return object_identity_from_head(key, head)


def local_parquet_identity(path: Path, object_key: str) -> ObjectIdentity:
    if not path.is_file():
        raise FileNotFoundError(path)
    return ObjectIdentity(
        object_key=object_key,
        size_bytes=path.stat().st_size,
        sha256=sha256_file(path),
        rows=int(pq.read_metadata(path).num_rows),
    )


def validate_identities(
    local: ObjectIdentity,
    remote: ObjectIdentity,
    *,
    require_rows: bool = True,
) -> None:
    failures: list[str] = []
    if local.size_bytes != remote.size_bytes:
        failures.append("byte length")
    if local.sha256 != remote.sha256:
        failures.append("sha256")
    if require_rows and (remote.rows is None or local.rows != remote.rows):
        failures.append("row count")
    if failures:
        raise RuntimeError(
            f"local/R2 identity mismatch ({', '.join(failures)}): "
            f"{local.object_key}"
        )


def download_verified_parquet(
    client: Any,
    bucket: str,
    key: str,
    destination: Path,
    *,
    transfer_workers: int = 2,
) -> ObjectIdentity:
    """Download one immutable Parquet object and publish it locally atomically."""

    remote = remote_identity(client, bucket, key)
    if remote.rows is None:
        raise RuntimeError(f"R2 Parquet object lacks row metadata: {key}")
    destination = destination.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(f".{destination.name}.{os.getpid()}.partial")
    if partial.exists():
        raise RuntimeError(f"stale staged partial requires inspection: {partial}")
    if destination.exists():
        validate_identities(local_parquet_identity(destination, key), remote)
        return remote
    try:
        client.download_file(
            bucket,
            key,
            str(partial),
            Config=transfer_config(transfer_workers),
        )
        validate_identities(local_parquet_identity(partial, key), remote)
        os.replace(partial, destination)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    return remote


def stage_symbol_day(
    client: Any,
    bucket: str,
    session_date: str,
    symbol: str,
    directory: Path,
    *,
    download_workers: int = 2,
    transfer_workers: int = 2,
) -> StagedSymbolDay:
    """Stage and verify one symbol-day pair into an explicit empty directory."""

    session_date, symbol = _validate_date_symbol(session_date, symbol)
    if download_workers < 1:
        raise ValueError("download worker count must be positive")
    directory = directory.expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=False)
    paths = {stream: directory / f"{stream}.parquet" for stream in STREAMS}
    identities: dict[str, ObjectIdentity] = {}
    try:
        with ThreadPoolExecutor(max_workers=min(download_workers, len(STREAMS))) as pool:
            futures = {
                pool.submit(
                    download_verified_parquet,
                    client,
                    bucket,
                    tq_object_key(session_date, symbol, stream),
                    paths[stream],
                    transfer_workers=transfer_workers,
                ): stream
                for stream in STREAMS
            }
            for future in as_completed(futures):
                stream = futures[future]
                identities[stream] = future.result()
    except BaseException:
        shutil.rmtree(directory, ignore_errors=True)
        raise
    return StagedSymbolDay(
        session_date=session_date,
        symbol=symbol,
        directory=directory,
        trades=paths["trades"],
        quotes=paths["quotes"],
        source_objects=tuple(identities[stream] for stream in STREAMS),
    )


@contextmanager
def staged_symbol_day(
    client: Any,
    bucket: str,
    session_date: str,
    symbol: str,
    scratch_root: Path,
    *,
    download_workers: int = 2,
    transfer_workers: int = 2,
) -> Iterator[StagedSymbolDay]:
    """Yield a verified symbol-day pair and always remove its private scratch."""

    scratch_root = scratch_root.expanduser().resolve()
    scratch_root.mkdir(parents=True, exist_ok=True)
    directory = Path(
        tempfile.mkdtemp(
            prefix=f"r2-tq-{session_date}-{symbol.upper()}-",
            dir=scratch_root,
        )
    )
    directory.rmdir()
    staged = stage_symbol_day(
        client,
        bucket,
        session_date,
        symbol,
        directory,
        download_workers=download_workers,
        transfer_workers=transfer_workers,
    )
    try:
        yield staged
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def publish_file_immutable(
    client: Any,
    bucket: str,
    path: Path,
    object_key: str,
    *,
    metadata: Mapping[str, str] | None = None,
    rows: int | None = None,
    content_type: str = "application/octet-stream",
    upload_workers: int = 2,
) -> dict[str, Any]:
    """Upload a content-identified artifact, or verify an identical prior object."""

    path = path.expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    local = ObjectIdentity(
        object_key=object_key,
        size_bytes=path.stat().st_size,
        sha256=sha256_file(path),
        rows=rows,
    )
    prior = _head_or_none(client, bucket, object_key)
    if prior is not None:
        validate_identities(
            local,
            object_identity_from_head(object_key, prior),
            require_rows=rows is not None,
        )
        return {"status": "skipped_verified", **asdict(local)}

    remote_metadata = {str(k): str(v) for k, v in (metadata or {}).items()}
    remote_metadata["sha256"] = local.sha256
    if rows is not None:
        remote_metadata["rows"] = str(rows)
    client.upload_file(
        str(path),
        bucket,
        object_key,
        ExtraArgs={"ContentType": content_type, "Metadata": remote_metadata},
        Config=transfer_config(upload_workers),
    )
    validate_identities(
        local,
        remote_identity(client, bucket, object_key),
        require_rows=rows is not None,
    )
    return {"status": "uploaded_verified", **asdict(local)}


def publish_parquet_immutable(
    client: Any,
    bucket: str,
    path: Path,
    object_key: str,
    *,
    metadata: Mapping[str, str] | None = None,
    upload_workers: int = 2,
) -> dict[str, Any]:
    rows = int(pq.read_metadata(path).num_rows)
    return publish_file_immutable(
        client,
        bucket,
        path,
        object_key,
        metadata=metadata,
        rows=rows,
        content_type="application/vnd.apache.parquet",
        upload_workers=upload_workers,
    )


def discover_local_tq(
    session_root: Path, dates: Sequence[str]
) -> dict[str, list[tuple[Path, str]]]:
    session_root = session_root.expanduser().resolve()
    discovered: dict[str, list[tuple[Path, str]]] = {}
    for session_date in dates:
        _validate_date_symbol(session_date, "SAFE")
        date_root = session_root / session_date
        if not date_root.is_dir() or date_root.is_symlink():
            raise FileNotFoundError(f"local date directory missing or unsafe: {date_root}")
        date_entries = sorted(date_root.iterdir())
        unexpected_date_entries = [path.name for path in date_entries if not path.is_dir()]
        if unexpected_date_entries:
            raise RuntimeError(
                f"unexpected local entries in {date_root}: {unexpected_date_entries}"
            )
        entries: list[tuple[Path, str]] = []
        symbol_dirs = date_entries
        if not symbol_dirs:
            raise ValueError(f"no local symbol directories: {date_root}")
        for symbol_dir in symbol_dirs:
            _, symbol = _validate_date_symbol(session_date, symbol_dir.name)
            if symbol_dir.is_symlink():
                raise RuntimeError(f"local symbol directory may not be a symlink: {symbol_dir}")
            unexpected = sorted(
                path.name
                for path in symbol_dir.iterdir()
                if path.name not in {f"{stream}.parquet" for stream in STREAMS}
            )
            if unexpected:
                raise RuntimeError(f"unexpected local T/Q files in {symbol_dir}: {unexpected}")
            for stream in STREAMS:
                path = symbol_dir / f"{stream}.parquet"
                if not path.is_file() or path.is_symlink():
                    raise FileNotFoundError(f"local T/Q artifact missing or unsafe: {path}")
                entries.append((path, tq_object_key(session_date, symbol, stream)))
        discovered[session_date] = entries
    return discovered


def audit_local_tq(
    client: Any,
    bucket: str,
    session_root: Path,
    dates: Sequence[str],
    *,
    workers: int = 2,
) -> dict[str, Any]:
    if workers < 1:
        raise ValueError("audit worker count must be positive")
    discovered = discover_local_tq(session_root, dates)
    items = [
        (session_date, path, key)
        for session_date, entries in discovered.items()
        for path, key in entries
    ]

    def verify(item: tuple[str, Path, str]) -> tuple[str, ObjectIdentity]:
        session_date, path, key = item
        local = local_parquet_identity(path, key)
        validate_identities(local, remote_identity(client, bucket, key))
        return session_date, local

    per_date = {
        session_date: {"objects": 0, "bytes": 0, "rows": 0}
        for session_date in discovered
    }
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(verify, item) for item in items]
        for future in as_completed(futures):
            session_date, identity = future.result()
            per_date[session_date]["objects"] += 1
            per_date[session_date]["bytes"] += identity.size_bytes
            per_date[session_date]["rows"] += int(identity.rows or 0)
    return {
        "status": "verified",
        "dates": per_date,
        "objects": sum(value["objects"] for value in per_date.values()),
        "bytes": sum(value["bytes"] for value in per_date.values()),
        "rows": sum(value["rows"] for value in per_date.values()),
    }


def delete_verified_local_dates(session_root: Path, dates: Sequence[str]) -> None:
    """Delete exact audited date roots; caller must run a successful audit first."""

    session_root = session_root.expanduser().resolve()
    for session_date in dates:
        _validate_date_symbol(session_date, "SAFE")
        date_root = session_root / session_date
        if date_root.parent != session_root or not date_root.is_dir() or date_root.is_symlink():
            raise RuntimeError(f"refusing unsafe local date deletion: {date_root}")
    for session_date in dates:
        shutil.rmtree(session_root / session_date)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    audit = subparsers.add_parser(
        "audit-local-tq",
        help="Compare every local T/Q Parquet with immutable R2 metadata.",
    )
    audit.add_argument("--dates", nargs="+", required=True)
    audit.add_argument("--session-root", type=Path, default=DEFAULT_SESSION_ROOT)
    audit.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    audit.add_argument("--workers", type=_positive_workers, default=2)
    audit.add_argument("--delete-local-after-verify", action="store_true")

    stage = subparsers.add_parser(
        "stage-symbol",
        help="Download one verified symbol-day pair into an empty directory.",
    )
    stage.add_argument("--date", required=True)
    stage.add_argument("--symbol", required=True)
    stage.add_argument("--output-dir", type=Path, required=True)
    stage.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    stage.add_argument("--download-workers", type=_positive_workers, default=2)
    stage.add_argument("--transfer-workers", type=_positive_workers, default=2)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_r2_settings(args.env_file)
    client = build_client(settings)
    client.head_bucket(Bucket=settings.bucket)
    if args.command == "audit-local-tq":
        report = audit_local_tq(
            client,
            settings.bucket,
            args.session_root,
            args.dates,
            workers=args.workers,
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        if args.delete_local_after_verify:
            delete_verified_local_dates(args.session_root, args.dates)
            print(
                "deleted_verified_local_dates="
                + ",".join(args.dates)
            )
        return 0
    if args.command == "stage-symbol":
        staged = stage_symbol_day(
            client,
            settings.bucket,
            args.date,
            args.symbol,
            args.output_dir,
            download_workers=args.download_workers,
            transfer_workers=args.transfer_workers,
        )
        print(json.dumps({
            "status": "staged_verified",
            "session_date": staged.session_date,
            "symbol": staged.symbol,
            "directory": str(staged.directory),
            "source_objects": [asdict(item) for item in staged.source_objects],
        }, indent=2, sort_keys=True))
        return 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
