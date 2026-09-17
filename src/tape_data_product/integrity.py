"""Bounded canonical JSON, file identities, and immutable member commits."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile

from .contracts.config import ContractError, canonical_json

MAX_JSON_BYTES = 1 << 20


def read_json(path: str | Path) -> dict:
    path = Path(path)
    if path.stat().st_size > MAX_JSON_BYTES:
        raise ContractError(f"JSON exceeds {MAX_JSON_BYTES} bytes: {path}")
    value = json.loads(path.read_text())
    if type(value) is not dict:
        raise ContractError("descriptor must be a JSON object")
    return value


def json_bytes(value: dict) -> bytes:
    data = canonical_json(value).encode() + b"\n"
    if len(data) > MAX_JSON_BYTES:
        raise ContractError("manifest exceeds 1 MiB")
    return data


def sha256_file(path: str | Path, chunk_size: int = 1 << 20) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with Path(path).open("rb", buffering=0) as handle:
        while block := handle.read(chunk_size):
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size


def safe_relative(value: str) -> Path:
    if type(value) is not str or not value or len(value) > 4096:
        raise ContractError("invalid relative path")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ContractError("absolute/traversal paths are forbidden")
    return path


def write_atomic_json(path: str | Path, value: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json_bytes(value)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def output_record(path: Path, *, rows: int, schema_sha256: str) -> dict:
    sha, size = sha256_file(path)
    return {"path": path.name, "sha256": sha, "bytes": size, "rows": rows,
            "schema_sha256": schema_sha256}


def verify_output(root: Path, record: dict) -> Path:
    if set(record) != {"path", "sha256", "bytes", "rows", "schema_sha256"}:
        raise ContractError("malformed output record")
    path = root / safe_relative(record["path"])
    sha, size = sha256_file(path)
    if sha != record["sha256"] or size != record["bytes"]:
        raise ContractError(f"output identity mismatch: {path}")
    return path
