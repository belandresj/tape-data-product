from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


from tape_data_product.storage import r2_tq_storage as storage


def parquet_bytes(path: Path, values: list[int]) -> bytes:
    pq.write_table(pa.table({"sip_timestamp": values}), path)
    return path.read_bytes()


class FakeClient:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = objects or {}
        self.metadata: dict[str, dict[str, str]] = {}
        self.upload_calls = 0
        self.download_calls = 0
        for key, payload in self.objects.items():
            self.metadata[key] = {
                "sha256": hashlib.sha256(payload).hexdigest(),
                "rows": "2",
            }

    def head_object(self, *, Bucket: str, Key: str) -> dict:
        del Bucket
        if Key not in self.objects:
            error = {
                "Error": {"Code": "404"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            }
            raise storage.ClientError(error, "HeadObject")
        return {
            "ContentLength": len(self.objects[Key]),
            "Metadata": self.metadata[Key],
        }

    def download_file(self, Bucket: str, Key: str, Filename: str, Config) -> None:
        del Bucket, Config
        self.download_calls += 1
        Path(Filename).write_bytes(self.objects[Key])

    def upload_file(
        self, Filename: str, Bucket: str, Key: str, ExtraArgs: dict, Config
    ) -> None:
        del Bucket, Config
        self.upload_calls += 1
        self.objects[Key] = Path(Filename).read_bytes()
        self.metadata[Key] = ExtraArgs["Metadata"]


def make_remote_pair(tmp_path: Path) -> FakeClient:
    source = tmp_path / "source.parquet"
    payload = parquet_bytes(source, [1, 2])
    objects = {
        storage.tq_object_key("2026-09-02", "NVDA", stream): payload
        for stream in storage.STREAMS
    }
    return FakeClient(objects)


def test_stage_symbol_day_downloads_and_verifies_complete_pair(tmp_path: Path) -> None:
    client = make_remote_pair(tmp_path)
    destination = tmp_path / "staged"
    result = storage.stage_symbol_day(
        client,
        "bucket",
        "2026-09-02",
        "nvda",
        destination,
        download_workers=8,
        transfer_workers=4,
    )
    assert result.trades.is_file()
    assert result.quotes.is_file()
    assert [item.rows for item in result.source_objects] == [2, 2]
    assert client.download_calls == 2


def test_staged_symbol_day_context_removes_private_scratch(tmp_path: Path) -> None:
    client = make_remote_pair(tmp_path)
    scratch = tmp_path / "scratch"
    with storage.staged_symbol_day(
        client, "bucket", "2026-09-02", "NVDA", scratch
    ) as result:
        directory = result.directory
        assert directory.is_dir()
    assert not directory.exists()


def test_stage_rejects_corrupt_download_and_cleans_directory(tmp_path: Path) -> None:
    client = make_remote_pair(tmp_path)
    key = storage.tq_object_key("2026-09-02", "NVDA", "trades")
    client.metadata[key]["sha256"] = "0" * 64
    destination = tmp_path / "staged"
    with pytest.raises(RuntimeError, match="identity mismatch"):
        storage.stage_symbol_day(
            client, "bucket", "2026-09-02", "NVDA", destination
        )
    assert not destination.exists()


def test_publish_is_immutable_verified_and_resumable(tmp_path: Path) -> None:
    path = tmp_path / "features.parquet"
    parquet_bytes(path, [10, 11])
    client = FakeClient()
    key = "features/model=v3/session_date=2026-09-02/symbol=NVDA/features.parquet"
    first = storage.publish_parquet_immutable(
        client, "bucket", path, key, upload_workers=6
    )
    second = storage.publish_parquet_immutable(client, "bucket", path, key)
    assert first["status"] == "uploaded_verified"
    assert second["status"] == "skipped_verified"
    assert client.upload_calls == 1

    client.metadata[key]["sha256"] = "f" * 64
    with pytest.raises(RuntimeError, match="identity mismatch"):
        storage.publish_parquet_immutable(client, "bucket", path, key)


def test_audit_then_delete_exact_local_date(tmp_path: Path) -> None:
    session_root = tmp_path / "sessions"
    symbol_root = session_root / "2026-09-02" / "NVDA"
    symbol_root.mkdir(parents=True)
    objects: dict[str, bytes] = {}
    for stream in storage.STREAMS:
        path = symbol_root / f"{stream}.parquet"
        payload = parquet_bytes(path, [1, 2])
        objects[storage.tq_object_key("2026-09-02", "NVDA", stream)] = payload
    report = storage.audit_local_tq(
        FakeClient(objects),
        "bucket",
        session_root,
        ["2026-09-02"],
        workers=5,
    )
    assert report["status"] == "verified"
    assert report["objects"] == 2

    storage.delete_verified_local_dates(session_root, ["2026-09-02"])
    assert not (session_root / "2026-09-02").exists()


def test_audit_refuses_unexpected_local_file(tmp_path: Path) -> None:
    symbol_root = tmp_path / "sessions" / "2026-09-02" / "NVDA"
    symbol_root.mkdir(parents=True)
    for stream in storage.STREAMS:
        parquet_bytes(symbol_root / f"{stream}.parquet", [1, 2])
    (symbol_root / "notes.txt").write_text("do not delete")
    with pytest.raises(RuntimeError, match="unexpected local T/Q files"):
        storage.discover_local_tq(tmp_path / "sessions", ["2026-09-02"])


def test_audit_refuses_unverified_file_at_date_root(tmp_path: Path) -> None:
    date_root = tmp_path / "sessions" / "2026-09-02"
    symbol_root = date_root / "NVDA"
    symbol_root.mkdir(parents=True)
    for stream in storage.STREAMS:
        parquet_bytes(symbol_root / f"{stream}.parquet", [1, 2])
    (date_root / "manifest.json").write_text("{}")
    with pytest.raises(RuntimeError, match="unexpected local entries"):
        storage.discover_local_tq(tmp_path / "sessions", ["2026-09-02"])
