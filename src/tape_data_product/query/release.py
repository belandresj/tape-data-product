"""Immutable local releases with indexed expected-member reconciliation.

O(C log C) time, O(C) disk, bounded resident SQLite cache and one member.
A missing or duplicate member cannot silently shrink a completed release.
"""

from pathlib import Path
import json
import sqlite3
import os
from tape_data_product.features import compact_product as product
from tape_data_product.stages import write_stage, verify_stage

VERSION = "local_compact_release_v1"


def _records(path):
    with Path(path).open() as stream:
        for line in stream:
            if len(line) > 1024 * 1024:
                raise ValueError("Membership record exceeds 1 MiB")
            if line.strip():
                yield json.loads(line)


def _database(path):
    db = sqlite3.connect(path)
    db.execute("PRAGMA cache_size=-8192")
    db.execute("PRAGMA temp_store=FILE")
    db.execute("PRAGMA mmap_size=0")
    return db


def build_release(expected_members, partitions, output, *, allow_partial=False):
    root = Path(output)
    root.mkdir(parents=True, exist_ok=False)
    database = root / "membership.sqlite"
    synthetic = False
    db = _database(database)
    try:
        db.execute(
            "CREATE TABLE members (session_date TEXT, symbol TEXT, record TEXT, PRIMARY KEY(session_date,symbol))"
        )
        for row in _records(expected_members):
            try:
                db.execute(
                    "INSERT INTO members VALUES (?,?,NULL)",
                    (row["session_date"], row["symbol"]),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("Duplicate expected member") from exc
        for row in _records(partitions):
            path = Path(row["path"])
            if not path.is_absolute():
                path = Path(partitions).resolve().parent / path
            manifest = product.verify_complete(path)
            meta = manifest["metadata"]
            synthetic = synthetic or bool(meta.get("synthetic"))
            if (row["session_date"], row["symbol"]) != (
                meta["session_date"],
                meta["symbol"],
            ):
                raise ValueError("Partition membership identity mismatch")
            if meta["expected_rows"] != 57600 and not allow_partial:
                raise ValueError("Partial partition requires explicit allow_partial")
            product.require_compatible(meta, [product.calculation_identity()])
            record = dict(
                session_date=meta["session_date"],
                symbol=meta["symbol"],
                path=os.path.relpath(path.resolve(), root.resolve()),
                partition_identity=manifest["partition_identity"],
                manifest_sha256=product.file_identity(path / "manifest.json")["sha256"],
                expected_rows=meta["expected_rows"],
            )
            changed = db.execute(
                "UPDATE members SET record=? WHERE session_date=? AND symbol=? AND record IS NULL",
                (
                    json.dumps(record, sort_keys=True),
                    row["session_date"],
                    row["symbol"],
                ),
            ).rowcount
            if changed != 1:
                raise ValueError("Unexpected or duplicate partition member")
        count = db.execute("SELECT COUNT(*) FROM members").fetchone()[0]
        missing = db.execute(
            "SELECT COUNT(*) FROM members WHERE record IS NULL"
        ).fetchone()[0]
        if missing or not count:
            raise ValueError(
                f"Incomplete release: {missing} missing of {count} expected members"
            )
        db.commit()
        with (root / "members.jsonl").open("x") as stream:
            for (record,) in db.execute(
                "SELECT record FROM members ORDER BY session_date,symbol"
            ):
                stream.write(record + "\n")
    finally:
        db.close()
    database.unlink()
    manifest = dict(
        version=VERSION,
        member_count=count,
        missing_members=0,
        allow_partial=bool(allow_partial),
        synthetic=synthetic,
        feature_contract=product.S.FEATURE_CONTRACT,
        calculation=product.calculation_identity(),
        expected_identity=product.file_identity(expected_members),
        members=product.file_identity(root / "members.jsonl"),
    )
    manifest["release_identity"] = product.digest(manifest)
    product.atomic_json(root / "manifest.json", manifest)
    write_stage(
        root,
        "release",
        {"expected_members": manifest["expected_identity"]},
        {"allow_partial": allow_partial},
        ["manifest.json", "members.jsonl"],
        {"expected_members": count, "accepted_members": count, "missing_members": 0},
        synthetic=synthetic,
    )
    return manifest


def iter_members(path):
    root = Path(path)
    for record in _records(root / "members.jsonl"):
        yield record | {"path": str((root / record["path"]).resolve())}


def verify_release(path, expected_release_hash=None):
    root = Path(path)
    verify_stage(root)
    manifest = json.loads((root / "manifest.json").read_text())
    identity = manifest.pop("release_identity")
    if product.digest(manifest) != identity:
        raise ValueError("Release identity mismatch")
    manifest["release_identity"] = identity
    if expected_release_hash is not None and identity != expected_release_hash:
        raise ValueError("Historical expected release identity mismatch")
    if (
        manifest["version"] != VERSION
        or manifest["feature_contract"] != product.S.FEATURE_CONTRACT
    ):
        raise ValueError("Incompatible release contract")
    if product.file_identity(root / "members.jsonl") != manifest["members"]:
        raise ValueError("Release membership corruption")
    count = 0
    previous = None
    for member in iter_members(root):
        key = member["session_date"], member["symbol"]
        if previous is not None and key <= previous:
            raise ValueError("Duplicate or unordered release member")
        previous = key
        partition = product.verify_complete(member["path"])
        meta = partition["metadata"]
        if meta["calculation"] != manifest["calculation"]:
            raise ValueError("Incompatible release calculation")
        if (meta["session_date"], meta["symbol"]) != key or meta[
            "expected_rows"
        ] != member["expected_rows"]:
            raise ValueError("Release member metadata mismatch")
        if meta["expected_rows"] != 57600 and not manifest["allow_partial"]:
            raise ValueError("Incomplete session in full release")
        if (
            partition["partition_identity"] != member["partition_identity"]
            or product.file_identity(Path(member["path"]) / "manifest.json")["sha256"]
            != member["manifest_sha256"]
        ):
            raise ValueError("Release partition identity mismatch")
        count += 1
    if count != manifest["member_count"] or manifest["missing_members"] != 0:
        raise ValueError("Release completeness mismatch")
    return manifest
