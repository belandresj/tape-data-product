"""Disk-backed immutable month selection. All incumbent SQLite access is read-only.

O(membership) disk, O(one CSV record + one bounded metadata object) memory.
Queue receipts locate objects; admission requires remote manifest/identity checks.
"""

from __future__ import annotations
import csv
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from tape_data_product.features.all_feature_month_runtime import (
    save,
    discover_incumbent,
)


def digest(value):
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for b in iter(lambda: f.read(1024**2), b""):
            h.update(b)
    return h.hexdigest()


def read(path):
    path = Path(path)
    if path.stat().st_size > 2 * 1024**2:
        raise ValueError("metadata exceeds 2 MiB bound")
    return json.loads(path.read_text())


def connect(path, readonly=False):
    db = sqlite3.connect(
        f"file:{Path(path).resolve()}?mode=ro" if readonly else str(path), uri=readonly
    )
    db.row_factory = sqlite3.Row
    if not readonly:
        db.execute("PRAGMA cache_size=-2048")
    return db


def freeze(universe, queue, output, month="2026-07"):
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError("invalid month")
    universe, queue, output = map(Path, (universe, queue, output))
    if output.exists():
        raise FileExistsError(output)
    output.mkdir(parents=True)
    plan = read(universe / "plan.json")
    plan_hash = sha(universe / "plan.json")
    db = connect(output / "selection.sqlite")
    db.execute(
        "CREATE TABLE members(day TEXT,symbol TEXT,discovery TEXT,entry TEXT,receipt TEXT,inventory_reason TEXT,PRIMARY KEY(day,symbol))"
    )
    db.execute("CREATE TABLE dates(day TEXT PRIMARY KEY,provenance TEXT,reason TEXT)")
    live = discover_incumbent(queue.parent)
    if live and Path(live["root"]).resolve() != queue.resolve():
        raise ValueError("specified queue is not current live supervisor")
    source = connect(queue / "queue.sqlite", True)
    source.execute("BEGIN")  # Consistent read snapshot, never copies a live db file.
    days = plan.get("dates") or plan.get("session_dates") or plan.get("sessions")
    if days is None:
        # Declared date directories are explicit provenance; absence of an
        # exchange calendar prevents a claim of complete acquisition membership.
        days = [p.name for p in (universe / "days").iterdir() if p.is_dir()]
        date_basis = (
            "available aggregate day directories; calendar completeness unverified"
        )
    else:
        date_basis = "aggregate run declared dates"
        days = [d["session_date"] if isinstance(d, dict) else d for d in days]
    count = 0
    try:
        for day in sorted(d for d in days if d.startswith(month + "-")):
            folder = universe / "days" / day
            reason = None
            provenance = {}
            try:
                meta = read(folder / "manifest.json")
                completion = read(folder / "complete.json")
                if completion["plan_sha256"] != plan_hash:
                    raise ValueError("aggregate plan identity mismatch")
                for name in ("manifest.json", "candidates.csv"):
                    if sha(folder / name) != completion["files"][name]:
                        raise ValueError("aggregate artifact hash mismatch")
                if meta.get("sample_only") or meta["session_date"] != day:
                    raise ValueError("sample or wrong aggregate date")
                if (
                    meta["outputs"]["candidates.csv"]
                    != completion["files"]["candidates.csv"]
                ):
                    raise ValueError("candidate identity mismatch")
                provenance = dict(
                    manifest_sha256=sha(folder / "manifest.json"),
                    candidate_sha256=sha(folder / "candidates.csv"),
                    aggregate_plan_sha256=plan_hash,
                    configuration_hash=meta["configuration_hash"],
                    source_identity=meta["source_identity"],
                    reference_sha256=meta["reference_sha256"],
                )
            except (OSError, ValueError, KeyError) as exc:
                reason = str(exc)
            db.execute(
                "INSERT INTO dates VALUES (?,?,?)",
                (day, json.dumps(provenance), reason),
            )
            if not (folder / "candidates.csv").exists():
                db.commit()
                continue
            with (folder / "candidates.csv").open(newline="") as f:
                for candidate in csv.DictReader(f):
                    why = reason
                    symbol = candidate["ticker"]
                    if not re.fullmatch(r"[A-Z0-9.\-]{1,20}", symbol):
                        raise ValueError("unsafe symbol")
                    discovery = dict(
                        verified=False,
                        endpoint_ns=None,
                        received_at_ns=None,
                        timing_basis="nominal_aggregate_completion; vendor received-at unavailable",
                        provenance_hash=digest(provenance),
                        provenance=provenance,
                    )
                    try:
                        endpoint = int(candidate["window_end_ns"])
                        if (
                            candidate["session_date"] != day
                            or candidate["configuration_hash"]
                            != provenance["configuration_hash"]
                        ):
                            raise ValueError("candidate provenance mismatch")
                        if endpoint != int(candidate["last_bar_start_ns"]) + 60 * 10**9:
                            raise ValueError("noncausal aggregate completion clock")
                        stamp = datetime.fromisoformat(
                            candidate["first_trigger_time_et"]
                        )
                        if (
                            stamp.utcoffset() is None
                            or int(stamp.timestamp()) * 10**9 != endpoint
                        ):
                            raise ValueError("discovery timestamp mismatch")
                        discovery.update(endpoint_ns=endpoint, verified=reason is None)
                    except (KeyError, ValueError) as exc:
                        why = why or str(exc)
                    job = source.execute(
                        "SELECT state,entry,result FROM jobs WHERE day=? AND symbol=?",
                        (day, symbol),
                    ).fetchone()
                    if job is None:
                        why = why or "missing raw/queue membership"
                    elif job["state"] != "complete":
                        why = why or "base not completed at selection freeze"
                    db.execute(
                        "INSERT INTO members VALUES (?,?,?,?,?,?)",
                        (
                            day,
                            symbol,
                            json.dumps(discovery),
                            job["entry"] if job else None,
                            job["result"] if job else None,
                            why,
                        ),
                    )
                    count += 1
            db.commit()
    finally:
        source.close()
        db.close()
    manifest = dict(
        version="tape_eda_selection_v1",
        month=month,
        members=count,
        date_basis=date_basis,
        aggregate_run=str(universe.resolve()),
        aggregate_plan_sha256=plan_hash,
        queue=str(queue.resolve()),
        incumbent_at_freeze=live,
        selection_sha256=sha(output / "selection.sqlite"),
        frozen_before_feature_values=True,
        remote_verification="required before admission",
    )
    manifest["selection_hash"] = digest(manifest)
    save(output / "selection_manifest.json", manifest)
    return manifest


def verify_selection(root):
    root = Path(root)
    meta = read(root / "selection_manifest.json")
    expected = meta.pop("selection_hash")
    if (
        digest(meta) != expected
        or sha(root / "selection.sqlite") != meta["selection_sha256"]
    ):
        raise ValueError("frozen selection identity changed")
    meta["selection_hash"] = expected
    return meta


def remote_entry(member, client, bucket):
    """No feature values read. Verify canonical pair, semantics, historical overlay."""
    from tape_data_product.features import tape_feature_store as STORE
    from tape_data_product.features.tape_snapshot_inventory import validate_metadata

    if member["inventory_reason"]:
        raise ValueError(member["inventory_reason"])
    entry = json.loads(member["entry"])
    receipt = json.loads(member["receipt"])
    source = entry["source"]
    if (source["session_date"], source["symbol"]) != (member["day"], member["symbol"]):
        raise ValueError("source membership mismatch")
    for stream in ("quotes", "trades"):
        obj = source[stream]
        head = STORE.S._head_or_none(client, bucket, obj["object_key"])
        if head is None:
            raise ValueError("missing raw " + stream)
        actual = STORE.S.object_identity_from_head(obj["object_key"], head)
        expected = STORE.S.ObjectIdentity(
            **{k: obj[k] for k in ("object_key", "sha256", "size_bytes", "rows")}
        )
        STORE.S.validate_identities(expected, actual, require_rows=True)
        validate_metadata(member["day"], stream, head.get("Metadata", {}))
        canonical = f"tq/session_date={member['day']}/symbol={member['symbol']}/{stream}.parquet"
        if obj["object_key"] != canonical:
            raise ValueError("noncanonical raw key")
    key = receipt["feature_prefix"]
    meta = STORE.read_manifest(client, bucket, key + "/manifest.json")
    if meta is None:
        raise ValueError("missing completed base manifest")
    if meta["contract"] != STORE.F.identity():
        raise ValueError(
            "base implementation requires new quote-event equivalence proof"
        )
    identity = meta["partition_identity"]
    if identity["source"] != STORE.source_identity(source):
        raise ValueError("base/raw source identity mismatch")
    if identity["feature"] != STORE.semantic_identity():
        raise ValueError("incompatible base semantic contract")
    if identity["halts"] != sorted(entry["halts"]) or not identity["halt_policy"]:
        raise ValueError("effective historical halt identity mismatch")
    if identity["seconds"] != 57600 or identity["quote_initialization_seconds"] != 300:
        raise ValueError("incomplete base coverage")
    if STORE.feature_prefix(identity) != key:
        raise ValueError("base partition hash mismatch")
    if (
        meta["counts"]["rows"] != 57600
        or meta["verification"]["rows_verified"] != 57600
        or meta["verification"]["sha256"] != meta["sha256"]
    ):
        raise ValueError("unverified base completion")
    STORE.verify_object(
        client,
        bucket,
        dict(
            object_key=key + "/features.parquet",
            sha256=meta["sha256"],
            size_bytes=meta["bytes"],
            rows=57600,
        ),
    )
    return dict(
        entry=entry,
        base=meta,
        base_key=key + "/features.parquet",
        discovery=json.loads(member["discovery"]),
    )
