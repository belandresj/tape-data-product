"""Supported bounded canonical T/Q to compact-feature calculation."""

from pathlib import Path
from collections import Counter
import json
import pyarrow.parquet as pq
from tape_data_product.features import direct_frozen_product, compact_product
from tape_data_product.stages import write_stage


def build_partition(
    quotes,
    trades,
    session_date,
    symbol,
    discovery,
    output,
    *,
    seconds=57600,
    halts=(),
    continuity_breaks_ns=(),
    synthetic=False,
    batch_size=4096,
    pair_manifest=None,
):
    """Stream O(T+Q+N F+N H) work with fixed H<=300 and bounded batches.

    Prefixes start at 04:00 ET, preserving initialization and causal maturity.
    Explicit synthetic prefixes never claim complete external-session coverage.
    """
    if not 1 <= seconds <= 57600:
        raise ValueError("seconds must be in 1..57600")
    discovery = dict(discovery)
    for field, expected in (("session_date", session_date), ("symbol", symbol)):
        if field in discovery and discovery[field] != expected:
            raise ValueError("Discovery member identity mismatch: " + field)
    receipt = receipt_path = receipt_identity = None
    source_admission = {}
    if not synthetic:
        from tape_data_product.storage.catalog import verify_pair

        if pair_manifest is None:
            raise ValueError(
                "Production features require an explicit canonical pair_manifest receipt"
            )
        receipt_path = Path(pair_manifest)
        receipt_identity = compact_product.file_identity(receipt_path)
        receipt = verify_pair(receipt_path.parent, allow_synthetic=False)
        if (
            receipt_path.name != "pair.json"
            or receipt["session_date"] != session_date
            or receipt["symbol"] != symbol
        ):
            raise ValueError("Canonical source receipt identity mismatch")
        if any(
            Path(value).resolve()
            != (receipt_path.parent / (name + ".parquet")).resolve()
            for name, value in [("quotes", quotes), ("trades", trades)]
        ):
            raise ValueError("Source paths differ from admitted canonical receipt")
        selected = receipt.get("selection_record")
        if not isinstance(selected, dict) or selected != discovery:
            raise ValueError("Discovery differs from admitted selection record")
        if (
            selected.get("session_date") != session_date
            or selected.get("symbol") != symbol
            or selected.get("verified") is not True
            or selected.get("discovery_verified") is not True
            or not selected.get("minute_source_stage_identity")
            or not selected.get("provenance_hash")
            or selected.get("synthetic") is not False
            or selected.get("timing_basis") != "two_minute_bar_close"
            or selected.get("endpoint_ns") != selected.get("discovery_endpoint_ns")
            or type(selected.get("endpoint_ns")) is not int
        ):
            raise ValueError(
                "Discovery lacks verified earliest-source timing provenance"
            )
        selected_identity = compact_product.digest(selected)
        if (
            selected_identity != receipt.get("selection_record_sha256")
            or not receipt.get("selection_sha256")
            or not receipt.get("selection_stage_identity")
        ):
            raise ValueError("Canonical selection provenance identity mismatch")
        source_admission = {
            "pair_manifest": receipt_identity,
            "selection_sha256": receipt["selection_sha256"],
            "selection_record_sha256": selected_identity,
            "selection_stage_identity": receipt["selection_stage_identity"],
        }
    discovery.setdefault("endpoint_ns", discovery.get("discovery_endpoint_ns"))
    discovery.setdefault("verified", discovery.get("discovery_verified", False))
    if discovery["verified"] and (
        discovery.get("endpoint_ns") is None or not discovery.get("provenance_hash")
    ):
        raise ValueError("Verified discovery requires endpoint and provenance hash")
    inputs = {}
    for name, path in [("quotes", quotes), ("trades", trades)]:
        inputs[name] = compact_product.file_identity(path) | {
            "rows": pq.ParquetFile(path).metadata.num_rows
        }
    if receipt is not None:
        for name, identity in inputs.items():
            admitted = receipt["streams"][name]
            if (
                identity["sha256"] != admitted["sha256"]
                or identity["size_bytes"] != admitted["bytes"]
                or identity["rows"] != admitted["rows"]
            ):
                raise ValueError("Source changed after canonical admission: " + name)
    metadata = dict(
        session_date=session_date,
        symbol=symbol,
        expected_rows=seconds,
        inputs=inputs,
        source_admission=source_admission,
        overlay={
            "halts": list(halts),
            "continuity_breaks_ns": list(continuity_breaks_ns),
        },
        discovery_verified=bool(discovery["verified"]),
        calculation=compact_product.calculation_identity(),
        synthetic=bool(synthetic),
        coverage="complete_session" if seconds == 57600 else "session_start_prefix",
    )
    stats = Counter()
    pairs = direct_frozen_product.product_pairs(
        quotes,
        trades,
        session_date,
        symbol,
        discovery,
        seconds=seconds,
        halts=halts,
        continuity_breaks_ns=continuity_breaks_ns,
        batch_size=batch_size,
        stats=stats,
    )

    def stable_source_pairs():
        """Recheck inputs after consumption, before the completion marker exists."""
        yield from pairs
        for name, path in (("quotes", quotes), ("trades", trades)):
            if compact_product.file_identity(path) != {
                key: inputs[name][key] for key in ("sha256", "size_bytes")
            }:
                raise ValueError("Source changed during feature calculation: " + name)
        if receipt_path is not None:
            if compact_product.file_identity(receipt_path) != receipt_identity:
                raise ValueError("Canonical pair receipt changed during calculation")
            if verify_pair(receipt_path.parent, allow_synthetic=False) != receipt:
                raise ValueError(
                    "Canonical source admission changed during calculation"
                )

    manifest = compact_product.write_partition(
        output,
        stable_source_pairs(),
        metadata,
        output_rows=min(4096, seconds),
        validation_mode="reconstruction",
    )
    write_stage(
        output,
        "features",
        inputs,
        {"seconds": seconds, "batch_size": batch_size, "discovery": discovery},
        ["features.parquet", "support.parquet", "manifest.json"],
        {
            "partition_identity": manifest["partition_identity"],
            "reconstruction": "passed",
            "rows": seconds,
            "input_rows": {k: inputs[k]["rows"] for k in inputs},
        },
        synthetic=synthetic,
    )
    return manifest


def verify_partition(path, *, reconstruction=False):
    return (
        compact_product.audit_complete(path)
        if reconstruction
        else compact_product.verify_complete(path)
    )


def build_inventory(
    inventory, output, *, seconds=57600, synthetic=False, batch_size=4096, contexts=None
):
    """Build acquisition inventory members sequentially and emit a release index.

    Input paths resolve relative to the inventory. Each member uses the exact
    selected record in its admitted pair receipt. Optional contexts are keyed
    JSONL records containing ``session_date``, ``symbol``, ``halts`` and
    ``continuity_breaks_ns``; omitted contexts explicitly mean empty overlays.
    No historical halt registry or continuity source is implicitly consulted.

    Membership and context reconciliation use an 8 MiB SQLite cache, O(C) disk,
    O(C log C) indexing work, and one bounded feature calculation at a time.
    JSONL records are capped at 1 MiB. Child identities stay in an on-disk ledger
    rather than an O(C) in-memory stage manifest. The public index appears only
    after every member has completed and immutable input checks have passed.
    """
    import datetime
    import os
    import re
    import sqlite3
    from tape_data_product.storage.catalog import verify_pair

    def records(path):
        with Path(path).open() as stream:
            while line := stream.readline(1024 * 1024 + 1):
                if len(line.encode()) > 1024 * 1024:
                    raise ValueError("Inventory/context record exceeds 1 MiB")
                if line.strip():
                    yield json.loads(line)

    def member_key(row):
        day, symbol = row["session_date"], row["symbol"]
        if datetime.date.fromisoformat(day).isoformat() != day:
            raise ValueError("Noncanonical inventory session date")
        if not isinstance(symbol, str) or not re.fullmatch(
            r"[A-Z0-9][A-Z0-9._-]{0,31}", symbol
        ):
            raise ValueError("Invalid inventory symbol")
        return day, symbol

    inventory, root = Path(inventory).resolve(), Path(output)
    source_identity = compact_product.file_identity(inventory)
    context_identity = compact_product.file_identity(contexts) if contexts else None
    root.mkdir(parents=True, exist_ok=False)
    database = root / "membership.sqlite"
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA cache_size=-8192")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute("PRAGMA mmap_size=0")
    connection.execute(
        "CREATE TABLE members (day TEXT,symbol TEXT,PRIMARY KEY(day,symbol)) WITHOUT ROWID"
    )
    connection.execute(
        "CREATE TABLE contexts (day TEXT,symbol TEXT,payload TEXT,used INTEGER DEFAULT 0,PRIMARY KEY(day,symbol)) WITHOUT ROWID"
    )
    count = 0
    try:
        if contexts:
            for row in records(contexts):
                day, symbol = member_key(row)
                if set(row) - {
                    "session_date",
                    "symbol",
                    "halts",
                    "continuity_breaks_ns",
                }:
                    raise ValueError("Unknown context fields")
                if not isinstance(row.get("halts", []), list) or not isinstance(
                    row.get("continuity_breaks_ns", []), list
                ):
                    raise ValueError(
                        "Context halts and continuity breaks must be arrays"
                    )
                try:
                    connection.execute(
                        "INSERT INTO contexts(day,symbol,payload) VALUES (?,?,?)",
                        (day, symbol, json.dumps(row)),
                    )
                except sqlite3.IntegrityError as error:
                    raise ValueError("Duplicate context member") from error
        with (
            (root / "partitions.jsonl.partial").open("x") as index,
            (root / "child-manifests.jsonl.partial").open("x") as ledger,
        ):
            for row in records(inventory):
                day, symbol = member_key(row)
                try:
                    connection.execute(
                        "INSERT INTO members VALUES (?,?)", (day, symbol)
                    )
                except sqlite3.IntegrityError as error:
                    raise ValueError("Duplicate inventory member") from error
                paths = {}
                for name in ("quotes_path", "trades_path", "pair_manifest_path"):
                    path = Path(row[name])
                    paths[name] = (
                        path if path.is_absolute() else inventory.parent / path
                    )
                receipt_path = paths["pair_manifest_path"]
                if receipt_path.name != "pair.json":
                    raise ValueError(
                        "Inventory must reference a canonical pair.json receipt"
                    )
                if (
                    compact_product.file_identity(receipt_path)["sha256"]
                    != row["pair_manifest_sha256"]
                ):
                    raise ValueError("Inventory pair receipt identity mismatch")
                receipt = verify_pair(receipt_path.parent, allow_synthetic=synthetic)
                selected = receipt.get("selection_record")
                if not isinstance(selected, dict) or member_key(selected) != (
                    day,
                    symbol,
                ):
                    raise ValueError("Inventory and selected member mismatch")
                if bool(receipt.get("synthetic")) != bool(synthetic):
                    raise ValueError(
                        "Inventory synthetic status differs from build mode"
                    )
                if compact_product.digest(selected) != receipt.get(
                    "selection_record_sha256"
                ):
                    raise ValueError("Selected record identity mismatch")
                if any(row.get(key) != value for key, value in selected.items()):
                    raise ValueError("Inventory discovery differs from selected record")
                for name in ("quotes", "trades"):
                    if (
                        paths[name + "_path"].resolve()
                        != (receipt_path.parent / (name + ".parquet")).resolve()
                    ):
                        raise ValueError(
                            "Inventory source path differs from canonical pair"
                        )
                saved = connection.execute(
                    "SELECT payload FROM contexts WHERE day=? AND symbol=?",
                    (day, symbol),
                ).fetchone()
                context = json.loads(saved[0]) if saved else {}
                if saved:
                    connection.execute(
                        "UPDATE contexts SET used=1 WHERE day=? AND symbol=?",
                        (day, symbol),
                    )
                relative = Path("session_date=" + day) / ("symbol=" + symbol)
                result = build_partition(
                    paths["quotes_path"],
                    paths["trades_path"],
                    day,
                    symbol,
                    selected,
                    root / relative,
                    seconds=seconds,
                    synthetic=synthetic,
                    batch_size=batch_size,
                    pair_manifest=receipt_path,
                    halts=context.get("halts", []),
                    continuity_breaks_ns=context.get("continuity_breaks_ns", []),
                )
                if (
                    compact_product.file_identity(receipt_path)["sha256"]
                    != row["pair_manifest_sha256"]
                ):
                    raise ValueError(
                        "Inventory pair receipt changed during calculation"
                    )
                index.write(
                    json.dumps(
                        {"session_date": day, "symbol": symbol, "path": str(relative)}
                    )
                    + "\n"
                )
                for filename in ("manifest.json", "stage.json"):
                    child = relative / filename
                    ledger.write(
                        json.dumps(
                            {
                                "path": str(child),
                                **compact_product.file_identity(root / child),
                            }
                        )
                        + "\n"
                    )
                if result["validation"]["rows_verified"] != seconds:
                    raise ValueError("Incomplete inventory member calculation")
                count += 1
            if not count:
                raise ValueError("Empty build inventory")
            if connection.execute(
                "SELECT COUNT(*) FROM contexts WHERE used=0"
            ).fetchone()[0]:
                raise ValueError("Context member absent from build inventory")
            if compact_product.file_identity(inventory) != source_identity:
                raise ValueError("Build inventory changed during calculation")
            if contexts and compact_product.file_identity(contexts) != context_identity:
                raise ValueError("Build contexts changed during calculation")
    finally:
        connection.close()
    database.unlink()
    for name in ("partitions.jsonl", "child-manifests.jsonl"):
        os.replace(root / (name + ".partial"), root / name)
    write_stage(
        root,
        "feature_inventory",
        {"inventory": source_identity, "contexts": context_identity},
        {
            "seconds": seconds,
            "batch_size": batch_size,
            "overlay_policy": "explicit keyed contexts; absent members use empty overlays",
        },
        ["partitions.jsonl", "child-manifests.jsonl"],
        {
            "complete": True,
            "expected_members": count,
            "completed_members": count,
            "rows": count * seconds,
        },
        synthetic=synthetic,
    )
    return root / "partitions.jsonl"
