"""Completed-only preview snapshots; no attempt reads or production mutations.

O(number of receipts) time/control memory, with each record capped at 256 KiB.
The receipt capture has a fixed cutoff; remote completion is checked on cache miss.
"""

from collections import Counter
from datetime import date
import hashlib
import json
from pathlib import Path
import re
import time
from tape_data_product.features import compact_product as P

MAX_RECORD = 256 * 1024


def read_json(path):
    with Path(path).open("rb") as f:
        data = f.read(MAX_RECORD + 1)
    if len(data) > MAX_RECORD:
        raise ValueError("control record exceeds 256 KiB")
    return json.loads(data)


def records(path):
    with Path(path).open() as f:
        while line := f.readline(MAX_RECORD + 1):
            if len(line) > MAX_RECORD:
                raise ValueError("inventory record exceeds 256 KiB")
            yield json.loads(line)


def object_identity(obj, prefix, name):
    suffix = ".json" if name == "manifest" else ".parquet"
    if obj.get("object_key") != prefix + "/" + name + suffix:
        raise ValueError("publication object namespace mismatch")
    if not re.fullmatch("[0-9a-f]{64}", obj.get("sha256", "")):
        raise ValueError("missing object SHA-256")
    if type(obj.get("size_bytes")) is not int or obj["size_bytes"] <= 0:
        raise ValueError("invalid object byte length")
    if obj.get("rows") != (None if name == "manifest" else 57600):
        raise ValueError("invalid object row count")
    if obj.get("status") not in ("uploaded_verified", "skipped_verified"):
        raise ValueError("unverified immutable publication")
    return {k: obj[k] for k in ("object_key", "sha256", "size_bytes", "rows")}


def entry_from_record(record):
    r, member = record["receipt"], record["member"]
    if (
        r.get("state") != "complete"
        or type(r.get("rows_verified")) is not int
        or r["rows_verified"] != 57600
    ):
        raise ValueError("not a complete verified full partition")
    pid = r.get("partition_identity", "")
    if not re.fullmatch("[0-9a-f]{64}", pid):
        raise ValueError("invalid partition identity")
    day, symbol = member["session_date"], member["symbol"]
    if date.fromisoformat(day).isoformat() != day or not symbol or len(symbol) > 32:
        raise ValueError("invalid partition keys")
    prefix = f"derived/tape_data_product/{P.S.LAYOUT_VERSION}/partitions/{pid}"
    pub = r["publication"]
    if (
        pub.get("bucket") != "massive-equities"
        or pub.get("manifest_key") != prefix + "/manifest.json"
    ):
        raise ValueError("invalid completion locator")
    objects = {
        name: object_identity(pub["objects"][name], prefix, name)
        for name in ("features", "support", "manifest")
    }
    return dict(
        session_date=day,
        symbol=symbol,
        expected_rows=57600,
        partition_identity=pid,
        bucket=pub["bucket"],
        objects=objects,
        receipt_sha256=record["receipt_sha256"],
        receipt_path=record["receipt_path"],
    )


def freeze(
    output,
    *,
    run=None,
    capture=None,
    label="Completed-only convergence preview; not a six-month release",
):
    """Exclude malformed publications with an explicit exclusion ledger."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    cutoff = time.time()
    exclusions = []
    counts = Counter()
    entries = []
    seen = set()

    def source():
        if capture:
            yield from records(capture)
            return
        for path in sorted((Path(run) / "receipts").glob("*.json")):
            try:
                raw = path.read_bytes()
                if len(raw) > MAX_RECORD:
                    raise ValueError("oversized receipt")
                r = json.loads(raw)
                counts[r.get("state", "missing_state")] += 1
                if (
                    r.get("state") != "complete"
                    or r.get("finished_at", cutoff + 1) > cutoff
                ):
                    continue
                payload = read_json(Path(run) / "payloads" / path.name)
                yield dict(
                    receipt=r,
                    member=payload["member"],
                    receipt_path=str(path.resolve()),
                    receipt_sha256=hashlib.sha256(raw).hexdigest(),
                )
            except (ValueError, KeyError, TypeError, OSError) as exc:
                exclusions.append(dict(path=str(path), reason=str(exc)))

    for record in source():
        try:
            e = entry_from_record(record)
            key = (e["session_date"], e["symbol"])
            if key in seen:
                raise ValueError("duplicate completed symbol-day")
            seen.add(key)
            entries.append(e)
        except (ValueError, KeyError, TypeError) as exc:
            exclusions.append(dict(path=record.get("receipt_path"), reason=str(exc)))
    if not entries:
        raise ValueError("no valid completed publications")
    entries.sort(key=lambda e: (e["session_date"], e["symbol"]))
    with (output / "inventory.jsonl").open("x") as f:
        for e in entries:
            f.write(json.dumps(e, sort_keys=True) + "\n")
    calc = P.calculation_identity()
    dates = sorted({e["session_date"] for e in entries})
    symbols = sorted({e["symbol"] for e in entries})
    manifest = dict(
        schema="compact_preview_snapshot_v1",
        state="complete",
        label=label,
        scope="completed_symbol_day_snapshot",
        cutoff_unix=cutoff,
        source_capture_sha256=P.file_identity(capture)["sha256"] if capture else None,
        source_capture_path=str(Path(capture).resolve()) if capture else None,
        partitions=len(entries),
        full_partitions=len(entries),
        partial_partitions=0,
        rows=57600 * len(entries),
        dates=dates,
        date_min=dates[0],
        date_max=dates[-1],
        symbols=symbols,
        inventory_sha256=P.file_identity(output / "inventory.jsonl")["sha256"],
        provenance=dict(
            layout_version=P.S.LAYOUT_VERSION,
            feature_contract=P.S.FEATURE_CONTRACT,
            calculation=calc,
            calculation_identity=P.digest(calc),
        ),
        run=str(Path(run).resolve()) if run else None,
        observed_states=dict(counts),
        exclusions=exclusions,
    )
    P.atomic_json(output / "manifest.json", manifest)
    return manifest


def read(snapshot):
    snapshot = Path(snapshot)
    m = read_json(snapshot / "manifest.json")
    if m.get("schema") != "compact_preview_snapshot_v1" or m.get("state") != "complete":
        raise ValueError("unsupported/incomplete snapshot")
    if P.file_identity(snapshot / "inventory.jsonl")["sha256"] != m["inventory_sha256"]:
        raise ValueError("snapshot inventory hash mismatch")
    P.require_compatible(m["provenance"], [P.calculation_identity()])
    if m["provenance"]["calculation_identity"] != P.digest(
        m["provenance"]["calculation"]
    ):
        raise ValueError("calculation digest mismatch")
    entries = list(records(snapshot / "inventory.jsonl"))
    keys = {(e["session_date"], e["symbol"]) for e in entries}
    if (
        len(keys) != len(entries)
        or len(entries) != m["partitions"]
        or sum(e["expected_rows"] for e in entries) != m["rows"]
    ):
        raise ValueError("snapshot coverage mismatch")
    for e in entries:
        prefix = f"derived/tape_data_product/{P.S.LAYOUT_VERSION}/partitions/{e['partition_identity']}"
        for name, obj in e["objects"].items():
            object_identity(obj | dict(status="uploaded_verified"), prefix, name)
    return m, entries
