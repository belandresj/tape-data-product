"""Exact two-minute range/activity screen, with disk-backed denominators.

The first endpoint is the end of the second *consecutive* bar. It is the earliest
bar-close discovery time, not a claim about historical vendor delivery latency.
"""

from pathlib import Path
import hashlib
import json
import math
import sqlite3
from .common import bounds, records, dump, digest, MINUTE_NS, validate_symbol

PARAMETERS = {
    "version": "two_minute_range_activity_v1",
    "range_bps": 700,
    "total_transactions": 1600,
    "each_minute_transactions": 100,
    "instrument_types": ["CS", "ADRC"],
    "dollar_volume_screen": False,
}


def screen(
    reference, minutes, session_date, output, *, synthetic=False, minute_receipts=None
):
    """O(U log U) disk sort, O(U) disk, 8 MiB SQLite cache + one bounded batch."""
    from tape_data_product.stages import write_stage, verify_stage

    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    start, end = bounds(session_date)
    reference_stage_identity = None
    reference_receipt_path = Path(reference).parent / "stage.json"
    if reference_receipt_path.exists():
        reference_receipt = verify_stage(reference_receipt_path)
        if (
            reference_receipt["stage"] != "acquire.reference"
            or reference_receipt["parameters"].get("session_date") != session_date
            or not reference_receipt["validation"].get("complete")
            or reference_receipt["synthetic"] != synthetic
            or set(reference_receipt["outputs"]) != {"reference.jsonl"}
            or reference_receipt["outputs"]["reference.jsonl"]["sha256"]
            != digest(reference)
        ):
            raise ValueError("Invalid reference acquisition coverage receipt")
        reference_stage_identity = reference_receipt["identity"]
    identity = hashlib.sha256(
        (
            digest(reference) + digest(minutes) + json.dumps(PARAMETERS, sort_keys=True)
        ).encode()
    ).hexdigest()
    con = sqlite3.connect(output / "screen.sqlite")
    con.execute("PRAGMA cache_size=-8192")
    con.execute("PRAGMA temp_store=FILE")
    con.execute(
        "CREATE TABLE reference(symbol TEXT PRIMARY KEY, kind TEXT) WITHOUT ROWID"
    )
    con.execute(
        "CREATE TABLE minute_sources(symbol TEXT PRIMARY KEY, identity TEXT) WITHOUT ROWID"
    )
    con.execute(
        "CREATE TABLE bars(symbol TEXT, ts INTEGER, tr REAL, op REAL, cl REAL, hi REAL, lo REAL, quality INTEGER, PRIMARY KEY(symbol,ts)) WITHOUT ROWID"
    )
    counts = dict(
        session_date=session_date,
        input_rows=0,
        eligible_rows=0,
        valid_session_rows=0,
        invalid_ohlc_rows=0,
        candidates=0,
        qualifying_windows=0,
    )
    try:
        for row in records(reference):
            if row.get("session_date") != session_date:
                raise ValueError("Reference date does not match requested session")
            kind = row.get("type", row.get("instrument_type"))
            if kind in PARAMETERS["instrument_types"]:
                symbol = validate_symbol(row.get("symbol", row.get("ticker")))
                con.execute("INSERT INTO reference VALUES (?,?)", (symbol, kind))
        counts["reference_symbols"] = con.execute(
            "SELECT count(*) FROM reference"
        ).fetchone()[0]
        combined_minutes = hashlib.sha256()
        for path in minute_receipts or ():
            receipt = verify_stage(path)
            parameters = receipt["parameters"]
            receipt_path = Path(path)
            receipt_root = (
                receipt_path if receipt_path.is_dir() else receipt_path.parent
            )
            if set(receipt["outputs"]) != {"minutes.jsonl"}:
                raise ValueError("Unexpected minute acquisition outputs")
            with (receipt_root / "minutes.jsonl").open("rb") as minute_source:
                for block in iter(lambda: minute_source.read(1024 * 1024), b""):
                    combined_minutes.update(block)
            if (
                receipt["stage"] != "acquire.minutes"
                or parameters["session_date"] != session_date
                or not receipt["validation"].get("complete")
            ):
                raise ValueError("Invalid minute acquisition coverage receipt")
            if receipt["synthetic"] != synthetic:
                raise ValueError("Minute receipt synthetic status mismatch")
            con.execute(
                "INSERT INTO minute_sources VALUES (?,?)",
                (parameters["symbol"], receipt["identity"]),
            )
        if minute_receipts and combined_minutes.hexdigest() != digest(minutes):
            raise ValueError(
                "Minute file differs from concatenated verified receipt outputs"
            )
        counts["verified_minute_sources"] = con.execute(
            "SELECT count(*) FROM minute_sources JOIN reference USING(symbol)"
        ).fetchone()[0]
        counts["missing_minute_sources"] = (
            counts["reference_symbols"] - counts["verified_minute_sources"]
        )
        counts["reference_coverage_verified"] = reference_stage_identity is not None
        counts["population_coverage_complete"] = (
            counts["reference_coverage_verified"]
            and counts["missing_minute_sources"] == 0
        )
        counts["coverage_basis"] = (
            "terminal_acquisition_receipts"
            if minute_receipts
            else "unverified_explicit_minute_file"
        )
        for row in records(minutes):
            counts["input_rows"] += 1
            symbol = row.get("symbol", row.get("ticker"))
            if not con.execute(
                "SELECT 1 FROM reference WHERE symbol=?", (symbol,)
            ).fetchone():
                continue
            counts["eligible_rows"] += 1
            try:
                ts = row["window_start"]
                tr, vol, op, cl, hi, lo = [
                    float(row.get(k, float("nan")))
                    for k in ("transactions", "volume", "open", "close", "high", "low")
                ]
            except (TypeError, ValueError):
                continue
            if not isinstance(ts, int) or not all(
                math.isfinite(v) for v in (tr, vol, cl)
            ):
                continue
            if not (
                start <= ts < end
                and ts % MINUTE_NS == 0
                and tr >= 0
                and vol >= 0
                and cl > 0
                and math.isfinite(vol * cl)
            ):
                continue
            quality = (
                all(math.isfinite(v) and v > 0 for v in (op, cl, hi, lo))
                and hi >= max(op, cl, lo)
                and lo <= min(op, cl, hi)
            )
            con.execute(
                "INSERT INTO bars VALUES (?,?,?,?,?,?,?,?)",
                (symbol, ts, tr, op, cl, hi, lo, int(quality)),
            )
            counts["valid_session_rows"] += 1
            counts["invalid_ohlc_rows"] += int(not quality)
        con.commit()
        counts["eligible_source_symbols"] = con.execute(
            "SELECT count(DISTINCT symbol) FROM bars"
        ).fetchone()[0]
        previous = None
        active = None
        selected = False
        with (output / "selection.jsonl").open("x") as sink:
            for symbol, kind, ts, tr, op, cl, hi, lo, quality in con.execute(
                "SELECT b.symbol,r.kind,ts,tr,op,cl,hi,lo,quality FROM bars b JOIN reference r USING(symbol) ORDER BY b.symbol,ts"
            ):
                if symbol != active:
                    previous, active, selected = None, symbol, False
                if not quality:
                    previous = None
                    continue
                current = (ts, tr, hi, lo)
                if previous is not None and ts == previous[0] + MINUTE_NS:
                    range_bps = 10000 * math.log(
                        max(hi, previous[2]) / min(lo, previous[3])
                    )
                    total, minimum = tr + previous[1], min(tr, previous[1])
                    if range_bps >= 700 and total >= 1600 and minimum >= 100:
                        counts["qualifying_windows"] += 1
                        if not selected:
                            endpoint = ts + MINUTE_NS
                            source_receipt = con.execute(
                                "SELECT identity FROM minute_sources WHERE symbol=?",
                                (symbol,),
                            ).fetchone()
                            discovery_verified = source_receipt is not None
                            result = dict(
                                session_date=session_date,
                                symbol=symbol,
                                instrument_type=kind,
                                discovery_endpoint_ns=endpoint,
                                discovery_verified=discovery_verified,
                                endpoint_ns=endpoint,
                                timing_basis="two_minute_bar_close",
                                verified=discovery_verified,
                                minute_source_stage_identity=(
                                    source_receipt[0] if source_receipt else None
                                ),
                                provenance_hash=identity,
                                window_start_ns=previous[0],
                                window_range_bps=range_bps,
                                window_transactions=total,
                                min_minute_transactions=minimum,
                                coverage_start_ns=start,
                                coverage_end_ns=end,
                                synthetic=synthetic,
                            )
                            sink.write(json.dumps(result, allow_nan=False) + "\n")
                            selected = True
                            counts["candidates"] += 1
                previous = current
    finally:
        con.close()
    dump(output / "denominators.json", counts)
    write_stage(
        output,
        "screen",
        inputs={
            "reference_sha256": digest(reference),
            "minutes_sha256": digest(minutes),
            "reference_stage_identity": reference_stage_identity,
        },
        parameters=PARAMETERS,
        outputs=["selection.jsonl", "denominators.json", "screen.sqlite"],
        validation={"complete": True, **counts},
        synthetic=synthetic,
    )
    return output / "selection.jsonl"
