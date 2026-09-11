#!/usr/bin/env python3
"""Acquire auditable historical Nasdaq Trader halt records from the RSS feed.

Nasdaq documents the historical query as::

    https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts&haltdate=MMDDYYYY

Adding ``resumedate`` returns the union of records halted or resumed on the
specified date.  The feed is date-keyed and has no pagination fields; each XML
response declares its row count in ``ndaq:numItems``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


BASE_URL = "https://www.nasdaqtrader.com/rss.aspx"
NDAQ_NAMESPACE = "http://www.nasdaqtrader.com/"
LULD_REASON_CODES = frozenset({"M", "LUDP", "LUDS"})
SCRIPT_VERSION = "1.0.0"
USER_AGENT = "tape-characterization-nasdaq-halt-research/1.0"

OUTPUT_COLUMNS = (
    "provider_event_id",
    "halt_date",
    "symbol",
    "issue_name",
    "market",
    "reason_code",
    "pause_threshold_price",
    "halt_time",
    "resumption_date",
    "quote_resume_time",
    "trade_resume_time",
)


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def atomic_bytes(payload: bytes, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    atomic_bytes(
        (json.dumps(dict(payload), indent=2, sort_keys=True, allow_nan=False) + "\n").encode(),
        path,
    )


def _text(item: ET.Element, name: str) -> str:
    value = item.findtext(f"{{{NDAQ_NAMESPACE}}}{name}")
    return "" if value is None else " ".join(value.split())


def _event_id(row: Mapping[str, str]) -> str:
    identity = {
        name: row[name]
        for name in (
            "halt_date",
            "symbol",
            "market",
            "reason_code",
            "halt_time",
            "resumption_date",
            "quote_resume_time",
            "trade_resume_time",
        )
    }
    return hashlib.sha256(canonical_json(identity).encode()).hexdigest()


def parse_halt_rss(payload: bytes) -> tuple[list[dict[str, str]], int]:
    root = ET.fromstring(payload.decode("utf-8-sig"))
    channel = root.find("channel")
    if channel is None:
        raise ValueError("Nasdaq RSS response lacks channel")
    declared_text = channel.findtext(f"{{{NDAQ_NAMESPACE}}}numItems")
    if declared_text is None:
        raise ValueError("Nasdaq RSS response lacks ndaq:numItems")
    declared = int(declared_text)
    rows: list[dict[str, str]] = []
    for item in channel.findall("item"):
        row = {
            "provider_event_id": "",
            "halt_date": _text(item, "HaltDate"),
            "symbol": _text(item, "IssueSymbol").upper(),
            "issue_name": _text(item, "IssueName"),
            "market": _text(item, "Mkt").upper(),
            "reason_code": _text(item, "ReasonCode").upper(),
            "pause_threshold_price": _text(item, "PauseThresholdPrice"),
            "halt_time": _text(item, "HaltTime").replace(" ", ""),
            "resumption_date": _text(item, "ResumptionDate"),
            "quote_resume_time": _text(item, "ResumptionQuoteTime").replace(" ", ""),
            "trade_resume_time": _text(item, "ResumptionTradeTime").replace(" ", ""),
        }
        if not row["halt_date"] or not row["symbol"] or not row["reason_code"]:
            raise ValueError(f"Nasdaq RSS item lacks required fields: {row}")
        row["provider_event_id"] = _event_id(row)
        rows.append(row)
    if declared != len(rows):
        raise ValueError(f"Nasdaq RSS row-count mismatch: declared={declared}, parsed={len(rows)}")
    return rows, declared


def request_url(day: str) -> tuple[str, dict[str, str]]:
    compact = date.fromisoformat(day).strftime("%m%d%Y")
    parameters = {"feed": "tradehalts", "haltdate": compact, "resumedate": compact}
    return f"{BASE_URL}?{urllib.parse.urlencode(parameters)}", parameters


def _write_csv(rows: Iterable[Mapping[str, str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def acquire(
    *,
    dates: list[str],
    output_dir: Path,
    request_interval_seconds: float = 1.0,
    timeout_seconds: float = 30.0,
    max_attempts: int = 5,
    resume: bool = False,
) -> dict[str, Any]:
    output_dir = output_dir.expanduser().resolve()
    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        raise FileExistsError(f"completed acquisition is immutable: {manifest_path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    response_dir = output_dir / "responses"
    response_dir.mkdir(exist_ok=True)
    query_records: list[dict[str, Any]] = []
    all_rows: dict[str, dict[str, str]] = {}
    last_request_end = 0.0
    for day in sorted(set(dates)):
        xml_path = response_dir / f"{day}.xml"
        metadata_path = response_dir / f"{day}.metadata.json"
        url, parameters = request_url(day)
        if xml_path.exists() or metadata_path.exists():
            if not resume or not xml_path.is_file() or not metadata_path.is_file():
                raise FileExistsError(f"partial response exists; rerun with --resume: {xml_path}")
            payload = xml_path.read_bytes()
            metadata = json.loads(metadata_path.read_text())
            if metadata.get("response_sha256") != sha256_bytes(payload):
                raise ValueError(f"cached response hash mismatch: {xml_path}")
        else:
            for attempt in range(1, max_attempts + 1):
                delay = request_interval_seconds - (time.monotonic() - last_request_end)
                if delay > 0:
                    time.sleep(delay)
                request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/xml"})
                acquired_at = datetime.now(timezone.utc).isoformat()
                with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                    payload = response.read()
                    response_headers = dict(response.headers.items())
                    status = int(response.status)
                last_request_end = time.monotonic()
                try:
                    rows, declared = parse_halt_rss(payload)
                    break
                except (UnicodeDecodeError, ET.ParseError, ValueError) as exc:
                    failed_path = response_dir / f"{day}.attempt-{attempt}.raw"
                    failed_metadata_path = response_dir / f"{day}.attempt-{attempt}.metadata.json"
                    atomic_bytes(payload, failed_path)
                    atomic_json(
                        {
                            "provider": "nasdaq_trader_rss",
                            "acquired_at_utc": acquired_at,
                            "request_url": url,
                            "request_parameters": parameters,
                            "http_status": status,
                            "response_headers": response_headers,
                            "response_sha256": sha256_bytes(payload),
                            "response_bytes": len(payload),
                            "parse_error": f"{type(exc).__name__}: {exc}",
                            "retry_attempt": attempt,
                        },
                        failed_metadata_path,
                    )
                    if attempt == max_attempts:
                        raise
            atomic_bytes(payload, xml_path)
            metadata = {
                "provider": "nasdaq_trader_rss",
                "acquired_at_utc": acquired_at,
                "request_url": url,
                "request_parameters": parameters,
                "http_status": status,
                "response_headers": response_headers,
                "response_sha256": sha256_bytes(payload),
                "response_bytes": len(payload),
                "declared_num_items": declared,
                "parsed_num_items": len(rows),
            }
            atomic_json(metadata, metadata_path)
        rows, declared = parse_halt_rss(payload)
        if metadata.get("declared_num_items") != declared:
            raise ValueError(f"cached metadata row count mismatch: {metadata_path}")
        for row in rows:
            existing = all_rows.get(row["provider_event_id"])
            if existing is not None and existing != row:
                raise ValueError(f"provider event ID collision: {row['provider_event_id']}")
            all_rows[row["provider_event_id"]] = row
        query_records.append(
            {
                "session_date": day,
                "request_url": url,
                "response_path": str(xml_path),
                "response_sha256": sha256_bytes(payload),
                "response_bytes": len(payload),
                "declared_num_items": declared,
            }
        )

    ordered = sorted(all_rows.values(), key=lambda row: (row["halt_date"], row["symbol"], row["halt_time"], row["provider_event_id"]))
    luld = [row for row in ordered if row["reason_code"] in LULD_REASON_CODES]
    all_path = output_dir / "all_halts.csv"
    luld_path = output_dir / "luld_halts.csv"
    queries_path = output_dir / "queries.csv"
    _write_csv(ordered, all_path)
    _write_csv(luld, luld_path)
    query_columns = tuple(query_records[0]) if query_records else (
        "session_date", "request_url", "response_path", "response_sha256", "response_bytes", "declared_num_items"
    )
    temporary = queries_path.with_name(f".{queries_path.name}.{os.getpid()}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=query_columns)
        writer.writeheader()
        writer.writerows(query_records)
    os.replace(temporary, queries_path)
    manifest = {
        "artifact_type": "nasdaq_trader_historical_halts",
        "status": "frozen",
        "provider": "nasdaq_trader_rss",
        "script_version": SCRIPT_VERSION,
        "base_url": BASE_URL,
        "request_method": "one date-keyed RSS request per acquired session date; haltdate and resumedate are unioned by Nasdaq",
        "pagination": "none; each response declares ndaq:numItems",
        "documented_history": "Nasdaq Trading Halt Search states that the last year is displayed",
        "luld_reason_codes": sorted(LULD_REASON_CODES),
        "requested_dates": dates,
        "query_count": len(query_records),
        "all_halt_rows": len(ordered),
        "luld_halt_rows": len(luld),
        "content_sha256": {
            "all_halts.csv": sha256_file(all_path),
            "luld_halts.csv": sha256_file(luld_path),
            "queries.csv": sha256_file(queries_path),
        },
        "response_sha256": {record["session_date"]: record["response_sha256"] for record in query_records},
    }
    atomic_json(manifest, manifest_path)
    return manifest


def _manifest_dates(path: Path, start: str, end: str) -> list[str]:
    with path.expanduser().resolve().open(newline="", encoding="utf-8") as handle:
        rows = csv.DictReader(handle)
        if "session_date" not in (rows.fieldnames or []):
            raise ValueError("acquired manifest lacks session_date")
        dates = sorted({row["session_date"] for row in rows if start <= row["session_date"] <= end})
    if not dates:
        raise ValueError("acquired manifest date intersection is empty")
    return dates


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acquired-manifest", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--request-interval-seconds", type=float, default=1.0)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    dates = _manifest_dates(args.acquired_manifest, args.start, args.end)
    result = acquire(
        dates=dates,
        output_dir=args.output_dir,
        request_interval_seconds=args.request_interval_seconds,
        timeout_seconds=args.timeout_seconds,
        max_attempts=args.max_attempts,
        resume=args.resume,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
