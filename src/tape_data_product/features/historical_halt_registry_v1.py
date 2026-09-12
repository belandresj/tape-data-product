#!/usr/bin/env python3
"""Build and apply the Historical T/Q Trading-Interruption Registry V1.

Each subcommand is an explicit stage.  No stage invokes a later stage:

``detect -> enrich -> normalize-external -> validate -> freeze -> overlay``.

The detector is historical research preprocessing.  It uses SIP timestamps and
the canonical activity-trade semantics from ``build_market_state.py``; it is
not a real-time halt feed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as datetime_time, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
from tape_data_product.features import build_market_state as MARKET  # noqa: E402

DETECTION_METHOD_VERSION = "halt_interruption_detection_v1"
REGISTRY_VERSION = "halt_interruption_registry_v1"
SCRIPT_VERSION = "1.0.0"
NY_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")
NS = 1_000_000_000
SESSION_SECONDS = 16 * 60 * 60
TRADE_SCAN_COLUMNS = (
    "sip_timestamp",
    "participant_timestamp",
    "sequence_number",
    "conditions",
    "correction",
    "price",
    "size",
    "decimal_size",
    "exchange",
)
QUOTE_EVIDENCE_COLUMNS = tuple(MARKET.QUOTE_COLUMNS)


@dataclass(frozen=True)
class DetectionConfig:
    gap_floor_seconds: int = 240
    max_reporting_latency_ns: int = NS
    batch_size: int = 250_000
    pre_gap_grace_seconds: int = 60
    activity_window_seconds: int = 300
    activity_total_trades: int = 4_000
    activity_minute_trades: int = 600
    activity_total_dollars: float = 5_000_000.0
    activity_minute_dollars: float = 500_000.0
    post_window_seconds: int = 300
    duration_multiple_tolerance_seconds: float = 90.0
    start_only_match_tolerance_seconds: float = 120.0
    broad_rejected_fraction: float = 0.20
    broad_rejected_minimum: int = 2
    overlapping_gap_fraction: float = 0.50
    manual_review_target: int = 50
    manual_review_seed: str = "halt_interruption_detection_v1_manual_review"

    def jsonable(self) -> dict[str, Any]:
        return asdict(self)

    def digest(self) -> str:
        return canonical_hash(self.jsonable())


def _json_default(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(
        f"Object of type {value.__class__.__name__} is not JSON serializable"
    )


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=_json_default,
    )


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _tmp(path: Path) -> Path:
    return path.with_name(f".{path.name}.{os.getpid()}.tmp")


def atomic_json(value: Mapping[str, Any], path: Path, *, replace: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not replace:
        raise FileExistsError(path)
    temporary = _tmp(path)
    temporary.write_text(
        json.dumps(
            dict(value),
            indent=2,
            sort_keys=True,
            allow_nan=False,
            default=_json_default,
        )
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def atomic_text(value: str, path: Path, *, replace: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not replace:
        raise FileExistsError(path)
    temporary = _tmp(path)
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def atomic_csv(frame: pd.DataFrame, path: Path, *, replace: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not replace:
        raise FileExistsError(path)
    temporary = _tmp(path)
    frame.to_csv(temporary, index=False)
    os.replace(temporary, path)


def atomic_parquet(
    value: pa.Table | pd.DataFrame,
    path: Path,
    *,
    replace: bool = True,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not replace:
        raise FileExistsError(path)
    table = (
        pa.Table.from_pandas(value, preserve_index=False)
        if isinstance(value, pd.DataFrame)
        else value
    )
    temporary = _tmp(path)
    pq.write_table(table, temporary, compression="zstd", compression_level=3)
    os.replace(temporary, path)


def session_bounds_ns(session_date: str) -> tuple[int, int]:
    day = date.fromisoformat(session_date)
    start = datetime.combine(day, datetime_time(4, 0), NY_TZ)
    end = datetime.combine(day, datetime_time(20, 0), NY_TZ)
    return int(start.timestamp() * NS), int(end.timestamp() * NS)


def ceil_second_strictly_after(timestamp_ns: int) -> int:
    return ((int(timestamp_ns) // NS) + 1) * NS


def _ns(value: Any) -> int | None:
    if (
        value is None
        or value is pd.NaT
        or (isinstance(value, float) and math.isnan(value))
    ):
        return None
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize("UTC")
    else:
        parsed = parsed.tz_convert("UTC")
    return int(parsed.value)


def _utc(value: int | None) -> pd.Timestamp | pd.NaT:
    return pd.NaT if value is None else pd.Timestamp(value, unit="ns", tz="UTC")


def parquet_identity(path: Path) -> dict[str, Any]:
    path = path.expanduser().resolve()
    parquet = pq.ParquetFile(path)
    row_groups = []
    for index in range(parquet.metadata.num_row_groups):
        group = parquet.metadata.row_group(index)
        row_groups.append(
            {
                "rows": group.num_rows,
                "total_byte_size": group.total_byte_size,
                "columns": [
                    {
                        "path": group.column(j).path_in_schema,
                        "compressed_size": group.column(j).total_compressed_size,
                        "uncompressed_size": group.column(j).total_uncompressed_size,
                    }
                    for j in range(group.num_columns)
                ],
            }
        )
    metadata_fingerprint = canonical_hash(
        {
            "schema": str(parquet.schema_arrow),
            "rows": parquet.metadata.num_rows,
            "row_groups": row_groups,
        }
    )
    return {
        "path": str(path),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
        "rows": parquet.metadata.num_rows,
        "row_groups": parquet.metadata.num_row_groups,
        "parquet_metadata_sha256": metadata_fingerprint,
    }


def _read_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix in {".csv", ".gz"}:
        # Market symbols such as "NA" are valid identifiers, not missing values.
        return pd.read_csv(path, keep_default_na=False)
    if suffix in {".json", ".jsonl"}:
        try:
            return pd.read_json(path, lines=suffix == ".jsonl")
        except ValueError:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                for key in ("data", "results", "halts"):
                    if isinstance(payload.get(key), list):
                        payload = payload[key]
                        break
            return pd.DataFrame(payload)
    raise ValueError(f"unsupported table format: {path}")


def verify_acquisition_manifest(
    path: Path, *, allow_unverified: bool = False
) -> dict[str, Any]:
    path = path.expanduser().resolve()
    sidecar = path.with_name(path.name + ".manifest.json")
    frozen_manifest = path.parent / "manifest.json"
    if sidecar.is_file():
        payload = json.loads(sidecar.read_text())
        expected = payload.get("jobs_sha256") or payload.get("registry_sha256")
        if expected != sha256_file(path):
            raise ValueError(f"acquisition manifest sidecar hash mismatch: {sidecar}")
        return {
            "verification": "hash_sidecar",
            "manifest": str(sidecar),
            "manifest_sha256": sha256_file(sidecar),
        }
    if frozen_manifest.is_file():
        payload = json.loads(frozen_manifest.read_text())
        hashes = payload.get("content_sha256", {})
        expected = hashes.get(path.name) if isinstance(hashes, dict) else None
        if expected != sha256_file(path):
            raise ValueError(
                f"frozen acquisition registry hash mismatch: {frozen_manifest}"
            )
        return {
            "verification": "frozen_registry_manifest",
            "manifest": str(frozen_manifest),
            "manifest_sha256": sha256_file(frozen_manifest),
        }
    if not allow_unverified:
        raise ValueError(
            "acquisition manifest has no verifiable hash sidecar/frozen manifest; "
            "use --allow-unverified-manifest only for deliberate recovery or synthetic work"
        )
    return {
        "verification": "explicitly_unverified",
        "manifest": None,
        "manifest_sha256": None,
    }


def load_symbol_day_manifest(
    path: Path,
    *,
    tick_data_root: Path,
    start: str | None = None,
    end: str | None = None,
    allow_unverified: bool = False,
) -> pd.DataFrame:
    verify_acquisition_manifest(path, allow_unverified=allow_unverified)
    frame = _read_frame(path.expanduser().resolve()).copy()
    if "symbol" not in frame and "ticker" in frame:
        frame["symbol"] = frame["ticker"]
    required = {"universe_version", "session_date", "symbol", "instrument_type"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"symbol-day manifest missing columns: {missing}")
    frame["session_date"] = frame["session_date"].astype(str)
    frame["symbol"] = frame["symbol"].map(_canonical_symbol)
    if start is not None:
        frame = frame[frame.session_date >= start]
    if end is not None:
        frame = frame[frame.session_date <= end]
    if frame.empty:
        raise ValueError("symbol-day manifest selection is empty")
    if frame.duplicated(["session_date", "symbol"]).any():
        raise ValueError(
            "symbol-day manifest has duplicate (session_date, symbol) rows"
        )
    if frame.universe_version.nunique() != 1:
        raise ValueError("symbol-day manifest mixes universe versions")
    tick_data_root = tick_data_root.expanduser().resolve()
    frame["trade_path"] = [
        str(tick_data_root / "sessions" / day / symbol / "trades.parquet")
        for day, symbol in zip(frame.session_date, frame.symbol)
    ]
    frame["quote_path"] = [
        str(tick_data_root / "sessions" / day / symbol / "quotes.parquet")
        for day, symbol in zip(frame.session_date, frame.symbol)
    ]
    return frame.sort_values(["session_date", "symbol"], kind="mergesort").reset_index(
        drop=True
    )


def _condition_payload(value: Any) -> tuple[int, ...]:
    return tuple() if value is None else tuple(int(v) for v in value)


def _duplicate_payload(table: pa.Table, row: int) -> tuple[Any, ...]:
    return tuple(
        (
            _condition_payload(table[name][row].as_py())
            if name == "conditions"
            else table[name][row].as_py()
        )
        for name in TRADE_SCAN_COLUMNS
        if name not in {"sip_timestamp", "sequence_number"}
    )


def _range_bps(prices: np.ndarray) -> float | None:
    finite = prices[np.isfinite(prices) & (prices > 0.0)]
    if finite.size == 0:
        return None
    return float(10_000.0 * math.log(float(np.max(finite)) / float(np.min(finite))))


def _abs_return_bps(prices: np.ndarray) -> float | None:
    finite = prices[np.isfinite(prices) & (prices > 0.0)]
    if finite.size < 2:
        return None
    return float(abs(10_000.0 * math.log(float(finite[-1]) / float(finite[0]))))


def _window_values(
    times: np.ndarray, values: np.ndarray, start_ns: int, end_ns: int
) -> np.ndarray:
    left = int(np.searchsorted(times, start_ns, side="left"))
    right = int(np.searchsorted(times, end_ns, side="left"))
    return values[left:right]


def _prefix_interval(
    times: np.ndarray,
    prefix: np.ndarray,
    start_ns: int,
    end_ns: int,
) -> float:
    left = int(np.searchsorted(times, start_ns, side="left"))
    right = int(np.searchsorted(times, end_ns, side="left"))
    return float(prefix[right] - prefix[left])


def activity_predicate(
    counts: np.ndarray,
    dollars: np.ndarray,
    config: DetectionConfig,
) -> np.ndarray:
    """Return predicate values at session row ends 0..T1 inclusive.

    Element ``j`` evaluates ``[T0, T0+j)``'s final 300 seconds.  Values before
    the first complete window are false and are never treated as available by
    callers.
    """

    n = len(counts)
    if len(dollars) != n:
        raise ValueError("trade and dollar activity arrays differ in length")
    width = config.activity_window_seconds
    if width != 300:
        raise ValueError("V1 activity window must remain 300 seconds")
    count_prefix = np.r_[0, np.cumsum(counts, dtype=np.int64)]
    dollar_prefix = np.r_[0.0, np.cumsum(dollars, dtype=np.float64)]
    result = np.zeros(n + 1, dtype=np.bool_)
    endpoints = np.arange(width, n + 1, dtype=np.int64)
    left = endpoints - width
    qualified = (
        count_prefix[endpoints] - count_prefix[left] >= config.activity_total_trades
    ) & (
        dollar_prefix[endpoints] - dollar_prefix[left] >= config.activity_total_dollars
    )
    for minute in range(5):
        lo = left + 60 * minute
        hi = lo + 60
        qualified &= (
            count_prefix[hi] - count_prefix[lo] >= config.activity_minute_trades
        ) & (dollar_prefix[hi] - dollar_prefix[lo] >= config.activity_minute_dollars)
    result[endpoints] = qualified
    return result


def _pre_gap_qualified(
    predicate: np.ndarray, halt_start_index: int, config: DetectionConfig
) -> bool:
    right = min(max(halt_start_index, 0), len(predicate) - 1)
    left = max(config.activity_window_seconds, right - config.pre_gap_grace_seconds + 1)
    return bool(np.any(predicate[left : right + 1])) if right >= left else False


def _candidate_hash(
    session_date: str,
    symbol: str,
    last_pre_ns: int,
    first_post_ns: int,
    config_hash: str,
) -> str:
    return canonical_hash(
        {
            "detection_method_version": DETECTION_METHOD_VERSION,
            "candidate_config_hash": config_hash,
            "session_date": session_date,
            "symbol": symbol,
            "last_pre_gap_trade_sip": last_pre_ns,
            "first_post_gap_trade_sip": first_post_ns,
        }
    )


def scan_trade_symbol_day(
    *,
    session_date: str,
    symbol: str,
    instrument_type: str,
    universe_version: str,
    trade_path: Path,
    quote_path: Path,
    config: DetectionConfig,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Stream one trade Parquet and construct exact eligible-trade gaps."""

    trade_path = trade_path.expanduser().resolve()
    quote_path = quote_path.expanduser().resolve()
    if not trade_path.is_file():
        raise FileNotFoundError(trade_path)
    if not quote_path.is_file():
        raise FileNotFoundError(quote_path)
    parquet = pq.ParquetFile(trade_path, memory_map=True)
    quote_parquet = pq.ParquetFile(quote_path, memory_map=True)
    missing_quotes = sorted(
        set(QUOTE_EVIDENCE_COLUMNS) - set(quote_parquet.schema_arrow.names)
    )
    if missing_quotes:
        raise ValueError(
            f"{quote_path}: missing canonical quote columns: {missing_quotes}"
        )
    available = set(parquet.schema_arrow.names)
    mandatory = set(TRADE_SCAN_COLUMNS) - {"decimal_size"}
    missing = sorted(mandatory - available)
    if missing:
        raise ValueError(f"{trade_path}: missing trade columns: {missing}")
    columns = [name for name in TRADE_SCAN_COLUMNS if name in available]

    start_ns, end_ns = session_bounds_ns(session_date)
    raw_counts = np.zeros(SESSION_SECONDS, dtype=np.int64)
    eligible_counts = np.zeros(SESSION_SECONDS, dtype=np.int64)
    eligible_dollars = np.zeros(SESSION_SECONDS, dtype=np.float64)
    eligible_time_parts: list[np.ndarray] = []
    eligible_price_parts: list[np.ndarray] = []
    eligible_size_parts: list[np.ndarray] = []
    raw_time_parts: list[np.ndarray] = []
    raw_price_time_parts: list[np.ndarray] = []
    raw_price_parts: list[np.ndarray] = []
    raw_rows = 0
    in_session_raw_rows = 0
    previous_key: tuple[int, int] | None = None
    previous_payload: tuple[Any, ...] | None = None
    duplicate_key_count = 0
    exact_duplicate_count = 0

    for batch in parquet.iter_batches(
        batch_size=config.batch_size,
        columns=columns,
        use_threads=False,
    ):
        table = pa.Table.from_batches([batch])
        if "decimal_size" not in table.column_names:
            table = table.append_column(
                "decimal_size", pa.nulls(table.num_rows, pa.string())
            )
        table = table.select(list(TRADE_SCAN_COLUMNS))
        if table.num_rows == 0:
            continue
        for required in ("sip_timestamp", "participant_timestamp", "sequence_number"):
            if table[required].null_count:
                raise ValueError(f"{trade_path}: null {required}")
        sip = np.asarray(table["sip_timestamp"].to_numpy(), dtype=np.int64)
        seq = np.asarray(table["sequence_number"].to_numpy(), dtype=np.int64)
        if np.any(sip[1:] < sip[:-1]) or np.any(
            (sip[1:] == sip[:-1]) & (seq[1:] < seq[:-1])
        ):
            raise ValueError(
                f"{trade_path}: rows are not ordered by (sip_timestamp, sequence_number, raw_row_index)"
            )
        first_key = (int(sip[0]), int(seq[0]))
        if previous_key is not None and first_key < previous_key:
            raise ValueError(f"{trade_path}: ordering decreases across Parquet batches")
        if previous_key is not None and first_key == previous_key:
            duplicate_key_count += 1
            payload = _duplicate_payload(table, 0)
            if payload == previous_payload:
                exact_duplicate_count += 1
                raise ValueError(
                    f"{trade_path}: exact duplicate trade row for key {first_key}"
                )
            raise ValueError(
                f"{trade_path}: conflicting duplicate trade key {first_key}"
            )
        duplicate_positions = (
            np.flatnonzero((sip[1:] == sip[:-1]) & (seq[1:] == seq[:-1])) + 1
        )
        if duplicate_positions.size:
            position = int(duplicate_positions[0])
            duplicate_key_count += int(duplicate_positions.size)
            key = (int(sip[position]), int(seq[position]))
            if _duplicate_payload(table, position) == _duplicate_payload(
                table, position - 1
            ):
                exact_duplicate_count += 1
                raise ValueError(
                    f"{trade_path}: exact duplicate trade row for key {key}"
                )
            raise ValueError(f"{trade_path}: conflicting duplicate trade key {key}")
        previous_key = (int(sip[-1]), int(seq[-1]))
        previous_payload = _duplicate_payload(table, table.num_rows - 1)

        semantics = MARKET.activity_trade_semantics(
            table, max_reporting_latency_ns=config.max_reporting_latency_ns
        )
        price = np.asarray(table["price"].to_numpy(), dtype=np.float64)
        in_session = (sip >= start_ns) & (sip < end_ns)
        session_sip = sip[in_session]
        indices = ((session_sip - start_ns) // NS).astype(np.int64)
        if indices.size:
            raw_counts += np.bincount(indices, minlength=SESSION_SECONDS).astype(
                np.int64
            )
            raw_time_parts.append(session_sip.copy())
        eligible = in_session & semantics.eligible_activity_trade
        eligible_sip = sip[eligible]
        if eligible_sip.size:
            eligible_indices = ((eligible_sip - start_ns) // NS).astype(np.int64)
            eligible_counts += np.bincount(
                eligible_indices, minlength=SESSION_SECONDS
            ).astype(np.int64)
            dollars = price[eligible] * semantics.analytic_size[eligible]
            eligible_dollars += np.bincount(
                eligible_indices, weights=dollars, minlength=SESSION_SECONDS
            ).astype(np.float64)
            eligible_time_parts.append(eligible_sip.copy())
            eligible_price_parts.append(price[eligible].copy())
            eligible_size_parts.append(semantics.analytic_size[eligible].copy())
        raw_price_valid = in_session & np.isfinite(price) & (price > 0.0)
        if np.any(raw_price_valid):
            raw_price_time_parts.append(sip[raw_price_valid].copy())
            raw_price_parts.append(price[raw_price_valid].copy())
        raw_rows += table.num_rows
        in_session_raw_rows += int(np.count_nonzero(in_session))

    if raw_rows != parquet.metadata.num_rows:
        raise AssertionError(f"{trade_path}: Parquet row count changed while scanning")

    concat_i64 = lambda parts: (
        np.concatenate(parts) if parts else np.empty(0, dtype=np.int64)
    )
    concat_f64 = lambda parts: (
        np.concatenate(parts) if parts else np.empty(0, dtype=np.float64)
    )
    eligible_times = concat_i64(eligible_time_parts)
    eligible_prices = concat_f64(eligible_price_parts)
    eligible_sizes = concat_f64(eligible_size_parts)
    raw_times = concat_i64(raw_time_parts)
    raw_price_times = concat_i64(raw_price_time_parts)
    raw_prices = concat_f64(raw_price_parts)
    predicate = activity_predicate(eligible_counts, eligible_dollars, config)
    dollar_prefix = np.r_[0.0, np.cumsum(eligible_prices * eligible_sizes)]
    candidates: list[dict[str, Any]] = []
    config_hash = config.digest()
    trade_identity = parquet_identity(trade_path)
    quote_identity = parquet_identity(quote_path)

    for index in np.flatnonzero(
        np.diff(eligible_times) >= config.gap_floor_seconds * NS
    ):
        last_pre = int(eligible_times[index])
        first_post = int(eligible_times[index + 1])
        if not (start_ns <= last_pre < first_post < end_ns):
            continue
        halt_start = ceil_second_strictly_after(last_pre)
        start_index = int((halt_start - start_ns) // NS)
        if not _pre_gap_qualified(predicate, start_index, config):
            continue
        gap_seconds = (first_post - last_pre) / NS
        post_counts: list[int | None] = []
        post_dollars: list[float | None] = []
        for minute in range(5):
            lo = first_post + minute * 60 * NS
            hi = lo + 60 * NS
            if hi > end_ns:
                post_counts.append(None)
                post_dollars.append(None)
                continue
            left = int(np.searchsorted(eligible_times, lo, side="left"))
            right = int(np.searchsorted(eligible_times, hi, side="left"))
            post_counts.append(right - left)
            post_dollars.append(float(dollar_prefix[right] - dollar_prefix[left]))
        post_qualified = any(
            count is not None
            and count >= config.activity_minute_trades
            and dollars is not None
            and dollars >= config.activity_minute_dollars
            for count, dollars in zip(post_counts, post_dollars)
        )
        raw_left = int(np.searchsorted(raw_times, last_pre, side="right"))
        raw_right = int(np.searchsorted(raw_times, first_post, side="left"))
        pre60_count = int(
            np.searchsorted(eligible_times, halt_start, side="left")
            - np.searchsorted(eligible_times, halt_start - 60 * NS, side="left")
        )
        pre300_count = int(
            np.searchsorted(eligible_times, halt_start, side="left")
            - np.searchsorted(eligible_times, halt_start - 300 * NS, side="left")
        )
        pre60_dollars = _prefix_interval(
            eligible_times, dollar_prefix, halt_start - 60 * NS, halt_start
        )
        pre300_dollars = _prefix_interval(
            eligible_times, dollar_prefix, halt_start - 300 * NS, halt_start
        )
        distances = [abs(gap_seconds - 300 * k) for k in (1, 2, 3, 4)]
        nearest_position = int(np.argmin(distances))

        raw_pre60 = _window_values(
            raw_price_times, raw_prices, halt_start - 60 * NS, halt_start
        )
        raw_pre300 = _window_values(
            raw_price_times, raw_prices, halt_start - 300 * NS, halt_start
        )
        raw_post60 = _window_values(
            raw_price_times, raw_prices, first_post, min(first_post + 60 * NS, end_ns)
        )
        raw_post300 = _window_values(
            raw_price_times, raw_prices, first_post, min(first_post + 300 * NS, end_ns)
        )
        eligible_pre60 = _window_values(
            eligible_times, eligible_prices, halt_start - 60 * NS, halt_start
        )
        eligible_pre300 = _window_values(
            eligible_times, eligible_prices, halt_start - 300 * NS, halt_start
        )
        eligible_post60 = _window_values(
            eligible_times,
            eligible_prices,
            first_post,
            min(first_post + 60 * NS, end_ns),
        )
        eligible_post300 = _window_values(
            eligible_times,
            eligible_prices,
            first_post,
            min(first_post + 300 * NS, end_ns),
        )
        review_start = max(start_ns, halt_start - 300 * NS)
        review_end = min(end_ns, first_post + 300 * NS)
        review_raw_left = int(
            np.searchsorted(raw_price_times, review_start, side="left")
        )
        review_raw_right = int(
            np.searchsorted(raw_price_times, review_end, side="left")
        )
        review_eligible_left = int(
            np.searchsorted(eligible_times, review_start, side="left")
        )
        review_eligible_right = int(
            np.searchsorted(eligible_times, review_end, side="left")
        )

        row: dict[str, Any] = {
            "candidate_id": _candidate_hash(
                session_date, symbol, last_pre, first_post, config_hash
            ),
            "universe_version": universe_version,
            "session_date": session_date,
            "symbol": symbol,
            "instrument_type": instrument_type,
            "detection_method_version": DETECTION_METHOD_VERSION,
            "registry_version": REGISTRY_VERSION,
            "candidate_config_hash": config_hash,
            "last_pre_gap_trade_sip": _utc(last_pre),
            "first_post_gap_trade_sip": _utc(first_post),
            "inferred_halt_interval_start": _utc(halt_start),
            "inferred_trade_resume_time": _utc(first_post),
            "halt_interval_start": _utc(halt_start),
            "trade_resume_time": _utc(first_post),
            "eligible_gap_seconds": float(gap_seconds),
            "nearest_300s_multiple": nearest_position + 1,
            "distance_to_nearest_300s_multiple": float(distances[nearest_position]),
            "near_300s_multiple_90s": bool(
                distances[nearest_position]
                <= config.duration_multiple_tolerance_seconds
            ),
            "raw_trade_count_in_gap": raw_right - raw_left,
            "eligible_trade_count_in_gap": 0,
            "pre_gap_trade_count_60s": pre60_count,
            "pre_gap_trade_count_300s": pre300_count,
            "pre_gap_dollar_volume_60s": pre60_dollars,
            "pre_gap_dollar_volume_300s": pre300_dollars,
            "post_gap_trade_counts_60s": post_counts,
            "post_gap_dollar_volumes_60s": post_dollars,
            "pre_gap_activity_qualified": True,
            "post_gap_activity_qualified": bool(post_qualified),
            "pre_gap_range_bps_60s": _range_bps(eligible_pre60),
            "pre_gap_range_bps_300s": _range_bps(eligible_pre300),
            "pre_gap_abs_return_bps_60s": _abs_return_bps(eligible_pre60),
            "pre_gap_abs_return_bps_300s": _abs_return_bps(eligible_pre300),
            "post_gap_range_bps_60s": _range_bps(eligible_post60),
            "post_gap_range_bps_300s": _range_bps(eligible_post300),
            "raw_pre_gap_range_bps_60s": _range_bps(raw_pre60),
            "raw_pre_gap_range_bps_300s": _range_bps(raw_pre300),
            "raw_pre_gap_abs_return_bps_60s": _abs_return_bps(raw_pre60),
            "raw_pre_gap_abs_return_bps_300s": _abs_return_bps(raw_pre300),
            "raw_post_gap_range_bps_60s": _range_bps(raw_post60),
            "raw_post_gap_range_bps_300s": _range_bps(raw_post300),
            "reopening_jump_bps": float(
                10_000.0 * math.log(eligible_prices[index + 1] / eligible_prices[index])
            ),
            "review_raw_trade_sip": raw_price_times[
                review_raw_left:review_raw_right
            ].tolist(),
            "review_raw_trade_prices": raw_prices[
                review_raw_left:review_raw_right
            ].tolist(),
            "review_eligible_trade_sip": eligible_times[
                review_eligible_left:review_eligible_right
            ].tolist(),
            "review_eligible_trade_prices": eligible_prices[
                review_eligible_left:review_eligible_right
            ].tolist(),
            "trade_source_path": str(trade_path),
            "trade_source_sha256": trade_identity["sha256"],
            "trade_source_parquet_metadata_sha256": trade_identity[
                "parquet_metadata_sha256"
            ],
            "quote_source_path": str(quote_path),
            "quote_source_sha256": quote_identity["sha256"],
            "quote_source_parquet_metadata_sha256": quote_identity[
                "parquet_metadata_sha256"
            ],
            "source_provenance": canonical_json(
                {"trade": trade_identity, "quote": quote_identity}
            ),
        }
        candidates.append(row)

    active_seconds = np.flatnonzero(eligible_counts > 0)
    presence: list[dict[str, Any]] = []
    for second in active_seconds:
        lo = start_ns + int(second) * NS
        hi = lo + NS
        left = int(np.searchsorted(eligible_times, lo, side="left"))
        right = int(np.searchsorted(eligible_times, hi, side="left"))
        presence.append(
            {
                "session_date": session_date,
                "symbol": symbol,
                "second_index": int(second),
                "first_eligible_sip": int(eligible_times[left]),
                "last_eligible_sip": int(eligible_times[right - 1]),
            }
        )
    qualified_end_indices = np.flatnonzero(predicate)
    summary = {
        "universe_version": universe_version,
        "session_date": session_date,
        "symbol": symbol,
        "instrument_type": instrument_type,
        "status": "processed",
        "failure_reason": None,
        "raw_trade_rows": raw_rows,
        "in_session_raw_trade_rows": in_session_raw_rows,
        "raw_trade_active_seconds": int(np.count_nonzero(raw_counts)),
        "eligible_activity_trade_rows": int(eligible_times.size),
        "eligible_activity_dollars": float(np.sum(eligible_prices * eligible_sizes)),
        "candidate_count": len(candidates),
        "activity_qualified_end_ns": [
            int(start_ns + index * NS) for index in qualified_end_indices
        ],
        "duplicate_key_count": duplicate_key_count,
        "exact_duplicate_count": exact_duplicate_count,
        "trade_source_path": str(trade_path),
        "trade_source_sha256": trade_identity["sha256"],
        "trade_source_parquet_metadata_sha256": trade_identity[
            "parquet_metadata_sha256"
        ],
        "quote_source_path": str(quote_path),
        "quote_source_sha256": quote_identity["sha256"],
        "quote_source_parquet_metadata_sha256": quote_identity[
            "parquet_metadata_sha256"
        ],
        "candidate_config_hash": config_hash,
    }
    return summary, candidates, presence


def _empty_candidate_table() -> pa.Table:
    return pa.table(
        {
            "candidate_id": pa.array([], pa.string()),
            "session_date": pa.array([], pa.string()),
            "symbol": pa.array([], pa.string()),
            "candidate_config_hash": pa.array([], pa.string()),
        }
    )


def _upstream_rejection_reason(job: Mapping[str, Any]) -> str | None:
    for field in (
        "upstream_accepted",
        "source_file_accepted",
        "trade_source_file_accepted",
        "quote_source_file_accepted",
    ):
        if field in job and not pd.isna(job[field]):
            value = job[field]
            accepted = (
                str(value).strip().lower()
                in {"true", "1", "yes", "accepted", "verified"}
                if isinstance(value, str)
                else bool(value)
            )
            if not accepted:
                return f"upstream manifest marks {field}=false"
    status = str(job.get("status", "")).strip().lower()
    if status in {"rejected", "failed", "invalid", "schema_invalid", "unreadable"}:
        return f"upstream manifest status={status}"
    return None


def _source_stat(path: Path) -> dict[str, Any]:
    path = path.expanduser().resolve()
    if not path.is_file():
        return {
            "path": str(path),
            "exists": False,
            "size_bytes": None,
            "mtime_ns": None,
        }
    stat = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def _checkpoint_dir(root: Path, session_date: str, symbol: str) -> Path:
    key = canonical_hash({"session_date": session_date, "symbol": symbol})[:16]
    return root / session_date / f"{symbol}.{key}"


def _write_symbol_day_checkpoint(
    *,
    root: Path,
    job: Mapping[str, Any],
    summary: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    presence: Sequence[Mapping[str, Any]],
    config: DetectionConfig,
) -> Path:
    target = _checkpoint_dir(root, str(job["session_date"]), str(job["symbol"]))
    if target.exists():
        raise FileExistsError(f"symbol-day checkpoint already exists: {target}")
    staging = target.with_name(f".{target.name}.{os.getpid()}.staging")
    if staging.exists():
        raise FileExistsError(staging)
    staging.mkdir(parents=True)
    summary_path = staging / "summary.parquet"
    candidate_path = staging / "candidates.parquet"
    presence_path = staging / "presence.parquet"
    atomic_parquet(pd.DataFrame([dict(summary)]), summary_path, replace=False)
    if candidates:
        atomic_parquet(
            pd.DataFrame([dict(row) for row in candidates]),
            candidate_path,
            replace=False,
        )
    else:
        atomic_parquet(_empty_candidate_table(), candidate_path, replace=False)
    atomic_parquet(
        pd.DataFrame(
            [dict(row) for row in presence],
            columns=[
                "session_date",
                "symbol",
                "second_index",
                "first_eligible_sip",
                "last_eligible_sip",
            ],
        ),
        presence_path,
        replace=False,
    )
    complete = {
        "artifact_type": "halt_detection_symbol_day_checkpoint",
        "session_date": str(job["session_date"]),
        "symbol": str(job["symbol"]),
        "candidate_config_hash": config.digest(),
        "trade_source_stat": _source_stat(Path(job["trade_path"])),
        "quote_source_stat": _source_stat(Path(job["quote_path"])),
        "content_sha256": {
            "summary.parquet": sha256_file(summary_path),
            "candidates.parquet": sha256_file(candidate_path),
            "presence.parquet": sha256_file(presence_path),
        },
    }
    atomic_json(complete, staging / "complete.json", replace=False)
    target.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staging, target)
    return target


def _load_symbol_day_checkpoint(
    *,
    root: Path,
    job: Mapping[str, Any],
    config: DetectionConfig,
    load_data: bool = True,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]] | None:
    target = _checkpoint_dir(root, str(job["session_date"]), str(job["symbol"]))
    if not target.exists():
        return None
    complete_path = target / "complete.json"
    if not complete_path.is_file():
        raise ValueError(f"incomplete published symbol-day checkpoint: {target}")
    complete = json.loads(complete_path.read_text())
    if (
        complete.get("session_date") != str(job["session_date"])
        or complete.get("symbol") != str(job["symbol"])
        or complete.get("candidate_config_hash") != config.digest()
    ):
        raise ValueError(f"symbol-day checkpoint identity mismatch: {target}")
    for name, expected in complete.get("content_sha256", {}).items():
        path = target / name
        if not path.is_file() or sha256_file(path) != expected:
            raise ValueError(f"symbol-day checkpoint hash mismatch: {path}")
    if complete.get("trade_source_stat") != _source_stat(Path(job["trade_path"])):
        raise ValueError(
            f"trade source changed since checkpoint: {job['session_date']} {job['symbol']}"
        )
    if complete.get("quote_source_stat") != _source_stat(Path(job["quote_path"])):
        raise ValueError(
            f"quote source changed since checkpoint: {job['session_date']} {job['symbol']}"
        )
    if not load_data:
        return {"__checkpoint_path": str(target)}, [], []
    summary_rows = pd.read_parquet(target / "summary.parquet").to_dict("records")
    if len(summary_rows) != 1:
        raise ValueError(
            f"symbol-day checkpoint must contain one summary row: {target}"
        )
    candidate_frame = pd.read_parquet(target / "candidates.parquet")
    presence_frame = pd.read_parquet(target / "presence.parquet")
    return (
        summary_rows[0],
        candidate_frame.to_dict("records"),
        presence_frame.to_dict("records"),
    )


def _align_table(table: pa.Table, schema: pa.Schema) -> pa.Table:
    arrays: list[pa.Array | pa.ChunkedArray] = []
    for field in schema:
        if field.name in table.column_names:
            column = table[field.name]
            arrays.append(
                column if column.type == field.type else pc.cast(column, field.type)
            )
        else:
            arrays.append(pa.nulls(table.num_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def _consolidate_checkpoints(
    checkpoint_dirs: Sequence[Path],
    *,
    filename: str,
    output_path: Path,
    empty_table: pa.Table | None = None,
) -> int:
    schemas: list[pa.Schema] = []
    for checkpoint in checkpoint_dirs:
        parquet = pq.ParquetFile(checkpoint / filename)
        if parquet.metadata.num_rows:
            schemas.append(parquet.schema_arrow)
    if not schemas:
        if empty_table is None:
            raise ValueError(f"no checkpoint rows available for {filename}")
        atomic_parquet(empty_table, output_path, replace=False)
        return 0
    schema = pa.unify_schemas(schemas, promote_options="permissive")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _tmp(output_path)
    writer = pq.ParquetWriter(
        temporary, schema, compression="zstd", compression_level=3
    )
    rows = 0
    try:
        for checkpoint in checkpoint_dirs:
            table = pq.read_table(checkpoint / filename)
            if not table.num_rows:
                continue
            writer.write_table(_align_table(table, schema))
            rows += table.num_rows
    finally:
        writer.close()
    os.replace(temporary, output_path)
    return rows


def detect_candidates(
    *,
    manifest_path: Path,
    tick_data_root: Path,
    run_dir: Path,
    start: str | None = None,
    end: str | None = None,
    config: DetectionConfig | None = None,
    resume: bool = False,
    allow_unverified_manifest: bool = False,
    checkpoint_root: Path | None = None,
    checkpoint_read_roots: Sequence[Path] = (),
) -> dict[str, Path]:
    config = config or DetectionConfig()
    run_dir = run_dir.expanduser().resolve()
    manifest_path = manifest_path.expanduser().resolve()
    tick_data_root = tick_data_root.expanduser().resolve()
    jobs = load_symbol_day_manifest(
        manifest_path,
        tick_data_root=tick_data_root,
        start=start,
        end=end,
        allow_unverified=allow_unverified_manifest,
    )
    manifest_verification = verify_acquisition_manifest(
        manifest_path, allow_unverified=allow_unverified_manifest
    )
    run_config = {
        "artifact_type": "halt_detection_run",
        "script_version": SCRIPT_VERSION,
        "detection_method_version": DETECTION_METHOD_VERSION,
        "registry_version": REGISTRY_VERSION,
        "candidate_config": config.jsonable(),
        "candidate_config_hash": config.digest(),
        "source_manifest": str(manifest_path),
        "source_manifest_sha256": sha256_file(manifest_path),
        "source_manifest_verification": manifest_verification,
        "tick_data_root": str(tick_data_root),
        "start": start,
        "end": end,
        "universe_version": str(jobs.universe_version.iloc[0]),
        "job_count": len(jobs),
    }
    config_path = run_dir / "run_config.json"
    if run_dir.exists():
        if not resume:
            raise FileExistsError(
                f"detection run already exists; pass --resume only for an identical run: {run_dir}"
            )
        if (
            not config_path.is_file()
            or json.loads(config_path.read_text()) != run_config
        ):
            raise ValueError(
                "resume refused: run configuration or source manifest identity differs"
            )
        exact = run_dir / "exact_candidates.parquet"
        freeze = run_dir / "candidate_freeze.json"
        if exact.is_file() and freeze.is_file():
            record = json.loads(freeze.read_text())
            if record.get("exact_candidates_sha256") != sha256_file(exact):
                raise ValueError(
                    "resume refused: frozen candidate artifact hash mismatch"
                )
            summaries = pd.read_parquet(run_dir / "symbol_day_trade_summaries.parquet")
            for row in summaries[summaries.status.eq("processed")].to_dict("records"):
                for stream in ("trade", "quote"):
                    identity = parquet_identity(Path(row[f"{stream}_source_path"]))
                    if (
                        identity["sha256"] != row[f"{stream}_source_sha256"]
                        or identity["parquet_metadata_sha256"]
                        != row[f"{stream}_source_parquet_metadata_sha256"]
                    ):
                        raise ValueError(
                            f"resume refused: {stream} source identity changed for {row['session_date']} {row['symbol']}"
                        )
            return {
                "processed": run_dir / "processed_symbol_days.parquet",
                "summaries": run_dir / "symbol_day_trade_summaries.parquet",
                "candidates": exact,
                "presence": run_dir / "date_event_presence.parquet",
            }
    else:
        run_dir.mkdir(parents=True)
        atomic_json(run_config, config_path, replace=False)

    checkpoint_root = (
        run_dir / "symbol_day_checkpoints"
        if checkpoint_root is None
        else checkpoint_root.expanduser().resolve()
    )
    checkpoint_root.mkdir(exist_ok=True)
    read_roots = [checkpoint_root]
    for root in checkpoint_read_roots:
        resolved = root.expanduser().resolve()
        if resolved not in read_roots:
            read_roots.append(resolved)
    checkpoint_dirs: list[Path] = []
    for job in jobs.itertuples(index=False):
        job_record = job._asdict()
        checkpoint = None
        for read_root in read_roots:
            checkpoint = _load_symbol_day_checkpoint(
                root=read_root,
                job=job_record,
                config=config,
                load_data=False,
            )
            if checkpoint is not None:
                break
        if checkpoint is not None:
            checkpoint_dirs.append(Path(checkpoint[0]["__checkpoint_path"]))
            continue
        try:
            rejection = _upstream_rejection_reason(job_record)
            if rejection is not None:
                raise ValueError(rejection)
            summary, job_candidates, job_presence = scan_trade_symbol_day(
                session_date=job.session_date,
                symbol=job.symbol,
                instrument_type=job.instrument_type,
                universe_version=job.universe_version,
                trade_path=Path(job.trade_path),
                quote_path=Path(job.quote_path),
                config=config,
            )
        except Exception as exc:
            summary = {
                "universe_version": job.universe_version,
                "session_date": job.session_date,
                "symbol": job.symbol,
                "instrument_type": job.instrument_type,
                "status": "failed",
                "failure_reason": f"{type(exc).__name__}: {exc}",
                "candidate_count": 0,
                "activity_qualified_end_ns": [],
                "trade_source_path": job.trade_path,
                "quote_source_path": job.quote_path,
                "candidate_config_hash": config.digest(),
            }
            job_candidates = []
            job_presence = []
        checkpoint_dirs.append(
            _write_symbol_day_checkpoint(
                root=checkpoint_root,
                job=job_record,
                summary=summary,
                candidates=job_candidates,
                presence=job_presence,
                config=config,
            )
        )

    processed_columns = [
        "universe_version",
        "session_date",
        "symbol",
        "instrument_type",
        "status",
        "failure_reason",
        "candidate_count",
        "candidate_config_hash",
        "trade_source_path",
        "quote_source_path",
    ]
    paths = {
        "processed": run_dir / "processed_symbol_days.parquet",
        "summaries": run_dir / "symbol_day_trade_summaries.parquet",
        "candidates": run_dir / "exact_candidates.parquet",
        "presence": run_dir / "date_event_presence.parquet",
    }
    _consolidate_checkpoints(
        checkpoint_dirs, filename="summary.parquet", output_path=paths["summaries"]
    )
    candidate_rows = _consolidate_checkpoints(
        checkpoint_dirs,
        filename="candidates.parquet",
        output_path=paths["candidates"],
        empty_table=_empty_candidate_table(),
    )
    _consolidate_checkpoints(
        checkpoint_dirs,
        filename="presence.parquet",
        output_path=paths["presence"],
        empty_table=pa.table(
            {
                "session_date": pa.array([], pa.string()),
                "symbol": pa.array([], pa.string()),
                "second_index": pa.array([], pa.int64()),
                "first_eligible_sip": pa.array([], pa.int64()),
                "last_eligible_sip": pa.array([], pa.int64()),
            }
        ),
    )
    processed = pd.read_parquet(paths["summaries"], columns=processed_columns)
    atomic_parquet(processed, paths["processed"], replace=False)
    candidate_ids = pd.read_parquet(paths["candidates"], columns=["candidate_id"])[
        "candidate_id"
    ]
    if candidate_ids.duplicated().any():
        raise ValueError("detector produced duplicate candidate IDs")
    freeze_record = {
        "frozen_before_external_matching": True,
        "exact_candidates_sha256": sha256_file(paths["candidates"]),
        "candidate_rows": candidate_rows,
        "candidate_config_hash": config.digest(),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    atomic_json(freeze_record, run_dir / "candidate_freeze.json", replace=False)
    atomic_json(
        {
            "artifact_type": "halt_detection_working_manifest",
            "content_sha256": {name: sha256_file(path) for name, path in paths.items()},
            "candidate_freeze_sha256": sha256_file(run_dir / "candidate_freeze.json"),
            "symbol_day_checkpoint_count": len(jobs),
        },
        run_dir / "manifest.json",
        replace=False,
    )
    return paths


def _quote_evidence_for_candidate(
    row: Mapping[str, Any],
    state: MARKET.QuoteState,
) -> dict[str, Any]:
    last_pre = _ns(row["last_pre_gap_trade_sip"])
    first_post = _ns(row["first_post_gap_trade_sip"])
    halt_start = _ns(row["inferred_halt_interval_start"])
    assert last_pre is not None and first_post is not None and halt_start is not None
    sip = state.sip_timestamp
    in_gap = (sip > last_pre) & (sip < first_post)
    transition = state.state_change & in_gap
    session_start, _ = session_bounds_ns(str(row["session_date"]))
    minute_count = int(
        np.unique(((sip[in_gap] - session_start) // (60 * NS)).astype(np.int64)).size
    )
    pre_guard = (sip >= halt_start - 60 * NS) & (sip < halt_start)
    post_guard = (sip >= first_post) & (sip < first_post + 60 * NS)
    review = (sip >= halt_start - 300 * NS) & (sip < first_post + 300 * NS)
    pre_valid = np.flatnonzero(pre_guard & state.price_state_valid)
    post_valid = np.flatnonzero(post_guard & state.price_state_valid)
    quote_resume_position = int(np.searchsorted(sip, first_post, side="left"))
    quote_resume = (
        int(sip[quote_resume_position]) if quote_resume_position < sip.size else None
    )
    invalid_transition = transition & (
        state.one_sided | state.condition_invalid | ~state.price_state_valid
    )
    return {
        "candidate_id": row["candidate_id"],
        "session_date": row["session_date"],
        "symbol": row["symbol"],
        "quote_message_count_in_gap": int(np.count_nonzero(in_gap)),
        "quote_active_minute_count_in_gap": minute_count,
        "semantic_quote_state_change_count_in_gap": int(np.count_nonzero(transition)),
        "valid_quote_message_count_in_gap": int(
            np.count_nonzero(in_gap & state.price_state_valid)
        ),
        "invalid_or_one_sided_transition_count": int(
            np.count_nonzero(invalid_transition)
        ),
        "locked_transition_count": int(np.count_nonzero(transition & state.locked)),
        "crossed_transition_count": int(np.count_nonzero(transition & state.crossed)),
        "last_pre_gap_valid_midpoint": (
            float(state.midpoint[pre_valid[-1]]) if pre_valid.size else None
        ),
        "first_post_gap_valid_midpoint": (
            float(state.midpoint[post_valid[0]]) if post_valid.size else None
        ),
        "last_pre_gap_spread_bps": (
            float(state.spread_bps[pre_valid[-1]])
            if pre_valid.size and np.isfinite(state.spread_bps[pre_valid[-1]])
            else None
        ),
        "first_post_gap_spread_bps": (
            float(state.spread_bps[post_valid[0]])
            if post_valid.size and np.isfinite(state.spread_bps[post_valid[0]])
            else None
        ),
        "quote_resume_time": _utc(quote_resume),
        "same_symbol_quote_stream_observed_in_gap": bool(np.any(in_gap)),
        "review_quote_sip": sip[review].tolist(),
        "review_quote_midpoint": [
            float(value) if np.isfinite(value) else None
            for value in state.midpoint[review]
        ],
        "review_quote_valid": state.price_state_valid[review].tolist(),
    }


def _witness_count(
    candidate: Mapping[str, Any],
    *,
    presence_symbols: np.ndarray,
    presence_first: np.ndarray,
    presence_last: np.ndarray,
    date_slices: Mapping[str, tuple[int, int]],
) -> int:
    day = str(candidate["session_date"])
    symbol = str(candidate["symbol"])
    last_pre = _ns(candidate["last_pre_gap_trade_sip"])
    first_post = _ns(candidate["first_post_gap_trade_sip"])
    assert last_pre is not None and first_post is not None
    bounds = date_slices.get(day)
    if bounds is None:
        return 0
    start, stop = bounds
    in_gap = (presence_first[start:stop] < first_post) & (
        presence_last[start:stop] > last_pre
    )
    witnesses = np.unique(presence_symbols[start:stop][in_gap])
    return int(np.count_nonzero(witnesses != symbol))


def _presence_date_slices(presence_dates: np.ndarray) -> dict[str, tuple[int, int]]:
    """Index a date-sorted event-presence stream without copying its 100M+ rows."""
    if presence_dates.size == 0:
        return {}
    changes = np.flatnonzero(presence_dates[1:] != presence_dates[:-1]) + 1
    starts = np.concatenate(([0], changes))
    stops = np.concatenate((changes, [presence_dates.size]))
    result: dict[str, tuple[int, int]] = {}
    for start, stop in zip(starts, stops):
        day = str(presence_dates[start])
        if day in result:
            raise ValueError("date_event_presence must be contiguous by session_date")
        result[day] = (int(start), int(stop))
    return result


def _overlap_fraction(a_start: int, a_end: int, b_start: int, b_end: int) -> float:
    overlap = max(0, min(a_end, b_end) - max(a_start, b_start))
    denominator = min(a_end - a_start, b_end - b_start)
    return overlap / denominator if denominator > 0 else 0.0


def classify_source_health(
    *,
    same_symbol_quote_stream_observed_in_gap: bool,
    other_symbol_event_witness_count: int,
    same_date_upstream_rejected_symbol_count: int,
    acquired_symbol_count: int,
    overlapping_candidate_symbol_count: int,
    externally_confirmed: bool = False,
    config: DetectionConfig | None = None,
) -> str:
    config = config or DetectionConfig()
    broad_rejected = (
        same_date_upstream_rejected_symbol_count >= config.broad_rejected_minimum
        and same_date_upstream_rejected_symbol_count / max(1, acquired_symbol_count)
        >= config.broad_rejected_fraction
    )
    if broad_rejected or overlapping_candidate_symbol_count > 0:
        return "possible_source_interruption"
    if (
        same_symbol_quote_stream_observed_in_gap
        or other_symbol_event_witness_count >= 2
        or externally_confirmed
    ):
        return "healthy_witnessed"
    return "unresolved"


def enrich_candidates(
    *,
    run_dir: Path,
    config: DetectionConfig | None = None,
) -> Path:
    config = config or DetectionConfig()
    run_dir = run_dir.expanduser().resolve()
    freeze_path = run_dir / "candidate_freeze.json"
    candidate_path = run_dir / "exact_candidates.parquet"
    if not freeze_path.is_file() or not candidate_path.is_file():
        raise FileNotFoundError(
            "detect stage and candidate freeze are required before enrichment"
        )
    freeze = json.loads(freeze_path.read_text())
    if freeze.get("exact_candidates_sha256") != sha256_file(candidate_path):
        raise ValueError("exact candidate artifact changed after its blind freeze")
    run_config = json.loads((run_dir / "run_config.json").read_text())
    if run_config.get("candidate_config_hash") != config.digest():
        raise ValueError("enrichment config differs from detection config")
    candidates = pd.read_parquet(candidate_path)
    output_path = run_dir / "candidate_quote_evidence.parquet"
    if output_path.exists():
        raise FileExistsError(output_path)
    if candidates.empty:
        atomic_parquet(
            pa.table(
                {
                    "candidate_id": pa.array([], pa.string()),
                    "session_date": pa.array([], pa.string()),
                    "symbol": pa.array([], pa.string()),
                }
            ),
            output_path,
            replace=False,
        )
        return output_path
    presence = pd.read_parquet(run_dir / "date_event_presence.parquet")
    processed = pd.read_parquet(run_dir / "processed_symbol_days.parquet")
    evidence_rows: list[dict[str, Any]] = []
    for (day, symbol), group in candidates.groupby(
        ["session_date", "symbol"], sort=True
    ):
        first = group.iloc[0]
        quote_path = Path(str(first.quote_source_path))
        identity = parquet_identity(quote_path)
        if (
            identity["sha256"] != first.quote_source_sha256
            or identity["parquet_metadata_sha256"]
            != first.quote_source_parquet_metadata_sha256
        ):
            raise ValueError(
                f"quote source identity changed before enrichment: {quote_path}"
            )
        state, _ = MARKET.prepare_quote_state(
            quote_path,
            str(day),
            MARKET.PreprocessConfig(batch_size=config.batch_size),
        )
        for row in group.to_dict("records"):
            evidence_rows.append(_quote_evidence_for_candidate(row, state))
    evidence = pd.DataFrame(evidence_rows)
    merged = candidates.merge(
        evidence, on=["candidate_id", "session_date", "symbol"], validate="one_to_one"
    )
    rejected_by_date = (
        processed.assign(rejected=processed.status.ne("processed"))
        .groupby("session_date", as_index=False)
        .agg(
            same_date_upstream_rejected_symbol_count=("rejected", "sum"),
            acquired_symbol_count=("symbol", "size"),
        )
    )
    merged = merged.merge(
        rejected_by_date, on="session_date", how="left", validate="many_to_one"
    )
    presence_dates = presence.session_date.to_numpy(copy=False)
    presence_symbols = presence.symbol.to_numpy(copy=False)
    presence_first = presence.first_eligible_sip.to_numpy(dtype=np.int64, copy=False)
    presence_last = presence.last_eligible_sip.to_numpy(dtype=np.int64, copy=False)
    date_slices = _presence_date_slices(presence_dates)
    merged["other_symbol_event_witness_count"] = [
        _witness_count(
            row,
            presence_symbols=presence_symbols,
            presence_first=presence_first,
            presence_last=presence_last,
            date_slices=date_slices,
        )
        for row in merged.to_dict("records")
    ]
    overlap_counts: list[int] = []
    for row in merged.to_dict("records"):
        last_pre = _ns(row["last_pre_gap_trade_sip"])
        first_post = _ns(row["first_post_gap_trade_sip"])
        assert last_pre is not None and first_post is not None
        count = 0
        for other in merged[merged.session_date == row["session_date"]].to_dict(
            "records"
        ):
            if (
                other["symbol"] == row["symbol"]
                or int(other["quote_message_count_in_gap"]) != 0
            ):
                continue
            other_start = _ns(other["last_pre_gap_trade_sip"])
            other_end = _ns(other["first_post_gap_trade_sip"])
            assert other_start is not None and other_end is not None
            if (
                _overlap_fraction(last_pre, first_post, other_start, other_end)
                >= config.overlapping_gap_fraction
            ):
                count += 1
        overlap_counts.append(count)
    merged["overlapping_candidate_symbol_count"] = overlap_counts
    merged["source_health_class"] = [
        classify_source_health(
            same_symbol_quote_stream_observed_in_gap=bool(
                row["same_symbol_quote_stream_observed_in_gap"]
            ),
            other_symbol_event_witness_count=int(
                row["other_symbol_event_witness_count"]
            ),
            same_date_upstream_rejected_symbol_count=int(
                row["same_date_upstream_rejected_symbol_count"]
            ),
            acquired_symbol_count=int(row["acquired_symbol_count"]),
            overlapping_candidate_symbol_count=int(
                row["overlapping_candidate_symbol_count"]
            ),
            config=config,
        )
        for row in merged.to_dict("records")
    ]
    evidence_columns = [
        column
        for column in merged.columns
        if column not in candidates.columns
        or column in {"candidate_id", "session_date", "symbol"}
    ]
    atomic_parquet(merged[evidence_columns], output_path, replace=False)
    return output_path


EXTERNAL_ALIASES = {
    "provider_event_id": ("provider_event_id", "event_id", "id", "halt_id"),
    "session_date": ("session_date", "date", "halt_date"),
    "symbol": ("symbol", "ticker", "issue_symbol"),
    "official_halt_start": (
        "official_halt_start",
        "halt_start",
        "halt_time",
        "halt_timestamp",
    ),
    "official_quote_resume_time": (
        "official_quote_resume_time",
        "quote_resume_time",
        "quote_resume",
    ),
    "official_trade_resume_time": (
        "official_trade_resume_time",
        "trade_resume_time",
        "resume_time",
        "trade_resume",
    ),
    "official_resume_date": ("official_resume_date", "resumption_date", "resume_date"),
    "official_reason": ("official_reason", "reason", "reason_code"),
    "official_market": ("official_market", "market", "listing_market", "exchange"),
}


def _canonical_symbol(value: Any) -> str:
    """Use the dot share-class convention used by the acquired T/Q corpus."""
    return str(value).strip().upper().replace("-", ".")


def _alias_value(row: Mapping[str, Any], target: str) -> Any:
    for name in EXTERNAL_ALIASES[target]:
        if name in row and row[name] is not None and not pd.isna(row[name]):
            return row[name]
    return None


def _official_timestamp(value: Any, session_date: str) -> pd.Timestamp | pd.NaT:
    if value is None or pd.isna(value):
        return pd.NaT
    text = str(value).strip()
    if not text:
        return pd.NaT
    if len(text) <= 15 and ":" in text and "T" not in text and "-" not in text:
        parsed = pd.Timestamp(f"{session_date} {text}", tz="America/New_York")
    else:
        parsed = pd.Timestamp(text)
        if parsed.tzinfo is None:
            parsed = parsed.tz_localize("America/New_York")
    return parsed.tz_convert("UTC")


def normalize_external_halts(
    *,
    input_path: Path,
    provider: str,
    run_dir: Path,
    reference_root: Path,
    request_parameters: Mapping[str, Any] | None = None,
) -> Path:
    input_path = input_path.expanduser().resolve()
    run_dir = run_dir.expanduser().resolve()
    reference_root = reference_root.expanduser().resolve()
    if not input_path.is_file():
        raise FileNotFoundError(input_path)
    output_path = run_dir / "external_halts_normalized.parquet"
    if output_path.exists():
        raise FileExistsError(output_path)
    raw_hash = sha256_file(input_path)
    provider_root = reference_root / "raw/reference/trading_halts" / provider
    provider_root.mkdir(parents=True, exist_ok=True)
    suffixes = "".join(input_path.suffixes) or ".raw"
    cached = provider_root / f"{raw_hash}{suffixes}"
    if cached.exists():
        if sha256_file(cached) != raw_hash:
            raise ValueError(f"immutable external cache collision: {cached}")
    else:
        temporary = _tmp(cached)
        shutil.copyfile(input_path, temporary)
        os.replace(temporary, cached)
    metadata_path = cached.with_name(cached.name + ".metadata.json")
    metadata = {
        "provider": provider,
        "acquired_at_utc": datetime.now(timezone.utc).isoformat(),
        "request_parameters": dict(request_parameters or {}),
        "source_path": str(input_path),
        "cached_path": str(cached),
        "raw_source_sha256": raw_hash,
    }
    if metadata_path.exists():
        existing = json.loads(metadata_path.read_text())
        if (
            existing.get("raw_source_sha256") != raw_hash
            or existing.get("provider") != provider
        ):
            raise ValueError(f"external cache metadata mismatch: {metadata_path}")
    else:
        atomic_json(metadata, metadata_path, replace=False)

    raw = _read_frame(input_path)
    rows: list[dict[str, Any]] = []
    for position, source_row in enumerate(raw.to_dict("records")):
        day_value = _alias_value(source_row, "session_date")
        symbol_value = _alias_value(source_row, "symbol")
        if day_value is None or symbol_value is None:
            raise ValueError(f"external row {position} lacks session_date or symbol")
        day = pd.Timestamp(day_value).date().isoformat()
        symbol = _canonical_symbol(symbol_value)
        start = _official_timestamp(
            _alias_value(source_row, "official_halt_start"), day
        )
        resume_day_value = _alias_value(source_row, "official_resume_date")
        resume_day = (
            day
            if resume_day_value is None
            else pd.Timestamp(resume_day_value).date().isoformat()
        )
        quote_resume = _official_timestamp(
            _alias_value(source_row, "official_quote_resume_time"), resume_day
        )
        trade_resume = _official_timestamp(
            _alias_value(source_row, "official_trade_resume_time"), resume_day
        )
        event_id = _alias_value(source_row, "provider_event_id")
        if event_id is None:
            event_id = canonical_hash(
                {
                    "provider": provider,
                    "session_date": day,
                    "symbol": symbol,
                    "official_halt_start": (
                        None if pd.isna(start) else start.isoformat()
                    ),
                    "official_trade_resume_time": (
                        None if pd.isna(trade_resume) else trade_resume.isoformat()
                    ),
                    "raw_row": position,
                }
            )
        rows.append(
            {
                "provider": provider,
                "provider_event_id": str(event_id),
                "session_date": day,
                "symbol": symbol,
                "official_halt_start": start,
                "official_quote_resume_time": quote_resume,
                "official_trade_resume_time": trade_resume,
                "official_reason": _alias_value(source_row, "official_reason"),
                "official_market": _alias_value(source_row, "official_market"),
                "raw_source_sha256": raw_hash,
                "raw_source_path": str(cached),
            }
        )
    normalized = pd.DataFrame(rows)
    if not normalized.empty:
        if normalized.duplicated(["provider", "provider_event_id"]).any():
            raise ValueError(
                "normalized external records contain duplicate provider event IDs"
            )
        normalized = normalized.sort_values(
            ["session_date", "symbol", "official_halt_start", "provider_event_id"],
            kind="mergesort",
        )
    atomic_parquet(normalized, output_path, replace=False)
    return output_path


def _match_score(
    candidate: Mapping[str, Any], official: Mapping[str, Any], config: DetectionConfig
) -> tuple[int, str] | None:
    candidate_start = _ns(candidate["inferred_halt_interval_start"])
    candidate_end = _ns(candidate["inferred_trade_resume_time"])
    official_start = _ns(official.get("official_halt_start"))
    official_end = _ns(official.get("official_trade_resume_time"))
    if candidate_start is None or candidate_end is None or official_start is None:
        return None
    if official_end is not None:
        overlap = max(
            0, min(candidate_end, official_end) - max(candidate_start, official_start)
        )
        return (overlap, "interval_overlap") if overlap > 0 else None
    if (
        abs(candidate_start - official_start)
        <= config.start_only_match_tolerance_seconds * NS
    ):
        return (1, "start_only_external_match")
    return None


def match_external_one_to_one(
    candidates: pd.DataFrame,
    official: pd.DataFrame,
    config: DetectionConfig | None = None,
) -> pd.DataFrame:
    """Maximum-total-positive-overlap matching within each symbol-day."""

    config = config or DetectionConfig()
    output: list[dict[str, Any]] = []
    keys = sorted(
        set(zip(candidates.session_date.astype(str), candidates.symbol.astype(str)))
        | set(zip(official.session_date.astype(str), official.symbol.astype(str)))
    )
    for day, symbol in keys:
        crows = candidates[
            (candidates.session_date.astype(str) == day)
            & (candidates.symbol.astype(str) == symbol)
        ].to_dict("records")
        orows = official[
            (official.session_date.astype(str) == day)
            & (official.symbol.astype(str) == symbol)
        ].to_dict("records")
        scores: dict[tuple[int, int], tuple[int, str]] = {}
        for i, candidate in enumerate(crows):
            for j, external in enumerate(orows):
                score = _match_score(candidate, external, config)
                if score is not None:
                    scores[(i, j)] = score

        pairs: list[tuple[int, int]] = []
        if len(orows) <= 20:
            from functools import lru_cache

            @lru_cache(maxsize=None)
            def solve(i: int, used: int) -> tuple[int, tuple[tuple[int, int], ...]]:
                if i >= len(crows):
                    return 0, tuple()
                best_score, best_pairs = solve(i + 1, used)
                for j in range(len(orows)):
                    if used & (1 << j) or (i, j) not in scores:
                        continue
                    downstream, downstream_pairs = solve(i + 1, used | (1 << j))
                    total = scores[(i, j)][0] + downstream
                    proposal = ((i, j),) + downstream_pairs
                    if total > best_score or (
                        total == best_score and proposal < best_pairs
                    ):
                        best_score, best_pairs = total, proposal
                return best_score, best_pairs

            pairs = list(solve(0, 0)[1])
        else:
            # Pathological vendor files can contain many events for one symbol.
            # Deterministic maximum-edge matching avoids exponential state while
            # preserving the positive-overlap and one-to-one invariants.
            used_i: set[int] = set()
            used_j: set[int] = set()
            for (i, j), (score, _) in sorted(
                scores.items(), key=lambda item: (-item[1][0], item[0])
            ):
                if i not in used_i and j not in used_j:
                    pairs.append((i, j))
                    used_i.add(i)
                    used_j.add(j)

        for i, j in pairs:
            score, match_type = scores[(i, j)]
            output.append(
                {
                    "candidate_id": crows[i]["candidate_id"],
                    "provider": orows[j]["provider"],
                    "provider_event_id": orows[j]["provider_event_id"],
                    "session_date": day,
                    "symbol": symbol,
                    "match_type": match_type,
                    "positive_overlap_seconds": (
                        score / NS if match_type == "interval_overlap" else None
                    ),
                }
            )
    return pd.DataFrame(
        output,
        columns=[
            "candidate_id",
            "provider",
            "provider_event_id",
            "session_date",
            "symbol",
            "match_type",
            "positive_overlap_seconds",
        ],
    )


def _activity_before_official(
    official_row: Mapping[str, Any], summaries: pd.DataFrame, config: DetectionConfig
) -> bool:
    match = summaries[
        (summaries.session_date.astype(str) == str(official_row["session_date"]))
        & (summaries.symbol.astype(str) == str(official_row["symbol"]))
        & summaries.status.eq("processed")
    ]
    if len(match) != 1:
        return False
    start = _ns(official_row.get("official_halt_start"))
    if start is None:
        return False
    endpoints = match.iloc[0].get("activity_qualified_end_ns")
    if endpoints is None:
        return False
    values = np.asarray(endpoints, dtype=np.int64)
    return bool(
        np.any(
            (values <= start) & (values >= start - config.pre_gap_grace_seconds * NS)
        )
    )


def _official_relevance_reason(
    row: Mapping[str, Any], summaries: pd.DataFrame, config: DetectionConfig
) -> tuple[bool, str]:
    start_ns, end_ns = session_bounds_ns(str(row["session_date"]))
    halt_start = _ns(row.get("official_halt_start"))
    resume = _ns(row.get("official_trade_resume_time"))
    if halt_start is None:
        return False, "missing_official_halt_start"
    effective_end = end_ns if resume is None else resume
    if max(start_ns, halt_start) >= min(end_ns, effective_end):
        return False, "outside_feature_session"
    accepted = summaries[
        (summaries.session_date.astype(str) == str(row["session_date"]))
        & (summaries.symbol.astype(str) == str(row["symbol"]))
        & summaries.status.eq("processed")
    ]
    if accepted.empty:
        return False, "symbol_day_raw_tq_failed_or_not_acquired"
    if resume is None or not (start_ns < resume < end_ns):
        return False, "no_in_session_trade_resumption"
    if not _activity_before_official(row, summaries, config):
        return False, "pre_gap_activity_not_qualified"
    return True, "experiment_relevant"


def _session_segment(value: Any) -> str:
    ts = pd.Timestamp(value).tz_convert("America/New_York")
    minute = ts.hour * 60 + ts.minute
    if minute < 9 * 60 + 30:
        return "premarket"
    if minute < 16 * 60:
        return "rth"
    return "after_hours"


def deterministic_review_sample(
    frame: pd.DataFrame, config: DetectionConfig
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    sample = frame.copy()
    sample["duration_band"] = np.where(
        sample.near_300s_multiple_90s,
        sample.nearest_300s_multiple.map(
            {1: "near_5m", 2: "near_10m", 3: "near_15m", 4: "near_20m"}
        ),
        "non_multiple",
    )
    sample["session_segment"] = sample.inferred_halt_interval_start.map(
        _session_segment
    )
    median_volatility = pd.to_numeric(
        sample.pre_gap_range_bps_300s, errors="coerce"
    ).median()
    sample["pre_gap_volatility_band"] = np.where(
        pd.to_numeric(sample.pre_gap_range_bps_300s, errors="coerce")
        >= median_volatility,
        "high",
        "low",
    )
    sample["quote_activity_band"] = np.where(
        sample.quote_message_count_in_gap > 0, "quote_active", "quote_silent"
    )
    sample["review_rank_hash"] = sample.candidate_id.map(
        lambda value: canonical_hash(
            {"seed": config.manual_review_seed, "candidate_id": value}
        )
    )
    strata = [
        "duration_band",
        "session_segment",
        "instrument_type",
        "pre_gap_volatility_band",
        "quote_activity_band",
    ]
    sample = sample.sort_values(strata + ["review_rank_hash"], kind="mergesort")
    sample["stratum_rank"] = sample.groupby(strata, dropna=False).cumcount()
    sample = sample.sort_values(["stratum_rank", "review_rank_hash"], kind="mergesort")
    return sample.head(min(config.manual_review_target, len(sample))).reset_index(
        drop=True
    )


def _render_review_plot(row: Mapping[str, Any], path: Path) -> None:
    import matplotlib.pyplot as plt

    path.parent.mkdir(parents=True, exist_ok=True)
    raw_times = pd.to_datetime(row.get("review_raw_trade_sip", []), unit="ns", utc=True)
    raw_prices = np.asarray(row.get("review_raw_trade_prices", []), dtype=float)
    eligible_times = pd.to_datetime(
        row.get("review_eligible_trade_sip", []), unit="ns", utc=True
    )
    eligible_prices = np.asarray(
        row.get("review_eligible_trade_prices", []), dtype=float
    )
    quote_times = pd.to_datetime(row.get("review_quote_sip", []), unit="ns", utc=True)
    quote_mid = np.asarray(
        [
            np.nan if value is None else value
            for value in row.get("review_quote_midpoint", [])
        ],
        dtype=float,
    )
    start = pd.Timestamp(row["inferred_halt_interval_start"])
    resume = pd.Timestamp(row["inferred_trade_resume_time"])
    figure, axis = plt.subplots(figsize=(10, 4))
    if len(raw_times):
        axis.scatter(
            raw_times, raw_prices, s=5, alpha=0.25, label="raw finite-price trades"
        )
    if len(eligible_times):
        axis.scatter(
            eligible_times, eligible_prices, s=8, alpha=0.8, label="eligible trades"
        )
    if len(quote_times):
        axis.plot(
            quote_times,
            quote_mid,
            linewidth=0.8,
            alpha=0.7,
            label="valid quote midpoint",
        )
    axis.axvspan(
        start, resume, color="tab:red", alpha=0.14, label="inferred interruption"
    )
    axis.set_title(
        f"{row['session_date']} {row['symbol']} — raw T/Q interruption evidence"
    )
    axis.set_ylabel("price")
    axis.legend(loc="best", fontsize=8)
    figure.autofmt_xdate()
    figure.tight_layout()
    temporary = _tmp(path)
    figure.savefig(temporary, dpi=140, format="png")
    plt.close(figure)
    os.replace(temporary, path)


def _registry_interval_id(row: Mapping[str, Any]) -> str:
    return canonical_hash(
        {
            "registry_version": REGISTRY_VERSION,
            "session_date": str(row["session_date"]),
            "symbol": str(row["symbol"]),
            "halt_interval_start": pd.Timestamp(row["halt_interval_start"]).isoformat(),
            "trade_resume_time": (
                None
                if pd.isna(row.get("trade_resume_time"))
                else pd.Timestamp(row["trade_resume_time"]).isoformat()
            ),
            "acceptance_class": str(row["acceptance_class"]),
        }
    )


def _candidate_registry(
    candidates: pd.DataFrame,
    evidence: pd.DataFrame,
    official: pd.DataFrame,
    matches: pd.DataFrame,
    summaries: pd.DataFrame,
    registry_config_hash: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    combined = candidates.merge(
        evidence,
        on=["candidate_id", "session_date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    matched = (
        matches.merge(
            official,
            on=["provider", "provider_event_id", "session_date", "symbol"],
            how="left",
            validate="one_to_one",
        )
        if not matches.empty
        else matches.copy()
    )
    match_lookup = {row["candidate_id"]: row for row in matched.to_dict("records")}
    rows: list[dict[str, Any]] = []
    for candidate in combined.to_dict("records"):
        external = match_lookup.get(candidate["candidate_id"])
        candidate["external_match"] = external is not None
        if external is not None:
            for name in (
                "provider",
                "provider_event_id",
                "official_halt_start",
                "official_quote_resume_time",
                "official_trade_resume_time",
                "official_reason",
                "official_market",
                "raw_source_sha256",
                "match_type",
                "positive_overlap_seconds",
            ):
                candidate[name] = external.get(name)
            provenance = json.loads(candidate["source_provenance"])
            provenance["external"] = {
                "provider": external.get("provider"),
                "provider_event_id": external.get("provider_event_id"),
                "raw_source_sha256": external.get("raw_source_sha256"),
            }
            candidate["source_provenance"] = canonical_json(provenance)
            if candidate.get("source_health_class") != "possible_source_interruption":
                candidate["source_health_class"] = "healthy_witnessed"
            official_start = _ns(external.get("official_halt_start"))
            official_resume = _ns(external.get("official_trade_resume_time"))
            if official_start is not None and official_resume is not None:
                candidate["halt_interval_start"] = _utc(official_start)
                candidate["trade_resume_time"] = _utc(official_resume)
                candidate["effective_interval_source"] = (
                    "official_halt_and_trade_resume"
                )
            elif official_start is not None:
                candidate["halt_interval_start"] = _utc(official_start)
                candidate["trade_resume_time"] = candidate["inferred_trade_resume_time"]
                candidate["effective_interval_source"] = (
                    "official_start_inferred_trade_resume"
                )
            else:
                candidate["effective_interval_source"] = "conservative_inferred_bounds"
            candidate["acceptance_class"] = "externally_confirmed"
            candidate["accepted_for_experiment"] = True
            candidate["acceptance_reason"] = "matched_authoritative_positive"
        elif candidate.get("source_health_class") == "possible_source_interruption":
            candidate["acceptance_class"] = "possible_source_interruption"
            candidate["accepted_for_experiment"] = False
            candidate["acceptance_reason"] = "cross_symbol_or_upstream_source_concern"
            candidate["effective_interval_source"] = "conservative_inferred_bounds"
        else:
            post = bool(candidate.get("post_gap_activity_qualified"))
            healthy = candidate.get("source_health_class") == "healthy_witnessed"
            pre = bool(candidate.get("pre_gap_activity_qualified"))
            if pre and post and healthy:
                candidate["acceptance_class"] = "inferred_high_confidence"
                candidate["accepted_for_experiment"] = True
                candidate["acceptance_reason"] = (
                    "active_gap_resumption_with_healthy_source_witness"
                )
            elif pre and (post ^ healthy):
                candidate["acceptance_class"] = "probable_trading_interruption"
                candidate["accepted_for_experiment"] = False
                candidate["acceptance_reason"] = (
                    "exactly_one_of_post_activity_or_source_health_unresolved"
                )
            else:
                candidate["acceptance_class"] = "ambiguous_symbol_gap"
                candidate["accepted_for_experiment"] = False
                candidate["acceptance_reason"] = "resumption_or_source_evidence_is_weak"
            candidate["effective_interval_source"] = "conservative_inferred_bounds"
        candidate["detection_method_version"] = DETECTION_METHOD_VERSION
        candidate["registry_version"] = REGISTRY_VERSION
        candidate["registry_config_hash"] = registry_config_hash
        candidate["candidate_origin"] = "raw_tq_exact_candidate"
        candidate["manual_review_status"] = "not_selected"
        rows.append(candidate)

    matched_external_ids = (
        set(zip(matched.provider, matched.provider_event_id))
        if not matched.empty
        else set()
    )
    processed_keys = set(
        zip(
            summaries.loc[summaries.status.eq("processed"), "session_date"].astype(str),
            summaries.loc[summaries.status.eq("processed"), "symbol"].astype(str),
        )
    )
    summary_lookup = {
        (str(row["session_date"]), str(row["symbol"])): row
        for row in summaries.to_dict("records")
        if row.get("status") == "processed"
    }
    external_only_rows: list[dict[str, Any]] = []
    for external in official.to_dict("records"):
        if (
            external["provider"],
            external["provider_event_id"],
        ) in matched_external_ids:
            continue
        key = (str(external["session_date"]), str(external["symbol"]))
        start = _ns(external.get("official_halt_start"))
        resume = _ns(external.get("official_trade_resume_time"))
        usable = (
            key in processed_keys
            and start is not None
            and resume is not None
            and resume > start
        )
        source = summary_lookup.get(key, {})
        row = {
            "candidate_id": None,
            "universe_version": source.get("universe_version"),
            "session_date": key[0],
            "symbol": key[1],
            "instrument_type": source.get("instrument_type"),
            "detection_method_version": DETECTION_METHOD_VERSION,
            "registry_version": REGISTRY_VERSION,
            "registry_config_hash": registry_config_hash,
            "candidate_config_hash": source.get("candidate_config_hash"),
            "candidate_origin": "external_only",
            "halt_interval_start": _utc(start),
            "trade_resume_time": _utc(resume),
            "inferred_halt_interval_start": pd.NaT,
            "inferred_trade_resume_time": pd.NaT,
            "quote_resume_time": external.get("official_quote_resume_time"),
            "effective_interval_source": (
                "official_halt_and_trade_resume"
                if usable
                else "unusable_external_bounds"
            ),
            "acceptance_class": (
                "externally_confirmed" if usable else "ambiguous_symbol_gap"
            ),
            "accepted_for_experiment": bool(usable),
            "acceptance_reason": (
                "unmatched_official_positive_with_usable_bounds"
                if usable
                else "external_positive_lacks_usable_bounds_or_raw_tq_qa"
            ),
            "external_match": False,
            "provider": external.get("provider"),
            "provider_event_id": external.get("provider_event_id"),
            "official_halt_start": external.get("official_halt_start"),
            "official_quote_resume_time": external.get("official_quote_resume_time"),
            "official_trade_resume_time": external.get("official_trade_resume_time"),
            "official_reason": external.get("official_reason"),
            "official_market": external.get("official_market"),
            "raw_source_sha256": external.get("raw_source_sha256"),
            "source_health_class": "healthy_witnessed" if usable else "unresolved",
            "source_provenance": canonical_json(
                {
                    "external": {
                        "provider": external.get("provider"),
                        "event_id": external.get("provider_event_id"),
                        "sha256": external.get("raw_source_sha256"),
                    },
                    "trade": {
                        "path": source.get("trade_source_path"),
                        "sha256": source.get("trade_source_sha256"),
                        "parquet_metadata_sha256": source.get(
                            "trade_source_parquet_metadata_sha256"
                        ),
                    },
                    "quote": {
                        "path": source.get("quote_source_path"),
                        "sha256": source.get("quote_source_sha256"),
                        "parquet_metadata_sha256": source.get(
                            "quote_source_parquet_metadata_sha256"
                        ),
                    },
                }
            ),
            "manual_review_status": "not_applicable_external_only",
        }
        if usable:
            external_only_rows.append(row)
            rows.append(row)
    registry = pd.DataFrame(rows)
    if not registry.empty:
        registry["halt_interval_id"] = [
            _registry_interval_id(row) for row in registry.to_dict("records")
        ]
    return registry, pd.DataFrame(external_only_rows)


def validate_detection_run(
    *,
    run_dir: Path,
    config: DetectionConfig | None = None,
    render_plots: bool = True,
    presence_artifact_name: str = "date_event_presence.parquet",
) -> dict[str, Any]:
    config = config or DetectionConfig()
    run_dir = run_dir.expanduser().resolve()
    candidate_path = run_dir / "exact_candidates.parquet"
    evidence_path = run_dir / "candidate_quote_evidence.parquet"
    external_path = run_dir / "external_halts_normalized.parquet"
    freeze_path = run_dir / "candidate_freeze.json"
    for path in (candidate_path, evidence_path, external_path, freeze_path):
        if not path.is_file():
            raise FileNotFoundError(f"required prior-stage artifact is missing: {path}")
    candidate_freeze = json.loads(freeze_path.read_text())
    if candidate_freeze.get("exact_candidates_sha256") != sha256_file(candidate_path):
        raise ValueError(
            "blind candidate freeze hash no longer matches exact_candidates.parquet"
        )
    run_config = json.loads((run_dir / "run_config.json").read_text())
    if run_config.get("candidate_config_hash") != config.digest():
        raise ValueError(
            "validation configuration differs from frozen detection configuration"
        )

    candidates = pd.read_parquet(candidate_path)
    evidence = pd.read_parquet(evidence_path)
    official = pd.read_parquet(external_path)
    summaries = pd.read_parquet(run_dir / "symbol_day_trade_summaries.parquet")
    base_matches = match_external_one_to_one(candidates, official, config)
    matches = base_matches.copy()
    if not matches.empty:
        candidate_bounds = candidates[
            [
                "candidate_id",
                "inferred_halt_interval_start",
                "inferred_trade_resume_time",
            ]
        ]
        official_bounds = official[
            [
                "provider",
                "provider_event_id",
                "official_halt_start",
                "official_trade_resume_time",
            ]
        ]
        matches = matches.merge(
            candidate_bounds, on="candidate_id", validate="one_to_one"
        )
        matches = matches.merge(
            official_bounds,
            on=["provider", "provider_event_id"],
            validate="one_to_one",
        )
        candidate_duration = (
            pd.to_datetime(matches.inferred_trade_resume_time, utc=True)
            - pd.to_datetime(matches.inferred_halt_interval_start, utc=True)
        ).dt.total_seconds()
        matches["candidate_to_official_overlap_fraction"] = (
            pd.to_numeric(matches.positive_overlap_seconds, errors="coerce")
            / candidate_duration
        )
        matches["inferred_start_minus_official_start_seconds"] = (
            pd.to_datetime(matches.inferred_halt_interval_start, utc=True)
            - pd.to_datetime(matches.official_halt_start, utc=True)
        ).dt.total_seconds()
        matches["inferred_resume_minus_official_trade_resume_seconds"] = (
            pd.to_datetime(matches.inferred_trade_resume_time, utc=True)
            - pd.to_datetime(matches.official_trade_resume_time, utc=True)
        ).dt.total_seconds()
    atomic_parquet(matches, run_dir / "external_matches.parquet")
    registry_config_hash = canonical_hash(
        {
            "detection_method_version": DETECTION_METHOD_VERSION,
            "registry_version": REGISTRY_VERSION,
            "candidate_config_hash": config.digest(),
            "exact_candidates_sha256": sha256_file(candidate_path),
            "external_halts_normalized_sha256": sha256_file(external_path),
        }
    )
    registry, external_only = _candidate_registry(
        candidates, evidence, official, base_matches, summaries, registry_config_hash
    )

    matched_external = (
        set(zip(matches.provider, matches.provider_event_id))
        if not matches.empty
        else set()
    )
    unmatched_rows: list[dict[str, Any]] = []
    official_records: list[dict[str, Any]] = []
    for row in official.to_dict("records"):
        relevant, reason = _official_relevance_reason(row, summaries, config)
        matched = (row["provider"], row["provider_event_id"]) in matched_external
        candidates_for_symbol = candidates[
            (candidates.session_date.astype(str) == str(row["session_date"]))
            & (candidates.symbol.astype(str) == str(row["symbol"]))
        ]
        miss_reason = None
        if not matched:
            if not relevant:
                miss_reason = reason
            elif candidates_for_symbol.empty:
                miss_reason = "no_exact_candidate_after_gap_and_activity_rules"
            else:
                miss_reason = (
                    "frozen_candidate_interval_did_not_match_official_interval"
                )
        enriched = dict(row)
        enriched.update(
            {
                "experiment_relevant": relevant,
                "relevance_reason": reason,
                "detector_matched": matched,
                "failure_reason": miss_reason,
            }
        )
        official_records.append(enriched)
        if not matched:
            unmatched_rows.append(enriched)
    official_eval = pd.DataFrame(official_records)
    if not official_eval.empty:
        official_eval["session_segment"] = official_eval.official_halt_start.map(
            lambda value: None if pd.isna(value) else _session_segment(value)
        )
        durations = (
            pd.to_datetime(official_eval.official_trade_resume_time, utc=True)
            - pd.to_datetime(official_eval.official_halt_start, utc=True)
        ).dt.total_seconds()
        official_eval["duration_band"] = pd.cut(
            durations,
            bins=[-np.inf, 390, 690, 990, 1_290, np.inf],
            labels=[
                "up_to_6_5m",
                "6_5_to_11_5m",
                "11_5_to_16_5m",
                "16_5_to_21_5m",
                "over_21_5m",
            ],
        ).astype("string")
    unmatched_official = pd.DataFrame(
        unmatched_rows, columns=list(official_eval.columns)
    )
    atomic_parquet(unmatched_official, run_dir / "unmatched_official_intervals.parquet")

    dates = (
        sorted(official.session_date.astype(str).unique()) if not official.empty else []
    )
    split_index = int(math.ceil(0.70 * len(dates)))
    development_dates = set(dates[:split_index])
    confirmation_dates = set(dates[split_index:])
    if not official_eval.empty:
        official_eval["detector_split"] = np.where(
            official_eval.session_date.astype(str).isin(confirmation_dates),
            "confirmation",
            "development_audit",
        )
    confirmation_relevant = (
        official_eval[
            official_eval.get("experiment_relevant", pd.Series(dtype=bool)).fillna(
                False
            )
            & official_eval.get("detector_split", pd.Series(dtype=str)).eq(
                "confirmation"
            )
        ]
        if not official_eval.empty
        else official_eval
    )
    recall_denominator = len(confirmation_relevant)
    recall_numerator = (
        int(confirmation_relevant.detector_matched.sum()) if recall_denominator else 0
    )
    interval_recall = (
        recall_numerator / recall_denominator if recall_denominator else None
    )
    relevant_symbol_days = (
        confirmation_relevant[["session_date", "symbol"]].drop_duplicates()
        if recall_denominator
        else pd.DataFrame(columns=["session_date", "symbol"])
    )
    matched_symbol_days = (
        confirmation_relevant.loc[
            confirmation_relevant.detector_matched, ["session_date", "symbol"]
        ].drop_duplicates()
        if recall_denominator
        else relevant_symbol_days
    )
    symbol_day_recall = (
        len(matched_symbol_days) / len(relevant_symbol_days)
        if len(relevant_symbol_days)
        else None
    )

    inferred_pool = (
        registry[
            registry.acceptance_class.eq("inferred_high_confidence")
            & ~registry.external_match.fillna(False)
        ]
        if not registry.empty
        else registry
    )
    review_manifest = deterministic_review_sample(inferred_pool, config)
    review_columns = [
        "candidate_id",
        "session_date",
        "symbol",
        "instrument_type",
        "inferred_halt_interval_start",
        "inferred_trade_resume_time",
        "eligible_gap_seconds",
        "duration_band",
        "session_segment",
        "pre_gap_volatility_band",
        "quote_activity_band",
        "source_health_class",
        "post_gap_activity_qualified",
        "review_rank_hash",
    ]
    review_manifest_path = run_dir / "manual_review_manifest.csv"
    plots_root = run_dir / "review_plots"
    if not review_manifest.empty:
        review_manifest["plot_path"] = [
            str(plots_root / f"{candidate_id}.png")
            for candidate_id in review_manifest.candidate_id
        ]
        if render_plots:
            for row in review_manifest.to_dict("records"):
                plot = Path(row["plot_path"])
                if not plot.exists():
                    _render_review_plot(row, plot)
    else:
        review_manifest["plot_path"] = pd.Series(dtype=str)
    atomic_csv(
        review_manifest.reindex(columns=review_columns + ["plot_path"]),
        review_manifest_path,
    )

    review_path = run_dir / "manual_review.csv"
    if review_path.exists():
        review = pd.read_csv(review_path, keep_default_na=False)
        expected_ids = set(review_manifest.candidate_id.astype(str))
        if set(review.candidate_id.astype(str)) != expected_ids:
            raise ValueError(
                "manual_review.csv candidate IDs differ from the deterministic review manifest"
            )
    else:
        review = review_manifest.reindex(
            columns=["candidate_id", "session_date", "symbol"]
        ).copy()
        review["credible_interruption"] = ""
        review["source_concern"] = ""
        review["review_notes"] = ""
        atomic_csv(review, review_path, replace=False)
    valid_credible = {"yes", "no", "unclear"}
    valid_source = {"yes", "no"}
    credible = (
        review.credible_interruption.astype(str).str.lower()
        if "credible_interruption" in review
        else pd.Series(dtype=str)
    )
    source_concern = (
        review.source_concern.astype(str).str.lower()
        if "source_concern" in review
        else pd.Series(dtype=str)
    )
    review_complete = (
        len(review) == len(review_manifest)
        and credible.isin(valid_credible).all()
        and source_concern.isin(valid_source).all()
        and (
            len(inferred_pool) < config.manual_review_target
            or len(review) >= config.manual_review_target
        )
    )
    credible_fraction = (
        float(credible.eq("yes").mean()) if review_complete and len(review) else None
    )
    no_reviewed_source_concern = (
        bool(~source_concern.eq("yes").any()) if review_complete else False
    )
    if not registry.empty and len(review):
        reviewed_yes = set(review.loc[credible.eq("yes"), "candidate_id"].astype(str))
        reviewed_unclear = set(
            review.loc[credible.eq("unclear"), "candidate_id"].astype(str)
        )
        selected = set(review.candidate_id.astype(str))
        registry.loc[
            registry.candidate_id.astype(str).isin(selected), "manual_review_status"
        ] = "reviewed_no"
        registry.loc[
            registry.candidate_id.astype(str).isin(reviewed_yes), "manual_review_status"
        ] = "reviewed_credible"
        registry.loc[
            registry.candidate_id.astype(str).isin(reviewed_unclear),
            "manual_review_status",
        ] = "reviewed_unclear"

    source_rows_accepted = (
        bool(
            (
                (registry.acceptance_class == "possible_source_interruption")
                & registry.accepted_for_experiment
            ).any()
        )
        if not registry.empty
        else False
    )
    relevant_misses = (
        confirmation_relevant[~confirmation_relevant.detector_matched]
        if recall_denominator
        else confirmation_relevant
    )
    misses_reported = (
        bool(
            relevant_misses.failure_reason.fillna("").astype(str).str.len().gt(0).all()
        )
        if len(relevant_misses)
        else True
    )
    gate_checks = {
        "confirmation_recall_at_least_95_percent": interval_recall is not None
        and interval_recall >= 0.95,
        "every_confirmation_miss_has_reported_reason": misses_reported,
        "no_source_interruption_accepted": not source_rows_accepted,
        "manual_review_complete": review_complete,
        "manual_credible_fraction_at_least_90_percent": credible_fraction is not None
        and credible_fraction >= 0.90,
        "manual_review_has_no_source_concern": no_reviewed_source_concern,
        "configuration_unchanged_since_candidate_freeze": candidate_freeze.get(
            "candidate_config_hash"
        )
        == config.digest(),
    }
    validation_gate_passed = all(gate_checks.values())

    if not registry.empty:
        registry = registry.sort_values(
            [
                "session_date",
                "symbol",
                "halt_interval_start",
                "trade_resume_time",
                "halt_interval_id",
            ],
            kind="mergesort",
            na_position="last",
        ).reset_index(drop=True)
    atomic_parquet(registry, run_dir / "validation_registry.parquet")
    class_rows = []
    if not registry.empty:
        for acceptance_class, frame in registry.groupby(
            "acceptance_class", dropna=False
        ):
            class_rows.append(
                {
                    "dimension": "acceptance_class",
                    "value": acceptance_class,
                    "rows": len(frame),
                    "accepted_rows": int(frame.accepted_for_experiment.sum()),
                }
            )
    if not official_eval.empty:
        for dimension in (
            "official_market",
            "official_reason",
            "session_segment",
            "duration_band",
        ):
            for value, frame in official_eval.groupby(dimension, dropna=False):
                relevant_frame = frame[frame.experiment_relevant]
                class_rows.append(
                    {
                        "dimension": dimension,
                        "value": value,
                        "rows": len(relevant_frame),
                        "accepted_rows": (
                            int(relevant_frame.detector_matched.sum())
                            if len(relevant_frame)
                            else 0
                        ),
                    }
                )
    atomic_csv(
        pd.DataFrame(
            class_rows, columns=["dimension", "value", "rows", "accepted_rows"]
        ),
        run_dir / "validation_by_class.csv",
    )

    summary = {
        "detection_method_version": DETECTION_METHOD_VERSION,
        "registry_version": REGISTRY_VERSION,
        "registry_config_hash": registry_config_hash,
        "development_dates": sorted(development_dates),
        "confirmation_dates": sorted(confirmation_dates),
        "official_experiment_relevant_confirmation_intervals": recall_denominator,
        "official_experiment_relevant_confirmation_matches": recall_numerator,
        "official_experiment_relevant_interval_recall": interval_recall,
        "official_symbol_day_recall": symbol_day_recall,
        "exact_candidate_count": len(candidates),
        "external_match_count": len(matches),
        "external_only_accepted_count": len(external_only),
        "unmatched_official_count": len(unmatched_official),
        "inferred_high_confidence_unmatched_count": len(inferred_pool),
        "manual_review_population": len(review_manifest),
        "manual_review_complete": review_complete,
        "manual_credible_fraction": credible_fraction,
        "validation_gate_checks": gate_checks,
        "validation_gate_passed": validation_gate_passed,
        "external_only_publication_allowed": bool(len(external_only)),
        "inferred_older_year_publication_allowed": validation_gate_passed,
    }
    atomic_json(summary, run_dir / "validation_summary.json")
    report = (
        f"# Historical interruption detection V1\n\n"
        f"The untouched-date recall is {('unavailable' if interval_recall is None else f'{interval_recall:.1%}')} "
        f"({recall_numerator}/{recall_denominator} experiment-relevant official intervals). "
        f"The manual audit is {'complete' if review_complete else 'incomplete'}; "
        f"the inferred-publication gate {'passed' if validation_gate_passed else 'did not pass'}.\n\n"
        f"Exact raw-T/Q candidates: {len(candidates)}. Official matches: {len(matches)}. "
        f"Unmatched official records: {len(unmatched_official)}. External-only accepted intervals: {len(external_only)}.\n"
    )
    atomic_text(report, run_dir / "REPORT.md")
    manifest_files = [
        "run_config.json",
        "processed_symbol_days.parquet",
        "symbol_day_trade_summaries.parquet",
        "exact_candidates.parquet",
        "candidate_freeze.json",
        presence_artifact_name,
        "candidate_quote_evidence.parquet",
        "external_halts_normalized.parquet",
        "external_matches.parquet",
        "unmatched_official_intervals.parquet",
        "manual_review_manifest.csv",
        "manual_review.csv",
        "validation_registry.parquet",
        "validation_summary.json",
        "validation_by_class.csv",
        "REPORT.md",
    ]
    atomic_json(
        {
            "artifact_type": "halt_detection_validation_run",
            "status": (
                "validation_complete" if review_complete else "manual_review_pending"
            ),
            "detection_method_version": DETECTION_METHOD_VERSION,
            "registry_version": REGISTRY_VERSION,
            "registry_config_hash": registry_config_hash,
            "content_sha256": {
                name: sha256_file(run_dir / name)
                for name in manifest_files
                if (run_dir / name).is_file()
            },
        },
        run_dir / "manifest.json",
    )
    return summary


REGISTRY_REQUIRED_COLUMNS = {
    "halt_interval_id",
    "session_date",
    "symbol",
    "halt_interval_start",
    "trade_resume_time",
    "quote_resume_time",
    "accepted_for_experiment",
    "acceptance_class",
    "detection_method_version",
    "registry_version",
    "registry_config_hash",
    "source_provenance",
    "effective_interval_source",
    "candidate_config_hash",
}


def validate_registry_frame(registry: pd.DataFrame, acquired: pd.DataFrame) -> None:
    missing = sorted(REGISTRY_REQUIRED_COLUMNS - set(registry.columns))
    if missing:
        raise ValueError(f"registry missing required columns: {missing}")
    if registry.empty:
        raise ValueError("registry is empty")
    if (
        registry.halt_interval_id.isna().any()
        or registry.halt_interval_id.duplicated().any()
    ):
        raise ValueError("registry contains null or duplicate interval IDs")
    if not registry.detection_method_version.eq(DETECTION_METHOD_VERSION).all():
        raise ValueError("registry detection-method version mismatch")
    if not registry.registry_version.eq(REGISTRY_VERSION).all():
        raise ValueError("registry version mismatch")
    hashes = registry.registry_config_hash.astype(str)
    if not hashes.str.fullmatch(r"[0-9a-f]{64}").all() or hashes.nunique() != 1:
        raise ValueError("registry configuration hashes are invalid or mixed")
    candidate_hashes = registry.candidate_config_hash.dropna().astype(str)
    if (
        not candidate_hashes.str.fullmatch(r"[0-9a-f]{64}").all()
        or candidate_hashes.nunique() > 1
    ):
        raise ValueError("candidate configuration hashes are invalid or mixed")
    if registry.accepted_for_experiment.isna().any():
        raise ValueError("accepted_for_experiment must be non-null")
    if registry.source_provenance.fillna("").astype(str).str.len().eq(0).any():
        raise ValueError("registry row lacks auditable source provenance")
    acquired_keys = set(
        zip(acquired.session_date.astype(str), acquired.symbol.astype(str))
    )
    registry_keys = set(
        zip(registry.session_date.astype(str), registry.symbol.astype(str))
    )
    outside = registry_keys - acquired_keys
    if outside:
        raise ValueError(
            f"registry contains rows outside acquired manifest: {sorted(outside)[:5]}"
        )

    clipped: list[tuple[str, str, int, int, bool, str]] = []
    for row in registry.to_dict("records"):
        start = _ns(row["halt_interval_start"])
        resume = _ns(row.get("trade_resume_time"))
        if start is None:
            raise ValueError("registry interval has null start")
        session_start, session_end = session_bounds_ns(str(row["session_date"]))
        effective_end = (
            session_end if resume is None or resume > session_end else resume
        )
        effective_start = max(session_start, start)
        if resume is not None and resume <= start:
            raise ValueError("registry interval is reversed or zero length")
        if effective_start >= effective_end:
            raise ValueError("registry interval does not overlap the feature session")
        accepted = bool(row["accepted_for_experiment"])
        acceptance_class = str(row["acceptance_class"])
        if acceptance_class == "inferred_high_confidence" and accepted:
            if not (
                bool(row.get("pre_gap_activity_qualified"))
                and bool(row.get("post_gap_activity_qualified"))
                and row.get("source_health_class") == "healthy_witnessed"
            ):
                raise ValueError(
                    "accepted inferred interval lacks mandatory activity/source evidence"
                )
        if acceptance_class == "possible_source_interruption" and accepted:
            raise ValueError("possible source interruption is marked accepted")
        clipped.append(
            (
                str(row["session_date"]),
                str(row["symbol"]),
                effective_start,
                effective_end,
                accepted,
                str(row["halt_interval_id"]),
            )
        )
    accepted_rows = sorted((row for row in clipped if row[4]), key=lambda row: row[:4])
    previous: tuple[str, str, int, int, bool, str] | None = None
    for row in accepted_rows:
        if previous is not None and row[0:2] == previous[0:2] and row[2] < previous[3]:
            raise ValueError(
                f"accepted same-symbol intervals overlap: {previous[5]} and {row[5]}"
            )
        previous = row


def verify_frozen_registry(registry_dir: Path) -> dict[str, Any]:
    registry_dir = registry_dir.expanduser().resolve()
    manifest_path = registry_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    if (
        manifest.get("status") != "frozen"
        or manifest.get("registry_version") != REGISTRY_VERSION
    ):
        raise ValueError("registry manifest is not the frozen V1 registry")
    for name, expected in manifest.get("content_sha256", {}).items():
        path = registry_dir / name
        if not path.is_file() or sha256_file(path) != expected:
            raise ValueError(f"frozen registry tamper detected: {path}")
    registry = pd.read_parquet(registry_dir / "registry.parquet")
    accepted = pd.read_parquet(registry_dir / "accepted_intervals.parquet")
    expected = registry[registry.accepted_for_experiment].reset_index(drop=True)
    if not pa.Table.from_pandas(expected, preserve_index=False).equals(
        pa.Table.from_pandas(accepted.reset_index(drop=True), preserve_index=False)
    ):
        raise ValueError(
            "accepted interval projection differs from authoritative registry"
        )
    return manifest


def freeze_registry(
    *,
    run_dir: Path,
    output_dir: Path,
    external_only: bool = False,
) -> dict[str, Path]:
    run_dir = run_dir.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        raise FileExistsError(
            f"registry version directory is immutable and already exists: {output_dir}"
        )
    summary_path = run_dir / "validation_summary.json"
    draft_path = run_dir / "validation_registry.parquet"
    if not summary_path.is_file() or not draft_path.is_file():
        raise FileNotFoundError("validation stage must complete before registry freeze")
    validation = json.loads(summary_path.read_text())
    if external_only:
        if not validation.get("external_only_publication_allowed"):
            raise ValueError(
                "no usable authoritative external-only intervals are available"
            )
    elif not validation.get("validation_gate_passed"):
        raise ValueError(
            "inferred V1 registry freeze refused: required validation gate did not pass"
        )
    registry = pd.read_parquet(draft_path)
    publication_mode = (
        "external_only" if external_only else "validated_external_and_inferred"
    )
    if external_only:
        registry = registry[registry.acceptance_class.eq("externally_confirmed")].copy()
    acquired = pd.read_parquet(run_dir / "processed_symbol_days.parquet")
    acquired = acquired[acquired.status.eq("processed")]
    if registry.empty:
        raise ValueError("publication selection contains no registry intervals")
    registry = registry.sort_values(
        [
            "session_date",
            "symbol",
            "halt_interval_start",
            "trade_resume_time",
            "halt_interval_id",
        ],
        kind="mergesort",
        na_position="last",
    ).reset_index(drop=True)
    validate_registry_frame(registry, acquired)
    accepted = registry[registry.accepted_for_experiment].reset_index(drop=True)
    staging = output_dir.with_name(f".{output_dir.name}.{os.getpid()}.staging")
    if staging.exists():
        raise FileExistsError(staging)
    staging.mkdir(parents=True)
    try:
        registry_path = staging / "registry.parquet"
        accepted_path = staging / "accepted_intervals.parquet"
        summary_csv = staging / "symbol_day_summary.csv"
        definition_path = staging / "definition.json"
        readme_path = staging / "README.md"
        manifest_path = staging / "manifest.json"
        atomic_parquet(registry, registry_path, replace=False)
        atomic_parquet(accepted, accepted_path, replace=False)
        symbol_day = (
            registry.groupby(["session_date", "symbol"], as_index=False)
            .agg(
                candidate_intervals=("halt_interval_id", "size"),
                accepted_intervals=("accepted_for_experiment", "sum"),
            )
            .sort_values(["session_date", "symbol"], kind="mergesort")
        )
        atomic_csv(symbol_day, summary_csv, replace=False)
        definition = {
            "status": "frozen",
            "publication_mode": publication_mode,
            "detection_method_version": DETECTION_METHOD_VERSION,
            "registry_version": REGISTRY_VERSION,
            "registry_config_hash": validation["registry_config_hash"],
            "accepted_interval_semantics": "[max(T0, halt_interval_start), min(T1, trade_resume_time))",
            "candidate_config": json.loads((run_dir / "run_config.json").read_text())[
                "candidate_config"
            ],
            "validation_summary_sha256": sha256_file(summary_path),
        }
        atomic_json(definition, definition_path, replace=False)
        atomic_text(
            f"# {REGISTRY_VERSION}\n\n"
            f"Immutable {publication_mode} historical research registry. It contains {len(registry)} rows, "
            f"of which {len(accepted)} are accepted for the historical experiment overlay. "
            "It is ex-post research information and is not a live-reproducible status feed.\n",
            readme_path,
            replace=False,
        )
        content = {
            path.name: sha256_file(path)
            for path in (
                registry_path,
                accepted_path,
                summary_csv,
                definition_path,
                readme_path,
            )
        }
        manifest = {
            "artifact_type": "historical_halt_registry",
            "status": "frozen",
            "publication_mode": publication_mode,
            "detection_method_version": DETECTION_METHOD_VERSION,
            "registry_version": REGISTRY_VERSION,
            "registry_config_hash": validation["registry_config_hash"],
            "registry_rows": len(registry),
            "accepted_rows": len(accepted),
            "content_sha256": content,
            "source_run": str(run_dir),
            "source_validation_summary_sha256": sha256_file(summary_path),
        }
        atomic_json(manifest, manifest_path, replace=False)
        os.replace(staging, output_dir)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    verify_frozen_registry(output_dir)
    return {
        "registry": output_dir / "registry.parquet",
        "accepted": output_dir / "accepted_intervals.parquet",
        "manifest": output_dir / "manifest.json",
        "definition": output_dir / "definition.json",
    }


def _set_or_append(
    table: pa.Table, name: str, values: pa.Array | pa.ChunkedArray
) -> pa.Table:
    index = table.schema.get_field_index(name)
    if index >= 0:
        return table.set_column(index, name, values)
    return table.append_column(name, values)


def _accepted_for_symbol_day(
    registry: pd.DataFrame,
    session_date: str,
    symbol: str,
) -> pd.DataFrame:
    return registry[
        registry.accepted_for_experiment.fillna(False)
        & registry.session_date.astype(str).eq(session_date)
        & registry.symbol.astype(str).eq(symbol)
    ].sort_values(["halt_interval_start", "trade_resume_time"], kind="mergesort")


def halt_adjusted_activity_state(
    counts: np.ndarray,
    dollars: np.ndarray,
    interval_ends_ns: np.ndarray,
    halt_active: np.ndarray,
    config: DetectionConfig | None = None,
) -> dict[str, pa.Array]:
    """Rebuild the economic activity clock without advancing it through halts."""

    config = config or DetectionConfig()
    counts = np.asarray(counts, dtype=np.int64)
    dollars = np.asarray(dollars, dtype=np.float64)
    ends = np.asarray(interval_ends_ns, dtype=np.int64)
    halt_active = np.asarray(halt_active, dtype=np.bool_)
    n = len(counts)
    if not (len(dollars) == len(ends) == len(halt_active) == n):
        raise ValueError("halt-adjusted activity inputs differ in length")
    predicate_values = activity_predicate(counts, dollars, config)
    halt_prefix = np.r_[0, np.cumsum(halt_active, dtype=np.int64)]
    available = np.zeros(n, dtype=np.bool_)
    predicate_object = np.full(n, None, dtype=object)
    for row in range(config.activity_window_seconds - 1, n):
        left = row + 1 - config.activity_window_seconds
        if halt_prefix[row + 1] == halt_prefix[left]:
            available[row] = True
            predicate_object[row] = bool(predicate_values[row + 1])

    state = np.full(n, "inactive", dtype=object)
    episode_number = np.full(n, None, dtype=object)
    activation = np.full(n, None, dtype=object)
    origin = np.full(n, None, dtype=object)
    episode_end = np.full(n, None, dtype=object)
    end_reason = np.full(n, None, dtype=object)
    inactivity = np.zeros(n, dtype=np.int16)
    active_episode = False
    current_number = 0
    current_activation: int | None = None
    current_origin: int | None = None
    previous_end: int | None = None
    false_run = 0
    for row in range(n):
        if halt_active[row]:
            if active_episode:
                previous_end = int(ends[row] - NS)
            active_episode = False
            false_run = 0
            continue
        predicate = predicate_object[row]
        if not available[row]:
            if active_episode:
                previous_end = int(ends[row])
                end_reason[row] = "activity_source_unavailable"
                episode_end[row] = previous_end
            active_episode = False
            false_run = 0
            continue
        if not active_episode and predicate is True:
            current_number += 1
            current_activation = int(ends[row])
            candidate_origin = current_activation - config.activity_window_seconds * NS
            current_origin = max(candidate_origin, previous_end or candidate_origin)
            active_episode = True
            false_run = 0
        elif active_episode and predicate is True:
            false_run = 0
        elif active_episode and predicate is False:
            false_run += 1
            if false_run >= 300:
                state[row] = "inactive"
                episode_number[row] = current_number
                activation[row] = current_activation
                origin[row] = current_origin
                inactivity[row] = 300
                previous_end = int(ends[row])
                episode_end[row] = previous_end
                end_reason[row] = "economic_inactivity"
                active_episode = False
                false_run = 0
                continue
        if active_episode:
            state[row] = "active"
            episode_number[row] = current_number
            activation[row] = current_activation
            origin[row] = current_origin
            inactivity[row] = false_run

    return {
        "halt_adjusted_activity_predicate_available": pa.array(available),
        "halt_adjusted_activity_predicate": pa.array(predicate_object, type=pa.bool_()),
        "halt_adjusted_activity_state": pa.array(state, type=pa.string()),
        "halt_adjusted_activity_episode_number": pa.array(
            episode_number, type=pa.int32()
        ),
        "halt_adjusted_activity_activation_time": pa.array(
            activation, type=pa.timestamp("ns", tz="UTC")
        ),
        "halt_adjusted_activity_episode_origin": pa.array(
            origin, type=pa.timestamp("ns", tz="UTC")
        ),
        "halt_adjusted_activity_episode_end": pa.array(
            episode_end, type=pa.timestamp("ns", tz="UTC")
        ),
        "halt_adjusted_activity_end_reason": pa.array(end_reason, type=pa.string()),
        "halt_adjusted_inactivity_run_seconds": pa.array(inactivity, type=pa.int16()),
    }


def apply_halt_overlay(
    panel: pa.Table,
    registry: pd.DataFrame,
    *,
    session_date: str,
    symbol: str,
    registry_manifest: Mapping[str, Any],
    episode_local_fields: Sequence[str] = (),
    maximum_reference_warmup_seconds: int = 1_230,
) -> pa.Table:
    """Apply accepted interval/reset semantics to a historical second panel.

    The returned adjusted continuity ID is the input that downstream rolling or
    episode-local feature construction must use.  Existing episode-local fields
    named explicitly by the caller are nulled through the accepted interval and
    until their pre-halt history can no longer enter a 1,200-endpoint/30-second
    reference.  A primitive-panel build can instead recompute expanding
    episode-local features immediately from the adjusted continuity boundary.
    """

    if "interval_end" not in panel.column_names:
        raise ValueError("overlay panel requires interval_end")
    intervals = _accepted_for_symbol_day(registry, session_date, symbol)
    ends = np.asarray(
        pc.cast(panel["interval_end"].combine_chunks(), pa.int64()).to_numpy(),
        dtype=np.int64,
    )
    starts = ends - NS
    active = np.zeros(panel.num_rows, dtype=np.bool_)
    interval_ids = np.full(panel.num_rows, None, dtype=object)
    close_reason = np.full(panel.num_rows, None, dtype=object)
    resume_reset = np.zeros(panel.num_rows, dtype=np.int64)
    post_activity_warmup = np.zeros(panel.num_rows, dtype=np.bool_)
    post_reference_warmup = np.zeros(panel.num_rows, dtype=np.bool_)
    session_start, session_end = session_bounds_ns(session_date)
    for interval in intervals.to_dict("records"):
        halt_start = max(session_start, int(_ns(interval["halt_interval_start"])))
        resume_value = _ns(interval.get("trade_resume_time"))
        halt_end = (
            session_end
            if resume_value is None or resume_value > session_end
            else resume_value
        )
        mask = (starts < halt_end) & (ends > halt_start)
        if np.any(active & mask):
            raise ValueError("accepted intervals overlap while applying overlay")
        active[mask] = True
        interval_ids[mask] = interval["halt_interval_id"]
        first = np.flatnonzero(mask)
        if first.size:
            close_reason[first[0]] = "halt_registry_interval"
        if halt_end < session_end:
            reset_position = int(np.searchsorted(ends, halt_end, side="right"))
            if reset_position < panel.num_rows:
                resume_reset[reset_position] += 1
            post_activity_warmup |= (ends > halt_end) & (ends <= halt_end + 300 * NS)
            post_reference_warmup |= (ends > halt_end) & (
                ends <= halt_end + maximum_reference_warmup_seconds * NS
            )

    output = panel
    output = _set_or_append(
        output, "halt_registry_applied", pa.array([True] * panel.num_rows)
    )
    output = _set_or_append(
        output,
        "halt_registry_version",
        pa.array([registry_manifest["registry_version"]] * panel.num_rows),
    )
    output = _set_or_append(
        output,
        "halt_registry_config_hash",
        pa.array([registry_manifest["registry_config_hash"]] * panel.num_rows),
    )
    output = _set_or_append(output, "halt_interval_active", pa.array(active))
    output = _set_or_append(
        output, "halt_interval_id", pa.array(interval_ids, type=pa.string())
    )
    output = _set_or_append(
        output, "halt_overlay_end_reason", pa.array(close_reason, type=pa.string())
    )
    output = _set_or_append(
        output, "post_halt_insufficient_activity_warmup", pa.array(post_activity_warmup)
    )
    output = _set_or_append(
        output,
        "post_halt_insufficient_reference_capacity",
        pa.array(post_reference_warmup),
    )
    base_segment = (
        np.asarray(
            panel["continuity_segment_id"].combine_chunks().to_numpy(), dtype=np.int64
        )
        if "continuity_segment_id" in panel.column_names
        else np.zeros(panel.num_rows, dtype=np.int64)
    )
    reset_number = np.cumsum(resume_reset, dtype=np.int64)
    pairs = pd.MultiIndex.from_arrays([base_segment, reset_number])
    adjusted = pd.factorize(pairs, sort=False)[0].astype(np.int64)
    output = _set_or_append(
        output, "halt_adjusted_continuity_segment_id", pa.array(adjusted)
    )

    if {"eligible_trade_count", "eligible_dollar_volume"} <= set(panel.column_names):
        activity_columns = halt_adjusted_activity_state(
            np.asarray(
                panel["eligible_trade_count"].combine_chunks().to_numpy(),
                dtype=np.int64,
            ),
            np.asarray(
                panel["eligible_dollar_volume"].combine_chunks().to_numpy(),
                dtype=np.float64,
            ),
            ends,
            active,
        )
        for name, values in activity_columns.items():
            output = _set_or_append(output, name, values)

    null_mask = active | post_reference_warmup
    for name in episode_local_fields:
        if name not in output.column_names:
            raise ValueError(f"episode-local overlay field is absent: {name}")
        column = output[name].combine_chunks()
        masked = pc.if_else(
            pa.array(null_mask), pa.nulls(panel.num_rows, column.type), column
        )
        output = _set_or_append(output, name, masked)
    metadata = dict(output.schema.metadata or {})
    metadata.update(
        {
            b"halt_registry_applied": b"true",
            b"halt_registry_version": str(
                registry_manifest["registry_version"]
            ).encode(),
            b"halt_registry_config_hash": str(
                registry_manifest["registry_config_hash"]
            ).encode(),
            b"halt_registry_sha256": str(
                registry_manifest["content_sha256"]["registry.parquet"]
            ).encode(),
            b"accepted_intervals_sha256": str(
                registry_manifest["content_sha256"]["accepted_intervals.parquet"]
            ).encode(),
            b"historical_ex_post_overlay": b"true",
        }
    )
    return output.replace_schema_metadata(metadata)


def interval_overlaps_any(
    start_ns: int, end_ns: int, intervals: pd.DataFrame, session_date: str, symbol: str
) -> bool:
    if end_ns <= start_ns:
        raise ValueError("experiment interval is reversed or zero length")
    for row in _accepted_for_symbol_day(intervals, session_date, symbol).to_dict(
        "records"
    ):
        session_start, session_end = session_bounds_ns(session_date)
        halt_start = max(session_start, int(_ns(row["halt_interval_start"])))
        resume = _ns(row.get("trade_resume_time"))
        halt_end = session_end if resume is None or resume > session_end else resume
        if start_ns < halt_end and end_ns > halt_start:
            return True
    return False


def add_experiment_overlap_flags(
    anchors: pd.DataFrame,
    registry: pd.DataFrame,
    *,
    anchor_time: str = "anchor_time",
    reference_start: str = "reference_start",
    reference_end: str = "reference_end",
    diagnostic_start: str = "diagnostic_start",
    diagnostic_end: str = "diagnostic_end",
    future_start: str = "future_start",
    future_end: str = "future_end",
) -> pd.DataFrame:
    result = anchors.copy()
    flags = {
        "anchor_in_accepted_halt": [],
        "reference_overlaps_accepted_halt": [],
        "diagnostic_window_overlaps_accepted_halt": [],
        "future_outcome_overlaps_accepted_halt": [],
        "post_halt_insufficient_activity_warmup": [],
        "post_halt_insufficient_reference_capacity": [],
    }
    for row in result.to_dict("records"):
        day, symbol = str(row["session_date"]), str(row["symbol"])
        point = _ns(row[anchor_time])
        if point is None:
            raise ValueError("anchor time is null")
        intervals = _accepted_for_symbol_day(registry, day, symbol)
        flags["anchor_in_accepted_halt"].append(
            interval_overlaps_any(point, point + 1, registry, day, symbol)
        )
        for output_name, left_name, right_name in (
            ("reference_overlaps_accepted_halt", reference_start, reference_end),
            (
                "diagnostic_window_overlaps_accepted_halt",
                diagnostic_start,
                diagnostic_end,
            ),
            ("future_outcome_overlaps_accepted_halt", future_start, future_end),
        ):
            left, right = _ns(row.get(left_name)), _ns(row.get(right_name))
            flags[output_name].append(
                False
                if left is None or right is None
                else interval_overlaps_any(left, right, registry, day, symbol)
            )
        activity_warmup = False
        reference_warmup = False
        for interval in intervals.to_dict("records"):
            resume = _ns(interval.get("trade_resume_time"))
            if resume is not None and resume <= point:
                activity_warmup |= point < resume + 300 * NS
                reference_warmup |= point < resume + 1_230 * NS
        flags["post_halt_insufficient_activity_warmup"].append(activity_warmup)
        flags["post_halt_insufficient_reference_capacity"].append(reference_warmup)
    for name, values in flags.items():
        result[name] = values
    return result


def overlay_file(
    *,
    panel_path: Path,
    registry_dir: Path,
    output_path: Path,
    session_date: str,
    symbol: str,
    episode_local_fields: Sequence[str] = (),
) -> Path:
    panel_path = panel_path.expanduser().resolve()
    output_path = output_path.expanduser().resolve()
    if output_path.exists():
        raise FileExistsError(output_path)
    manifest = verify_frozen_registry(registry_dir)
    registry = pd.read_parquet(Path(registry_dir) / "registry.parquet")
    raw_hash = sha256_file(panel_path)
    panel = pq.read_table(panel_path)
    adjusted = apply_halt_overlay(
        panel,
        registry,
        session_date=session_date,
        symbol=symbol,
        registry_manifest=manifest,
        episode_local_fields=episode_local_fields,
    )
    atomic_parquet(adjusted, output_path, replace=False)
    if sha256_file(panel_path) != raw_hash:
        output_path.unlink(missing_ok=True)
        raise ValueError("source panel changed while applying halt overlay")
    atomic_json(
        {
            "artifact_type": "halt_adjusted_historical_panel",
            "historical_ex_post_overlay": True,
            "source_panel": str(panel_path),
            "source_panel_sha256": raw_hash,
            "output_panel_sha256": sha256_file(output_path),
            "halt_registry_path": str(Path(registry_dir).expanduser().resolve()),
            "registry_version": manifest["registry_version"],
            "detection_method_version": manifest["detection_method_version"],
            "registry_config_hash": manifest["registry_config_hash"],
            "registry_sha256": manifest["content_sha256"]["registry.parquet"],
            "accepted_intervals_sha256": manifest["content_sha256"][
                "accepted_intervals.parquet"
            ],
        },
        output_path.with_name(output_path.name + ".manifest.json"),
        replace=False,
    )
    return output_path


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    detect = subparsers.add_parser(
        "detect", help="stream raw trades and freeze exact T/Q candidates"
    )
    detect.add_argument("--manifest", required=True, type=Path)
    detect.add_argument("--tick-data-root", required=True, type=Path)
    detect.add_argument("--run-dir", required=True, type=Path)
    detect.add_argument("--start")
    detect.add_argument("--end")
    detect.add_argument("--resume", action="store_true")
    detect.add_argument("--allow-unverified-manifest", action="store_true")
    detect.add_argument("--batch-size", type=int, default=DetectionConfig.batch_size)
    detect.add_argument(
        "--workers",
        type=int,
        choices=[1],
        default=1,
        help="V1 defaults to one bounded sequential worker",
    )
    detect.add_argument("--checkpoint-root", type=Path)
    detect.add_argument(
        "--checkpoint-read-root", type=Path, action="append", default=[]
    )

    enrich = subparsers.add_parser(
        "enrich", help="read quotes only for exact-candidate symbol-days"
    )
    enrich.add_argument("--run-dir", required=True, type=Path)
    enrich.add_argument("--batch-size", type=int, default=DetectionConfig.batch_size)

    normalize = subparsers.add_parser(
        "normalize-external", help="immutably cache and normalize official positives"
    )
    normalize.add_argument("--input", required=True, type=Path)
    normalize.add_argument("--provider", required=True)
    normalize.add_argument("--run-dir", required=True, type=Path)
    normalize.add_argument("--data-root", required=True, type=Path)
    normalize.add_argument("--request-parameters-json", default="{}")

    validate = subparsers.add_parser(
        "validate", help="blind-match, split, audit, and evaluate the V1 gate"
    )
    validate.add_argument("--run-dir", required=True, type=Path)
    validate.add_argument("--no-review-plots", action="store_true")

    freeze = subparsers.add_parser(
        "freeze", help="publish an immutable curated registry"
    )
    freeze.add_argument("--run-dir", required=True, type=Path)
    freeze.add_argument("--output-dir", required=True, type=Path)
    freeze.add_argument("--external-only", action="store_true")

    overlay = subparsers.add_parser(
        "overlay", help="apply a frozen registry to a historical second panel"
    )
    overlay.add_argument("--panel", required=True, type=Path)
    overlay.add_argument("--registry-dir", required=True, type=Path)
    overlay.add_argument("--output", required=True, type=Path)
    overlay.add_argument("--session-date", required=True)
    overlay.add_argument("--symbol", required=True)
    overlay.add_argument("--episode-local-field", action="append", default=[])

    verify = subparsers.add_parser(
        "verify", help="rehash and verify an immutable curated registry"
    )
    verify.add_argument("--registry-dir", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.command == "detect":
        if args.batch_size <= 0:
            raise ValueError("batch size must be positive")
        outputs = detect_candidates(
            manifest_path=args.manifest,
            tick_data_root=args.tick_data_root,
            run_dir=args.run_dir,
            start=args.start,
            end=args.end,
            config=DetectionConfig(batch_size=args.batch_size),
            resume=args.resume,
            allow_unverified_manifest=args.allow_unverified_manifest,
            checkpoint_root=args.checkpoint_root,
            checkpoint_read_roots=args.checkpoint_read_root,
        )
        print(
            f"Frozen {pq.ParquetFile(outputs['candidates']).metadata.num_rows} exact candidates at {outputs['candidates']}"
        )
    elif args.command == "enrich":
        output = enrich_candidates(
            run_dir=args.run_dir, config=DetectionConfig(batch_size=args.batch_size)
        )
        print(f"Candidate quote/source evidence: {output}")
    elif args.command == "normalize-external":
        parameters = json.loads(args.request_parameters_json)
        if not isinstance(parameters, dict):
            raise ValueError("request parameters JSON must be an object")
        output = normalize_external_halts(
            input_path=args.input,
            provider=args.provider,
            run_dir=args.run_dir,
            reference_root=args.data_root,
            request_parameters=parameters,
        )
        print(f"Normalized official positives: {output}")
    elif args.command == "validate":
        summary = validate_detection_run(
            run_dir=args.run_dir, render_plots=not args.no_review_plots
        )
        print(
            f"Validation gate passed={summary['validation_gate_passed']} "
            f"recall={summary['official_experiment_relevant_interval_recall']}"
        )
    elif args.command == "freeze":
        outputs = freeze_registry(
            run_dir=args.run_dir,
            output_dir=args.output_dir,
            external_only=args.external_only,
        )
        print(f"Frozen registry: {outputs['registry']}")
    elif args.command == "overlay":
        output = overlay_file(
            panel_path=args.panel,
            registry_dir=args.registry_dir,
            output_path=args.output,
            session_date=args.session_date,
            symbol=args.symbol.upper(),
            episode_local_fields=args.episode_local_field,
        )
        print(f"Halt-adjusted historical panel: {output}")
    elif args.command == "verify":
        manifest = verify_frozen_registry(args.registry_dir)
        print(
            f"Verified {manifest['registry_version']} rows={manifest['registry_rows']} "
            f"accepted={manifest['accepted_rows']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
