"""Bounded serialization and exact extended-session clocks."""

from datetime import datetime, time
from zoneinfo import ZoneInfo
from pathlib import Path
import hashlib
import json
import re

MINUTE_NS = 60_000_000_000
BATCH_ROWS = 4096
MAX_PAGE_BYTES = 8 * 1024 * 1024


def bounds(day, stream="trades"):
    start = time(3, 55) if stream == "quotes" else time(4)
    date = datetime.strptime(day, "%Y-%m-%d").date()
    return tuple(
        int(datetime.combine(date, t, ZoneInfo("America/New_York")).timestamp())
        * 1_000_000_000
        for t in (start, time(20))
    )


def validate_symbol(symbol):
    if not isinstance(symbol, str) or not re.fullmatch(
        r"[A-Z0-9][A-Z0-9._-]{0,31}", symbol
    ):
        raise ValueError("Expected safe uppercase equity symbol")
    return symbol


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def dump(path, value):
    with Path(path).open("x") as output:
        json.dump(value, output, sort_keys=True, indent=2, allow_nan=False)
        output.write("\n")


def records(path):
    """JSONL has a decoded-record byte ceiling; Parquet uses fixed row batches."""
    path = Path(path)
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq

        for batch in pq.ParquetFile(path).iter_batches(
            batch_size=BATCH_ROWS, use_threads=False
        ):
            yield from batch.to_pylist()
    else:
        with path.open("rb") as source:
            while line := source.readline(MAX_PAGE_BYTES + 1):
                if len(line) > MAX_PAGE_BYTES:
                    raise ValueError("JSONL record exceeds byte ceiling")
                yield json.loads(line)
