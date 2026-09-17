"""Select one reproducible structured-tape window per symbol-day.

A symbol-day qualifies when one fully observed, single-session window contains
at least ``--min-occupancy`` matching seconds.  The input is the projected
one-second Parquet produced by the Section 5 screen; this script does not read
raw T/Q or recalculate features.

The default definition is a 600-second window with at least 50% of seconds
simultaneously satisfying the screen.  If several windows qualify, the script
selects the highest-occupancy window, breaking ties by earliest endpoint and
then session name.

Example:

    python scripts/research/select_structured_tape_windows.py \
      --input projected.parquet --output structured-windows.parquet
"""
from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import json
from pathlib import Path
import time

import duckdb
import pyarrow.parquet as pq


REQUIRED_COLUMNS = {
    "session_date",
    "symbol",
    "session",
    "interval_end_ns",
    "eligible",
    "matching",
}


def _arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--window-seconds", type=int, default=600)
    parser.add_argument("--min-occupancy", default="0.50")
    parser.add_argument("--memory-limit", default="1536MiB")
    return parser.parse_args(argv)


def _minimum_matches(window_seconds: int, occupancy_text: str):
    if not 1 <= window_seconds <= 86_400:
        raise ValueError("window seconds must be in 1..86400")
    try:
        occupancy = Decimal(occupancy_text)
    except InvalidOperation as error:
        raise ValueError("minimum occupancy must be a decimal in (0,1]") from error
    if not Decimal("0") < occupancy <= Decimal("1"):
        raise ValueError("minimum occupancy must be a decimal in (0,1]")
    matches = int(
        (occupancy * Decimal(window_seconds)).to_integral_value(
            rounding=ROUND_CEILING
        )
    )
    return occupancy, matches


def _validate_input(path: Path):
    if not path.is_file():
        raise FileNotFoundError(path)
    parquet = pq.ParquetFile(path)
    columns = set(parquet.schema_arrow.names)
    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        raise ValueError(f"projected input lacks required columns: {missing}")
    return parquet.metadata.num_rows


def _selection_sql(window_seconds: int, minimum_matches: int):
    preceding = window_seconds - 1
    exact_span_ns = preceding * 1_000_000_000
    window_ns = window_seconds * 1_000_000_000
    return f"""
WITH rolled AS (
    SELECT
        session_date,
        symbol,
        session,
        interval_end_ns AS window_end_ns,
        min(interval_end_ns) OVER selected_window AS first_endpoint_ns,
        count(*) OVER selected_window AS represented_seconds,
        sum(eligible::INTEGER) OVER selected_window AS eligible_seconds,
        sum(matching::INTEGER) OVER selected_window AS matching_seconds
    FROM read_parquet(?, hive_partitioning = false)
    WINDOW selected_window AS (
        PARTITION BY session_date, symbol, session
        ORDER BY interval_end_ns
        ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
    )
), candidates AS (
    SELECT
        session_date,
        symbol,
        session,
        window_end_ns - {window_ns} AS window_start_ns,
        window_end_ns,
        represented_seconds,
        eligible_seconds,
        matching_seconds,
        matching_seconds::DOUBLE / represented_seconds AS occupancy
    FROM rolled
    WHERE represented_seconds = {window_seconds}
      AND eligible_seconds = {window_seconds}
      AND matching_seconds >= {minimum_matches}
      AND window_end_ns - first_endpoint_ns = {exact_span_ns}
), ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY session_date, symbol
        ORDER BY matching_seconds DESC, window_end_ns ASC, session ASC
    ) AS selection_rank
    FROM candidates
)
SELECT
    session_date,
    symbol,
    session,
    window_start_ns,
    window_end_ns,
    CAST(make_timestamp_ns(window_start_ns) AT TIME ZONE 'UTC' AS VARCHAR)
        AS window_start_utc,
    CAST(make_timestamp_ns(window_end_ns) AT TIME ZONE 'UTC' AS VARCHAR)
        AS window_end_utc,
    represented_seconds,
    eligible_seconds,
    matching_seconds,
    occupancy
FROM ranked
WHERE selection_rank = 1
ORDER BY session_date, symbol
"""


def run(input_path, output_path, *, window_seconds=600, min_occupancy="0.50", memory_limit="1536MiB"):
    input_path = Path(input_path).resolve()
    output_path = Path(output_path).resolve()
    input_rows = _validate_input(input_path)
    occupancy, minimum_matches = _minimum_matches(window_seconds, min_occupancy)
    if output_path.exists():
        raise FileExistsError(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    connection = duckdb.connect(
        ":memory:",
        config={
            "threads": "1",
            "memory_limit": memory_limit,
            "max_temp_directory_size": "0B",
        },
    )
    try:
        table = connection.execute(
            _selection_sql(window_seconds, minimum_matches), [str(input_path)]
        ).to_arrow_table()
    finally:
        connection.close()
    pq.write_table(table, output_path, compression="zstd")
    elapsed = time.perf_counter() - started
    result = {
        "definition": "best_fully_observed_single_session_occupancy_window_v1",
        "input": str(input_path),
        "input_rows": input_rows,
        "output": str(output_path),
        "qualified_symbol_days": table.num_rows,
        "window_seconds": window_seconds,
        "minimum_occupancy": str(occupancy),
        "minimum_matching_seconds": minimum_matches,
        "selection": "maximum matching seconds; earliest endpoint; session name",
        "elapsed_seconds": round(elapsed, 6),
    }
    print(json.dumps(result, sort_keys=True))
    return result


def main(argv=None):
    args = _arguments(argv)
    run(
        args.input,
        args.output,
        window_seconds=args.window_seconds,
        min_occupancy=args.min_occupancy,
        memory_limit=args.memory_limit,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
