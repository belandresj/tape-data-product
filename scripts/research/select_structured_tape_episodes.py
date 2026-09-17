"""Reduce one-second tape matches into gap-tolerant research episodes.

The input is the projected one-second Parquet produced by the Section 5 screen.
An episode starts on a matching second, remains open while consecutive eligible
nonmatches stay below ``--off-delay-seconds``, and splits immediately across an
unavailable observation or session boundary.  Retained episodes meet both the
minimum elapsed duration and minimum matching occupancy.

This is a research reducer.  It does not recalculate features or read raw T/Q.

Example:

    python scripts/research/select_structured_tape_episodes.py \
      --input projected.parquet --date 2026-06-01 \
      --date 2026-06-02 --date 2026-06-03 \
      --output structured-episodes.parquet

Repeat ``--date`` to process several dates in one output.  Dates not named on
the command line are never scanned into the result.
"""
from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
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
    parser.add_argument("--date", required=True, action="append", dest="dates")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--off-delay-seconds", type=int, default=30)
    parser.add_argument("--minimum-episode-seconds", type=int, default=600)
    parser.add_argument("--minimum-occupancy", default="0.50")
    parser.add_argument("--memory-limit", default="512MiB")
    return parser.parse_args(argv)


def _validated_parameters(off_delay_seconds, minimum_episode_seconds, occupancy_text):
    if not 1 <= off_delay_seconds <= 3_600:
        raise ValueError("off delay seconds must be in 1..3600")
    if not 1 <= minimum_episode_seconds <= 86_400:
        raise ValueError("minimum episode seconds must be in 1..86400")
    try:
        occupancy = Decimal(occupancy_text)
    except InvalidOperation as error:
        raise ValueError("minimum occupancy must be a decimal in (0,1]") from error
    if not Decimal("0") < occupancy <= Decimal("1"):
        raise ValueError("minimum occupancy must be a decimal in (0,1]")
    return occupancy


def _validate_input(path):
    if not path.is_file():
        raise FileNotFoundError(path)
    parquet = pq.ParquetFile(path)
    missing = sorted(REQUIRED_COLUMNS - set(parquet.schema_arrow.names))
    if missing:
        raise ValueError(f"projected input lacks required columns: {missing}")
    return parquet.metadata.num_rows


def _episode_sql(
    off_delay_seconds, minimum_episode_seconds, occupancy, *, date_count=1
):
    off_delay_ns = off_delay_seconds * 1_000_000_000
    occupancy_sql = format(occupancy, "f")
    date_parameters = ", ".join("?" for _ in range(date_count))
    return f"""
WITH scoped AS (
    SELECT
        session_date,
        symbol,
        session,
        interval_end_ns,
        eligible,
        matching,
        sum((NOT eligible)::INTEGER) OVER (
            PARTITION BY session_date, symbol, session
            ORDER BY interval_end_ns
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS availability_segment
    FROM read_parquet(?, hive_partitioning = false)
    WHERE session_date IN ({date_parameters})
), matching_rows AS (
    SELECT
        *,
        lag(interval_end_ns) OVER (
            PARTITION BY session_date, symbol, session, availability_segment
            ORDER BY interval_end_ns
        ) AS previous_match_ns
    FROM scoped
    WHERE matching
), marked AS (
    SELECT
        *,
        CASE
            WHEN previous_match_ns IS NULL
              OR interval_end_ns - previous_match_ns > {off_delay_ns}
            THEN 1 ELSE 0
        END AS new_episode,
        CASE
            WHEN previous_match_ns IS NULL
              OR interval_end_ns - previous_match_ns > {off_delay_ns}
            THEN 0
             ELSE CAST((interval_end_ns - previous_match_ns) / 1000000000 AS BIGINT) - 1
        END AS preceding_nonmatching_gap_seconds
    FROM matching_rows
), assigned AS (
    SELECT
        *,
        sum(new_episode) OVER (
            PARTITION BY session_date, symbol, session, availability_segment
            ORDER BY interval_end_ns
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS episode_group
    FROM marked
), aggregated AS (
    SELECT
        session_date,
        symbol,
        session,
        availability_segment,
        episode_group,
        min(interval_end_ns) AS first_matching_endpoint_ns,
        max(interval_end_ns) AS last_matching_endpoint_ns,
        count(*) AS matching_seconds,
        max(preceding_nonmatching_gap_seconds) AS maximum_nonmatching_gap_seconds
    FROM assigned
    GROUP BY
        session_date, symbol, session, availability_segment, episode_group
), measured AS (
    SELECT
        *,
        first_matching_endpoint_ns - 1000000000 AS episode_start_ns,
        last_matching_endpoint_ns AS episode_end_ns,
        CAST(
            (last_matching_endpoint_ns - first_matching_endpoint_ns)
                / 1000000000 + 1
            AS BIGINT
        ) AS elapsed_seconds
    FROM aggregated
), retained AS (
    SELECT
        *,
        elapsed_seconds - matching_seconds AS eligible_nonmatching_seconds,
        matching_seconds::DOUBLE / elapsed_seconds AS occupancy
    FROM measured
    WHERE elapsed_seconds >= {minimum_episode_seconds}
      AND matching_seconds::DECIMAL / elapsed_seconds >= {occupancy_sql}
), numbered AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY session_date, symbol, session
            ORDER BY episode_start_ns
        ) AS episode_id
    FROM retained
)
SELECT
    session_date,
    symbol,
    session,
    episode_id,
    episode_start_ns,
    episode_end_ns,
    CAST(make_timestamp_ns(episode_start_ns) AT TIME ZONE 'UTC' AS VARCHAR)
        AS episode_start_utc,
    CAST(make_timestamp_ns(episode_end_ns) AT TIME ZONE 'UTC' AS VARCHAR)
        AS episode_end_utc,
    elapsed_seconds,
    matching_seconds,
    eligible_nonmatching_seconds,
    occupancy,
    maximum_nonmatching_gap_seconds
FROM numbered
ORDER BY session_date, symbol, session, episode_start_ns
"""


def run(
    input_path,
    output_path,
    *,
    session_date=None,
    session_dates=None,
    off_delay_seconds=30,
    minimum_episode_seconds=600,
    minimum_occupancy="0.50",
    memory_limit="512MiB",
):
    if session_dates is None:
        session_dates = [session_date] if session_date is not None else []
    elif session_date is not None:
        raise ValueError("provide session_date or session_dates, not both")
    session_dates = tuple(dict.fromkeys(session_dates))
    if not session_dates or any(not value for value in session_dates):
        raise ValueError("at least one session date is required")
    input_path = Path(input_path).resolve()
    output_path = Path(output_path).resolve()
    input_rows = _validate_input(input_path)
    occupancy = _validated_parameters(
        off_delay_seconds, minimum_episode_seconds, minimum_occupancy
    )
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
        invalid = connection.execute(
            "SELECT count(*) FROM read_parquet(?, hive_partitioning=false) "
            f"WHERE session_date IN ({', '.join('?' for _ in session_dates)}) "
            "AND matching AND NOT eligible",
            [str(input_path), *session_dates],
        ).fetchone()[0]
        if invalid:
            raise ValueError("matching observations must also be eligible")
        table = connection.execute(
            _episode_sql(
                off_delay_seconds,
                minimum_episode_seconds,
                occupancy,
                date_count=len(session_dates),
            ),
            [str(input_path), *session_dates],
        ).to_arrow_table()
    finally:
        connection.close()
    pq.write_table(table, output_path, compression="zstd")
    result = {
        "definition": "structured_tape_episode_v1",
        "input": str(input_path),
        "input_rows": input_rows,
        "session_dates": list(session_dates),
        "output": str(output_path),
        "retained_episodes": table.num_rows,
        "retained_symbol_days": len(
            set(
                zip(
                    table.column("session_date").to_pylist(),
                    table.column("symbol").to_pylist(),
                )
            )
        ),
        "off_delay_seconds": off_delay_seconds,
        "minimum_episode_seconds": minimum_episode_seconds,
        "minimum_occupancy": str(occupancy),
        "elapsed_seconds": round(time.perf_counter() - started, 6),
    }
    print(json.dumps(result, sort_keys=True))
    return result


def main(argv=None):
    args = _arguments(argv)
    run(
        args.input,
        args.output,
        session_dates=args.dates,
        off_delay_seconds=args.off_delay_seconds,
        minimum_episode_seconds=args.minimum_episode_seconds,
        minimum_occupancy=args.minimum_occupancy,
        memory_limit=args.memory_limit,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
