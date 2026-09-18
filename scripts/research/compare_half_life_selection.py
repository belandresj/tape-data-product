#!/usr/bin/env python3
"""Compare the existing 30-second and 120-second EW research screens.

The runner reads one bounded, ordered projection through ``open_tape_database``.
It never reads canonical Parquet files directly and deletes the detailed
projection after producing compact aggregate/audit artifacts.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
from decimal import Decimal
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import time
from typing import NamedTuple

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from tape_data_product.query import open_tape_database
from tape_data_product.query.endpoint_catalog import read_endpoint_query_catalog


PILOT_START = date(2026, 6, 1)
PILOT_END = date(2026, 6, 5)
HALF_LIVES = (30, 120)
OFF_DELAY_SECONDS = 30
MINIMUM_EPISODE_SECONDS = 600
MINIMUM_OCCUPANCY = Decimal("0.80")
FREE_DISK_RESERVE_BYTES = 20 * 1024**3
CHECKPOINT_SCHEMA = "half_life_selection_checkpoint_v1"
EXPECTED_FAST_BASELINE = {
    "eligible_endpoints": 15_995_023,
    "matching_endpoints": 189_162,
    "symbol_days_with_match": 177,
    "retained_periods": 55,
    "retained_symbol_days": 35,
    "retained_symbols": 31,
}


class Condition(NamedTuple):
    key: str
    field: str
    operator: str
    threshold: str

    def column(self, half_life: int) -> str:
        return self.field.format(h=half_life)


CONDITIONS = (
    Condition("movement", "midpoint_rms_5s_bps_hl{h}s", ">", "10"),
    Condition("spread", "quoted_spread_bps_hl{h}s", "<", "100"),
    Condition("movement_to_spread", "midpoint_rms_5s_to_spread_hl{h}s", ">", "2"),
    Condition("participation", "movement_participation_hl{h}s", ">=", "0.4"),
    Condition("trade_rate", "trade_rate_per_second_hl{h}s", ">=", "10"),
    Condition("quote_freshness", "quote_age_p90_seconds_window60s", "<=", "2"),
    Condition("trade_freshness", "trade_age_p90_seconds_window60s", "<=", "2"),
)

KEY_COLUMNS = ("session_date", "symbol", "session", "interval_end_ns")
FEATURE_COLUMNS = tuple(
    dict.fromkeys(condition.column(half_life) for half_life in HALF_LIVES for condition in CONDITIONS)
)
PILOT_DATES = tuple(
    (PILOT_START.fromordinal(PILOT_START.toordinal() + offset)).isoformat()
    for offset in range((PILOT_END - PILOT_START).days + 1)
)


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _predicate(condition: Condition, half_life: int) -> str:
    return f"{_quote(condition.column(half_life))} {condition.operator} {condition.threshold}"


def _availability(half_life: int) -> str:
    return " AND ".join(
        f"{_quote(condition.column(half_life))} IS NOT NULL" for condition in CONDITIONS
    )


def _all_pass(half_life: int, *, omit: str | None = None) -> str:
    predicates = [
        _predicate(condition, half_life)
        for condition in CONDITIONS
        if condition.key != omit
    ]
    return " AND ".join(predicates) if predicates else "TRUE"


def _pass_all(label: str, *, omit: str | None = None) -> str:
    predicates = [
        f"{label}_pass_{condition.key}"
        for condition in CONDITIONS
        if condition.key != omit
    ]
    return " AND ".join(predicates) if predicates else "TRUE"


def _parse_date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label} must be an ISO date") from error


def _validate_scope(start_text: str, end_text: str, allow_expanded_scope: bool) -> tuple[date, date]:
    start = _parse_date(start_text, "start date")
    end = _parse_date(end_text, "end date")
    if start > end:
        raise ValueError("date bounds are reversed")
    outside = start < PILOT_START or end > PILOT_END
    if outside and not allow_expanded_scope:
        raise ValueError(
            "dates outside 2026-06-01..2026-06-05 require --allow-expanded-scope"
        )
    return start, end


def _arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default=os.environ.get("QUERY_CATALOG"))
    parser.add_argument("--identity", default=os.environ.get("QUERY_CATALOG_IDENTITY"))
    parser.add_argument("--base-root", default=os.environ.get("BASE_ROOT"))
    parser.add_argument("--feature-root", default=os.environ.get("FEATURE_ROOT"))
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--allow-expanded-scope", action="store_true")
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--memory-limit", default="4GiB")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--temp-directory", type=Path)
    parser.add_argument("--max-temp-directory-size", default="8GiB")
    return parser.parse_args(argv)


def _require_product_arguments(args) -> None:
    missing = [
        name
        for name, value in (
            ("--catalog/QUERY_CATALOG", args.catalog),
            ("--identity/QUERY_CATALOG_IDENTITY", args.identity),
            ("--base-root/BASE_ROOT", args.base_root),
            ("--feature-root/FEATURE_ROOT", args.feature_root),
        )
        if not value
    ]
    if missing:
        raise ValueError("missing required product configuration: " + ", ".join(missing))
    if not 1 <= args.batch_size <= 25_000:
        raise ValueError("batch size must be in 1..25000")
    if not 1 <= args.threads <= 8:
        raise ValueError("analysis threads must be in 1..8")
    if not isinstance(args.max_temp_directory_size, str) or not args.max_temp_directory_size:
        raise ValueError("max temp directory size must be a nonempty DuckDB size string")


def _load_projection_sql() -> str:
    path = Path(__file__).with_name("half_life_comparison") / "projection.sql"
    return path.read_text(encoding="utf-8").strip()


def _load_reducer():
    path = Path(__file__).with_name("select_structured_tape_episodes.py")
    spec = importlib.util.spec_from_file_location("half_life_episode_reducer", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_projection_results(results, path: Path, batch_size: int) -> tuple[int, int]:
    writer = None
    rows = 0
    buffered = []
    buffered_rows = 0
    target_row_group_rows = max(65_536, batch_size)

    def flush():
        nonlocal buffered, buffered_rows
        if not buffered:
            return
        writer.write_table(
            pa.Table.from_batches(buffered), row_group_size=target_row_group_rows
        )
        buffered = []
        buffered_rows = 0

    try:
        for result in results:
            for batch in result.arrow_batches(batch_size=batch_size):
                if writer is None:
                    writer = pq.ParquetWriter(path, batch.schema, compression="zstd")
                elif not batch.schema.equals(writer.schema):
                    raise ValueError("member projections returned inconsistent schemas")
                buffered.append(batch)
                buffered_rows += batch.num_rows
                rows += batch.num_rows
                if buffered_rows >= target_row_group_rows:
                    flush()
    finally:
        if writer is not None:
            flush()
            writer.close()
    if writer is None:
        raise ValueError("bounded query returned no represented rows")
    return rows, path.stat().st_size


def _write_projection(result, path: Path, batch_size: int) -> tuple[int, int]:
    return _write_projection_results((result,), path, batch_size)


def _jsonable(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, Path):
        return str(value)
    return value


def _write_records(root: Path, stem: str, records: list[dict]) -> None:
    payload = [{key: _jsonable(value) for key, value in row.items()} for row in records]
    (root / f"{stem}.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    columns = list(payload[0]) if payload else []
    with (root / f"{stem}.csv").open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        if columns:
            writer.writeheader()
            writer.writerows(payload)


def _rows(connection, sql: str, params=None) -> list[dict]:
    table = connection.execute(sql, params or []).to_arrow_table()
    return table.to_pylist()


def _create_views(connection, projection: Path) -> None:
    projection_literal = str(projection).replace("'", "''")
    fast_available = _availability(30)
    slow_available = _availability(120)
    condition_columns = []
    for half_life in HALF_LIVES:
        label = "fast" if half_life == 30 else "slow"
        for condition in CONDITIONS:
            condition_columns.append(
                f"coalesce({_predicate(condition, half_life)}, false) "
                f"AS {label}_pass_{condition.key}"
            )
    connection.execute(
        f"""
        CREATE TEMP TABLE evaluated AS
        WITH condition_rows AS (
        SELECT session_date, symbol, session, interval_end_ns,
               {fast_available} AS fast_available,
               {slow_available} AS slow_available,
               {', '.join(condition_columns)}
        FROM read_parquet('{projection_literal}', hive_partitioning=false)
        )
        SELECT *,
               fast_available AND slow_available AS common_eligible,
               coalesce(fast_available AND ({_pass_all('fast')}), false) AS fast_own_match,
               coalesce(fast_available AND slow_available AND ({_pass_all('fast')}), false)
                   AS fast_match,
               coalesce(fast_available AND slow_available AND ({_pass_all('slow')}), false)
                   AS slow_match
        FROM condition_rows;
        """
    )


def _availability_records(connection) -> list[dict]:
    grouping_sets = "GROUP BY GROUPING SETS ((session_date, symbol), (session_date), ())"
    return _rows(
        connection,
        f"""
        SELECT CASE WHEN grouping(session_date)=1 THEN 'overall'
                    WHEN grouping(symbol)=1 THEN 'date' ELSE 'member' END AS scope,
               session_date,
               symbol,
               count(*)::BIGINT AS represented_seconds,
               count(*) FILTER (WHERE fast_available AND slow_available)::BIGINT
                   AS both_available_seconds,
               count(*) FILTER (WHERE fast_available AND NOT slow_available)::BIGINT
                   AS fast_only_available_seconds,
               count(*) FILTER (WHERE slow_available AND NOT fast_available)::BIGINT
                   AS slow_only_available_seconds,
               count(*) FILTER (WHERE NOT fast_available AND NOT slow_available)::BIGINT
                   AS neither_available_seconds
        FROM evaluated
        {grouping_sets}
        ORDER BY scope, session_date, symbol
        """,
    )


def _endpoint_overlap_records(connection) -> list[dict]:
    return _rows(
        connection,
        """
        WITH grouped AS (
            SELECT session_date, symbol,
                   count(*) FILTER (WHERE fast_match)::BIGINT AS fast_seconds,
                   count(*) FILTER (WHERE slow_match)::BIGINT AS slow_seconds,
                   count(*) FILTER (WHERE fast_match AND slow_match)::BIGINT AS shared_seconds,
                   count(*) FILTER (WHERE fast_match AND NOT slow_match)::BIGINT AS fast_only_seconds,
                   count(*) FILTER (WHERE slow_match AND NOT fast_match)::BIGINT AS slow_only_seconds,
                   count(*) FILTER (WHERE fast_match OR slow_match)::BIGINT AS union_seconds
            FROM evaluated
            GROUP BY session_date, symbol
        ), all_scopes AS (
            SELECT 'member' AS scope, session_date, symbol, * EXCLUDE(session_date, symbol)
            FROM grouped
            UNION ALL
            SELECT 'date', session_date, NULL, sum(fast_seconds), sum(slow_seconds),
                   sum(shared_seconds), sum(fast_only_seconds), sum(slow_only_seconds),
                   sum(union_seconds)
            FROM grouped GROUP BY session_date
            UNION ALL
            SELECT 'overall', NULL, NULL, sum(fast_seconds), sum(slow_seconds),
                   sum(shared_seconds), sum(fast_only_seconds), sum(slow_only_seconds),
                   sum(union_seconds)
            FROM grouped
        )
        SELECT *,
               shared_seconds::DOUBLE / nullif(union_seconds, 0) AS shared_over_union,
               shared_seconds::DOUBLE / nullif(fast_seconds, 0) AS shared_over_fast,
               shared_seconds::DOUBLE / nullif(slow_seconds, 0) AS shared_over_slow
        FROM all_scopes ORDER BY scope, session_date, symbol
        """,
    )


def _membership_records(connection, period_members: dict[str, set[tuple[str, str]]]) -> list[dict]:
    endpoint = _rows(
        connection,
        """
        SELECT session_date, symbol,
               count(*) FILTER (WHERE common_eligible)::BIGINT AS common_eligible_seconds,
               bool_or(fast_match) AS fast_has_match,
               bool_or(slow_match) AS slow_has_match
        FROM evaluated GROUP BY session_date, symbol ORDER BY session_date, symbol
        """,
    )
    records = []
    for row in endpoint:
        key = (str(row["session_date"]), row["symbol"])
        fast_period = key in period_members["fast"]
        slow_period = key in period_members["slow"]
        for definition, fast, slow in (
            ("matching_second", bool(row["fast_has_match"]), bool(row["slow_has_match"])),
            ("retained_period", fast_period, slow_period),
        ):
            classification = "both" if fast and slow else "fast_only" if fast else "slow_only" if slow else "neither"
            records.append(
                {
                    "definition": definition,
                    "session_date": key[0],
                    "symbol": key[1],
                    "member": f"{key[0]}/{key[1]}",
                    "common_eligible_seconds": row["common_eligible_seconds"],
                    "no_common_eligible_time": row["common_eligible_seconds"] == 0,
                    "classification": classification,
                }
            )
    return records


def _stock_membership(membership: list[dict]) -> list[dict]:
    records = []
    for definition in ("matching_second", "retained_period"):
        by_symbol: dict[str, set[str]] = {}
        for row in membership:
            if row["definition"] != definition:
                continue
            views = by_symbol.setdefault(row["symbol"], set())
            if row["classification"] in ("both", "fast_only"):
                views.add("fast")
            if row["classification"] in ("both", "slow_only"):
                views.add("slow")
        for symbol, views in sorted(by_symbol.items()):
            classification = "both" if views == {"fast", "slow"} else "fast_only" if views == {"fast"} else "slow_only" if views == {"slow"} else "neither"
            records.append(
                {"definition": definition, "symbol": symbol, "classification": classification}
            )
    return records


def _variant_matches() -> list[tuple[str, str, str]]:
    variants = [("fast_own_baseline", "fast_segment", "fast_own_match")]
    for half_life, label in ((30, "fast"), (120, "slow")):
        variants.append((label, "common_segment", f"{label}_match"))
        variants.extend(
            (
                f"{label}_without_{condition.key}",
                "common_segment",
                f"common_eligible AND ({_pass_all(label, omit=condition.key)})",
            )
            for condition in CONDITIONS
        )
    return variants


def _period_variants(connection, reducer, projection: Path, dates: tuple[str, ...]):
    """Reduce all 17 baseline/removal variants in one vectorized query plan."""
    del reducer, projection
    variants = _variant_matches()
    variant_items = ",\n".join(
        "CASE WHEN ({matching}) THEN struct_pack("
        "variant := '{variant}', availability_segment := {segment}) END".format(
            matching=matching,
            variant=variant.replace("'", "''"),
            segment=segment,
        )
        for variant, segment, matching in variants
    )
    date_parameters = ", ".join("?" for _ in dates)
    off_delay_ns = OFF_DELAY_SECONDS * 1_000_000_000
    occupancy_sql = format(MINIMUM_OCCUPANCY, "f")
    sql = f"""
    WITH ordered AS (
        SELECT *, lag(interval_end_ns) OVER member_window AS previous_endpoint_ns
        FROM evaluated
        WHERE session_date IN ({date_parameters})
        WINDOW member_window AS (
            PARTITION BY session_date, symbol, session ORDER BY interval_end_ns
        )
    ), segmented AS (
        SELECT *,
               sum((NOT fast_available OR (previous_endpoint_ns IS NOT NULL AND
                    interval_end_ns - previous_endpoint_ns <> 1000000000))::INTEGER)
                   OVER member_rows AS fast_segment,
               sum((NOT common_eligible OR (previous_endpoint_ns IS NOT NULL AND
                    interval_end_ns - previous_endpoint_ns <> 1000000000))::INTEGER)
                   OVER member_rows AS common_segment
        FROM ordered
        WINDOW member_rows AS (
            PARTITION BY session_date, symbol, session ORDER BY interval_end_ns
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ), matching_rows AS (
        SELECT session_date, symbol, session, interval_end_ns,
               item.variant AS variant,
               item.availability_segment AS availability_segment
        FROM segmented
        CROSS JOIN unnest([{variant_items}]) AS expanded(item)
        WHERE item IS NOT NULL
    ), with_previous AS (
        SELECT *, lag(interval_end_ns) OVER variant_window AS previous_match_ns
        FROM matching_rows
        WINDOW variant_window AS (
            PARTITION BY variant, session_date, symbol, session, availability_segment
            ORDER BY interval_end_ns
        )
    ), marked AS (
        SELECT *,
               CASE WHEN previous_match_ns IS NULL
                          OR interval_end_ns - previous_match_ns > {off_delay_ns}
                    THEN 1 ELSE 0 END AS new_episode,
               CASE WHEN previous_match_ns IS NULL
                          OR interval_end_ns - previous_match_ns > {off_delay_ns}
                    THEN 0
                    ELSE CAST((interval_end_ns - previous_match_ns) / 1000000000 AS BIGINT) - 1
               END AS preceding_nonmatching_gap_seconds
        FROM with_previous
    ), assigned AS (
        SELECT *, sum(new_episode) OVER variant_rows AS episode_group
        FROM marked
        WINDOW variant_rows AS (
            PARTITION BY variant, session_date, symbol, session, availability_segment
            ORDER BY interval_end_ns ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ), aggregated AS (
        SELECT variant, session_date, symbol, session, availability_segment, episode_group,
               min(interval_end_ns) AS first_matching_endpoint_ns,
               max(interval_end_ns) AS last_matching_endpoint_ns,
               count(*)::BIGINT AS matching_seconds,
               max(preceding_nonmatching_gap_seconds)::BIGINT
                   AS maximum_nonmatching_gap_seconds
        FROM assigned
        GROUP BY variant, session_date, symbol, session, availability_segment, episode_group
    ), measured AS (
        SELECT *, first_matching_endpoint_ns - 1000000000 AS episode_start_ns,
               last_matching_endpoint_ns AS episode_end_ns,
               CAST((last_matching_endpoint_ns - first_matching_endpoint_ns) /
                    1000000000 + 1 AS BIGINT) AS elapsed_seconds
        FROM aggregated
    ), retained AS (
        SELECT *, elapsed_seconds - matching_seconds AS eligible_nonmatching_seconds,
               matching_seconds::DOUBLE / elapsed_seconds AS occupancy
        FROM measured
        WHERE elapsed_seconds >= {MINIMUM_EPISODE_SECONDS}
          AND matching_seconds::DECIMAL / elapsed_seconds >= {occupancy_sql}
    ), numbered AS (
        SELECT *, row_number() OVER (
            PARTITION BY variant, session_date, symbol, session ORDER BY episode_start_ns
        ) AS episode_id
        FROM retained
    )
    SELECT variant, session_date, symbol, session, episode_id,
           episode_start_ns, episode_end_ns,
           CAST(make_timestamp_ns(episode_start_ns) AT TIME ZONE 'UTC' AS VARCHAR)
               AS episode_start_utc,
           CAST(make_timestamp_ns(episode_end_ns) AT TIME ZONE 'UTC' AS VARCHAR)
               AS episode_end_utc,
           elapsed_seconds, matching_seconds, eligible_nonmatching_seconds,
           occupancy, maximum_nonmatching_gap_seconds
    FROM numbered
    ORDER BY variant, session_date, symbol, session, episode_start_ns
    """
    combined = connection.execute(sql, list(dates)).to_arrow_table()
    variant_column = combined.column("variant")
    tables = {}
    for variant, _, _ in variants:
        filtered = combined.filter(pc.equal(variant_column, variant))
        tables[variant] = filtered.drop(["variant"])
    return tables


def _period_records(tables: dict[str, pa.Table]) -> list[dict]:
    records = []
    for variant, table in tables.items():
        for row in table.to_pylist():
            records.append({"variant": variant, **row})
    return records


def _union_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[list[int]] = []
    for start, end in sorted(intervals):
        if end < start:
            raise ValueError("period interval ends before it starts")
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return [(start, end) for start, end in merged]


def _interval_seconds(intervals: list[tuple[int, int]]) -> int:
    return sum((end - start) // 1_000_000_000 for start, end in intervals)


def _intersection_seconds(left: list[tuple[int, int]], right: list[tuple[int, int]]) -> int:
    left = _union_intervals(left)
    right = _union_intervals(right)
    i = j = total = 0
    while i < len(left) and j < len(right):
        start = max(left[i][0], right[j][0])
        end = min(left[i][1], right[j][1])
        if end > start:
            total += (end - start) // 1_000_000_000
        if left[i][1] <= right[j][1]:
            i += 1
        else:
            j += 1
    return total


def _period_overlap_records(tables: dict[str, pa.Table], members: list[tuple[str, str]]) -> list[dict]:
    grouped: dict[str, dict[tuple[str, str], list[tuple[int, int]]]] = {
        "fast": {},
        "slow": {},
    }
    for label in grouped:
        for row in tables[label].to_pylist():
            key = (str(row["session_date"]), row["symbol"])
            grouped[label].setdefault(key, []).append(
                (row["episode_start_ns"], row["episode_end_ns"])
            )
    member_rows = []
    for session_date, symbol in members:
        fast = _union_intervals(grouped["fast"].get((session_date, symbol), []))
        slow = _union_intervals(grouped["slow"].get((session_date, symbol), []))
        fast_seconds = _interval_seconds(fast)
        slow_seconds = _interval_seconds(slow)
        shared = _intersection_seconds(fast, slow)
        union = fast_seconds + slow_seconds - shared
        member_rows.append(
            {
                "scope": "member",
                "session_date": session_date,
                "symbol": symbol,
                "fast_seconds": fast_seconds,
                "slow_seconds": slow_seconds,
                "shared_seconds": shared,
                "fast_only_seconds": fast_seconds - shared,
                "slow_only_seconds": slow_seconds - shared,
                "union_seconds": union,
            }
        )
    records = list(member_rows)
    for scope, key_name in (("date", "session_date"), ("overall", None)):
        keys = sorted({row[key_name] for row in member_rows}) if key_name else [None]
        for key in keys:
            selected = [row for row in member_rows if key_name is None or row[key_name] == key]
            aggregate = {
                name: sum(row[name] for row in selected)
                for name in (
                    "fast_seconds",
                    "slow_seconds",
                    "shared_seconds",
                    "fast_only_seconds",
                    "slow_only_seconds",
                    "union_seconds",
                )
            }
            records.append(
                {"scope": scope, "session_date": key, "symbol": None, **aggregate}
            )
    for row in records:
        row["shared_over_union"] = row["shared_seconds"] / row["union_seconds"] if row["union_seconds"] else None
        row["shared_over_fast"] = row["shared_seconds"] / row["fast_seconds"] if row["fast_seconds"] else None
        row["shared_over_slow"] = row["shared_seconds"] / row["slow_seconds"] if row["slow_seconds"] else None
    return records


def _threshold_disagreements(
    connection, *, all_failure_combinations: bool = False
) -> list[dict]:
    records = []
    for direction, source_label, failed_label in (
        ("fast_pass_slow_fail", "fast", "slow"),
        ("slow_pass_fast_fail", "slow", "fast"),
    ):
        where = f"{source_label}_match AND NOT {failed_label}_match"
        failures = [f"NOT {failed_label}_pass_{condition.key}" for condition in CONDITIONS]
        failure_total = " + ".join(f"({item})::INTEGER" for item in failures)
        aggregates = ["count(*)::BIGINT AS denominator"]
        for condition, failure in zip(CONDITIONS, failures):
            aggregates.extend(
                (
                    f"count(*) FILTER (WHERE {failure})::BIGINT AS {condition.key}_count",
                    f"count(*) FILTER (WHERE {failure} AND ({failure_total}) = 1)::BIGINT "
                    f"AS {condition.key}_sole",
                )
            )
        aggregate = connection.execute(
            f"SELECT {', '.join(aggregates)} FROM evaluated WHERE {where}"
        ).fetchone()
        denominator = aggregate[0]
        for index, condition in enumerate(CONDITIONS):
            count = aggregate[1 + index * 2]
            sole = aggregate[2 + index * 2]
            records.append(
                {
                    "direction": direction,
                    "record_type": "condition",
                    "failure": condition.key,
                    "seconds": count,
                    "share_of_direction": count / denominator if denominator else None,
                    "sole_failure_seconds": sole,
                    "direction_seconds": denominator,
                }
            )
        combo_parts = [
            f"CASE WHEN {failure} THEN '{condition.key},' ELSE '' END"
            for condition, failure in zip(CONDITIONS, failures)
        ]
        limit_sql = "" if all_failure_combinations else "LIMIT 20"
        combos = _rows(
            connection,
            f"""
            SELECT rtrim({' || '.join(combo_parts)}, ',') AS failure,
                   count(*)::BIGINT AS seconds
            FROM evaluated WHERE {where}
            GROUP BY failure ORDER BY seconds DESC, failure {limit_sql}
            """,
        )
        for combo in combos:
            records.append(
                {
                    "direction": direction,
                    "record_type": "combination",
                    "failure": combo["failure"],
                    "seconds": combo["seconds"],
                    "share_of_direction": combo["seconds"] / denominator if denominator else None,
                    "sole_failure_seconds": None,
                    "direction_seconds": denominator,
                }
            )
    return records


def _example_stretches(connection) -> list[dict]:
    records = []
    for direction, flag, failed_label in (
        ("fast_only", "fast_match AND NOT slow_match", "slow"),
        ("slow_only", "slow_match AND NOT fast_match", "fast"),
    ):
        failures = [f"NOT {failed_label}_pass_{condition.key}" for condition in CONDITIONS]
        failure_sums = ", ".join(
            f"sum(({failure})::INTEGER)::BIGINT AS failed_{condition.key}_seconds"
            for condition, failure in zip(CONDITIONS, failures)
        )
        rows = _rows(
            connection,
            f"""
            WITH flagged AS (
                SELECT *, ({flag}) AS directional,
                       lag(interval_end_ns) OVER (
                           PARTITION BY session_date, symbol, session ORDER BY interval_end_ns
                       ) AS previous_ns
                FROM evaluated
            ), marked AS (
                SELECT *,
                       CASE WHEN directional AND (
                           coalesce(lag(directional) OVER (
                               PARTITION BY session_date, symbol, session ORDER BY interval_end_ns
                           ), false) = false
                           OR previous_ns IS NULL OR interval_end_ns - previous_ns <> 1000000000
                       ) THEN 1 ELSE 0 END AS new_stretch
                FROM flagged
            ), assigned AS (
                SELECT *, sum(new_stretch) OVER (
                    PARTITION BY session_date, symbol, session ORDER BY interval_end_ns
                ) AS stretch_id
                FROM marked
            ), grouped AS (
                SELECT session_date, symbol, session, stretch_id,
                       min(interval_end_ns) - 1000000000 AS stretch_start_ns,
                       max(interval_end_ns) AS stretch_end_ns,
                       count(*)::BIGINT AS seconds,
                       {failure_sums}
                FROM assigned WHERE directional
                GROUP BY session_date, symbol, session, stretch_id
            )
            SELECT *,
                   CAST(make_timestamp_ns(stretch_start_ns) AT TIME ZONE 'UTC' AS VARCHAR)
                       AS stretch_start_utc,
                   CAST(make_timestamp_ns(stretch_end_ns) AT TIME ZONE 'UTC' AS VARCHAR)
                       AS stretch_end_utc
            FROM grouped ORDER BY seconds DESC, session_date, symbol, stretch_start_ns
            LIMIT 3
            """,
        )
        for row in rows:
            row["direction"] = direction
            records.append(row)
    return records


def _filter_contribution(connection, tables: dict[str, pa.Table]) -> list[dict]:
    records = []
    expressions = ["count(*) FILTER (WHERE common_eligible)::BIGINT AS common"]
    for half_life, label in ((30, "fast"), (120, "slow")):
        expressions.append(
            f"count(*) FILTER (WHERE {label}_match)::BIGINT AS {label}_baseline"
        )
        for condition in CONDITIONS:
            expressions.extend(
                (
                    f"count(*) FILTER (WHERE common_eligible AND "
                    f"{label}_pass_{condition.key})::BIGINT "
                    f"AS {label}_{condition.key}_standalone",
                    f"count(*) FILTER (WHERE common_eligible AND "
                    f"({_pass_all(label, omit=condition.key)}))::BIGINT "
                    f"AS {label}_{condition.key}_removed",
                )
            )
    aggregate = connection.execute(
        f"SELECT {', '.join(expressions)} FROM evaluated"
    ).fetchone()
    names = [expression.rsplit(" AS ", 1)[1] for expression in expressions]
    values = dict(zip(names, aggregate))
    common = values["common"]
    for half_life, label in ((30, "fast"), (120, "slow")):
        baseline_matches = values[f"{label}_baseline"]
        baseline_members = {
            (str(row["session_date"]), row["symbol"])
            for row in tables[label].to_pylist()
        }
        for condition in CONDITIONS:
            standalone = values[f"{label}_{condition.key}_standalone"]
            removed_matches = values[f"{label}_{condition.key}_removed"]
            variant = f"{label}_without_{condition.key}"
            variant_members = {
                (str(row["session_date"]), row["symbol"])
                for row in tables[variant].to_pylist()
            }
            records.append(
                {
                    "view": label,
                    "half_life_seconds": half_life,
                    "condition": condition.key,
                    "field": condition.column(half_life),
                    "operator": condition.operator,
                    "threshold": condition.threshold,
                    "common_eligible_seconds": common,
                    "standalone_pass_seconds": standalone,
                    "standalone_pass_rate": standalone / common if common else None,
                    "baseline_matching_seconds": baseline_matches,
                    "removed_matching_seconds": removed_matches,
                    "added_matching_seconds": removed_matches - baseline_matches,
                    "retained_symbol_days": len(variant_members),
                    "gained_symbol_days": len(variant_members - baseline_members),
                    "lost_symbol_days": len(baseline_members - variant_members),
                }
            )
    return records


def _baseline_check(connection, table: pa.Table, exact_pilot: bool) -> dict:
    actual = {
        "eligible_endpoints": connection.execute(
            "SELECT count(*) FROM evaluated WHERE fast_available"
        ).fetchone()[0],
        "matching_endpoints": connection.execute(
            "SELECT count(*) FROM evaluated WHERE fast_own_match"
        ).fetchone()[0],
        "symbol_days_with_match": connection.execute(
            "SELECT count(*) FROM (SELECT 1 FROM evaluated WHERE fast_own_match "
            "GROUP BY session_date, symbol)"
        ).fetchone()[0],
        "retained_periods": table.num_rows,
        "retained_symbol_days": len(
            {(str(row["session_date"]), row["symbol"]) for row in table.to_pylist()}
        ),
        "retained_symbols": len({row["symbol"] for row in table.to_pylist()}),
    }
    return {
        "actual": actual,
        "expected": EXPECTED_FAST_BASELINE if exact_pilot else None,
        "matches_expected": actual == EXPECTED_FAST_BASELINE if exact_pilot else None,
    }


def _membership_summary(records: list[dict], stock_records: list[dict]) -> list[dict]:
    summary = []
    for level, source in (("symbol_day", records), ("stock", stock_records)):
        for definition in ("matching_second", "retained_period"):
            selected = [row for row in source if row["definition"] == definition]
            for classification in ("both", "fast_only", "slow_only", "neither"):
                members = sorted(
                    row.get("member", row.get("symbol"))
                    for row in selected
                    if row["classification"] == classification
                )
                summary.append(
                    {
                        "level": level,
                        "definition": definition,
                        "classification": classification,
                        "count": len(members),
                        "members": members,
                    }
                )
    return summary


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _source_revision() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _summary_markdown(
    membership_summary,
    endpoint_overlap,
    period_overlap,
    disagreements,
    contributions,
    availability,
    baseline,
    start_date=PILOT_START.isoformat(),
    end_date=PILOT_END.isoformat(),
) -> str:
    def summary_count(definition, classification):
        return next(
            row["count"]
            for row in membership_summary
            if row["level"] == "symbol_day"
            and row["definition"] == definition
            and row["classification"] == classification
        )

    endpoint = next(row for row in endpoint_overlap if row["scope"] == "overall")
    period = next(row for row in period_overlap if row["scope"] == "overall")
    available = next(row for row in availability if row["scope"] == "overall")
    dominant = sorted(
        (row for row in disagreements if row["record_type"] == "condition"),
        key=lambda row: row["seconds"],
        reverse=True,
    )[:4]
    additions = sorted(contributions, key=lambda row: row["added_matching_seconds"], reverse=True)[:4]
    exact_pilot = baseline["expected"] is not None
    if exact_pilot:
        baseline_status = (
            "matched all six published counts"
            if baseline["matches_expected"]
            else "DID NOT match all six published counts"
        )
    else:
        baseline_status = "is not applicable outside the exact five-date pilot"
    lines = [
        "# 30-second versus 120-second EW selection — bounded comparison",
        "",
        f"This is a descriptive comparison of the existing feature views for {start_date} through {end_date}. It is not parameter optimization or a trading backtest.",
        "",
        "## Universe membership",
        "",
        f"At the matching-second definition, {summary_count('matching_second', 'both')} symbol-days appear in both screens, {summary_count('matching_second', 'fast_only')} only in fast, {summary_count('matching_second', 'slow_only')} only in slow, and {summary_count('matching_second', 'neither')} in neither.",
        f"At the retained-period definition, the corresponding counts are {summary_count('retained_period', 'both')}, {summary_count('retained_period', 'fast_only')}, {summary_count('retained_period', 'slow_only')}, and {summary_count('retained_period', 'neither')}.",
        f"Common eligibility covers {available['both_available_seconds']:,} of {available['represented_seconds']:,} represented seconds. Members with zero common-eligible time are flagged in `membership.csv`.",
        "",
        "## Time overlap",
        "",
        f"Matching endpoints: fast {int(endpoint['fast_seconds']):,}s, slow {int(endpoint['slow_seconds']):,}s, shared {int(endpoint['shared_seconds']):,}s, union {int(endpoint['union_seconds']):,}s; shared/union = {endpoint['shared_over_union'] if endpoint['shared_over_union'] is not None else 'undefined'}.",
        f"Retained-period coverage: fast {period['fast_seconds']:,}s, slow {period['slow_seconds']:,}s, shared {period['shared_seconds']:,}s, union {period['union_seconds']:,}s; shared/union = {period['shared_over_union'] if period['shared_over_union'] is not None else 'undefined'}.",
        "",
        "## Main disagreement and contribution diagnostics",
        "",
    ]
    lines.extend(
        f"- {row['direction']}: `{row['failure']}` fails on {row['seconds']:,} seconds ({row['share_of_direction']:.2%} of that directional disagreement)."
        for row in dominant
        if row["share_of_direction"] is not None
    )
    lines.extend(
        f"- Removing `{row['condition']}` from the {row['view']} screen adds {row['added_matching_seconds']:,} matching seconds and leaves {row['retained_symbol_days']} retained symbol-days ({row['gained_symbol_days']} gained, {row['lost_symbol_days']} lost)."
        for row in additions
    )
    lines.extend(
        [
            "",
            "These removal results measure contribution inside this combined screen. A small increment can reflect redundancy with other conditions; it is not causal feature importance.",
            "",
            "## Baseline and limits",
            "",
            f"The original fast-screen sanity check {baseline_status}. Exact actual and expected values are in `baseline_sanity.json`.",
            "This acquisition-selected population does not establish a preferred half-life or full-release frequency. Consecutive seconds share overlapping returns and EW history and are not independent observations.",
            "",
        ]
    )
    return "\n".join(lines)


def _analyze_projection(
    projection: Path,
    output: Path,
    dates: tuple[str, ...],
    *,
    memory_limit: str,
    threads: int = 1,
    temp_directory: Path | None = None,
    max_temp_directory_size: str = "0B",
    all_failure_combinations: bool = False,
) -> dict:
    reducer = _load_reducer()
    config = {
        "threads": str(threads),
        "memory_limit": memory_limit,
        "max_temp_directory_size": max_temp_directory_size,
    }
    if temp_directory is not None:
        temp_directory.mkdir(parents=True, exist_ok=True)
        config["temp_directory"] = str(temp_directory.resolve())
    connection = duckdb.connect(
        ":memory:",
        config=config,
    )
    try:
        materialize_started = time.perf_counter()
        _create_views(connection, projection)
        materialize_seconds = time.perf_counter() - materialize_started
        period_started = time.perf_counter()
        periods = _period_variants(connection, reducer, projection, dates)
        period_seconds = time.perf_counter() - period_started
        aggregate_started = time.perf_counter()
        availability = _availability_records(connection)
        endpoint_overlap = _endpoint_overlap_records(connection)
        member_keys = [
            (str(row[0]), row[1])
            for row in connection.execute(
                "SELECT DISTINCT session_date, symbol FROM evaluated ORDER BY 1, 2"
            ).fetchall()
        ]
        period_members = {
            label: {(str(row["session_date"]), row["symbol"]) for row in periods[label].to_pylist()}
            for label in ("fast", "slow")
        }
        membership = _membership_records(connection, period_members)
        stocks = _stock_membership(membership)
        membership_summary = _membership_summary(membership, stocks)
        period_overlap = _period_overlap_records(periods, member_keys)
        disagreements = _threshold_disagreements(
            connection, all_failure_combinations=all_failure_combinations
        )
        freshness_failures = sum(
            row["seconds"]
            for row in disagreements
            if row["record_type"] == "condition"
            and row["failure"] in {"quote_freshness", "trade_freshness"}
        )
        if freshness_failures:
            raise AssertionError("shared freshness unexpectedly explains directional disagreement")
        examples = _example_stretches(connection)
        contributions = _filter_contribution(connection, periods)
        baseline = _baseline_check(
            connection,
            periods["fast_own_baseline"],
            dates == PILOT_DATES,
        )
        aggregate_seconds = time.perf_counter() - aggregate_started

        writing_started = time.perf_counter()
        _write_records(output, "availability", availability)
        _write_records(output, "membership", membership)
        _write_records(output, "membership_summary", membership_summary)
        _write_records(output, "stock_membership", stocks)
        _write_records(output, "matching_endpoint_overlap", endpoint_overlap)
        _write_records(output, "retained_period_overlap", period_overlap)
        _write_records(output, "threshold_disagreements", disagreements)
        _write_records(output, "filter_contribution", contributions)
        _write_records(output, "example_stretches", examples)
        _write_records(output, "retained_periods", _period_records(periods))
        (output / "baseline_sanity.json").write_text(
            json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        (output / "summary.md").write_text(
            _summary_markdown(
                membership_summary,
                endpoint_overlap,
                period_overlap,
                disagreements,
                contributions,
                availability,
                baseline,
                dates[0],
                dates[-1],
            ),
            encoding="utf-8",
        )
        writing_seconds = time.perf_counter() - writing_started
        return {
            "represented_members": len(member_keys),
            "represented_rows": connection.execute("SELECT count(*) FROM evaluated").fetchone()[0],
            "baseline": baseline,
            "phase_seconds": {
                "condition_materialization": materialize_seconds,
                "period_reduction": period_seconds,
                "aggregate_statistics": aggregate_seconds,
                "output_writing": writing_seconds,
            },
        }
    finally:
        connection.close()


def _read_records(root: Path, stem: str) -> list[dict]:
    return json.loads((root / f"{stem}.json").read_text(encoding="utf-8"))


def _rollup_member_records(records: list[dict], value_fields: tuple[str, ...]) -> list[dict]:
    members = [dict(row) for row in records if row["scope"] == "member"]
    string_fields = {
        field
        for field in value_fields
        if any(isinstance(row[field], str) for row in members)
    }
    dates = sorted({row["session_date"] for row in members})
    output = list(members)
    for scope, session_date in [*(('date', value) for value in dates), ('overall', None)]:
        selected = [
            row for row in members
            if session_date is None or row["session_date"] == session_date
        ]
        output.append(
            {
                "scope": scope,
                "session_date": session_date,
                "symbol": None,
                **{
                    field: (
                        str(sum(int(row[field]) for row in selected))
                        if field in string_fields
                        else sum(row[field] for row in selected)
                    )
                    for field in value_fields
                },
            }
        )
    return sorted(
        output,
        key=lambda row: (
            row["scope"],
            row["session_date"] or "",
            row["symbol"] or "",
        ),
    )


def _add_overlap_ratios(records: list[dict]) -> list[dict]:
    for row in records:
        shared = int(row["shared_seconds"])
        union = int(row["union_seconds"])
        fast = int(row["fast_seconds"])
        slow = int(row["slow_seconds"])
        row["shared_over_union"] = (
            shared / union if union else None
        )
        row["shared_over_fast"] = (
            shared / fast if fast else None
        )
        row["shared_over_slow"] = (
            shared / slow if slow else None
        )
    return records


def _tables_from_period_records(records: list[dict]) -> dict[str, pa.Table]:
    tables = {}
    for variant, _, _ in _variant_matches():
        tables[variant] = pa.Table.from_pylist(
            [{key: value for key, value in row.items() if key != "variant"}
             for row in records if row["variant"] == variant]
        )
    return tables


def _merge_disagreements(chunks: list[list[dict]]) -> list[dict]:
    directions = ("fast_pass_slow_fail", "slow_pass_fast_fail")
    totals = {direction: 0 for direction in directions}
    counts: dict[tuple[str, str, str], int] = {}
    sole: dict[tuple[str, str, str], int] = {}
    for rows in chunks:
        for direction in directions:
            direction_rows = [row for row in rows if row["direction"] == direction]
            if direction_rows:
                totals[direction] += direction_rows[0]["direction_seconds"]
        for row in rows:
            key = (row["direction"], row["record_type"], row["failure"])
            counts[key] = counts.get(key, 0) + row["seconds"]
            if row["record_type"] == "condition":
                sole[key] = sole.get(key, 0) + row["sole_failure_seconds"]
    output = []
    for direction in directions:
        denominator = totals[direction]
        for condition in CONDITIONS:
            key = (direction, "condition", condition.key)
            seconds = counts.get(key, 0)
            output.append(
                {
                    "direction": direction,
                    "record_type": "condition",
                    "failure": condition.key,
                    "seconds": seconds,
                    "share_of_direction": seconds / denominator if denominator else None,
                    "sole_failure_seconds": sole.get(key, 0),
                    "direction_seconds": denominator,
                }
            )
        combinations = sorted(
            (
                (failure, seconds)
                for (item_direction, record_type, failure), seconds in counts.items()
                if item_direction == direction and record_type == "combination"
            ),
            key=lambda item: (-item[1], item[0]),
        )[:20]
        output.extend(
            {
                "direction": direction,
                "record_type": "combination",
                "failure": failure,
                "seconds": seconds,
                "share_of_direction": seconds / denominator if denominator else None,
                "sole_failure_seconds": None,
                "direction_seconds": denominator,
            }
            for failure, seconds in combinations
        )
    return output


def _merge_contributions(chunks: list[list[dict]]) -> list[dict]:
    output = []
    for half_life, label in ((30, "fast"), (120, "slow")):
        for condition in CONDITIONS:
            selected = [
                row for rows in chunks for row in rows
                if row["view"] == label and row["condition"] == condition.key
            ]
            common = sum(row["common_eligible_seconds"] for row in selected)
            standalone = sum(row["standalone_pass_seconds"] for row in selected)
            baseline = sum(row["baseline_matching_seconds"] for row in selected)
            removed = sum(row["removed_matching_seconds"] for row in selected)
            output.append(
                {
                    "view": label,
                    "half_life_seconds": half_life,
                    "condition": condition.key,
                    "field": condition.column(half_life),
                    "operator": condition.operator,
                    "threshold": condition.threshold,
                    "common_eligible_seconds": common,
                    "standalone_pass_seconds": standalone,
                    "standalone_pass_rate": standalone / common if common else None,
                    "baseline_matching_seconds": baseline,
                    "removed_matching_seconds": removed,
                    "added_matching_seconds": removed - baseline,
                    "retained_symbol_days": sum(row["retained_symbol_days"] for row in selected),
                    "gained_symbol_days": sum(row["gained_symbol_days"] for row in selected),
                    "lost_symbol_days": sum(row["lost_symbol_days"] for row in selected),
                }
            )
    return output


def _merge_examples(chunks: list[list[dict]]) -> list[dict]:
    output = []
    for direction in ("fast_only", "slow_only"):
        selected = [
            row for rows in chunks for row in rows if row["direction"] == direction
        ]
        output.extend(
            sorted(
                selected,
                key=lambda row: (
                    -row["seconds"],
                    row["session_date"],
                    row["symbol"],
                    row["stretch_start_ns"],
                ),
            )[:3]
        )
    return output


def _implementation_identity() -> str:
    digest = hashlib.sha256()
    for path in (
        Path(__file__).resolve(),
        Path(__file__).with_name("half_life_comparison") / "projection.sql",
        Path(__file__).with_name("half_life_comparison") / "pilot_config.json",
        Path(__file__).with_name("select_structured_tape_episodes.py"),
    ):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _checkpoint_artifacts(root: Path) -> list[dict]:
    return [
        {"path": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path)}
        for path in sorted(root.iterdir())
        if path.is_file() and path.name != "checkpoint.json"
    ]


def _valid_checkpoint(root: Path, expected: dict) -> bool:
    manifest_path = root / "checkpoint.json"
    if not manifest_path.is_file():
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    for key, value in expected.items():
        if manifest.get(key) != value:
            return False
    for artifact in manifest.get("artifacts", []):
        path = root / artifact["path"]
        if (
            not path.is_file()
            or path.stat().st_size != artifact["bytes"]
            or _sha256(path) != artifact["sha256"]
        ):
            return False
    return bool(manifest.get("artifacts"))


def _merge_chunk_outputs(
    chunk_roots: list[Path], output: Path, dates: tuple[str, ...]
) -> dict:
    availability_chunks = [_read_records(root, "availability") for root in chunk_roots]
    availability = _rollup_member_records(
        [row for rows in availability_chunks for row in rows],
        (
            "represented_seconds",
            "both_available_seconds",
            "fast_only_available_seconds",
            "slow_only_available_seconds",
            "neither_available_seconds",
        ),
    )
    endpoint_chunks = [_read_records(root, "matching_endpoint_overlap") for root in chunk_roots]
    endpoint_overlap = _add_overlap_ratios(
        _rollup_member_records(
            [row for rows in endpoint_chunks for row in rows],
            (
                "fast_seconds",
                "slow_seconds",
                "shared_seconds",
                "fast_only_seconds",
                "slow_only_seconds",
                "union_seconds",
            ),
        )
    )
    membership = [
        row for root in chunk_roots for row in _read_records(root, "membership")
    ]
    period_records = [
        row for root in chunk_roots for row in _read_records(root, "retained_periods")
    ]
    variant_order = {variant: index for index, (variant, _, _) in enumerate(_variant_matches())}
    period_records.sort(
        key=lambda row: (
            variant_order[row["variant"]],
            row["session_date"],
            row["symbol"],
            row["session"],
            row["episode_start_ns"],
        )
    )
    period_tables = _tables_from_period_records(period_records)
    members = sorted(
        {
            (row["session_date"], row["symbol"])
            for row in membership if row["definition"] == "matching_second"
        }
    )
    stocks = _stock_membership(membership)
    membership_summary = _membership_summary(membership, stocks)
    period_overlap = _period_overlap_records(period_tables, members)
    disagreement_chunks = [
        _read_records(root, "threshold_disagreements") for root in chunk_roots
    ]
    disagreements = _merge_disagreements(disagreement_chunks)
    contributions = _merge_contributions(
        [_read_records(root, "filter_contribution") for root in chunk_roots]
    )
    examples = _merge_examples(
        [_read_records(root, "example_stretches") for root in chunk_roots]
    )
    chunk_baselines = [
        json.loads((root / "baseline_sanity.json").read_text(encoding="utf-8"))["actual"]
        for root in chunk_roots
    ]
    fast_own_periods = [
        row for row in period_records if row["variant"] == "fast_own_baseline"
    ]
    actual_baseline = {
        "eligible_endpoints": sum(row["eligible_endpoints"] for row in chunk_baselines),
        "matching_endpoints": sum(row["matching_endpoints"] for row in chunk_baselines),
        "symbol_days_with_match": sum(
            row["symbol_days_with_match"] for row in chunk_baselines
        ),
        "retained_periods": len(fast_own_periods),
        "retained_symbol_days": len(
            {(row["session_date"], row["symbol"]) for row in fast_own_periods}
        ),
        "retained_symbols": len({row["symbol"] for row in fast_own_periods}),
    }
    exact_pilot = dates == PILOT_DATES
    baseline = {
        "actual": actual_baseline,
        "expected": EXPECTED_FAST_BASELINE if exact_pilot else None,
        "matches_expected": actual_baseline == EXPECTED_FAST_BASELINE if exact_pilot else None,
    }
    _write_records(output, "availability", availability)
    _write_records(output, "membership", membership)
    _write_records(output, "membership_summary", membership_summary)
    _write_records(output, "stock_membership", stocks)
    _write_records(output, "matching_endpoint_overlap", endpoint_overlap)
    _write_records(output, "retained_period_overlap", period_overlap)
    _write_records(output, "threshold_disagreements", disagreements)
    _write_records(output, "filter_contribution", contributions)
    _write_records(output, "example_stretches", examples)
    _write_records(output, "retained_periods", period_records)
    (output / "baseline_sanity.json").write_text(
        json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output / "summary.md").write_text(
        _summary_markdown(
            membership_summary,
            endpoint_overlap,
            period_overlap,
            disagreements,
            contributions,
            availability,
            baseline,
            dates[0],
            dates[-1],
        ),
        encoding="utf-8",
    )
    overall = next(row for row in availability if row["scope"] == "overall")
    return {
        "represented_members": len(members),
        "represented_rows": overall["represented_seconds"],
        "baseline": baseline,
    }


def run(args) -> dict:
    _require_product_arguments(args)
    start, end = _validate_scope(args.start_date, args.end_date, args.allow_expanded_scope)
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(output.parent).free < FREE_DISK_RESERVE_BYTES:
        raise ValueError("output destination violates the 20 GiB free-disk reserve")
    attempt = output.parent / f".{output.name}.in-progress"
    attempt.mkdir(exist_ok=True)
    checkpoints = attempt / "checkpoints"
    checkpoints.mkdir(exist_ok=True)
    query = _load_projection_sql()
    started = time.perf_counter()
    implementation_identity = _implementation_identity()
    settings_identity = {
        "batch_size": args.batch_size,
        "memory_limit": args.memory_limit,
        "threads": args.threads,
        "max_temp_directory_size": args.max_temp_directory_size,
        "projection_sha256": hashlib.sha256(query.encode()).hexdigest(),
    }
    catalog_started = time.perf_counter()
    catalog_manifest, catalog_records, _, _, catalog_bytes = read_endpoint_query_catalog(
        args.catalog, expected_identity=args.identity
    )
    catalog_seconds = time.perf_counter() - catalog_started
    selected_records = [
        record for record in catalog_records
        if start.isoformat() <= record["session_date"] <= end.isoformat()
    ]
    if not selected_records:
        raise ValueError("requested scope contains no represented dates")
    dates = tuple(sorted({record["session_date"] for record in selected_records}))
    members_by_date = {
        current: tuple(
            record["member"] for record in selected_records
            if record["session_date"] == current
        )
        for current in dates
    }
    chunk_roots = []
    chunk_results = []
    validation_seconds = 0.0
    validation_bytes = 0
    database = None
    try:
        for index, current_date in enumerate(dates, 1):
            date_started = time.perf_counter()
            print(
                f"half-life comparison: date {index}/{len(dates)} {current_date} "
                f"opening {len(members_by_date[current_date])} members",
                flush=True,
            )
            date_temp = (
                args.temp_directory.resolve() / current_date
                if args.temp_directory is not None
                else attempt / "duckdb-scratch" / current_date
            )
            database = open_tape_database(
                args.catalog,
                expected_identity=args.identity,
                data_roots={"base": args.base_root, "features": args.feature_root},
                start_date=current_date,
                end_date=current_date,
                members=members_by_date[current_date],
                memory_limit=args.memory_limit,
                threads=args.threads,
                temp_directory=date_temp / "reader",
                max_temp_directory_size=args.max_temp_directory_size,
            )
            validation_seconds += database.validation_seconds
            validation_bytes += database.validation_bytes
            source_identity = {
                "query_catalog_identity": database.catalog_identity,
                "release_source_revision": database.release_source_revision,
                "release_wheel_sha256": database.release_wheel_sha256,
            }
            checkpoint_root = checkpoints / current_date
            expected_checkpoint = {
                "schema": CHECKPOINT_SCHEMA,
                "date": current_date,
                "implementation_identity": implementation_identity,
                "settings_identity": settings_identity,
                "source_identity": source_identity,
                "members": list(database.selected_members),
            }
            if _valid_checkpoint(checkpoint_root, expected_checkpoint):
                manifest = json.loads(
                    (checkpoint_root / "checkpoint.json").read_text(encoding="utf-8")
                )
                database.close()
                database = None
                chunk_roots.append(checkpoint_root)
                chunk_results.append(manifest["result"])
                print(
                    f"half-life comparison: date {current_date} reused checkpoint "
                    f"rows={manifest['result']['represented_rows']:,} "
                    f"elapsed={time.perf_counter() - started:.1f}s",
                    flush=True,
                )
                continue
            if checkpoint_root.exists():
                shutil.rmtree(checkpoint_root)
            chunk_attempt = checkpoints / f".{current_date}.attempt-{os.getpid()}"
            if chunk_attempt.exists():
                shutil.rmtree(chunk_attempt)
            chunk_attempt.mkdir()
            projection = chunk_attempt / "projection.parquet"
            projection_started = time.perf_counter()
            rows, projection_bytes = _write_projection(
                database.sql(query), projection, args.batch_size
            )
            database._check_inputs()
            projection_seconds = time.perf_counter() - projection_started
            database.close()
            database = None
            analysis_started = time.perf_counter()
            result = _analyze_projection(
                projection,
                chunk_attempt,
                (current_date,),
                memory_limit=args.memory_limit,
                threads=args.threads,
                temp_directory=date_temp / "analysis",
                max_temp_directory_size=args.max_temp_directory_size,
                all_failure_combinations=True,
            )
            analysis_seconds = time.perf_counter() - analysis_started
            projection.unlink()
            shutil.rmtree(date_temp, ignore_errors=True)
            chunk_result = {
                **result,
                "projection_rows": rows,
                "projection_bytes": projection_bytes,
                "projection_seconds": projection_seconds,
                "analysis_seconds": analysis_seconds,
                "total_seconds": time.perf_counter() - date_started,
            }
            manifest = {
                **expected_checkpoint,
                "result": chunk_result,
                "artifacts": _checkpoint_artifacts(chunk_attempt),
            }
            (chunk_attempt / "checkpoint.json").write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            os.rename(chunk_attempt, checkpoint_root)
            chunk_roots.append(checkpoint_root)
            chunk_results.append(chunk_result)
            print(
                f"half-life comparison: date {current_date} complete rows={rows:,} "
                f"projection={projection_seconds:.1f}s analysis={analysis_seconds:.1f}s "
                f"elapsed={time.perf_counter() - started:.1f}s",
                flush=True,
            )

        for stem in (
            "availability",
            "membership",
            "membership_summary",
            "stock_membership",
            "matching_endpoint_overlap",
            "retained_period_overlap",
            "threshold_disagreements",
            "filter_contribution",
            "example_stretches",
            "retained_periods",
        ):
            for suffix in ("json", "csv"):
                (attempt / f"{stem}.{suffix}").unlink(missing_ok=True)
        for name in ("baseline_sanity.json", "summary.md", "query.sql", "config.json"):
            (attempt / name).unlink(missing_ok=True)
        merge_started = time.perf_counter()
        result = _merge_chunk_outputs(chunk_roots, attempt, dates)
        merge_seconds = time.perf_counter() - merge_started
        (attempt / "query.sql").write_text(query + "\n", encoding="utf-8")
        config_path = Path(__file__).with_name("half_life_comparison") / "pilot_config.json"
        shutil.copyfile(config_path, attempt / "config.json")
        artifacts = []
        for path in sorted(attempt.iterdir()):
            if path.is_file() and path.name != "run_metadata.json":
                artifacts.append(
                    {
                        "path": path.name,
                        "bytes": path.stat().st_size,
                        "sha256": _sha256(path),
                    }
                )
        source = {
            "query_catalog_identity": catalog_manifest["catalog_identity"],
            "release_source_revision": catalog_manifest["release"].get("source_revision"),
            "release_wheel_sha256": catalog_manifest["release"].get("wheel_sha256"),
            "catalog_discovery_seconds": catalog_seconds,
            "catalog_discovery_bytes": catalog_bytes,
            "date_scope_validation_seconds": validation_seconds,
            "date_scope_validation_bytes": validation_bytes,
        }
        metadata = {
            "schema": "half_life_selection_study_v1",
            "dates": {"start": start.isoformat(), "end": end.isoformat(), "inclusive": True},
            "actual_member_count": result["represented_members"],
            "actual_row_count": result["represented_rows"],
            "source": source,
            "runner_source_revision": _source_revision(),
            "settings": {
                "half_lives_seconds": list(HALF_LIVES),
                "freshness_window_seconds": 60,
                "off_delay_seconds": OFF_DELAY_SECONDS,
                "minimum_episode_seconds": MINIMUM_EPISODE_SECONDS,
                "minimum_occupancy": str(MINIMUM_OCCUPANCY),
                "batch_size": args.batch_size,
                "analysis_memory_limit": args.memory_limit,
                "analysis_threads": args.threads,
                "free_disk_reserve_bytes": FREE_DISK_RESERVE_BYTES,
                "duckdb_temp_directory": str(
                    args.temp_directory.resolve()
                    if args.temp_directory is not None
                    else attempt / "duckdb-scratch"
                ),
                "duckdb_max_temp_directory_size": args.max_temp_directory_size,
            },
            "runtime_seconds": {
                "catalog_discovery": catalog_seconds,
                "projection": sum(row["projection_seconds"] for row in chunk_results),
                "condition_materialization": sum(
                    row["phase_seconds"]["condition_materialization"]
                    for row in chunk_results
                ),
                "period_reduction": sum(
                    row["phase_seconds"]["period_reduction"] for row in chunk_results
                ),
                "aggregate_statistics": sum(
                    row["phase_seconds"]["aggregate_statistics"] for row in chunk_results
                ),
                "chunk_output_writing": sum(
                    row["phase_seconds"]["output_writing"] for row in chunk_results
                ),
                "final_merge_and_output": merge_seconds,
                "total": time.perf_counter() - started,
            },
            "date_chunks": chunk_results,
            "artifacts": artifacts,
            "reproducible_commands": {
                "pilot": "$QUERY_RELEASE/bin/python scripts/research/compare_half_life_selection.py --start-date 2026-06-01 --end-date 2026-06-05 --output PRIVATE_OUTPUT_DIRECTORY",
                "expanded_template_not_executed": "$QUERY_RELEASE/bin/python scripts/research/compare_half_life_selection.py --start-date START --end-date END --output PRIVATE_OUTPUT_DIRECTORY --allow-expanded-scope",
            },
            **result,
        }
        (attempt / "run_metadata.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.rename(attempt, output)
        return {**metadata, "output": str(output)}
    except Exception:
        if database is not None:
            database.close()
        print(
            f"half-life comparison: preserving resumable work at {attempt}",
            file=os.sys.stderr,
            flush=True,
        )
        raise


def main(argv=None) -> int:
    args = _arguments(argv)
    try:
        result = run(args)
    except Exception as error:
        print(f"half-life comparison failed: {error}", file=os.sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
