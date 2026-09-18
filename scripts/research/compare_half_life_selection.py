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
import pyarrow.parquet as pq

from tape_data_product.query import open_tape_database


PILOT_START = date(2026, 6, 1)
PILOT_END = date(2026, 6, 5)
HALF_LIVES = (30, 120)
OFF_DELAY_SECONDS = 30
MINIMUM_EPISODE_SECONDS = 600
MINIMUM_OCCUPANCY = Decimal("0.80")
FREE_DISK_RESERVE_BYTES = 20 * 1024**3
OUTPUT_SCRATCH_CAP_BYTES = 4 * 1024**3
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
    parser.add_argument("--memory-limit", default="512MiB")
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
    try:
        for result in results:
            for batch in result.arrow_batches(batch_size=batch_size):
                if writer is None:
                    writer = pq.ParquetWriter(path, batch.schema, compression="zstd")
                elif not batch.schema.equals(writer.schema):
                    raise ValueError("member projections returned inconsistent schemas")
                writer.write_batch(batch)
                rows += batch.num_rows
    finally:
        if writer is not None:
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
        CREATE TEMP VIEW projected AS
        SELECT * FROM read_parquet('{projection_literal}', hive_partitioning=false);
        CREATE TEMP VIEW condition_rows AS
        SELECT *,
               {fast_available} AS fast_available,
               {slow_available} AS slow_available,
               {', '.join(condition_columns)}
        FROM projected;
        CREATE TEMP VIEW evaluated AS
        SELECT *,
               fast_available AND slow_available AS common_eligible,
               coalesce(fast_available AND ({_all_pass(30)}), false) AS fast_own_match,
               coalesce(fast_available AND slow_available AND ({_all_pass(30)}), false)
                   AS fast_match,
               coalesce(fast_available AND slow_available AND ({_all_pass(120)}), false)
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


def _episode_table(
    connection,
    reducer,
    projection: Path,
    dates: tuple[str, ...],
    eligible_sql: str,
    matching_sql: str,
) -> pa.Table:
    source_sql = f"""
        SELECT session_date, symbol, session, interval_end_ns,
               ({eligible_sql}) AS eligible,
               coalesce(({matching_sql}), false) AS matching
        FROM evaluated
    """
    # The reusable reducer SQL owns gap, boundary, trimming, duration, and occupancy semantics.
    sql = reducer._episode_sql(
        OFF_DELAY_SECONDS,
        MINIMUM_EPISODE_SECONDS,
        MINIMUM_OCCUPANCY,
        date_count=len(dates),
        source_sql=source_sql,
    )
    return connection.execute(sql, list(dates)).to_arrow_table()


def _period_variants(connection, reducer, projection: Path, dates: tuple[str, ...]):
    tables: dict[str, pa.Table] = {}
    tables["fast_own_baseline"] = _episode_table(
        connection, reducer, projection, dates, "fast_available", "fast_own_match"
    )
    for half_life, label in ((30, "fast"), (120, "slow")):
        tables[label] = _episode_table(
            connection,
            reducer,
            projection,
            dates,
            "common_eligible",
            f"{label}_match",
        )
        for condition in CONDITIONS:
            tables[f"{label}_without_{condition.key}"] = _episode_table(
                connection,
                reducer,
                projection,
                dates,
                "common_eligible",
                f"common_eligible AND ({_all_pass(half_life, omit=condition.key)})",
            )
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


def _threshold_disagreements(connection) -> list[dict]:
    records = []
    for direction, source_label, failed_label in (
        ("fast_pass_slow_fail", "fast", "slow"),
        ("slow_pass_fast_fail", "slow", "fast"),
    ):
        where = f"{source_label}_match AND NOT {failed_label}_match"
        failures = [f"NOT {failed_label}_pass_{condition.key}" for condition in CONDITIONS]
        denominator = connection.execute(f"SELECT count(*) FROM evaluated WHERE {where}").fetchone()[0]
        for condition, failure in zip(CONDITIONS, failures):
            count = connection.execute(
                f"SELECT count(*) FROM evaluated WHERE {where} AND {failure}"
            ).fetchone()[0]
            sole = connection.execute(
                f"SELECT count(*) FROM evaluated WHERE {where} AND {failure} "
                f"AND ({' + '.join(f'({item})::INTEGER' for item in failures)}) = 1"
            ).fetchone()[0]
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
        combos = _rows(
            connection,
            f"""
            SELECT rtrim({' || '.join(combo_parts)}, ',') AS failure,
                   count(*)::BIGINT AS seconds
            FROM evaluated WHERE {where}
            GROUP BY failure ORDER BY seconds DESC, failure LIMIT 20
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
    common = connection.execute(
        "SELECT count(*) FROM evaluated WHERE common_eligible"
    ).fetchone()[0]
    for half_life, label in ((30, "fast"), (120, "slow")):
        baseline_matches = connection.execute(
            f"SELECT count(*) FROM evaluated WHERE {label}_match"
        ).fetchone()[0]
        baseline_members = {
            (str(row["session_date"]), row["symbol"])
            for row in tables[label].to_pylist()
        }
        for condition in CONDITIONS:
            standalone = connection.execute(
                f"SELECT count(*) FROM evaluated WHERE common_eligible "
                f"AND ({_predicate(condition, half_life)})"
            ).fetchone()[0]
            removed_matches = connection.execute(
                f"SELECT count(*) FROM evaluated WHERE common_eligible "
                f"AND ({_all_pass(half_life, omit=condition.key)})"
            ).fetchone()[0]
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
    baseline_status = "matched all six published counts" if baseline["matches_expected"] else "DID NOT match all six published counts"
    lines = [
        "# 30-second versus 120-second EW selection — bounded pilot",
        "",
        "This is a descriptive comparison of the existing feature views for 2026-06-01 through 2026-06-05. It is not parameter optimization or a trading backtest.",
        "",
        "## Universe membership",
        "",
        f"At the matching-second definition, {summary_count('matching_second', 'both')} symbol-days appear in both screens, {summary_count('matching_second', 'fast_only')} only in fast, {summary_count('matching_second', 'slow_only')} only in slow, and {summary_count('matching_second', 'neither')} in neither.",
        f"At the retained-period definition, the corresponding counts are {summary_count('retained_period', 'both')}, {summary_count('retained_period', 'fast_only')}, {summary_count('retained_period', 'slow_only')}, and {summary_count('retained_period', 'neither')}.",
        f"Common eligibility covers {available['both_available_seconds']:,} of {available['represented_seconds']:,} represented seconds. Members with zero common-eligible time are flagged in `membership.csv`.",
        "",
        "## Time overlap",
        "",
        f"Matching endpoints: fast {endpoint['fast_seconds']:,}s, slow {endpoint['slow_seconds']:,}s, shared {endpoint['shared_seconds']:,}s, union {endpoint['union_seconds']:,}s; shared/union = {endpoint['shared_over_union'] if endpoint['shared_over_union'] is not None else 'undefined'}.",
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
            "This five-date, acquisition-selected population does not establish a preferred half-life or full-release frequency. Consecutive seconds share overlapping returns and EW history and are not independent observations.",
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
) -> dict:
    reducer = _load_reducer()
    connection = duckdb.connect(
        ":memory:",
        config={"threads": "1", "memory_limit": memory_limit, "max_temp_directory_size": "0B"},
    )
    try:
        _create_views(connection, projection)
        periods = _period_variants(connection, reducer, projection, dates)
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
        disagreements = _threshold_disagreements(connection)
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
            ),
            encoding="utf-8",
        )
        return {
            "represented_members": len(member_keys),
            "represented_rows": connection.execute("SELECT count(*) FROM evaluated").fetchone()[0],
            "baseline": baseline,
        }
    finally:
        connection.close()


def run(args) -> dict:
    _require_product_arguments(args)
    start, end = _validate_scope(args.start_date, args.end_date, args.allow_expanded_scope)
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(output.parent).free < FREE_DISK_RESERVE_BYTES:
        raise ValueError("output destination violates the 20 GiB free-disk reserve")
    attempt = output.parent / f".{output.name}.attempt-{os.getpid()}"
    if attempt.exists():
        raise FileExistsError(attempt)
    attempt.mkdir()
    projection = attempt / "disposable_projection.parquet"
    query = _load_projection_sql()
    started = time.perf_counter()
    database = None
    try:
        database = open_tape_database(
            args.catalog,
            expected_identity=args.identity,
            data_roots={"base": args.base_root, "features": args.feature_root},
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            memory_limit="256MiB",
        )
        selected_members = list(database.selected_members)
        projection_started = time.perf_counter()

        def member_results():
            for member in selected_members:
                session_date, symbol = member.split("/", 1)
                yield database.sql(query, params=[session_date, symbol])

        rows, projection_bytes = _write_projection_results(
            member_results(), projection, args.batch_size
        )
        if projection_bytes > OUTPUT_SCRATCH_CAP_BYTES:
            raise ValueError("disposable projection exceeds the 4 GiB output/scratch cap")
        projection_seconds = time.perf_counter() - projection_started
        source = {
            "query_catalog_identity": database.catalog_identity,
            "release_source_revision": database.release_source_revision,
            "release_wheel_sha256": database.release_wheel_sha256,
            "validation_seconds": database.validation_seconds,
            "validation_bytes": database.validation_bytes,
        }
        database.close()
        database = None

        analysis_started = time.perf_counter()
        dates = tuple(
            date.fromordinal(start.toordinal() + offset).isoformat()
            for offset in range((end - start).days + 1)
        )
        result = _analyze_projection(
            projection, attempt, dates, memory_limit=args.memory_limit
        )
        analysis_seconds = time.perf_counter() - analysis_started
        projection.unlink()
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
        metadata = {
            "schema": "half_life_selection_study_v1",
            "dates": {"start": start.isoformat(), "end": end.isoformat(), "inclusive": True},
            "actual_member_count": len(selected_members),
            "actual_row_count": rows,
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
                "free_disk_reserve_bytes": FREE_DISK_RESERVE_BYTES,
                "output_scratch_cap_bytes": OUTPUT_SCRATCH_CAP_BYTES,
            },
            "runtime_seconds": {
                "projection": projection_seconds,
                "analysis": analysis_seconds,
                "total": time.perf_counter() - started,
            },
            "disposable_projection": {
                "rows": rows,
                "bytes_before_deletion": projection_bytes,
                "deleted": True,
            },
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
        shutil.rmtree(attempt, ignore_errors=True)
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
