#!/usr/bin/env python3
"""Storage-bounded full-release EW dollar-throughput follow-up.

Each session date is queried through the supported endpoint interface, reduced
immediately, and committed as a compact identity-bound checkpoint.  Wide
endpoint projections are transient scratch and are deleted after checkpoint
verification.  The only endpoint-level durable values are dollar rates for
original matches, retained narrowly to support exact global quantiles.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date
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

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from tape_data_product.query import open_tape_database
from tape_data_product.query.endpoint_catalog import read_endpoint_query_catalog


FULL_START = date(2026, 3, 9)
FULL_END = date(2026, 8, 31)
FLOOR = Decimal("10000")
RESERVE_BYTES = 20 * 1024**3
CHECKPOINT_SCHEMA = "half_life_dollar_throughput_date_v2"
STUDY_SCHEMA = "half_life_dollar_throughput_study_v2"
PERIOD_VARIANTS = (
    ("fast_original", "common_segment", "fast_original_match"),
    ("slow_original", "common_segment", "slow_original_match"),
    ("fast_constrained", "common_segment", "fast_constrained_match"),
    ("slow_constrained", "common_segment", "slow_constrained_match"),
)


def _load(name: str, filename: str):
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


DOLLAR = _load("dollar_reference", "analyze_half_life_dollar_throughput.py")
BASE = DOLLAR.BASE


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for flag, env in (
        ("catalog", "QUERY_CATALOG"),
        ("identity", "QUERY_CATALOG_IDENTITY"),
        ("base-root", "BASE_ROOT"),
        ("feature-root", "FEATURE_ROOT"),
        ("baseline-study", "BASELINE_STUDY"),
    ):
        parser.add_argument("--" + flag, default=os.environ.get(env))
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--date", action="append", default=[], help="exact represented date; repeat for a non-contiguous benchmark")
    parser.add_argument("--member", action="append", default=[])
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--checkpoint-root", required=True, type=Path)
    parser.add_argument("--temp-directory", required=True, type=Path)
    parser.add_argument("--dollar-floor", default="10000")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="4GiB")
    parser.add_argument("--max-temp-directory-size", default="8GiB")
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--allow-expanded-scope", action="store_true")
    parser.add_argument("--finalize-only", action="store_true")
    return parser.parse_args(argv)


def validate(args):
    missing = [name for name in ("catalog", "identity", "base_root", "feature_root", "baseline_study") if not getattr(args, name)]
    if missing:
        raise ValueError("missing product configuration: " + ", ".join(missing))
    start, end = date.fromisoformat(args.start_date), date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("reversed date scope")
    if (start < FULL_START or end > FULL_END) and not args.allow_expanded_scope:
        raise ValueError("scope outside the immutable endpoint release")
    selected_dates = [date.fromisoformat(value) for value in args.date]
    if len(set(selected_dates)) != len(selected_dates) or any(value < start or value > end for value in selected_dates):
        raise ValueError("--date values must be unique and inside the requested bounds")
    floor = Decimal(args.dollar_floor)
    if floor != FLOOR:
        raise ValueError("this fixed follow-up requires exactly 10000 USD/s")
    if not 1 <= args.threads <= 8 or not 1 <= args.batch_size <= 25_000:
        raise ValueError("threads or batch size outside supported bounds")
    if not Path(args.baseline_study).is_dir():
        raise ValueError("baseline study is missing")
    return start, end


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def jsonable(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def write_records(root: Path, stem: str, records: list[dict]) -> None:
    payload = [{key: jsonable(value) for key, value in row.items()} for row in records]
    (root / f"{stem}.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    fields = list(payload[0]) if payload else []
    with (root / f"{stem}.csv").open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        if fields:
            writer.writeheader()
            writer.writerows(payload)


def rows(connection, sql: str, params=()) -> list[dict]:
    return connection.execute(sql, params).to_arrow_table().to_pylist()


def _query() -> str:
    return (Path(__file__).with_name("half_life_dollar_throughput") / "projection.sql").read_text().strip()


def implementation_identity() -> str:
    digest = hashlib.sha256()
    for path in (
        Path(__file__),
        Path(__file__).with_name("analyze_half_life_dollar_throughput.py"),
        Path(__file__).with_name("compare_half_life_selection.py"),
        Path(__file__).with_name("select_structured_tape_episodes.py"),
        Path(__file__).with_name("half_life_dollar_throughput") / "projection.sql",
    ):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def analysis_revision() -> str:
    configured = os.environ.get("ANALYSIS_SOURCE_REVISION")
    if configured:
        return configured
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=Path(__file__).parents[2], check=True, capture_output=True, text=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise ValueError("ANALYSIS_SOURCE_REVISION is required outside a Git checkout") from error


def artifact_records(root: Path) -> list[dict]:
    return [
        {"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)}
        for path in sorted(root.iterdir())
        if path.is_file() and path.name != "checkpoint.json"
    ]


def validate_checkpoint(root: Path, expected: dict) -> dict | None:
    if not root.exists():
        return None
    manifest_path = root / "checkpoint.json"
    if not root.is_dir() or not manifest_path.is_file():
        raise ValueError(f"incomplete checkpoint: {root}")
    try:
        manifest = json.loads(manifest_path.read_text())
    except (OSError, ValueError) as error:
        raise ValueError(f"corrupt checkpoint manifest: {root}") from error
    mismatches = [key for key, value in expected.items() if manifest.get(key) != value]
    if mismatches:
        raise ValueError(f"checkpoint identity mismatch for {root.name}: {mismatches}")
    artifacts = manifest.get("artifacts")
    if not artifacts:
        raise ValueError(f"checkpoint has no artifacts: {root}")
    for artifact in artifacts:
        path = root / artifact["path"]
        if not path.is_file() or path.stat().st_size != artifact["bytes"] or sha256(path) != artifact["sha256"]:
            raise ValueError(f"checkpoint artifact failed verification: {path}")
    return manifest


def expected_checkpoint(current: str, members: tuple[str, ...], source: dict, args, query_sha: str, impl: str) -> dict:
    return {
        "schema": CHECKPOINT_SCHEMA,
        "date": current,
        "ordered_members": list(members),
        "implementation_identity": impl,
        "query_catalog_identity": args.identity,
        "endpoint_release_source_revision": source["source_revision"],
        "endpoint_release_wheel_sha256": source["wheel_sha256"],
        "dollar_floor_usd_per_second": "10000",
        "query_projection_identity": query_sha,
        "condition_semantics": "original common-eligible seven-condition fast/slow screen plus own-view EW dollar rate >= 10000; both dollar rates required for primary comparison",
        "period_semantics": {"minimum_elapsed_seconds": 600, "minimum_occupancy": "0.80", "off_delay_seconds": 30, "first_last_matching_trim": True, "half_open": True, "unavailable_and_grid_gaps_break": True},
        "execution_settings": {"batch_size": args.batch_size, "threads": args.threads, "memory_limit": args.memory_limit, "max_temp_directory_size": args.max_temp_directory_size},
    }


def period_tables(connection, current: str) -> dict[str, pa.Table]:
    original = DOLLAR.reduce_periods(
        connection,
        [("fast_original", "common_segment", "fast_original_match"), ("slow_original", "common_segment", "slow_original_match")],
        "original_common_eligible",
        (current,),
    )
    constrained = DOLLAR.reduce_periods(
        connection,
        [("fast_constrained", "common_segment", "fast_constrained_match"), ("slow_constrained", "common_segment", "slow_constrained_match")],
        "common_dollar_eligible",
        (current,),
    )
    return {**original, **constrained}


def write_periods(path: Path, tables: dict[str, pa.Table]) -> None:
    records = []
    for variant, _, _ in PERIOD_VARIANTS:
        records.extend({"variant": variant, **row} for row in tables[variant].to_pylist())
    records.sort(key=lambda r: (r["variant"], str(r["session_date"]), r["symbol"], r["session"], r["episode_start_ns"]))
    table = pa.Table.from_pylist(records) if records else pa.table({
        "variant": pa.array([], type=pa.string()), "session_date": pa.array([], type=pa.date32()),
        "symbol": pa.array([], type=pa.string()), "session": pa.array([], type=pa.string()),
        "episode_id": pa.array([], type=pa.int64()), "episode_start_ns": pa.array([], type=pa.int64()),
        "episode_end_ns": pa.array([], type=pa.int64()), "episode_start_utc": pa.array([], type=pa.string()),
        "episode_end_utc": pa.array([], type=pa.string()), "elapsed_seconds": pa.array([], type=pa.int64()),
        "matching_seconds": pa.array([], type=pa.int64()), "eligible_nonmatching_seconds": pa.array([], type=pa.int64()),
        "occupancy": pa.array([], type=pa.float64()), "maximum_nonmatching_gap_seconds": pa.array([], type=pa.int64()),
    })
    pq.write_table(table, path, compression="zstd")


def _inside_period_sql(view: str) -> str:
    return (
        "EXISTS (SELECT 1 FROM period_rows p WHERE p.variant='" + view + "_original' "
        "AND p.session_date=s.session_date AND p.symbol=s.symbol AND p.session=s.session "
        "AND s.interval_end_ns>p.episode_start_ns AND s.interval_end_ns<=p.episode_end_ns)"
    )


def period_relation(records: list[dict]) -> pa.Table:
    if records:
        return pa.Table.from_pylist(records)
    return pa.table({
        "variant": pa.array([], type=pa.string()),
        "session_date": pa.array([], type=pa.date32()),
        "symbol": pa.array([], type=pa.string()),
        "session": pa.array([], type=pa.string()),
        "episode_start_ns": pa.array([], type=pa.int64()),
        "episode_end_ns": pa.array([], type=pa.int64()),
    })


def write_quantile_values(connection, path: Path, tables: dict[str, pa.Table]) -> None:
    records = []
    for variant in ("fast_original", "slow_original"):
        records.extend({"variant": variant, **row} for row in tables[variant].to_pylist())
    period_rows = period_relation(records)
    connection.register("period_rows", period_rows)
    unions = []
    for view, half_life in (("fast", 30), ("slow", 120)):
        field = f"dollar_rate_usd_per_second_hl{half_life}s"
        valid = f"{view}_dollar_valid"
        match = f"{view}_original_match"
        unions.append(f"SELECT '{view}' AS \"view\", 'all_original_matches' AS population, {field} AS value FROM source s WHERE {match} AND {valid}")
        unions.append(f"SELECT '{view}' AS \"view\", 'original_retained_periods' AS population, {field} AS value FROM source s WHERE {match} AND {valid} AND {_inside_period_sql(view)}")
    connection.execute("COPY (" + " UNION ALL ".join(unions) + f") TO '{str(path).replace(chr(39), chr(39)*2)}' (FORMAT PARQUET, COMPRESSION ZSTD)")


def date_aggregates(connection, tables: dict[str, pa.Table]) -> dict:
    aggregate_rows = []
    groupings = (
        ("symbol_day", "CAST(session_date AS VARCHAR)", "symbol", "NULL", "GROUP BY session_date,symbol"),
        ("session", "NULL", "NULL", "session", "GROUP BY session"),
        ("date", "CAST(session_date AS VARCHAR)", "NULL", "NULL", "GROUP BY session_date"),
        ("overall", "NULL", "NULL", "NULL", ""),
    )
    for view, half_life in (("fast", 30), ("slow", 120)):
        dollar = f"dollar_rate_usd_per_second_hl{half_life}s"
        for dimension, day, symbol, session, group in groupings:
            aggregate_rows.extend(rows(connection, f"""
                SELECT '{dimension}' AS dimension,{day} AS session_date,{symbol} AS symbol,{session} AS "session",'{view}' AS "view",
                 count(*)::BIGINT represented_seconds,
                 count(*) FILTER(WHERE original_common_eligible)::BIGINT common_eligible_seconds,
                 count(*) FILTER(WHERE original_common_eligible AND NOT(fast_dollar_valid AND slow_dollar_valid))::BIGINT dollar_unavailable_seconds,
                 count(*) FILTER(WHERE {view}_original_match)::BIGINT original_matching_seconds,
                 count(*) FILTER(WHERE {view}_original_match AND NOT(fast_dollar_valid AND slow_dollar_valid))::BIGINT original_matching_dollar_unavailable_seconds,
                 count(*) FILTER(WHERE {view}_original_match AND fast_dollar_valid AND slow_dollar_valid AND {dollar}<10000)::BIGINT matching_seconds_below_floor,
                 count(*) FILTER(WHERE {view}_constrained_match)::BIGINT remaining_matching_seconds,
                 count(*) FILTER(WHERE {view}_own_match)::BIGINT own_original_matching_seconds,
                 count(*) FILTER(WHERE {view}_own_match AND {view}_dollar_valid)::BIGINT own_dollar_valid_matching_seconds,
                 count(*) FILTER(WHERE {view}_own_match AND NOT {view}_dollar_valid)::BIGINT own_dollar_unavailable_matching_seconds
                FROM source {group} ORDER BY session_date,symbol,"session"
            """))
    overlap = rows(connection, """
        SELECT CAST(session_date AS VARCHAR) session_date,symbol,
         count(*) FILTER(WHERE fast_constrained_match)::BIGINT fast_seconds,
         count(*) FILTER(WHERE slow_constrained_match)::BIGINT slow_seconds,
         count(*) FILTER(WHERE fast_constrained_match AND slow_constrained_match)::BIGINT shared_seconds,
         count(*) FILTER(WHERE fast_constrained_match AND NOT slow_constrained_match)::BIGINT fast_only_seconds,
         count(*) FILTER(WHERE slow_constrained_match AND NOT fast_constrained_match)::BIGINT slow_only_seconds,
         count(*) FILTER(WHERE fast_constrained_match OR slow_constrained_match)::BIGINT union_seconds
        FROM source GROUP BY session_date,symbol ORDER BY session_date,symbol
    """)
    original_overlap = rows(connection, """
        SELECT CAST(session_date AS VARCHAR) session_date,symbol,
         count(*) FILTER(WHERE fast_original_match)::BIGINT fast_seconds,
         count(*) FILTER(WHERE slow_original_match)::BIGINT slow_seconds,
         count(*) FILTER(WHERE fast_original_match AND slow_original_match)::BIGINT shared_seconds,
         count(*) FILTER(WHERE fast_original_match AND NOT slow_original_match)::BIGINT fast_only_seconds,
         count(*) FILTER(WHERE slow_original_match AND NOT fast_original_match)::BIGINT slow_only_seconds,
         count(*) FILTER(WHERE fast_original_match OR slow_original_match)::BIGINT union_seconds
        FROM source GROUP BY session_date,symbol ORDER BY session_date,symbol
    """)
    profile_counts = []
    for view in ("fast", "slow"):
        inside = _inside_period_sql(view)
        for population, extra in (("all_original_matches", "TRUE"), ("original_retained_periods", inside)):
            profile_counts.append(rows(connection, f"""
                SELECT '{view}' AS "view",'{population}' AS population,
                 count(*) FILTER(WHERE {view}_original_match)::BIGINT original_matching_seconds,
                 count(*) FILTER(WHERE {view}_original_match AND {view}_dollar_valid)::BIGINT dollar_valid_matching_seconds,
                 count(*) FILTER(WHERE {view}_original_match AND NOT {view}_dollar_valid)::BIGINT dollar_unavailable_matching_seconds,
                 count(*) FILTER(WHERE {view}_original_match AND {view}_dollar_valid AND dollar_rate_usd_per_second_hl{'30' if view=='fast' else '120'}s=0)::BIGINT valid_zero_dollar_matching_seconds
                FROM source s WHERE {extra}
            """)[0])
    transition = rows(connection, """
        SELECT count(*) FILTER(WHERE original_common_eligible AND fast_dollar_valid AND slow_dollar_valid AND dollar_rate_usd_per_second_hl30s>=10000 AND dollar_rate_usd_per_second_hl120s<10000)::BIGINT fast_above_slow_below,
         count(*) FILTER(WHERE original_common_eligible AND fast_dollar_valid AND slow_dollar_valid AND dollar_rate_usd_per_second_hl120s>=10000 AND dollar_rate_usd_per_second_hl30s<10000)::BIGINT slow_above_fast_below,
         count(*) FILTER(WHERE fast_original_match AND NOT slow_original_match)::BIGINT original_fast_only_seconds,
         count(*) FILTER(WHERE slow_original_match AND NOT fast_original_match)::BIGINT original_slow_only_seconds,
         count(*) FILTER(WHERE fast_original_match AND NOT slow_original_match AND fast_dollar_valid AND slow_dollar_valid AND dollar_rate_usd_per_second_hl30s<10000)::BIGINT original_fast_only_low_own_dollar,
         count(*) FILTER(WHERE slow_original_match AND NOT fast_original_match AND fast_dollar_valid AND slow_dollar_valid AND dollar_rate_usd_per_second_hl120s<10000)::BIGINT original_slow_only_low_own_dollar
        FROM source
    """)[0]
    return {"aggregates": aggregate_rows, "matching_overlap_members": overlap, "original_matching_overlap_members": original_overlap, "profile_counts": profile_counts, "threshold_crossing": transition}


def reduce_date(projection: Path, root: Path, current: str, args) -> dict:
    scratch = args.temp_directory.resolve() / current / "analysis"
    scratch.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(":memory:", config={"threads": str(args.threads), "memory_limit": args.memory_limit, "temp_directory": str(scratch), "max_temp_directory_size": args.max_temp_directory_size})
    phase = {}
    try:
        started = time.perf_counter(); DOLLAR.create_views(connection, projection, FLOOR); phase["condition_evaluation"] = time.perf_counter() - started
        started = time.perf_counter(); tables = period_tables(connection, current); phase["period_reduction"] = time.perf_counter() - started
        period_records = []
        for variant, _, _ in PERIOD_VARIANTS:
            period_records.extend({"variant": variant, **row} for row in tables[variant].to_pylist())
        connection.register("period_rows", period_relation(period_records))
        started = time.perf_counter(); aggregate = date_aggregates(connection, tables); phase["aggregate_statistics"] = time.perf_counter() - started
        started = time.perf_counter(); write_quantile_values(connection, root / "dollar_values.parquet", tables); phase["dollar_profile_aggregation"] = time.perf_counter() - started
        started = time.perf_counter(); write_periods(root / "periods.parquet", tables); (root / "aggregates.json").write_text(json.dumps(aggregate, indent=2, sort_keys=True, default=jsonable) + "\n"); phase["checkpoint_writing"] = time.perf_counter() - started
        return {"phase_seconds": phase, "period_rows": len(period_records), "quantile_rows": pq.ParquetFile(root / "dollar_values.parquet").metadata.num_rows}
    finally:
        connection.close()


def rollup(records: list[dict], keys: tuple[str, ...], values: tuple[str, ...]) -> list[dict]:
    grouped = {}
    for row in records:
        key = tuple(row.get(k) for k in keys)
        target = grouped.setdefault(key, {k: row.get(k) for k in keys} | {v: 0 for v in values})
        for value in values:
            target[value] += int(row[value])
    return [grouped[key] for key in sorted(grouped, key=lambda x: tuple("" if v is None else str(v) for v in x))]


def overlap_ratios(records: list[dict]) -> list[dict]:
    for row in records:
        row["shared_over_union"] = row["shared_seconds"] / row["union_seconds"] if row["union_seconds"] else None
        row["shared_over_fast"] = row["shared_seconds"] / row["fast_seconds"] if row["fast_seconds"] else None
        row["shared_over_slow"] = row["shared_seconds"] / row["slow_seconds"] if row["slow_seconds"] else None
    return records


def load_periods(checkpoints: list[Path]) -> list[dict]:
    output = []
    for root in checkpoints:
        output.extend(pq.read_table(root / "periods.parquet").to_pylist())
    output.sort(key=lambda r: (r["variant"], str(r["session_date"]), r["symbol"], r["session"], r["episode_start_ns"]))
    return output


def period_overlap(periods: list[dict], members: list[tuple[str, str]]) -> list[dict]:
    tables = {}
    for prefix, variant in (("fast", "fast_constrained"), ("slow", "slow_constrained")):
        tables[prefix] = pa.Table.from_pylist([{k: v for k, v in row.items() if k != "variant"} for row in periods if row["variant"] == variant])
    return BASE._period_overlap_records(tables, members)


def membership_summary(aggregates: list[dict], periods: list[dict]) -> list[dict]:
    symbol_days = [row for row in aggregates if row["dimension"] == "symbol_day"]
    output = []
    for view in ("fast", "slow"):
        selected = [row for row in symbol_days if row["view"] == view]
        for cohort, field, variant in (("original", "original_matching_seconds", f"{view}_original"), ("constrained", "remaining_matching_seconds", f"{view}_constrained")):
            matched = [row for row in selected if row[field] > 0]
            retained = [row for row in periods if row["variant"] == variant]
            durations = sorted(int(row["elapsed_seconds"]) for row in retained)
            elapsed = sum(durations); matching = sum(int(row["matching_seconds"]) for row in retained)
            median = None if not durations else durations[len(durations)//2] if len(durations)%2 else (durations[len(durations)//2-1]+durations[len(durations)//2])/2
            overall = rollup(selected, ("view",), ("common_eligible_seconds", "dollar_unavailable_seconds", "original_matching_seconds", "matching_seconds_below_floor", "remaining_matching_seconds"))[0]
            output.append({"view": view, "cohort": cohort, **{k: overall[k] for k in ("common_eligible_seconds", "dollar_unavailable_seconds", "original_matching_seconds", "matching_seconds_below_floor", "remaining_matching_seconds")}, "pct_dollar_valid_original_matches_retained": overall["remaining_matching_seconds"] / (overall["original_matching_seconds"] - sum(r["original_matching_dollar_unavailable_seconds"] for r in selected)) if overall["original_matching_seconds"] - sum(r["original_matching_dollar_unavailable_seconds"] for r in selected) else None, "symbol_days_with_any_match": len(matched), "distinct_stocks_with_any_match": len({r["symbol"] for r in matched}), "retained_periods": len(retained), "retained_symbol_days": len({(str(r["session_date"]), r["symbol"]) for r in retained}), "distinct_retained_stocks": len({r["symbol"] for r in retained}), "retained_period_seconds": elapsed, "matching_seconds_inside_retained_periods": matching, "mean_retained_period_duration": elapsed/len(retained) if retained else None, "median_retained_period_duration": median, "weighted_retained_period_occupancy": matching/elapsed if elapsed else None, "premarket_periods": sum(r["session"]=="premarket" for r in retained), "rth_periods": sum(r["session"]=="rth" for r in retained), "after_hours_periods": sum(r["session"]=="after_hours" for r in retained)})
    return output


def dollar_profiles(checkpoints: list[Path], counts: list[dict], scratch: Path) -> list[dict]:
    connection = duckdb.connect(":memory:", config={"threads": "1", "memory_limit": "1GiB", "temp_directory": str(scratch)})
    try:
        paths = ",".join("'" + str(root / "dollar_values.parquet").replace("'", "''") + "'" for root in checkpoints)
        values = rows(connection, f"""SELECT \"view\",population,min(value) minimum,quantile_disc(value,.01) p1,quantile_disc(value,.05) p5,quantile_disc(value,.10) p10,quantile_disc(value,.25) p25,quantile_disc(value,.50) median,count(*) FILTER(WHERE value<10000)::DOUBLE/nullif(count(*),0) percentage_below_10000 FROM read_parquet([{paths}],union_by_name=true) GROUP BY \"view\",population ORDER BY \"view\",population""")
    finally:
        connection.close()
    count_map = {(r["view"], r["population"]): r for r in rollup(counts, ("view", "population"), ("original_matching_seconds", "dollar_valid_matching_seconds", "dollar_unavailable_matching_seconds", "valid_zero_dollar_matching_seconds"))}
    return [{**count_map[(row["view"], row["population"])], **{k: v for k, v in row.items() if k not in ("view", "population")}} for row in values]


def baseline_reproduction(baseline_root: Path, dates: set[str], original_overlap: list[dict], periods: list[dict]) -> dict:
    baseline_overlap = json.loads((baseline_root / "matching_endpoint_overlap.json").read_text())
    selected_members = {(r["session_date"], r["symbol"]) for r in original_overlap}
    baseline_members = [r for r in baseline_overlap if r["scope"] == "member" and (r["session_date"], r["symbol"]) in selected_members]
    expected = {name: sum(int(r[name]) for r in baseline_members) for name in ("fast_seconds", "slow_seconds", "shared_seconds", "fast_only_seconds", "slow_only_seconds", "union_seconds")}
    actual = {name: sum(int(r[name]) for r in original_overlap) for name in ("fast_seconds", "slow_seconds", "shared_seconds", "fast_only_seconds", "slow_only_seconds", "union_seconds")}
    saved_periods = json.loads((baseline_root / "retained_periods.json").read_text())
    expected_periods = [{k: v for k, v in r.items() if k != "variant"} | {"variant": r["variant"]} for r in saved_periods if (r["session_date"], r["symbol"]) in selected_members and r["variant"] in ("fast", "slow")]
    expected_periods = [{**r, "variant": r["variant"] + "_original"} for r in expected_periods]
    actual_periods = [{k: jsonable(v) for k, v in r.items()} for r in periods if r["variant"] in ("fast_original", "slow_original")]
    expected_periods.sort(key=lambda r: (r["variant"], r["session_date"], r["symbol"], r["episode_start_ns"]))
    actual_periods.sort(key=lambda r: (r["variant"], str(r["session_date"]), r["symbol"], r["episode_start_ns"]))
    return {"endpoint_counts_expected": expected, "endpoint_counts_actual": actual, "endpoint_counts_exact": actual == expected, "retained_periods_expected": len(expected_periods), "retained_periods_actual": len(actual_periods), "retained_periods_exact": actual_periods == expected_periods, "exact": actual == expected and actual_periods == expected_periods}


def finalize(attempt: Path, checkpoints: list[Path], manifests: list[dict], catalog: dict, catalog_bytes: int, start: date, end: date, args, started: float) -> dict:
    merge_started = time.perf_counter()
    payloads = [json.loads((root / "aggregates.json").read_text()) for root in checkpoints]
    aggregates = [r for payload in payloads for r in payload["aggregates"]]
    overlap_members = [r for payload in payloads for r in payload["matching_overlap_members"]]
    profile_counts = [r for payload in payloads for r in payload["profile_counts"]]
    crossing = rollup([payload["threshold_crossing"] for payload in payloads], tuple(), ("fast_above_slow_below", "slow_above_fast_below", "original_fast_only_seconds", "original_slow_only_seconds", "original_fast_only_low_own_dollar", "original_slow_only_low_own_dollar"))[0]
    original_overlap_members = [r for payload in payloads for r in payload["original_matching_overlap_members"]]
    periods = load_periods(checkpoints)
    members = sorted({(r["session_date"], r["symbol"]) for r in overlap_members})
    overlap = rollup(overlap_members, ("session_date", "symbol"), ("fast_seconds", "slow_seconds", "shared_seconds", "fast_only_seconds", "slow_only_seconds", "union_seconds"))
    overlap += rollup(overlap_members, ("session_date",), ("fast_seconds", "slow_seconds", "shared_seconds", "fast_only_seconds", "slow_only_seconds", "union_seconds"))
    overlap += rollup(overlap_members, tuple(), ("fast_seconds", "slow_seconds", "shared_seconds", "fast_only_seconds", "slow_only_seconds", "union_seconds"))
    for row in overlap:
        row["scope"] = "member" if "symbol" in row else "date" if "session_date" in row else "overall"
        row.setdefault("session_date", None); row.setdefault("symbol", None)
    overlap = overlap_ratios(overlap)
    retained_overlap = period_overlap(periods, members)
    summary = membership_summary(aggregates, periods)
    profile = dollar_profiles(checkpoints, profile_counts, args.temp_directory / "final-quantiles")
    base = baseline_reproduction(Path(args.baseline_study), {p.name for p in checkpoints}, original_overlap_members, periods)
    if not base["exact"]:
        raise AssertionError("original endpoint/period baseline reproduction failed")
    overall_gate = rollup([r for r in aggregates if r["dimension"] == "symbol_day"], ("view",), ("represented_seconds", "common_eligible_seconds", "dollar_unavailable_seconds", "original_matching_seconds", "original_matching_dollar_unavailable_seconds", "matching_seconds_below_floor", "remaining_matching_seconds", "own_original_matching_seconds", "own_dollar_valid_matching_seconds", "own_dollar_unavailable_matching_seconds"))
    gate_records = aggregates + overall_gate
    contribution_specs = {
        "contribution_by_session": ("session",), "contribution_by_date": ("session_date",),
        "contribution_by_symbol": ("symbol",), "contribution_by_symbol_day": ("session_date", "symbol"),
    }
    write_records(attempt, "dollar_profile", profile)
    write_records(attempt, "dollar_gate_accounting", gate_records)
    write_records(attempt, "threshold_crossing", [crossing])
    write_records(attempt, "constrained_matching_overlap", overlap)
    write_records(attempt, "constrained_membership_summary", summary)
    write_records(attempt, "constrained_retained_periods", [{k: jsonable(v) for k, v in r.items()} for r in periods])
    write_records(attempt, "constrained_retained_overlap", retained_overlap)
    for stem, keys in contribution_specs.items():
        source = [r for r in aggregates if (stem.endswith("session") and r["dimension"]=="session") or (stem.endswith("date") and r["dimension"]=="date") or (stem.endswith("symbol_day") and r["dimension"]=="symbol_day") or (stem.endswith("symbol") and not stem.endswith("symbol_day") and r["dimension"]=="symbol_day")]
        values = ("common_eligible_seconds", "dollar_unavailable_seconds", "original_matching_seconds", "original_matching_dollar_unavailable_seconds", "matching_seconds_below_floor", "remaining_matching_seconds")
        write_records(attempt, stem, rollup(source, keys + ("view",), values))
    (attempt / "baseline_reproduction.json").write_text(json.dumps(base, indent=2, sort_keys=True) + "\n")
    (attempt / "query.sql").write_text(_query() + "\n")
    selected_dates = [p.name for p in checkpoints]
    config = {"schema": "half_life_dollar_throughput_config_v2", "date_scope": {"start": str(start), "end": str(end), "inclusive": True, "selected_dates": selected_dates}, "dollar_floor_usd_per_second": "10000", "description": "fixed illustrative minimum in recently observed EW transaction throughput", "common_dollar_eligibility": "both corresponding dollar rates valid", "unavailable_dollar_breaks_periods": True, "below_floor_is_eligible_nonmatch": True, "minimum_episode_seconds": 600, "minimum_occupancy": "0.80", "off_delay_seconds": 30}
    (attempt / "config.json").write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
    fast = next(r for r in summary if r["view"]=="fast" and r["cohort"]=="constrained"); slow = next(r for r in summary if r["view"]=="slow" and r["cohort"]=="constrained")
    match_overall = next(r for r in overlap if r["scope"]=="overall"); period_overall = next(r for r in retained_overlap if r["scope"]=="overall")
    lines = ["# 30-second versus 120-second EW screen with a fixed $10,000/s throughput condition", "", "The added threshold is a fixed illustrative minimum in recently observed EW transaction throughput. It is not executable capacity, accessible liquidity, or a validated participation-rate limit.", "", "## Constrained selection", "", f"Fast retains {fast['remaining_matching_seconds']:,} matching seconds and {fast['retained_periods']:,} periods; slow retains {slow['remaining_matching_seconds']:,} matching seconds and {slow['retained_periods']:,} periods.", f"Matching shared/union is {match_overall['shared_over_union']:.6f}; retained-period shared/union is {period_overall['shared_over_union']:.6f}.", "", "This is retrospective descriptive measurement only. There is no routing model, depth-of-book data, fill simulation, slippage, transaction-cost, latency, adverse-excursion, P&L, predictive-evidence, or executable-expectancy analysis.", ""]
    (attempt / "summary.md").write_text("\n".join(lines))
    merge_seconds = time.perf_counter() - merge_started
    baseline_hashes = [{"path": p.name, "bytes": p.stat().st_size, "sha256": sha256(p)} for p in sorted(Path(args.baseline_study).iterdir()) if p.is_file()]
    artifacts = [{"path": p.name, "bytes": p.stat().st_size, "sha256": sha256(p)} for p in sorted(attempt.iterdir()) if p.is_file() and p.name != "run_metadata.json"]
    represented = sum(next(r for r in payload["aggregates"] if r["dimension"]=="overall" and r["view"]=="fast")["represented_seconds"] for payload in payloads)
    durable_bytes = sum(sum(a["bytes"] for a in m["artifacts"]) + (checkpoints[i]/"checkpoint.json").stat().st_size for i,m in enumerate(manifests))
    matching_value_rows = sum(m["result"]["quantile_rows"] for m in manifests)
    metadata = {"schema": STUDY_SCHEMA, "dates": {"start": str(start), "end": str(end), "inclusive": True, "selected_dates": selected_dates}, "dollar_floor_usd_per_second": "10000", "analysis_source_revision": analysis_revision(), "analysis_wheel_sha256": os.environ.get("ANALYSIS_WHEEL_SHA256"), "query_catalog_identity": args.identity, "endpoint_release_source_revision": catalog["release"]["source_revision"], "endpoint_release_wheel_sha256": catalog["release"]["wheel_sha256"], "baseline_study": {"path": str(Path(args.baseline_study).resolve()), "artifact_hashes": baseline_hashes}, "represented_endpoints": represented, "represented_members": len(members), "verification_bytes": {"catalog": catalog_bytes, "selected_files": sum(m["result"]["validation_bytes"] for m in manifests)}, "execution_settings": expected_checkpoint("x", tuple(), catalog["release"], args, hashlib.sha256(_query().encode()).hexdigest(), implementation_identity())["execution_settings"], "runtime_seconds": {"catalog_and_file_verification": sum(m["result"]["validation_seconds"] for m in manifests), "projection_query": sum(m["result"]["projection_seconds"] for m in manifests), "condition_evaluation": sum(m["result"]["phase_seconds"]["condition_evaluation"] for m in manifests), "dollar_profile_aggregation": sum(m["result"]["phase_seconds"]["dollar_profile_aggregation"] for m in manifests), "period_reduction": sum(m["result"]["phase_seconds"]["period_reduction"] for m in manifests), "checkpoint_writing": sum(m["result"]["phase_seconds"]["checkpoint_writing"] for m in manifests), "checkpoint_verification": sum(m["result"].get("checkpoint_verification_seconds", 0) for m in manifests), "final_merge": merge_seconds, "total_elapsed": time.perf_counter()-started}, "checkpoint_totals": {"durable_bytes": durable_bytes, "narrow_quantile_bytes": sum((p/"dollar_values.parquet").stat().st_size for p in checkpoints), "transient_projection_bytes_total": sum(m["result"]["projection_bytes"] for m in manifests), "transient_projection_bytes_peak_date": max(m["result"]["projection_bytes"] for m in manifests), "represented_endpoints": represented, "matching_value_rows": matching_value_rows, "durable_bytes_per_represented_endpoint": durable_bytes/represented, "durable_bytes_per_matching_value_row": durable_bytes/matching_value_rows if matching_value_rows else None}, "resource_measurement": {"peak_process_tree_rss_bytes": None, "peak_temporary_disk_bytes": None, "note": "filled from external bounded monitor"}, "artifacts": artifacts}
    (attempt / "run_metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    return metadata


def run(args) -> dict:
    start, end = validate(args); started = time.perf_counter()
    output = args.output.resolve(); checkpoint_root = args.checkpoint_root.resolve(); temp = args.temp_directory.resolve()
    for path in (output.parent, checkpoint_root.parent, temp.parent):
        path.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(path).free < RESERVE_BYTES:
            raise ValueError(f"20 GiB free-disk reserve violated at {path}")
    if output.exists():
        raise FileExistsError(output)
    attempt = output.parent / f".{output.name}.in-progress"
    if attempt.exists():
        raise FileExistsError(attempt)
    attempt.mkdir(); checkpoint_root.mkdir(exist_ok=True); temp.mkdir(exist_ok=True)
    catalog_started = time.perf_counter(); catalog, records, _, _, catalog_bytes = read_endpoint_query_catalog(args.catalog, expected_identity=args.identity); catalog_seconds = time.perf_counter()-catalog_started
    exact_dates = set(args.date)
    selected = [r for r in records if str(start)<=r["session_date"]<=str(end) and (not exact_dates or r["session_date"] in exact_dates) and (not args.member or r["member"] in set(args.member))]
    if not selected:
        raise ValueError("scope has no represented members")
    dates = tuple(sorted({r["session_date"] for r in selected})); query = _query(); query_sha = hashlib.sha256(query.encode()).hexdigest(); impl = implementation_identity(); source = catalog["release"]
    checkpoints=[]; manifests=[]
    for index,current in enumerate(dates,1):
        members=tuple(r["member"] for r in selected if r["session_date"]==current); root=checkpoint_root/current; expected=expected_checkpoint(current,members,source,args,query_sha,impl)
        existing=validate_checkpoint(root,expected)
        if existing is not None:
            checkpoints.append(root); manifests.append(existing); print(f"throughput follow-up: {current} reused verified checkpoint without endpoint read",flush=True); continue
        if args.finalize_only:
            raise ValueError(f"missing checkpoint in finalize-only mode: {current}")
        date_started=time.perf_counter(); date_temp=temp/current; attempt_root=checkpoint_root/f".{current}.attempt-{os.getpid()}"
        if attempt_root.exists():
            raise FileExistsError(attempt_root)
        attempt_root.mkdir(); date_temp.mkdir(parents=True,exist_ok=True); projection=date_temp/"projection.parquet"
        print(f"throughput follow-up: {index}/{len(dates)} {current} members={len(members)}",flush=True)
        db=open_tape_database(args.catalog, expected_identity=args.identity, data_roots={"base":args.base_root,"features":args.feature_root}, start_date=current,end_date=current,members=members,memory_limit=args.memory_limit,threads=args.threads,temp_directory=date_temp/"reader",max_temp_directory_size=args.max_temp_directory_size)
        if db.release_source_revision != source["source_revision"] or db.release_wheel_sha256 != source["wheel_sha256"]:
            raise ValueError("endpoint release identity changed")
        validation_seconds=db.validation_seconds; validation_bytes=db.validation_bytes
        projection_started=time.perf_counter(); count,projection_bytes=BASE._write_projection(db.sql(query),projection,args.batch_size); db._check_inputs(); db.close(); projection_seconds=time.perf_counter()-projection_started
        result=reduce_date(projection,attempt_root,current,args); result.update({"represented_rows":count,"represented_members":len(members),"validation_seconds":validation_seconds,"validation_bytes":validation_bytes,"projection_seconds":projection_seconds,"projection_bytes":projection_bytes,"total_seconds":time.perf_counter()-date_started})
        projection.unlink(); shutil.rmtree(date_temp,ignore_errors=True)
        manifest={**expected,"result":result,"artifacts":artifact_records(attempt_root)}
        (attempt_root/"checkpoint.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
        os.rename(attempt_root,root)
        checkpoint_verify_started = time.perf_counter(); verified=validate_checkpoint(root,expected); result["checkpoint_verification_seconds"] = time.perf_counter()-checkpoint_verify_started
        manifest["result"] = result; (root/"checkpoint.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n"); verified=validate_checkpoint(root,expected)
        if projection.exists() or date_temp.exists():
            raise AssertionError("transient projection or scratch survived checkpoint")
        checkpoints.append(root); manifests.append(verified); print(f"throughput follow-up: {current} checkpoint verified; transient projection deleted",flush=True)
    metadata=finalize(attempt,checkpoints,manifests,catalog,catalog_bytes,start,end,args,started)
    metadata["runtime_seconds"]["catalog_discovery"] = catalog_seconds
    metadata["resource_measurement"] = {"peak_process_tree_rss_bytes": int(os.environ["MEASURED_PEAK_RSS_BYTES"]) if os.environ.get("MEASURED_PEAK_RSS_BYTES") else None, "peak_temporary_disk_bytes": int(os.environ["MEASURED_PEAK_TEMP_BYTES"]) if os.environ.get("MEASURED_PEAK_TEMP_BYTES") else None, "note": "external monitor values are injected when present"}
    (attempt/"run_metadata.json").write_text(json.dumps(metadata,indent=2,sort_keys=True)+"\n")
    os.rename(attempt,output); return {**metadata,"output":str(output)}


def main(argv=None):
    try:
        result=run(arguments(argv))
    except Exception as error:
        print(f"throughput follow-up failed: {error}",file=os.sys.stderr,flush=True); return 2
    print(json.dumps(result,sort_keys=True)); return 0


if __name__ == "__main__":
    raise SystemExit(main())
