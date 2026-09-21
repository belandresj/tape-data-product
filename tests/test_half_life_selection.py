import importlib.util
import json
from pathlib import Path
import random

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "research" / "compare_half_life_selection.py"
REDUCER = ROOT / "scripts" / "research" / "select_structured_tape_episodes.py"


def _module():
    spec = importlib.util.spec_from_file_location("half_life_selection", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _row(index=0, *, symbol="TEST", slow=True):
    start = 1_780_000_000_000_000_000
    return {
        "session_date": "2026-06-01",
        "symbol": symbol,
        "session": "rth",
        "interval_end_ns": start + (index + 1) * 1_000_000_000,
        "midpoint_rms_5s_bps_hl30s": 11.0,
        "quoted_spread_bps_hl30s": 99.0,
        "midpoint_rms_5s_to_spread_hl30s": 3.0,
        "movement_participation_hl30s": 0.4,
        "trade_rate_per_second_hl30s": 10.0,
        "midpoint_rms_5s_bps_hl120s": 11.0 if slow else None,
        "quoted_spread_bps_hl120s": 99.0 if slow else None,
        "midpoint_rms_5s_to_spread_hl120s": 3.0 if slow else None,
        "movement_participation_hl120s": 0.4 if slow else None,
        "trade_rate_per_second_hl120s": 10.0 if slow else None,
        "quote_age_p90_seconds_window60s": 2.0,
        "trade_age_p90_seconds_window60s": 2.0,
    }


def _connection(module, tmp_path, rows):
    path = tmp_path / "projection.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    connection = duckdb.connect(":memory:")
    module._create_views(connection, path)
    return connection, path


def test_common_eligibility_threshold_boundaries_and_fixed_removal_population(tmp_path):
    module = _module()
    strict = _row()
    strict["midpoint_rms_5s_bps_hl30s"] = 10.0
    strict["quoted_spread_bps_hl30s"] = 100.0
    strict["midpoint_rms_5s_to_spread_hl30s"] = 2.0
    null_slow = _row(1, symbol="NULL_SLOW", slow=False)
    connection, _ = _connection(module, tmp_path, [strict, null_slow])
    try:
        first = connection.execute(
            "SELECT fast_pass_movement, fast_pass_spread, "
            "fast_pass_movement_to_spread, fast_pass_participation, "
            "fast_pass_trade_rate, fast_pass_quote_freshness, "
            "fast_pass_trade_freshness FROM evaluated WHERE symbol='TEST'"
        ).fetchone()
        assert first == (False, False, False, True, True, True, True)
        assert connection.execute(
            "SELECT common_eligible, fast_match, slow_match "
            "FROM evaluated WHERE symbol='NULL_SLOW'"
        ).fetchone() == (False, False, False)
        # Removing a fast condition cannot admit a row outside the fixed common mask.
        assert connection.execute(
            "SELECT count(*) FROM evaluated WHERE common_eligible AND ("
            + module._pass_all("fast", omit="movement")
            + ")"
        ).fetchone()[0] == 0
    finally:
        connection.close()


def test_directional_disagreement_records_multiple_failures_and_shared_freshness_invariant(tmp_path):
    module = _module()
    row = _row()
    row["midpoint_rms_5s_bps_hl120s"] = 9.0
    row["quoted_spread_bps_hl120s"] = 101.0
    connection, _ = _connection(module, tmp_path, [row])
    try:
        records = module._threshold_disagreements(connection)
    finally:
        connection.close()
    conditions = {
        row["failure"]: row
        for row in records
        if row["direction"] == "fast_pass_slow_fail"
        and row["record_type"] == "condition"
    }
    assert conditions["movement"]["seconds"] == 1
    assert conditions["spread"]["seconds"] == 1
    assert conditions["movement"]["sole_failure_seconds"] == 0
    assert conditions["spread"]["sole_failure_seconds"] == 0
    assert conditions["quote_freshness"]["seconds"] == 0
    assert conditions["trade_freshness"]["seconds"] == 0
    combinations = [
        row
        for row in records
        if row["direction"] == "fast_pass_slow_fail"
        and row["record_type"] == "combination"
    ]
    assert combinations[0]["failure"] == "movement,spread"


def test_exact_half_open_interval_overlap_and_undefined_denominators():
    module = _module()
    billion = 1_000_000_000
    tables = {
        "fast": pa.Table.from_pylist(
            [
                {
                    "session_date": "2026-06-01",
                    "symbol": "A",
                    "episode_start_ns": 0,
                    "episode_end_ns": 10 * billion,
                }
            ]
        ),
        "slow": pa.Table.from_pylist(
            [
                {
                    "session_date": "2026-06-01",
                    "symbol": "A",
                    "episode_start_ns": 5 * billion,
                    "episode_end_ns": 15 * billion,
                }
            ]
        ),
    }
    records = module._period_overlap_records(
        tables, [("2026-06-01", "A"), ("2026-06-01", "EMPTY")]
    )
    member = next(row for row in records if row["scope"] == "member" and row["symbol"] == "A")
    assert (member["fast_seconds"], member["slow_seconds"]) == (10, 10)
    assert (member["shared_seconds"], member["union_seconds"]) == (5, 15)
    assert member["shared_over_union"] == pytest.approx(1 / 3)
    empty = next(row for row in records if row["scope"] == "member" and row["symbol"] == "EMPTY")
    assert empty["shared_over_union"] is None
    assert empty["shared_over_fast"] is None
    assert empty["shared_over_slow"] is None


def test_existing_reducer_splits_missing_physical_grid_and_applies_eighty_percent(tmp_path):
    spec = importlib.util.spec_from_file_location("episode_reducer", REDUCER)
    reducer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reducer)
    start = 1_780_000_000_000_000_000
    rows = []
    for index in range(751):
        if index == 300:
            continue
        rows.append(
            {
                "session_date": "2026-06-01",
                "symbol": "GRID_GAP",
                "session": "rth",
                "interval_end_ns": start + (index + 1) * 1_000_000_000,
                "eligible": True,
                "matching": True,
            }
        )
    # Exactly 480 of 600 seconds match: retained at the inclusive 0.80 boundary.
    nonmatches = {1, *range(4, 599, 5)}
    assert len(nonmatches) == 120
    rows.extend(
        {
            "session_date": "2026-06-01",
            "symbol": "OCCUPANCY",
            "session": "rth",
            "interval_end_ns": start + (index + 1) * 1_000_000_000,
            "eligible": True,
            "matching": index not in nonmatches,
        }
        for index in range(600)
    )
    path = tmp_path / "rows.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    connection = duckdb.connect(":memory:")
    try:
        table = connection.execute(
            reducer._episode_sql(30, 600, reducer.Decimal("0.80")),
            [str(path), "2026-06-01"],
        ).to_arrow_table()
    finally:
        connection.close()
    output = table.to_pylist()
    assert [row["symbol"] for row in output] == ["OCCUPANCY"]
    assert output[0]["matching_seconds"] == 480
    assert output[0]["occupancy"] == pytest.approx(0.8)


class _FakeResult:
    def __init__(self, batches):
        self.batches = batches

    def arrow_batches(self, batch_size):
        assert 1 <= batch_size <= 25_000
        yield from self.batches


def test_projection_is_batch_independent(tmp_path):
    module = _module()
    table = pa.Table.from_pylist([_row(index) for index in range(7)])
    one = tmp_path / "one.parquet"
    many = tmp_path / "many.parquet"
    module._write_projection(_FakeResult(table.to_batches(max_chunksize=7)), one, 7)
    module._write_projection(_FakeResult(table.to_batches(max_chunksize=2)), many, 2)
    assert pq.read_table(one).equals(pq.read_table(many))


def test_expanded_dates_require_override():
    module = _module()
    assert module._validate_scope("2026-06-01", "2026-06-05", False)
    with pytest.raises(ValueError, match="allow-expanded-scope"):
        module._validate_scope("2026-05-31", "2026-06-05", False)
    assert module._validate_scope("2026-05-31", "2026-06-05", True)


def test_full_synthetic_analysis_reuses_period_semantics_and_writes_audit_tables(tmp_path):
    module = _module()
    rows = []
    for index in range(700):
        row = _row(index, symbol="OVERLAP")
        if index >= 600:
            row["midpoint_rms_5s_bps_hl30s"] = 9.0
        if index < 100:
            row["midpoint_rms_5s_bps_hl120s"] = 9.0
        rows.append(row)
    for index in range(700):
        row = _row(index, symbol="FAST_ONLY")
        row["midpoint_rms_5s_bps_hl120s"] = 9.0
        row["quoted_spread_bps_hl120s"] = 101.0
        rows.append(row)
    rows.extend(_row(index, symbol="NO_COMMON", slow=False) for index in range(20))
    projection = tmp_path / "projection.parquet"
    pq.write_table(pa.Table.from_pylist(rows), projection)
    output = tmp_path / "output"
    output.mkdir()

    result = module._analyze_projection(
        projection,
        output,
        ("2026-06-01",),
        memory_limit="256MiB",
    )

    assert result["represented_members"] == 3
    membership = __import__("json").loads((output / "membership.json").read_text())
    by_key = {
        (row["definition"], row["symbol"]): row for row in membership
    }
    assert by_key[("retained_period", "OVERLAP")]["classification"] == "both"
    assert by_key[("retained_period", "FAST_ONLY")]["classification"] == "fast_only"
    assert by_key[("matching_second", "NO_COMMON")]["classification"] == "neither"
    assert by_key[("matching_second", "NO_COMMON")]["no_common_eligible_time"] is True
    overlap = __import__("json").loads((output / "retained_period_overlap.json").read_text())
    overlap_member = next(
        row for row in overlap if row["scope"] == "member" and row["symbol"] == "OVERLAP"
    )
    assert overlap_member["fast_seconds"] == 600
    assert overlap_member["slow_seconds"] == 600
    assert overlap_member["shared_seconds"] == 500
    assert (output / "retained_periods.csv").is_file()
    assert (output / "summary.md").is_file()


def test_combined_period_reduction_matches_existing_sql_oracle(tmp_path, monkeypatch):
    module = _module()
    reducer_spec = importlib.util.spec_from_file_location("episode_reducer", REDUCER)
    reducer = importlib.util.module_from_spec(reducer_spec)
    reducer_spec.loader.exec_module(reducer)
    monkeypatch.setattr(module, "MINIMUM_EPISODE_SECONDS", 5)
    monkeypatch.setattr(module, "MINIMUM_OCCUPANCY", module.Decimal("0.40"))
    rng = random.Random(90210)
    dates = ("2026-06-01", "2026-06-02")
    rows = []
    for session_date in dates:
        for symbol in ("AAA", "BBB"):
            for session in ("rth", "eth"):
                for index in range(90):
                    if index == 37:
                        continue
                    row = _row(index, symbol=symbol)
                    row["session_date"] = session_date
                    row["session"] = session
                    if rng.random() < 0.18:
                        row["midpoint_rms_5s_bps_hl30s"] = 9.0
                    if rng.random() < 0.16:
                        row["quoted_spread_bps_hl120s"] = 101.0
                    if rng.random() < 0.08:
                        row["trade_rate_per_second_hl120s"] = None
                    rows.append(row)
    connection, projection = _connection(module, tmp_path, rows)
    try:
        optimized = module._period_variants(
            connection, reducer, projection, dates
        )
        for variant, segment, matching in module._variant_matches():
            eligible = "fast_available" if segment == "fast_segment" else "common_eligible"
            source_sql = (
                "SELECT session_date, symbol, session, interval_end_ns, "
                f"({eligible}) AS eligible, coalesce(({matching}), false) AS matching "
                "FROM evaluated"
            )
            oracle = connection.execute(
                reducer._episode_sql(
                    module.OFF_DELAY_SECONDS,
                    module.MINIMUM_EPISODE_SECONDS,
                    module.MINIMUM_OCCUPANCY,
                    date_count=len(dates),
                    source_sql=source_sql,
                ),
                list(dates),
            ).to_arrow_table()
            assert optimized[variant].to_pylist() == oracle.to_pylist(), variant
    finally:
        connection.close()


def test_date_chunk_merge_is_identical_to_whole_scope_analysis(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "MINIMUM_EPISODE_SECONDS", 10)
    monkeypatch.setattr(module, "MINIMUM_OCCUPANCY", module.Decimal("0.50"))
    dates = ("2026-06-01", "2026-06-02")
    rows = []
    for date_index, session_date in enumerate(dates):
        for symbol in ("AAA", "BBB"):
            for index in range(80):
                row = _row(index, symbol=symbol)
                row["session_date"] = session_date
                if (index + date_index) % 9 == 0:
                    row["midpoint_rms_5s_bps_hl30s"] = 9.0
                if (index + date_index) % 11 == 0:
                    row["quoted_spread_bps_hl120s"] = 101.0
                rows.append(row)

    combined_projection = tmp_path / "combined.parquet"
    pq.write_table(pa.Table.from_pylist(rows), combined_projection)
    combined = tmp_path / "combined"
    combined.mkdir()
    module._analyze_projection(
        combined_projection, combined, dates, memory_limit="256MiB"
    )

    chunks = []
    for session_date in dates:
        chunk = tmp_path / session_date
        chunk.mkdir()
        projection = chunk / "projection.parquet"
        pq.write_table(
            pa.Table.from_pylist(
                [row for row in rows if row["session_date"] == session_date]
            ),
            projection,
        )
        module._analyze_projection(
            projection,
            chunk,
            (session_date,),
            memory_limit="256MiB",
            all_failure_combinations=True,
        )
        projection.unlink()
        chunks.append(chunk)

    merged = tmp_path / "merged"
    merged.mkdir()
    module._merge_chunk_outputs(chunks, merged, dates)
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
        assert json.loads((merged / f"{stem}.json").read_text()) == json.loads(
            (combined / f"{stem}.json").read_text()
        ), stem
    assert json.loads((merged / "baseline_sanity.json").read_text()) == json.loads(
        (combined / "baseline_sanity.json").read_text()
    )
