import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/research/analyze_half_life_dollar_throughput_streaming.py"


def module():
    spec = importlib.util.spec_from_file_location("dollar_streaming", SCRIPT)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def row(i=0, day="2026-06-01", symbol="X", fast=10_000.0, slow=10_000.0):
    return {
        "session_date": day, "symbol": symbol, "session": "rth",
        "interval_end_ns": 1_800_000_000_000_000_000 + (i + 1) * 1_000_000_000,
        "midpoint_rms_5s_bps_hl30s": 11.0, "quoted_spread_bps_hl30s": 99.0,
        "midpoint_rms_5s_to_spread_hl30s": 3.0, "movement_participation_hl30s": .4,
        "trade_rate_per_second_hl30s": 10.0, "dollar_rate_usd_per_second_hl30s": fast,
        "midpoint_rms_5s_bps_hl120s": 11.0, "quoted_spread_bps_hl120s": 99.0,
        "midpoint_rms_5s_to_spread_hl120s": 3.0, "movement_participation_hl120s": .4,
        "trade_rate_per_second_hl120s": 10.0, "dollar_rate_usd_per_second_hl120s": slow,
        "quote_age_p90_seconds_window60s": 2.0, "trade_age_p90_seconds_window60s": 2.0,
    }


def connection(m, tmp_path, items):
    path = tmp_path / "projection.parquet"
    pq.write_table(pa.Table.from_pylist(items), path)
    con = duckdb.connect(":memory:")
    m.DOLLAR.create_views(con, path, m.FLOOR)
    return con, path


def test_projection_sql_compiles_and_executes(tmp_path):
    m = module(); con = duckdb.connect(":memory:")
    table = pa.Table.from_pylist([row()]); con.register("input_rows", table)
    con.execute("CREATE VIEW features AS SELECT * FROM input_rows")
    result = con.execute(m._query()).to_arrow_table()
    assert result.num_rows == 1
    assert result.column_names == list(table.column_names)
    con.close()


def test_fixed_floor_and_identity_bound_checkpoint(tmp_path):
    m = module(); artifact = tmp_path / "part.json"; artifact.write_text("{}\n")
    expected = {"schema": m.CHECKPOINT_SCHEMA, "date": "2026-06-01"}
    manifest = {**expected, "artifacts": [{"path": artifact.name, "bytes": artifact.stat().st_size, "sha256": m.sha256(artifact)}]}
    (tmp_path / "checkpoint.json").write_text(json.dumps(manifest))
    assert m.validate_checkpoint(tmp_path, expected) == manifest
    with pytest.raises(ValueError, match="identity mismatch"):
        m.validate_checkpoint(tmp_path, {**expected, "date": "2026-06-02"})
    artifact.write_text("corrupt")
    with pytest.raises(ValueError, match="failed verification"):
        m.validate_checkpoint(tmp_path, expected)


def test_date_local_reduction_equals_whole_scope(tmp_path, monkeypatch):
    m = module(); monkeypatch.setattr(m.BASE, "MINIMUM_EPISODE_SECONDS", 10)
    items = [row(i, day) for day in ("2026-06-01", "2026-06-02") for i in range(12)]
    con, _ = connection(m, tmp_path, items)
    variants = [("fast_constrained", "common_segment", "fast_constrained_match")]
    whole = m.DOLLAR.reduce_periods(con, variants, "common_dollar_eligible", ("2026-06-01", "2026-06-02"))["fast_constrained"].to_pylist()
    local = []
    for day in ("2026-06-01", "2026-06-02"):
        local.extend(m.DOLLAR.reduce_periods(con, variants, "common_dollar_eligible", (day,))["fast_constrained"].to_pylist())
    assert local == whole
    con.close()


def test_date_reducer_writes_only_compact_artifacts(tmp_path, monkeypatch):
    m = module(); monkeypatch.setattr(m.BASE, "MINIMUM_EPISODE_SECONDS", 10)
    items = [row(i, fast=10_000 if i != 5 else 0, slow=10_001) for i in range(12)]
    _, projection = connection(m, tmp_path, items)
    out = tmp_path / "checkpoint"; out.mkdir()
    args = SimpleNamespace(threads=1, memory_limit="256MiB", temp_directory=tmp_path / "scratch", max_temp_directory_size="256MiB")
    result = m.reduce_date(projection, out, "2026-06-01", args)
    assert result["period_rows"] > 0
    assert {p.name for p in out.iterdir()} == {"periods.parquet", "dollar_values.parquet", "aggregates.json"}
    assert pq.ParquetFile(out / "dollar_values.parquet").schema.names == ["view", "population", "value"]
    assert not any("projection" in p.name for p in out.iterdir())


def test_below_floor_is_eligible_nonmatch_but_unavailable_breaks(tmp_path, monkeypatch):
    m = module(); monkeypatch.setattr(m.BASE, "MINIMUM_EPISODE_SECONDS", 2); monkeypatch.setattr(m.BASE, "MINIMUM_OCCUPANCY", m.Decimal("0"))
    items = [row(0), row(1), row(2, fast=0), row(3), row(4)]
    items += [row(10, symbol="N"), row(11, symbol="N", fast=None), row(12, symbol="N")]
    con, _ = connection(m, tmp_path, items)
    periods = m.DOLLAR.reduce_periods(con, [("fast_constrained", "common_segment", "fast_constrained_match")], "common_dollar_eligible", ("2026-06-01",))["fast_constrained"].to_pylist()
    bridged = next(r for r in periods if r["symbol"] == "X")
    assert bridged["matching_seconds"] == 4 and bridged["eligible_nonmatching_seconds"] == 1
    assert not any(r["symbol"] == "N" for r in periods)
    con.close()


def test_original_matches_are_reproduced_independently_of_dollar_gate(tmp_path):
    m = module(); items = [row(0, symbol="LOW", fast=0, slow=0), row(1, symbol="NULL", fast=None, slow=None)]
    con, _ = connection(m, tmp_path, items)
    got = {r[0]: r[1:] for r in con.execute("SELECT symbol,fast_original_match,slow_original_match,fast_constrained_match,slow_constrained_match FROM source ORDER BY symbol").fetchall()}
    assert got["LOW"] == (True, True, False, False)
    assert got["NULL"] == (True, True, False, False)
    con.close()


def test_compact_merge_is_deterministic():
    m = module()
    values = ("fast_seconds", "slow_seconds")
    records = [
        {"session_date": "2026-06-02", "symbol": "B", "fast_seconds": 2, "slow_seconds": 3},
        {"session_date": "2026-06-01", "symbol": "A", "fast_seconds": 5, "slow_seconds": 7},
    ]
    assert m.rollup(records, ("session_date",), values) == m.rollup(list(reversed(records)), ("session_date",), values)
