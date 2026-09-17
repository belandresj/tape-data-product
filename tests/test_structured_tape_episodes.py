import importlib.util
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "research" / "select_structured_tape_episodes.py"


def _module():
    spec = importlib.util.spec_from_file_location("structured_tape_episodes", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _rows(
    symbol, matching_indices, *, session_date="2026-06-01", unavailable_indices=()
):
    start = 1_780_000_000_000_000_000
    matching_indices = set(matching_indices)
    unavailable_indices = set(unavailable_indices)
    return [
        {
            "session_date": session_date,
            "symbol": symbol,
            "session": "rth",
            "interval_end_ns": start + (index + 1) * 1_000_000_000,
            "eligible": index not in unavailable_indices,
            "matching": index in matching_indices and index not in unavailable_indices,
        }
        for index in range(700)
    ]


def test_merges_short_gaps_and_retains_half_occupied_episode(tmp_path):
    qualifying = {0, 599, *range(2, 598, 2)}
    split_by_long_gap = {*range(0, 300), *range(330, 630)}
    split_by_unavailable = {*range(0, 300), *range(301, 601)}
    rows = []
    rows.extend(_rows("PASS", qualifying))
    rows.extend(_rows("FAIL_GAP", split_by_long_gap))
    rows.extend(_rows("FAIL_UNAVAILABLE", split_by_unavailable, unavailable_indices={300}))
    input_path = tmp_path / "projected.parquet"
    output_path = tmp_path / "episodes.parquet"
    pq.write_table(pa.Table.from_pylist(rows), input_path)

    result = _module().run(
        input_path, output_path, session_date="2026-06-01"
    )
    output = pq.read_table(output_path).to_pylist()

    assert result["retained_episodes"] == 1
    assert result["retained_symbol_days"] == 1
    assert len(output) == 1
    assert output[0]["symbol"] == "PASS"
    assert output[0]["elapsed_seconds"] == 600
    assert output[0]["matching_seconds"] == 300
    assert output[0]["eligible_nonmatching_seconds"] == 300
    assert output[0]["occupancy"] == pytest.approx(0.5)
    assert output[0]["maximum_nonmatching_gap_seconds"] == 2


def test_off_delay_boundary_merges_29_misses_and_splits_30(tmp_path):
    rows = []
    rows.extend(_rows("MERGE", {0, 30}))
    rows.extend(_rows("SPLIT", {0, 31}))
    input_path = tmp_path / "projected.parquet"
    output_path = tmp_path / "episodes.parquet"
    pq.write_table(pa.Table.from_pylist(rows), input_path)
    module = _module()
    sql = module._episode_sql(30, 1, module.Decimal("0.01"))
    import duckdb

    connection = duckdb.connect(":memory:")
    table = connection.execute(
        sql, [str(input_path), "2026-06-01"]
    ).to_arrow_table()
    connection.close()
    episodes = {(row["symbol"], row["episode_id"]): row for row in table.to_pylist()}
    assert len([key for key in episodes if key[0] == "MERGE"]) == 1
    assert len([key for key in episodes if key[0] == "SPLIT"]) == 2
    assert episodes[("MERGE", 1)]["maximum_nonmatching_gap_seconds"] == 29
    assert all(
        row["maximum_nonmatching_gap_seconds"] == 0
        for key, row in episodes.items()
        if key[0] == "SPLIT"
    )


def test_combines_requested_dates_and_counts_symbol_days(tmp_path):
    rows = []
    rows.extend(_rows("SAME", range(600), session_date="2026-06-01"))
    rows.extend(_rows("SAME", range(600), session_date="2026-06-02"))
    rows.extend(_rows("OTHER", range(600), session_date="2026-06-03"))
    input_path = tmp_path / "projected.parquet"
    output_path = tmp_path / "episodes.parquet"
    pq.write_table(pa.Table.from_pylist(rows), input_path)

    result = _module().run(
        input_path,
        output_path,
        session_dates=["2026-06-01", "2026-06-02"],
        minimum_occupancy="0.80",
    )
    output = pq.read_table(output_path).to_pylist()

    assert result["session_dates"] == ["2026-06-01", "2026-06-02"]
    assert result["retained_episodes"] == 2
    assert result["retained_symbol_days"] == 2
    assert {row["session_date"] for row in output} == {
        "2026-06-01",
        "2026-06-02",
    }


def test_rejects_invalid_configuration_and_existing_output(tmp_path):
    module = _module()
    with pytest.raises(ValueError, match="off delay"):
        module._validated_parameters(0, 600, "0.5")
    with pytest.raises(ValueError, match="minimum episode"):
        module._validated_parameters(30, 0, "0.5")
    with pytest.raises(ValueError, match="minimum occupancy"):
        module._validated_parameters(30, 600, "2")

    input_path = tmp_path / "projected.parquet"
    output_path = tmp_path / "episodes.parquet"
    pq.write_table(pa.Table.from_pylist(_rows("PASS", range(700))), input_path)
    output_path.touch()
    with pytest.raises(FileExistsError):
        module.run(input_path, output_path, session_date="2026-06-01")
