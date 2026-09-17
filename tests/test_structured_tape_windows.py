from datetime import date
import importlib.util
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "research" / "select_structured_tape_windows.py"


def _module():
    spec = importlib.util.spec_from_file_location("structured_tape_windows", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _member(day, symbol, matching_count, *, unavailable_index=None):
    start = 1_780_000_000_000_000_000
    rows = []
    for index in range(620):
        rows.append(
            {
                "session_date": day,
                "symbol": symbol,
                "session": "rth",
                "interval_end_ns": start + (index + 1) * 1_000_000_000,
                "eligible": index != unavailable_index,
                "matching": index < matching_count and index != unavailable_index,
            }
        )
    return rows


def test_selects_best_fully_observed_half_occupied_window(tmp_path):
    rows = []
    rows.extend(_member("2026-06-01", "PASS", 300))
    rows.extend(_member("2026-06-01", "FAIL_OCCUPANCY", 299))
    rows.extend(_member("2026-06-01", "FAIL_UNAVAILABLE", 301, unavailable_index=10))
    input_path = tmp_path / "projected.parquet"
    output_path = tmp_path / "windows.parquet"
    pq.write_table(pa.Table.from_pylist(rows), input_path)

    result = _module().run(input_path, output_path)
    output = pq.read_table(output_path).to_pylist()

    assert result["qualified_symbol_days"] == 1
    assert result["minimum_matching_seconds"] == 300
    assert len(output) == 1
    assert output[0]["symbol"] == "PASS"
    assert output[0]["represented_seconds"] == 600
    assert output[0]["eligible_seconds"] == 600
    assert output[0]["matching_seconds"] == 300
    assert output[0]["occupancy"] == pytest.approx(0.5)
    assert output[0]["window_end_ns"] - output[0]["window_start_ns"] == 600_000_000_000


def test_rejects_invalid_configuration_and_existing_output(tmp_path):
    module = _module()
    with pytest.raises(ValueError, match="window seconds"):
        module._minimum_matches(0, "0.5")
    with pytest.raises(ValueError, match="minimum occupancy"):
        module._minimum_matches(600, "1.1")

    input_path = tmp_path / "projected.parquet"
    output_path = tmp_path / "windows.parquet"
    pq.write_table(pa.Table.from_pylist(_member("2026-06-01", "PASS", 300)), input_path)
    output_path.touch()
    with pytest.raises(FileExistsError):
        module.run(input_path, output_path)
