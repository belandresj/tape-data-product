import csv
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tape_data_product.analysis.endpoint_population import (
    render_daily_active_tape_hours,
    render_daily_completed_members,
)


def _inventory(path, members):
    path.write_text(json.dumps({"members": members}))
    return path


def test_daily_completed_members_reconciles_and_omits_member_details(tmp_path):
    source = _inventory(
        tmp_path / "private-inventory.json",
        [
            {"session_date": "2026-03-09", "symbol": "AAA"},
            {"session_date": "2026-03-09", "symbol": "BBB"},
            {"session_date": "2026-03-10", "symbol": "AAA"},
        ],
    )
    output = tmp_path / "report"

    summary = render_daily_completed_members(
        source, output, expected_members=3, session_hours=16
    )

    rows = list(csv.DictReader((output / "daily_completed_members.csv").open()))
    assert rows == [
        {
            "session_date": "2026-03-09",
            "completed_symbol_days": "2",
            "represented_symbol_hours": "32",
        },
        {
            "session_date": "2026-03-10",
            "completed_symbol_days": "1",
            "represented_symbol_hours": "16",
        },
    ]
    assert summary["daily_counts_sum"] == 3
    assert summary["represented_symbol_hours"] == 48
    assert summary["source_inventory"]["name"] == "private-inventory.json"
    assert str(tmp_path) not in (output / "daily_completed_members_summary.json").read_text()
    assert (output / "daily_completed_members.png").stat().st_size > 0
    assert (output / "daily_completed_members.svg").stat().st_size > 0
    artifacts = json.loads((output / "artifacts.json").read_text())
    assert artifacts["renderer"]["name"] == "endpoint_population.py"
    assert set(artifacts["outputs"]) == {
        "daily_completed_members.csv",
        "daily_completed_members.png",
        "daily_completed_members.svg",
        "daily_completed_members_summary.json",
    }


def test_daily_completed_members_rejects_duplicate_members(tmp_path):
    source = _inventory(
        tmp_path / "inventory.json",
        [
            {"session_date": "2026-03-09", "symbol": "AAA"},
            {"session_date": "2026-03-09", "symbol": "AAA"},
        ],
    )

    with pytest.raises(ValueError, match="duplicate"):
        render_daily_completed_members(source, tmp_path / "report")


def _gate_config(path):
    path.write_text(
        json.dumps(
            {
                "gate": {
                    "fast": {
                        "half_life_seconds": 30,
                        "trade_rate_minimum_inclusive": 1.0,
                        "trade_age_p90_maximum_inclusive": 2.0,
                        "trade_age_window_seconds": 60,
                    }
                }
            }
        )
    )
    return path


def _gate_accounting(path, rows):
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def test_daily_active_tape_hours_reconciles_and_omits_members(tmp_path):
    accounting = _gate_accounting(
        tmp_path / "private-member-gates.parquet",
        [
            {
                "member": "2026-03-09/AAA",
                "session": "pooled",
                "half_life_seconds": 30,
                "represented": 100,
                "gate_valid": 90,
                "gate_pass": 36,
                "gate_fail": 54,
                "gate_unavailable": 10,
            },
            {
                "member": "2026-03-09/BBB",
                "session": "pooled",
                "half_life_seconds": 30,
                "represented": 100,
                "gate_valid": 80,
                "gate_pass": 18,
                "gate_fail": 62,
                "gate_unavailable": 20,
            },
            {
                "member": "2026-03-10/AAA",
                "session": "pooled",
                "half_life_seconds": 30,
                "represented": 100,
                "gate_valid": 100,
                "gate_pass": 90,
                "gate_fail": 10,
                "gate_unavailable": 0,
            },
        ],
    )
    output = tmp_path / "activity-report"

    summary = render_daily_active_tape_hours(
        accounting,
        _gate_config(tmp_path / "config.json"),
        output,
        expected_members=3,
        expected_represented_seconds=300,
    )

    rows = list(csv.DictReader((output / "daily_active_tape_hours.csv").open()))
    assert [row["gate_pass_seconds"] for row in rows] == ["54", "90"]
    assert [row["active_stock_hours"] for row in rows] == ["0.015", "0.025"]
    assert summary["gate_pass_seconds"] == 144
    assert summary["gate_unavailable_seconds"] == 30
    assert summary["active_stock_hours"] == pytest.approx(0.04)
    assert str(tmp_path) not in (
        output / "daily_active_tape_hours_summary.json"
    ).read_text()
    assert "AAA" not in (output / "daily_active_tape_hours.csv").read_text()
    assert (output / "daily_active_tape_hours.png").stat().st_size > 0
    assert (output / "daily_active_tape_hours.svg").stat().st_size > 0


def test_daily_active_tape_hours_rejects_bad_accounting(tmp_path):
    accounting = _gate_accounting(
        tmp_path / "bad.parquet",
        [
            {
                "member": "2026-03-09/AAA",
                "session": "pooled",
                "half_life_seconds": 30,
                "represented": 100,
                "gate_valid": 90,
                "gate_pass": 40,
                "gate_fail": 40,
                "gate_unavailable": 10,
            }
        ],
    )

    with pytest.raises(ValueError, match="gate-valid count does not reconcile"):
        render_daily_active_tape_hours(
            accounting,
            _gate_config(tmp_path / "config.json"),
            tmp_path / "activity-report",
        )
