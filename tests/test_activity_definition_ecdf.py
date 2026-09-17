import json
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from tape_data_product.analysis.activity_definition_ecdf import calculate, render


def _endpoint(day, local_start):
    stamp = datetime.combine(date.fromisoformat(day), local_start, ZoneInfo("America/New_York"))
    return int((stamp.astimezone(timezone.utc) + timedelta(seconds=1)).timestamp() * 1e9)


def _member(root, day, symbol, values):
    path = root / f"session_date={day}" / f"symbol={symbol}"
    path.mkdir(parents=True)
    endpoints = [_endpoint(day, time(9, 29, 59)), _endpoint(day, time(9, 30)), _endpoint(day, time(16))]
    rows = []
    for index, endpoint in enumerate(endpoints):
        row = {"session_date": day, "symbol": symbol, "interval_end_ns": endpoint}
        for field, field_values in values.items():
            row[field] = field_values[index]
            row[field + "_reason_mask"] = 0 if field_values[index] is not None else 1
        rows.append(row)
    pq.write_table(pa.Table.from_pylist(rows), path / "features.parquet")


def test_activity_definition_calculation_and_render(tmp_path):
    root = tmp_path / "features"
    fields = {
        "trade_rate_per_second_hl30s": [0.0, 1.0, 10.0],
        "trade_age_p90_seconds_window60s": [10.0, 2.0, 1.0],
        "trade_rate_per_second_hl120s": [None, 2.0, 20.0],
        "trade_age_p90_seconds_window300s": [None, 3.0, 2.0],
    }
    _member(root, "2026-03-09", "AAA", fields)
    inventory = tmp_path / "inventory.json"
    inventory.write_text(json.dumps({"feature_root": str(root), "members": [{"session_date": "2026-03-09", "symbol": "AAA"}], "release": {"source_revision": "test"}}))
    numerical = calculate(inventory, tmp_path / "numerical", expected_members=1, probability_steps=100)
    by_field = {row["field"]: row for row in numerical["fields"]}
    fast_rate = by_field["trade_rate_per_second_hl30s"]["sessions"]
    assert fast_rate["pooled"]["valid"] == 3
    assert fast_rate["pooled"]["zeros"] == 1
    assert fast_rate["pooled"]["threshold_pass"] == 2
    assert fast_rate["premarket"]["valid"] == 1
    assert fast_rate["rth"]["valid"] == 1
    assert fast_rate["after_hours"]["valid"] == 1
    assert by_field["trade_rate_per_second_hl120s"]["sessions"]["pooled"]["valid"] == 2
    rendered = render(tmp_path / "numerical" / "numerical.json", tmp_path / "render")
    assert rendered["x_transform"] == "log1p"
    assert (tmp_path / "render" / "activity_definition_ecdf.png").stat().st_size > 0
    assert (tmp_path / "render" / "activity_definition_ecdf.svg").stat().st_size > 0
