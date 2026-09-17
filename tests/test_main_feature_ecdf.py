import json
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")
import pyarrow as pa
import pyarrow.parquet as pq

from tape_data_product.analysis.main_feature_ecdf import calculate


def _write_member(root: Path):
    day, symbol = "2026-03-09", "TEST"
    feature_dir = root / "features" / f"session_date={day}" / f"symbol={symbol}"
    base_dir = root / "base" / f"session_date={day}" / f"symbol={symbol}"
    feature_dir.mkdir(parents=True)
    base_dir.mkdir(parents=True)
    endpoints = [1773057601000000000, 1773077401000000000, 1773100801000000000]
    feature = {"interval_end_ns": endpoints}
    for view, suffix, window in (("fast", "hl30s", "window60s"), ("slow", "hl120s", "window300s")):
        feature[f"trade_rate_per_second_{suffix}"] = [2.0, 2.0, .5]
        feature[f"trade_rate_per_second_{suffix}_reason_mask"] = [0, 0, 0]
        feature[f"trade_age_p90_seconds_{window}"] = [1.0, 2.0, 1.0]
        feature[f"trade_age_p90_seconds_{window}_reason_mask"] = [0, 0, 0]
        for stem in ("midpoint_rms_5s_bps", "quoted_spread_bps", "midpoint_rms_5s_to_spread",
                     "movement_participation", "share_rate_per_second", "dollar_rate_usd_per_second",
                     "bid_size_mean_shares", "ask_size_mean_shares"):
            name = f"{stem}_{suffix}"
            feature[name] = [0.0, .5, 1.0]
            feature[name + "_reason_mask"] = [0, 0, 0]
        for stem in ("quote_age_p90_seconds", "midpoint_change_age_p90_seconds"):
            name = f"{stem}_{window}"
            feature[name] = [0.0, 2.0, 4.0]
            feature[name + "_reason_mask"] = [0, 0, 0]
    base = {"interval_end_ns": endpoints}
    for stem in ("trade_age_seconds", "quote_age_seconds", "midpoint_change_age_seconds"):
        base[stem] = [0.0, 1.0, 2.0]
        base[stem.removesuffix("_seconds") + "_reason_mask"] = [0, 0, 0]
    pq.write_table(pa.table(feature), feature_dir / "features.parquet")
    pq.write_table(pa.table(base), base_dir / "base.parquet")
    inventory = root / "inventory.json"
    inventory.write_text(json.dumps({"feature_root": str(root / "features"), "base_root": str(root / "base"),
                                     "members": [{"session_date": day, "symbol": symbol}], "release": {}}))
    return inventory


def test_calculate_one_pass_families_and_gate(tmp_path):
    inventory = _write_member(tmp_path)
    result = calculate(inventory, tmp_path / "out", batch_size=2)
    assert set(result["families"]) == {"movement_friction", "throughput_liquidity", "freshness"}
    movement = result["families"]["movement_friction"]["metrics"][0]
    assert movement["views"]["fast"]["gate_selected"]["pooled"] == 2
    assert movement["views"]["fast"]["sessions"]["pooled"]["valid"] == 2
    assert movement["views"]["fast"]["sessions"]["pooled"]["zeros"] == 1
    assert sum(movement["views"]["fast"]["sessions"][s]["valid"] for s in ("premarket", "rth", "after_hours")) == 2
    freshness = result["families"]["freshness"]["metrics"][0]
    assert freshness["views"]["slow"]["sessions"]["pooled"]["valid"] == 2
    assert (tmp_path / "out" / "numerical.json").is_file()
