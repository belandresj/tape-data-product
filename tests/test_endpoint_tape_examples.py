import hashlib
import json
from pathlib import Path

from PIL import Image
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tape_data_product.analysis.endpoint_tape_examples import TapeExampleError, run


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def partition(root, member, start, *, feature=False):
    day, symbol = member.split("/")
    root.mkdir(parents=True)
    rows = 300
    keys = {
        "session_date": [day] * rows,
        "symbol": [symbol] * rows,
        "interval_end_ns": [start + i * 1_000_000_000 for i in range(rows)],
    }
    if feature:
        values = dict(keys)
        for half_life in (30, 120):
            for stem, value in (
                ("midpoint_rms_5s_bps", 40.0 + half_life / 10),
                ("quoted_spread_bps", 20.0),
                ("midpoint_rms_5s_to_spread", 2.0),
                ("trade_rate_per_second", 12.0),
                ("movement_participation", 0.5),
            ):
                name = f"{stem}_hl{half_life}s"
                values[name] = [value] * rows
                values[name + "_reason_mask"] = pa.array([0] * rows, type=pa.uint16())
        filename = "features.parquet"
    else:
        values = dict(keys)
        values.update(
            bid_end_usd=[10.0 + i / 10_000 for i in range(rows)],
            ask_end_usd=[10.01 + i / 10_000 for i in range(rows)],
            price_end_reason_mask=pa.array([0] * rows, type=pa.uint16()),
            trade_count_1s=[i % 20 for i in range(rows)],
            share_volume_1s=pa.array([100.0] * rows, type=pa.float64()),
            dollar_volume_1s_usd=[1000.5 + i / 100 for i in range(rows)],
            activity_reason_mask=pa.array([0] * rows, type=pa.uint16()),
            halt_active=[False] * rows,
        )
        filename = "base.parquet"
    path = root / filename
    pq.write_table(pa.table(values), path)
    manifest = {
        "complete": True,
        "contract_identity": "contract",
        "member": {"session_date": day, "symbol": symbol},
        "outputs": [{"path": filename, "bytes": path.stat().st_size, "sha256": sha(path)}],
    }
    (root / "manifest.json").write_text(json.dumps(manifest))


def test_render_identity_checked_pair(tmp_path):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    day = "2026-06-18"
    start = int(datetime.fromisoformat(day + "T09:50:00").replace(tzinfo=ZoneInfo("America/New_York")).timestamp()) * 1_000_000_000
    examples = []
    for index, symbol in enumerate(("AAA", "BBB")):
        base = tmp_path / symbol / "base"
        features = tmp_path / symbol / "features"
        partition(base, f"{day}/{symbol}", start, feature=False)
        partition(features, f"{day}/{symbol}", start, feature=True)
        examples.append({"member": f"{day}/{symbol}", "start": "09:50:00", "end": "09:55:00", "base_partition": str(base), "feature_partition": str(features), "lineage_status": "synthetic fixture"})
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"schema": "endpoint_tape_pair_v1", "timezone": "America/New_York", "title": "Pair", "subtitle": "Synthetic", "examples": examples}))
    result = run(config, tmp_path / "output")
    assert len(result["examples"]) == 2
    assert result["examples"][0]["summaries"]["midpoint_rms_5s_bps_hl30s"]["mean"] == 43.0
    image = Image.open(tmp_path / "output/gpus_cast_endpoint_ew.png")
    assert image.width >= 3000 and image.height >= 2000
    assert json.loads((tmp_path / "output/artifacts.json").read_text())["artifacts"]


def test_rejects_changed_partition(tmp_path):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    day = "2026-06-18"; symbol = "AAA"
    start = int(datetime.fromisoformat(day + "T09:50:00").replace(tzinfo=ZoneInfo("America/New_York")).timestamp()) * 1_000_000_000
    base = tmp_path / "base"; features = tmp_path / "features"
    partition(base, f"{day}/{symbol}", start, feature=False); partition(features, f"{day}/{symbol}", start, feature=True)
    with (base / "base.parquet").open("ab") as stream:
        stream.write(b"changed")
    spec = {"member": f"{day}/{symbol}", "start": "09:50:00", "end": "09:55:00", "base_partition": str(base), "feature_partition": str(features), "lineage_status": "fixture"}
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"schema": "endpoint_tape_pair_v1", "timezone": "America/New_York", "title": "Pair", "subtitle": "Synthetic", "examples": [spec, spec]}))
    with pytest.raises(TapeExampleError, match="byte identity mismatch"):
        run(config, tmp_path / "output")
