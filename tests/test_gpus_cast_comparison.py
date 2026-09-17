import hashlib
import json
from pathlib import Path

from PIL import Image
import pyarrow as pa
import pyarrow.parquet as pq

from tape_data_product.analysis.gpus_cast_comparison import run


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _partition(root: Path, member: str, endpoint_ns: int, reference: float) -> str:
    day, symbol = member.split("/")
    root.mkdir(parents=True)
    path = root / "base.parquet"
    pq.write_table(
        pa.table(
            {
                "session_date": [day] * 300,
                "symbol": [symbol] * 300,
                "interval_end_ns": [endpoint_ns - (299 - index) * 1_000_000_000 for index in range(300)],
                "bid_end_usd": [reference - 0.01 + index / 100_000 for index in range(300)],
                "ask_end_usd": [reference + 0.01 + index / 100_000 for index in range(300)],
                "price_end_reason_mask": pa.array([0] * 300, type=pa.uint16()),
            }
        ),
        path,
    )
    identity = _sha256(path)
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "complete": True,
                "member": {"session_date": day, "symbol": symbol},
                "outputs": [{"path": "base.parquet", "bytes": path.stat().st_size, "sha256": identity}],
            }
        )
    )
    return identity


def test_renders_canonical_comparison_with_two_labeled_y_axes(tmp_path):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    specs = (("GPUS", "09:52:15", 0.53), ("CAST", "10:01:52", 10.09))
    examples = []
    partitions = []
    for symbol, endpoint, reference in specs:
        endpoint_ns = int(
            datetime.fromisoformat(f"2026-06-18T{endpoint}")
            .replace(tzinfo=ZoneInfo("America/New_York"))
            .timestamp()
        ) * 1_000_000_000
        partition = tmp_path / symbol
        identity = _partition(partition, f"2026-06-18/{symbol}", endpoint_ns, reference)
        partitions.append(partition)
        examples.append(
            {
                "member": f"2026-06-18/{symbol}",
                "symbol": symbol,
                "session_date": "2026-06-18",
                "endpoint": endpoint,
                "base_identity": identity,
                "reference_midpoint_usd": reference,
            }
        )
    source = tmp_path / "source.json"
    source.write_text(json.dumps({"schema": "endpoint_tape_pair_summary_v2", "examples": examples}))
    png, svg = tmp_path / "comparison.png", tmp_path / "comparison.svg"

    panels = run(source, partitions, png, svg)

    assert [panel["endpoint"] for panel in panels] == ["09:52:15", "10:01:52"]
    assert panels[0]["bid_bps"].shape == panels[1]["ask_bps"].shape == (300,)
    image = Image.open(png)
    assert image.width >= 3000 and image.height >= 1200
    text = svg.read_text()
    assert "Comparable Movement and Trading Activity, Different Quoted Spreads" in text
    assert "GPUS — June 18, 2026, 09:52:15 ET" in text
    assert "CAST — June 18, 2026, 10:01:52 ET" in text
    assert text.count("Price relative to first midpoint (bps)") == 2
    for forbidden in ("Candidate", "lineage", "reference midpoint", "Trailing five"):
        assert forbidden not in text
