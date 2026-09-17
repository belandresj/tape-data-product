"""Render the canonical GPUS/CAST endpoint-quote comparison for Report V2."""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, MinuteLocator
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


NS = 1_000_000_000
BATCH_SIZE = 4096
ROWS = 300
TIMEZONE = "America/New_York"
TITLE = "Comparable Movement and Trading Activity, Different Quoted Spreads"
BASE_COLUMNS = (
    "session_date",
    "symbol",
    "interval_end_ns",
    "bid_end_usd",
    "ask_end_usd",
    "price_end_reason_mask",
)
COLORS = {"bid": "#2F8F75", "ask": "#C95B66", "text": "#17212B", "grid": "#DDE3E8"}


class ComparisonError(ValueError):
    """The saved comparison or an input partition does not match its contract."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ComparisonError(message)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _clock_ns(day: str, value: str) -> int:
    parsed = datetime.strptime(f"{day} {value}", "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=ZoneInfo(TIMEZONE)
    )
    return int(parsed.timestamp()) * NS


def _verified_base(partition: Path, member: str, expected_sha256: str) -> Path:
    manifest_path = partition / "manifest.json"
    _require(manifest_path.is_file(), f"missing manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text())
    _require(manifest.get("complete") is True, f"incomplete partition: {partition}")
    actual_member = manifest.get("member", {})
    _require(
        f"{actual_member.get('session_date')}/{actual_member.get('symbol')}" == member,
        f"manifest member mismatch: {partition}",
    )
    outputs = [row for row in manifest.get("outputs", []) if row.get("path") == "base.parquet"]
    _require(len(outputs) == 1, f"missing unique base.parquet output: {partition}")
    output = outputs[0]
    path = partition / "base.parquet"
    _require(path.is_file(), f"missing output: {path}")
    _require(path.stat().st_size == output.get("bytes"), f"byte identity mismatch: {path}")
    actual_sha256 = _sha256(path)
    _require(actual_sha256 == output.get("sha256"), f"manifest hash mismatch: {path}")
    _require(actual_sha256 == expected_sha256, f"saved comparison identity mismatch: {path}")
    return path


def _scan_window(path: Path, start: int, stop: int) -> pa.Table:
    batches = []
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=BATCH_SIZE, columns=list(BASE_COLUMNS), use_threads=False
    ):
        ends = batch.column(batch.schema.get_field_index("interval_end_ns")).to_numpy()
        indices = np.flatnonzero((ends >= start) & (ends < stop))
        if len(indices):
            batches.append(batch.take(pa.array(indices, type=pa.int64())))
    _require(batches, f"comparison window has no rows: {path}")
    return pa.Table.from_batches(batches)


def _load_panel(saved: dict, partition: Path) -> dict:
    member = saved["member"]
    day, symbol = member.split("/")
    endpoint = _clock_ns(day, saved["endpoint"])
    start = endpoint - (ROWS - 1) * NS
    stop = endpoint + NS
    path = _verified_base(partition, member, saved["base_identity"])
    table = _scan_window(path, start, stop)
    _require(table.num_rows == ROWS, f"expected {ROWS} comparison rows for {member}")
    ends = np.asarray(table["interval_end_ns"].to_numpy(), dtype=np.int64)
    _require(np.array_equal(ends, np.arange(start, stop, NS)), f"endpoint grid mismatch: {member}")
    _require(set(table["session_date"].to_pylist()) == {day}, f"date mismatch: {member}")
    _require(set(table["symbol"].to_pylist()) == {symbol}, f"symbol mismatch: {member}")

    masks = np.asarray(table["price_end_reason_mask"].to_numpy(), dtype=np.uint16)
    bid = np.asarray(table["bid_end_usd"].to_numpy(zero_copy_only=False), dtype=np.float64)
    ask = np.asarray(table["ask_end_usd"].to_numpy(zero_copy_only=False), dtype=np.float64)
    valid = masks == 0
    _require(np.all(np.isfinite(bid[valid])) and np.all(np.isfinite(ask[valid])), f"invalid prices: {member}")
    midpoint = np.where(valid, (bid + ask) / 2.0, np.nan)
    finite_midpoints = midpoint[np.isfinite(midpoint)]
    _require(len(finite_midpoints) > 0, f"no valid reference midpoint: {member}")
    reference = float(finite_midpoints[0])
    _require(
        math.isclose(reference, saved["reference_midpoint_usd"], rel_tol=0.0, abs_tol=1e-12),
        f"reference midpoint changed: {member}",
    )

    def rebase(values: np.ndarray) -> np.ndarray:
        return 10_000.0 * (np.where(valid, values, np.nan) / reference - 1.0)

    return {
        "member": member,
        "symbol": symbol,
        "session_date": day,
        "endpoint": saved["endpoint"],
        "timestamps": [datetime.fromtimestamp(value / NS, tz=ZoneInfo(TIMEZONE)) for value in ends],
        "bid_bps": rebase(bid),
        "ask_bps": rebase(ask),
        "reference_midpoint_usd": reference,
        "base_identity": saved["base_identity"],
    }


def load_comparison(numerical_source: Path, partitions: list[Path]) -> list[dict]:
    source = json.loads(numerical_source.read_text())
    _require(source.get("schema") == "endpoint_tape_pair_summary_v2", "invalid numerical source schema")
    examples = source.get("examples")
    _require(isinstance(examples, list) and len(examples) == 2, "comparison needs exactly two examples")
    _require(len(partitions) == 2, "comparison needs exactly two base partitions")
    expected = (("GPUS", "2026-06-18", "09:52:15"), ("CAST", "2026-06-18", "10:01:52"))
    for saved, (symbol, day, endpoint) in zip(examples, expected, strict=True):
        _require(
            (saved.get("symbol"), saved.get("session_date"), saved.get("endpoint"))
            == (symbol, day, endpoint),
            "saved selected endpoints changed",
        )
    return [_load_panel(saved, partition) for saved, partition in zip(examples, partitions, strict=True)]


def render_comparison(panels: list[dict], png: Path, svg: Path, *, dpi: int = 220) -> None:
    _require(len(panels) == 2, "comparison needs exactly two panels")
    fig, axes = plt.subplots(1, 2, figsize=(15.5, 6.2), sharey=False)
    fig.patch.set_facecolor("white")
    fig.subplots_adjust(left=0.075, right=0.985, top=0.80, bottom=0.15, wspace=0.16)
    fig.suptitle(TITLE, x=0.075, y=0.95, ha="left", fontsize=22, fontweight="bold", color=COLORS["text"])

    finite = np.concatenate(
        [array[np.isfinite(array)] for panel in panels for array in (panel["bid_bps"], panel["ask_bps"])]
    )
    low, high = float(finite.min()), float(finite.max())
    padding = max(15.0, 0.06 * (high - low))
    limits = (low - padding, high + padding)

    for ax, panel in zip(axes, panels, strict=True):
        ax.step(panel["timestamps"], panel["ask_bps"], where="post", color=COLORS["ask"], linewidth=0.9)
        ax.step(panel["timestamps"], panel["bid_bps"], where="post", color=COLORS["bid"], linewidth=0.9)
        ax.set_ylim(*limits)
        ax.yaxis.set_major_locator(MaxNLocator(nbins=7))
        ax.tick_params(axis="y", labelleft=True)
        ax.xaxis.set_major_locator(MinuteLocator(interval=1, tz=ZoneInfo(TIMEZONE)))
        ax.xaxis.set_major_formatter(DateFormatter("%H:%M", tz=ZoneInfo(TIMEZONE)))
        ax.set_xlabel("Time (ET)", fontsize=10, color=COLORS["text"], labelpad=8)
        ax.set_ylabel("Price relative to first midpoint (bps)", fontsize=10, color=COLORS["text"], labelpad=8)
        endpoint = datetime.strptime(panel["session_date"], "%Y-%m-%d").strftime("%B %-d, %Y")
        ax.set_title(
            f"{panel['symbol']} — {endpoint}, {panel['endpoint']} ET",
            fontsize=13,
            fontweight="bold",
            color=COLORS["text"],
            pad=12,
        )
        ax.grid(True, axis="y", color=COLORS["grid"], linewidth=0.8)
        ax.tick_params(colors=COLORS["text"], labelsize=9)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color("#8794A1")
        ax.spines["bottom"].set_color("#8794A1")

    fig.legend(
        handles=(
            Line2D([0], [0], color=COLORS["bid"], label="Bid"),
            Line2D([0], [0], color=COLORS["ask"], label="Ask"),
        ),
        loc="upper right",
        bbox_to_anchor=(0.985, 0.88),
        ncol=2,
        frameon=False,
        fontsize=10,
        labelcolor=COLORS["text"],
    )
    png.parent.mkdir(parents=True, exist_ok=True)
    svg.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(png, dpi=dpi, facecolor="white")
    fig.savefig(svg, facecolor="white")
    plt.close(fig)


def run(numerical_source: Path, partitions: list[Path], png: Path, svg: Path) -> list[dict]:
    panels = load_comparison(numerical_source, partitions)
    render_comparison(panels, png, svg)
    return panels


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--numerical-source", type=Path, required=True)
    parser.add_argument("--gpus-base-partition", type=Path, required=True)
    parser.add_argument("--cast-base-partition", type=Path, required=True)
    parser.add_argument("--png", type=Path, required=True)
    parser.add_argument("--svg", type=Path, required=True)
    args = parser.parse_args(argv)
    panels = run(
        args.numerical_source,
        [args.gpus_base_partition, args.cast_base_partition],
        args.png,
        args.svg,
    )
    print(
        json.dumps(
            {
                "state": "complete",
                "endpoints": [f"{panel['member']} {panel['endpoint']} ET" for panel in panels],
                "png": str(args.png),
                "svg": str(args.svg),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
