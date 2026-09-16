"""Render identity-checked endpoint/EW tape examples from saved member partitions.

The renderer intentionally preserves the old two-column, tape-first comparison
while using the sparse typography and restrained styling of the endpoint/EW
population figure. It has no sibling-repository runtime dependency.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter, MaxNLocator
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


NS = 1_000_000_000
BATCH_SIZE = 4096
ROWS = 300
HALF_LIVES = (30, 120)
FEATURE_STEMS = (
    "midpoint_rms_5s_bps",
    "quoted_spread_bps",
    "midpoint_rms_5s_to_spread",
    "trade_rate_per_second",
    "movement_participation",
)
BASE_COLUMNS = (
    "session_date",
    "symbol",
    "interval_end_ns",
    "bid_end_usd",
    "ask_end_usd",
    "price_end_reason_mask",
    "trade_count_1s",
    "share_volume_1s",
    "dollar_volume_1s_usd",
    "activity_reason_mask",
    "halt_active",
)
FEATURE_COLUMNS = tuple(
    value
    for half_life in HALF_LIVES
    for stem in FEATURE_STEMS
    for value in (f"{stem}_hl{half_life}s", f"{stem}_hl{half_life}s_reason_mask")
)
COLORS = {
    "bid": "#2F8F75",
    "ask": "#C95B66",
    "midpoint": "#7A8793",
    "trade": "#557FA5",
    "rms": "#356E9D",
    "spread": "#C47A44",
    "text": "#17212B",
    "muted": "#586A82",
    "grid": "#DDE3E8",
}


class TapeExampleError(ValueError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise TapeExampleError(message)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_sha(value) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _clock_ns(day: str, value: str, timezone: str) -> int:
    parsed = datetime.strptime(f"{day} {value}", "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=ZoneInfo(timezone)
    )
    return int(parsed.timestamp()) * NS


def _manifest_output(partition: Path, filename: str, member: str) -> tuple[dict, dict, Path]:
    manifest_path = partition / "manifest.json"
    _require(manifest_path.is_file(), f"missing manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text())
    _require(manifest.get("complete") is True, f"incomplete partition: {partition}")
    actual_member = manifest.get("member", {})
    _require(
        f"{actual_member.get('session_date')}/{actual_member.get('symbol')}" == member,
        f"manifest member mismatch: {partition}",
    )
    matches = [row for row in manifest.get("outputs", []) if row.get("path") == filename]
    _require(len(matches) == 1, f"missing unique {filename} output")
    output = matches[0]
    path = partition / filename
    _require(path.is_file(), f"missing output: {path}")
    _require(path.stat().st_size == output.get("bytes"), f"byte identity mismatch: {path}")
    _require(_sha256(path) == output.get("sha256"), f"hash identity mismatch: {path}")
    return manifest, output, path


def _scan_window(path: Path, columns: tuple[str, ...], start: int, stop: int) -> pa.Table:
    batches = []
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=BATCH_SIZE, columns=list(columns), use_threads=False
    ):
        ends = batch.column(batch.schema.get_field_index("interval_end_ns")).to_numpy()
        indices = np.flatnonzero((ends >= start) & (ends < stop))
        if len(indices):
            batches.append(batch.take(pa.array(indices, type=pa.int64())))
    _require(batches, f"window has no rows: {path}")
    return pa.Table.from_batches(batches)


def _float(column: pa.ChunkedArray) -> np.ndarray:
    return np.asarray(column.to_numpy(zero_copy_only=False), dtype=np.float64)


def _valid_values(table: pa.Table, name: str) -> np.ndarray:
    values = _float(table[name])
    masks = np.asarray(table[name + "_reason_mask"].to_numpy(), dtype=np.uint16)
    valid = masks == 0
    _require(np.all(np.isfinite(values[valid])), f"nonfinite valid {name}")
    return np.where(valid, values, np.nan)


def load_example(spec: dict, *, timezone: str) -> dict:
    expected = {
        "member",
        "start",
        "end",
        "base_partition",
        "feature_partition",
        "lineage_status",
    }
    _require(isinstance(spec, dict) and set(spec) == expected, "invalid example specification")
    member = spec["member"]
    _require(isinstance(member, str) and member.count("/") == 1, "invalid member")
    day, symbol = member.split("/")
    start = _clock_ns(day, spec["start"], timezone)
    stop = _clock_ns(day, spec["end"], timezone)
    _require(stop - start == ROWS * NS, "example must be exactly five minutes")
    base_manifest, base_output, base_path = _manifest_output(
        Path(spec["base_partition"]), "base.parquet", member
    )
    feature_manifest, feature_output, feature_path = _manifest_output(
        Path(spec["feature_partition"]), "features.parquet", member
    )
    _require(
        base_manifest.get("contract_identity") == feature_manifest.get("contract_identity"),
        "base/feature contract mismatch",
    )
    base = _scan_window(base_path, BASE_COLUMNS, start, stop)
    features = _scan_window(feature_path, ("interval_end_ns", *FEATURE_COLUMNS), start, stop)
    _require(base.num_rows == features.num_rows == ROWS, "example endpoint count mismatch")
    expected_ends = np.arange(start, stop, NS, dtype=np.int64)
    base_ends = np.asarray(base["interval_end_ns"].to_numpy(), dtype=np.int64)
    feature_ends = np.asarray(features["interval_end_ns"].to_numpy(), dtype=np.int64)
    _require(np.array_equal(base_ends, expected_ends), "base endpoint grid mismatch")
    _require(np.array_equal(feature_ends, expected_ends), "feature endpoint grid mismatch")
    _require(set(base["session_date"].to_pylist()) == {day}, "base date mismatch")
    _require(set(base["symbol"].to_pylist()) == {symbol}, "base symbol mismatch")

    price_mask = np.asarray(base["price_end_reason_mask"].to_numpy(), dtype=np.uint16)
    bid = _float(base["bid_end_usd"])
    ask = _float(base["ask_end_usd"])
    price_valid = price_mask == 0
    _require(np.all(np.isfinite(bid[price_valid])) and np.all(np.isfinite(ask[price_valid])), "invalid endpoint prices")
    midpoint = np.where(price_valid, (bid + ask) / 2.0, np.nan)
    reference = midpoint[np.isfinite(midpoint)][0]
    rebase = lambda values: 10_000.0 * (values / reference - 1.0)

    activity_mask = np.asarray(base["activity_reason_mask"].to_numpy(), dtype=np.uint16)
    counts = np.asarray(base["trade_count_1s"].to_numpy(zero_copy_only=False), dtype=np.float64)
    shares = _float(base["share_volume_1s"])
    dollars = _float(base["dollar_volume_1s_usd"])
    activity_valid = activity_mask == 0
    counts = np.where(activity_valid, counts, np.nan)
    trade_vwap = np.full(ROWS, np.nan)
    positive = activity_valid & (shares > 0)
    trade_vwap[positive] = dollars[positive] / shares[positive]

    values = {name: _valid_values(features, name) for name in FEATURE_COLUMNS if not name.endswith("_reason_mask")}
    summaries = {}
    for name, array in values.items():
        good = array[np.isfinite(array)]
        summaries[name] = {
            "valid_seconds": int(len(good)),
            "mean": float(np.mean(good)) if len(good) else None,
            "median": float(np.median(good)) if len(good) else None,
        }
    return {
        "member": member,
        "symbol": symbol,
        "session_date": day,
        "start": spec["start"],
        "end": spec["end"],
        "lineage_status": spec["lineage_status"],
        "elapsed": np.arange(ROWS, dtype=np.float64),
        "bid_bps": rebase(np.where(price_valid, bid, np.nan)),
        "ask_bps": rebase(np.where(price_valid, ask, np.nan)),
        "midpoint_bps": rebase(midpoint),
        "trade_vwap_bps": rebase(trade_vwap),
        "trade_count_1s": counts,
        "features": values,
        "summaries": summaries,
        "reference_midpoint_usd": float(reference),
        "halt_seconds": int(np.count_nonzero(np.asarray(base["halt_active"].to_numpy(), dtype=bool))),
        "base_identity": base_output["sha256"],
        "feature_identity": feature_output["sha256"],
        "contract_identity": base_manifest["contract_identity"],
    }


def _style_axis(ax) -> None:
    ax.set_facecolor("white")
    ax.grid(True, axis="y", color=COLORS["grid"], linewidth=0.8)
    ax.tick_params(colors=COLORS["text"], labelsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#8794A1")
    ax.spines["bottom"].set_color("#8794A1")


def _format_mean(value: float | None, digits: int) -> str:
    return "unavailable" if value is None else f"{value:,.{digits}f}"


def render_pair(examples: list[dict], output: Path, *, title: str, subtitle: str) -> None:
    _require(len(examples) == 2, "exactly two examples are required")
    fig = plt.figure(figsize=(15.5, 11.2), facecolor="white")
    grid = fig.add_gridspec(
        4,
        2,
        height_ratios=(5.0, 1.35, 2.2, 2.05),
        left=0.075,
        right=0.985,
        top=0.84,
        bottom=0.095,
        hspace=0.12,
        wspace=0.12,
    )
    price_axes = [fig.add_subplot(grid[0, index]) for index in range(2)]
    count_axes = [fig.add_subplot(grid[1, index], sharex=price_axes[index]) for index in range(2)]
    feature_axes = [fig.add_subplot(grid[2, index], sharex=price_axes[index]) for index in range(2)]
    table_axes = [fig.add_subplot(grid[3, index]) for index in range(2)]
    fig.suptitle(title, x=0.075, y=0.96, ha="left", fontsize=24, fontweight="bold", color=COLORS["text"])
    fig.text(0.075, 0.915, subtitle, fontsize=13, color=COLORS["muted"])
    y_values = [value for item in examples for key in ("bid_bps", "ask_bps", "trade_vwap_bps") for value in item[key] if math.isfinite(value)]
    y_low, y_high = min(y_values), max(y_values)
    y_pad = max(15.0, 0.06 * (y_high - y_low))
    price_limits = (y_low - y_pad, y_high + y_pad)
    feature_high = max(
        np.nanmax(item["features"][f"midpoint_rms_5s_bps_hl30s"])
        for item in examples
    )
    feature_high = max(
        feature_high,
        max(np.nanmax(item["features"][f"quoted_spread_bps_hl30s"]) for item in examples),
    ) * 1.08
    count_high = max(np.nanmax(item["trade_count_1s"]) for item in examples) * 1.06

    for column, item in enumerate(examples):
        price_ax, count_ax, feature_ax, table_ax = (
            price_axes[column],
            count_axes[column],
            feature_axes[column],
            table_axes[column],
        )
        elapsed = item["elapsed"]
        price_ax.set_title(
            f"{item['symbol']}  ·  [{item['start']}, {item['end']}) ET",
            loc="left",
            fontsize=14,
            fontweight="bold",
            color=COLORS["text"],
            pad=10,
        )
        price_ax.step(elapsed, item["ask_bps"], where="post", color=COLORS["ask"], linewidth=0.8)
        price_ax.step(elapsed, item["bid_bps"], where="post", color=COLORS["bid"], linewidth=0.8)
        price_ax.step(elapsed, item["midpoint_bps"], where="post", color=COLORS["midpoint"], linewidth=0.55, alpha=0.7)
        trade = np.isfinite(item["trade_vwap_bps"])
        price_ax.scatter(elapsed[trade], item["trade_vwap_bps"][trade], s=5, color=COLORS["trade"], alpha=0.55, linewidths=0, rasterized=True)
        price_ax.axhline(0.0, color="#98A3AD", linewidth=0.7, linestyle=":")
        price_ax.set(xlim=(0, 300), ylim=price_limits)
        price_ax.text(
            0.012,
            0.975,
            f"Reference midpoint ${item['reference_midpoint_usd']:.4f}",
            transform=price_ax.transAxes,
            va="top",
            fontsize=9,
            color=COLORS["muted"],
        )
        count_ax.bar(elapsed + 0.5, item["trade_count_1s"], width=1.0, color=COLORS["trade"], linewidth=0)
        count_ax.set_ylim(0, max(1, count_high))
        count_ax.yaxis.set_major_locator(MaxNLocator(nbins=3, integer=True))
        rms = item["features"]["midpoint_rms_5s_bps_hl30s"]
        spread = item["features"]["quoted_spread_bps_hl30s"]
        feature_ax.plot(elapsed, rms, color=COLORS["rms"], linewidth=1.45)
        feature_ax.plot(elapsed, spread, color=COLORS["spread"], linewidth=1.45)
        feature_ax.set_ylim(0, feature_high)
        for ax in (price_ax, count_ax, feature_ax):
            _style_axis(ax)
        price_ax.tick_params(labelbottom=False)
        count_ax.tick_params(labelbottom=False)
        feature_ax.set_xticks((0, 60, 120, 180, 240, 300))
        feature_ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{int(value)//60}:{int(value)%60:02d}"))
        feature_ax.set_xlabel("Elapsed time", color=COLORS["text"], fontsize=10, labelpad=2)
        if column == 0:
            price_ax.set_ylabel("Price relative to first midpoint (bps)", color=COLORS["text"])
            count_ax.set_ylabel("Eligible trades / 1s", color=COLORS["text"])
            feature_ax.set_ylabel("30s EW measurement (bps)", color=COLORS["text"])

        table_ax.axis("off")
        rows = []
        for label, stem, digits in (
            ("RMS, bps", "midpoint_rms_5s_bps", 2),
            ("Quoted spread, bps", "quoted_spread_bps", 2),
            ("RMS / spread", "midpoint_rms_5s_to_spread", 2),
            ("Trade rate, trades/s", "trade_rate_per_second", 2),
            ("Participation", "movement_participation", 3),
        ):
            rows.append(
                [
                    label,
                    _format_mean(item["summaries"][f"{stem}_hl30s"]["mean"], digits),
                    _format_mean(item["summaries"][f"{stem}_hl120s"]["mean"], digits),
                ]
            )
        table = table_ax.table(
            cellText=rows,
            colLabels=("Five-minute mean", "30s half-life", "120s half-life"),
            cellLoc="right",
            colLoc="right",
            bbox=(0.0, 0.10, 1.0, 0.70),
            colWidths=(0.48, 0.26, 0.26),
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        for (row, col), cell in table.get_celld().items():
            cell.set_edgecolor("#E3E7EB")
            cell.set_linewidth(0.5)
            cell.set_facecolor("#F6F8FA" if row == 0 else "white")
            cell.get_text().set_color(COLORS["text"] if row else COLORS["muted"])
            if col == 0:
                cell.get_text().set_ha("left")
        table_ax.text(0.0, 0.0, item["lineage_status"], fontsize=8.2, color=COLORS["muted"], transform=table_ax.transAxes)

    fig.legend(
        handles=(
            Line2D([0], [0], color=COLORS["ask"], label="Best ask"),
            Line2D([0], [0], color=COLORS["bid"], label="Best bid"),
            Line2D([0], [0], color=COLORS["midpoint"], label="Midpoint"),
            Line2D([0], [0], marker="o", color="none", markerfacecolor=COLORS["trade"], markersize=5, label="Eligible 1s VWAP"),
            Line2D([0], [0], color=COLORS["rms"], linewidth=1.7, label="30s EW RMS"),
            Line2D([0], [0], color=COLORS["spread"], linewidth=1.7, label="30s EW spread"),
        ),
        loc="upper right",
        bbox_to_anchor=(0.985, 0.915),
        ncol=3,
        frameon=False,
        fontsize=9.3,
        labelcolor=COLORS["text"],
    )
    fig.text(
        0.075,
        0.025,
        "Each column is the original retrospectively selected five-minute window. Endpoint quotes and eligible trade activity use the current one-second contract;\n"
        "feature summaries average only valid displayed endpoints. Price rebasing is display-only.",
        fontsize=8.6,
        color=COLORS["muted"],
    )
    fig.savefig(output, dpi=220, facecolor="white")
    fig.savefig(output.with_suffix(".svg"), facecolor="white")
    plt.close(fig)


def _json_summary(example: dict) -> dict:
    return {
        key: value
        for key, value in example.items()
        if key
        not in {
            "elapsed",
            "bid_bps",
            "ask_bps",
            "midpoint_bps",
            "trade_vwap_bps",
            "trade_count_1s",
            "features",
        }
    } | {
        "mean_trade_count_1s": float(np.nanmean(example["trade_count_1s"])),
        "endpoint_count": ROWS,
    }


def run(config_path: Path, output: Path) -> dict:
    config = json.loads(config_path.read_text())
    _require(
        isinstance(config, dict)
        and set(config) == {"schema", "timezone", "title", "subtitle", "examples"}
        and config["schema"] == "endpoint_tape_pair_v1"
        and isinstance(config["examples"], list)
        and len(config["examples"]) == 2,
        "invalid tape-pair configuration",
    )
    output.mkdir(parents=True, exist_ok=False)
    examples = [load_example(spec, timezone=config["timezone"]) for spec in config["examples"]]
    figure = output / "gpus_cast_endpoint_ew.png"
    render_pair(examples, figure, title=config["title"], subtitle=config["subtitle"])
    numerical = {
        "schema": "endpoint_tape_pair_summary_v1",
        "config_sha256": _sha256(config_path),
        "pair_identity": _canonical_sha(
            [{"member": item["member"], "start": item["start"], "end": item["end"], "base": item["base_identity"], "features": item["feature_identity"]} for item in examples]
        ),
        "examples": [_json_summary(item) for item in examples],
    }
    numerical_path = output / "numerical_summary.json"
    numerical_path.write_text(json.dumps(numerical, indent=2, sort_keys=True, allow_nan=False) + "\n")
    artifacts = []
    for path in (figure, figure.with_suffix(".svg"), numerical_path):
        artifacts.append({"path": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path)})
    manifest = {
        "schema": "endpoint_tape_pair_artifacts_v1",
        "implementation": _sha256(Path(__file__)),
        "config": {"path": str(config_path), "sha256": _sha256(config_path)},
        "artifacts": artifacts,
    }
    (output / "artifacts.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return numerical


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    result = run(args.config, args.output)
    print(json.dumps({"pair_identity": result["pair_identity"], "output": str(args.output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
