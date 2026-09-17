"""First real-data endpoint/EW joint-distribution preview.

The reducer reads the fixed endpoint reference once, accumulates six bounded
histograms, writes numerical artifacts, and renders only from those artifacts.
"""
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import threading
import time

import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ..query import (
    EndpointSelection,
    describe_endpoint_fields,
    iter_endpoint_batches,
    open_endpoint_reference,
)


REFERENCE_IDENTITY = "4a410bd8321cfe0466dc7ecd02c8a5762a679c320d64376c24cea076465ae832"
SESSIONS = ("premarket", "rth", "after_hours")
HALF_LIVES = (30, 120)
RELATIONSHIPS = ("spread", "participation", "trade_rate")
FIELDS = tuple(
    f"{stem}_hl{half_life}s"
    for half_life in HALF_LIVES
    for stem in (
        "midpoint_rms_5s_bps",
        "quoted_spread_bps",
        "movement_participation",
        "trade_rate_per_second",
    )
)


def _log_edges(low: float, high: float, bins: int = 50) -> tuple[float, ...]:
    return tuple(float(value) for value in np.geomspace(low, high, bins + 1))


DEFAULT_AXES = {
    "rms": {"unit": "bps", "scale": "log", "edges": _log_edges(0.01, 1000.0)},
    "spread": {"unit": "bps", "scale": "log", "edges": _log_edges(0.03, 3000.0)},
    "participation": {
        "unit": "1",
        "scale": "linear",
        "edges": tuple(float(value) for value in np.linspace(0.0, 1.0, 51)),
    },
    "trade_rate": {"unit": "trades/s", "scale": "log", "edges": _log_edges(0.01, 1000.0)},
}


class PreviewError(ValueError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PreviewError(message)


@dataclass(frozen=True)
class Axis:
    name: str
    unit: str
    scale: str
    edges: tuple[float, ...]

    def __post_init__(self) -> None:
        _require(self.scale in {"log", "linear"}, "unsupported axis scale")
        _require(
            len(self.edges) >= 2
            and all(math.isfinite(value) for value in self.edges)
            and all(left < right for left, right in zip(self.edges, self.edges[1:])),
            "axis edges must be finite and strictly increasing",
        )
        if self.scale == "log":
            _require(self.edges[0] > 0, "log axis must be positive")
        else:
            _require(self.edges[0] == 0.0, "linear participation axis must start at zero")

    @property
    def finite_bins(self) -> int:
        return len(self.edges) - 1

    @property
    def total_bins(self) -> int:
        return self.finite_bins + (3 if self.scale == "log" else 2)

    @property
    def finite_offset(self) -> int:
        return 2 if self.scale == "log" else 1

    @property
    def finite_slice(self) -> slice:
        return slice(self.finite_offset, self.finite_offset + self.finite_bins)

    def classify(self, values: np.ndarray) -> np.ndarray:
        values = np.asarray(values, dtype=np.float64)
        _require(np.isfinite(values).all() and (values >= 0).all(), "valid values must be finite and nonnegative")
        if self.scale == "log":
            result = np.full(values.shape, self.total_bins - 1, dtype=np.int16)
            result[values == 0.0] = 0
            positive = values > 0.0
            result[positive & (values < self.edges[0])] = 1
            finite = positive & (values >= self.edges[0]) & (values <= self.edges[-1])
            positions = np.searchsorted(self.edges, values[finite], side="right") - 1
            positions[values[finite] == self.edges[-1]] = self.finite_bins - 1
            result[finite] = positions + self.finite_offset
            return result
        result = np.full(values.shape, self.total_bins - 1, dtype=np.int16)
        result[values < self.edges[0]] = 0
        finite = (values >= self.edges[0]) & (values <= self.edges[-1])
        positions = np.searchsorted(self.edges, values[finite], side="right") - 1
        positions[values[finite] == self.edges[-1]] = self.finite_bins - 1
        result[finite] = positions + self.finite_offset
        return result

    def category(self, index: int) -> tuple[str, int | None, float | None, float | None]:
        if self.scale == "log":
            if index == 0:
                return "zero", None, 0.0, 0.0
            if index == 1:
                return "positive_underflow", None, 0.0, self.edges[0]
        else:
            if index == 0:
                return "underflow", None, None, self.edges[0]
        if index == self.total_bins - 1:
            return "overflow", None, self.edges[-1], None
        finite_index = index - self.finite_offset
        return "displayed", finite_index, self.edges[finite_index], self.edges[finite_index + 1]


@dataclass
class PanelAccumulator:
    panel_id: str
    half_life_seconds: int
    relationship: str
    x_field: str
    y_field: str
    x_axis: Axis
    y_axis: Axis

    def __post_init__(self) -> None:
        self.counts = np.zeros((self.x_axis.total_bins, self.y_axis.total_bins), dtype=np.int64)
        self.selected = 0
        self.x_invalid_only = 0
        self.y_invalid_only = 0
        self.both_invalid = 0
        self.x_zero = 0
        self.y_zero = 0
        self.member_counts: dict[str, list[int]] = {}

    def add(self, batch: pa.RecordBatch) -> None:
        def column(name: str) -> np.ndarray:
            return batch.column(batch.schema.get_field_index(name)).to_numpy(zero_copy_only=False)

        x_mask = np.asarray(column(self.x_field + "_reason_mask"), dtype=np.uint16)
        y_mask = np.asarray(column(self.y_field + "_reason_mask"), dtype=np.uint16)
        x_valid = x_mask == 0
        y_valid = y_mask == 0
        pair_valid = x_valid & y_valid
        count = batch.num_rows
        self.selected += count
        self.x_invalid_only += int(np.count_nonzero(~x_valid & y_valid))
        self.y_invalid_only += int(np.count_nonzero(x_valid & ~y_valid))
        self.both_invalid += int(np.count_nonzero(~x_valid & ~y_valid))
        dates = batch.column(batch.schema.get_field_index("session_date"))
        symbols = batch.column(batch.schema.get_field_index("symbol"))
        _require(len(set(dates.to_pylist())) == 1 and len(set(symbols.to_pylist())) == 1, "batch crosses a member")
        member = f"{dates[0].as_py()}/{symbols[0].as_py()}"
        member_totals = self.member_counts.setdefault(member, [0, 0, 0])
        member_totals[0] += count
        if not pair_valid.any():
            return
        x = np.asarray(column(self.x_field)[pair_valid], dtype=np.float64)
        y = np.asarray(column(self.y_field)[pair_valid], dtype=np.float64)
        self.x_zero += int(np.count_nonzero(x == 0.0))
        self.y_zero += int(np.count_nonzero(y == 0.0))
        x_bin = self.x_axis.classify(x)
        y_bin = self.y_axis.classify(y)
        np.add.at(self.counts, (x_bin, y_bin), 1)
        pair_count = len(x)
        plotted = int(
            np.count_nonzero(
                (x_bin >= self.x_axis.finite_slice.start)
                & (x_bin < self.x_axis.finite_slice.stop)
                & (y_bin >= self.y_axis.finite_slice.start)
                & (y_bin < self.y_axis.finite_slice.stop)
            )
        )
        member_totals[1] += pair_count
        member_totals[2] += plotted

    def finish(self) -> dict:
        pair_valid = int(self.counts.sum())
        _require(
            self.selected == pair_valid + self.x_invalid_only + self.y_invalid_only + self.both_invalid,
            f"{self.panel_id}: validity accounting mismatch",
        )
        plotted = int(self.counts[self.x_axis.finite_slice, self.y_axis.finite_slice].sum())
        contributing = {member: values for member, values in self.member_counts.items() if values[1]}
        largest_member, largest = max(contributing.items(), key=lambda item: item[1][1]) if contributing else (None, [0, 0, 0])
        return {
            "panel_id": self.panel_id,
            "half_life_seconds": self.half_life_seconds,
            "relationship": self.relationship,
            "x_field": self.x_field,
            "y_field": self.y_field,
            "selected": self.selected,
            "pair_valid": pair_valid,
            "unavailable": self.selected - pair_valid,
            "x_invalid_only": self.x_invalid_only,
            "y_invalid_only": self.y_invalid_only,
            "both_invalid": self.both_invalid,
            "plotted": plotted,
            "off_axis_or_log_zero": pair_valid - plotted,
            "x_zero": self.x_zero,
            "y_zero": self.y_zero,
            "selected_members": len(self.member_counts),
            "contributing_members": len(contributing),
            "largest_member": largest_member,
            "largest_member_pair_valid": largest[1],
            "largest_member_share": largest[1] / pair_valid if pair_valid else None,
        }


def _panels() -> list[PanelAccumulator]:
    axes = {
        name: Axis(name, spec["unit"], spec["scale"], spec["edges"])
        for name, spec in DEFAULT_AXES.items()
    }
    stems = {
        "spread": "quoted_spread_bps",
        "participation": "movement_participation",
        "trade_rate": "trade_rate_per_second",
    }
    return [
        PanelAccumulator(
            panel_id=f"rms_vs_{relationship}_hl{half_life}s",
            half_life_seconds=half_life,
            relationship=relationship,
            x_field=f"{stems[relationship]}_hl{half_life}s",
            y_field=f"midpoint_rms_5s_bps_hl{half_life}s",
            x_axis=axes[relationship],
            y_axis=axes["rms"],
        )
        for relationship in RELATIONSHIPS
        for half_life in HALF_LIVES
    ]


class RSSSampler:
    def __init__(self, stop_bytes: int):
        self.stop_bytes = stop_bytes
        self.peak_bytes = 0
        self.exceeded = False
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self) -> None:
        import psutil

        process = psutil.Process()
        while not self._done.wait(0.05):
            try:
                rss = process.memory_info().rss + sum(child.memory_info().rss for child in process.children(recursive=True))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            self.peak_bytes = max(self.peak_bytes, rss)
            if rss > self.stop_bytes:
                self.exceeded = True
                os._exit(70)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_args):
        self._done.set()
        self._thread.join()


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _axis_config() -> dict:
    return {
        "schema": "endpoint_joint_preview_bins_v1",
        "selected_before_real_value_inspection": True,
        "selection_basis": "feature units and prior report axis ranges",
        "bin_convention": "finite bins are left-closed/right-open except the final right edge is included; log zeros, positive underflow, and overflow are retained separately",
        "axes": {
            name: {"unit": spec["unit"], "scale": spec["scale"], "edges": list(spec["edges"])}
            for name, spec in DEFAULT_AXES.items()
        },
    }


def _histogram_rows(panel: PanelAccumulator, summary: dict) -> list[dict]:
    rows = []
    denominator = summary["pair_valid"]
    for x_index in range(panel.x_axis.total_bins):
        x_category, x_display_bin, x_low, x_high = panel.x_axis.category(x_index)
        for y_index in range(panel.y_axis.total_bins):
            y_category, y_display_bin, y_low, y_high = panel.y_axis.category(y_index)
            count = int(panel.counts[x_index, y_index])
            rows.append(
                {
                    "panel_id": panel.panel_id,
                    "half_life_seconds": panel.half_life_seconds,
                    "relationship": panel.relationship,
                    "x_category": x_category,
                    "x_display_bin": x_display_bin,
                    "x_low": x_low,
                    "x_high": x_high,
                    "y_category": y_category,
                    "y_display_bin": y_display_bin,
                    "y_low": y_low,
                    "y_high": y_high,
                    "count": count,
                    "percent_pair_valid": 100.0 * count / denominator if denominator else None,
                }
            )
    return rows


def _render_from_tables(counts_path: Path, coverage_path: Path, output: Path) -> float:
    started = time.perf_counter()
    table = pq.read_table(counts_path).to_pylist()
    coverage = {row["panel_id"]: row for row in json.loads(coverage_path.read_text())}
    by_panel: dict[str, list[dict]] = {}
    for row in table:
        by_panel.setdefault(row["panel_id"], []).append(row)
    titles = {"spread": "Mean full quoted spread", "participation": "Movement participation", "trade_rate": "Eligible trade rate"}
    xlabels = {"spread": "Spread (bps)", "participation": "Participation", "trade_rate": "Trades / second"}
    panels = _panels()
    fig, axes = plt.subplots(3, 2, figsize=(14, 16), sharey=True)
    fig.subplots_adjust(left=0.09, right=0.92, top=0.92, bottom=0.10, hspace=0.30, wspace=0.18)
    for row_index, relationship in enumerate(RELATIONSHIPS):
        row_panels = [panel for panel in panels if panel.relationship == relationship]
        maximum = 0.0
        matrices = []
        for panel in row_panels:
            matrix = np.zeros((panel.x_axis.finite_bins, panel.y_axis.finite_bins), dtype=float)
            for record in by_panel[panel.panel_id]:
                if record["x_category"] == "displayed" and record["y_category"] == "displayed":
                    matrix[record["x_display_bin"], record["y_display_bin"]] = record["percent_pair_valid"]
            matrices.append(matrix)
            maximum = max(maximum, float(matrix.max(initial=0.0)))
        normalization = Normalize(vmin=0.0, vmax=max(maximum, 1e-12))
        for column_index, (panel, matrix) in enumerate(zip(row_panels, matrices)):
            ax = axes[row_index, column_index]
            image = ax.pcolormesh(panel.x_axis.edges, panel.y_axis.edges, matrix.T, shading="auto", cmap="viridis", norm=normalization)
            if panel.x_axis.scale == "log":
                ax.set_xscale("log")
            ax.set_yscale("log")
            if relationship == "spread":
                x = np.asarray(panel.x_axis.edges)
                for ratio, style in ((0.1, ":"), (1.0, "-"), (10.0, ":")):
                    y = ratio * x
                    visible = (y >= panel.y_axis.edges[0]) & (y <= panel.y_axis.edges[-1])
                    if visible.any():
                        ax.plot(x[visible], y[visible], color="white", linewidth=1.0 if ratio == 1 else 0.7, linestyle=style, alpha=0.9)
                ax.text(0.97, 0.06, "white solid: RMS / spread = 1", color="white", fontsize=8, ha="right", transform=ax.transAxes)
            summary = coverage[panel.panel_id]
            ax.set_title(f"{titles[relationship]} · EW half-life {panel.half_life_seconds}s")
            ax.set_xlabel(xlabels[relationship])
            if column_index == 0:
                ax.set_ylabel("RMS five-second midpoint return (bps)")
            ax.grid(alpha=0.12, which="both")
            ax.text(
                0.01,
                0.99,
                f"pair-valid {summary['pair_valid']:,}\nplotted {summary['plotted'] / summary['pair_valid']:.2%}\noff-axis/zero {summary['off_axis_or_log_zero'] / summary['pair_valid']:.2%}",
                va="top",
                transform=ax.transAxes,
                fontsize=8,
                bbox={"facecolor": "white", "alpha": 0.78, "edgecolor": "none"},
            )
        colorbar = fig.colorbar(image, ax=list(axes[row_index, :]), pad=0.01, fraction=0.025)
        colorbar.set_label("% of pair-valid observations per bin")
    fig.suptitle("Endpoint/EW joint distributions — 24-member stratified development pilot", fontsize=16)
    fig.text(
        0.09,
        0.035,
        "Retrospective historical_membership; complete premarket, RTH, and after-hours sessions. Each panel uses its own pair-valid denominator.\n"
        "Valid zeros and off-axis tails remain in the saved numerical tables. Pooled one-second observations; overlapping returns and EW smoothing mean rows are not independent.",
        fontsize=9,
    )
    fig.savefig(output, dpi=170)
    plt.close(fig)
    return time.perf_counter() - started


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def run(args: argparse.Namespace) -> dict:
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    _write_json(output / "bin_config.json", _axis_config())
    selection = EndpointSelection("historical_membership", sessions=SESSIONS)
    _write_json(output / "selection.json", selection.to_dict())
    total_started = time.perf_counter()
    blocks_before = resource.getrusage(resource.RUSAGE_SELF).ru_inblock
    with RSSSampler(args.rss_stop_bytes) as rss:
        verification_started = time.perf_counter()
        handle = open_endpoint_reference(
            args.reference,
            expected_identity=args.reference_identity,
            data_roots={"base": args.base_root, "features": args.features_root},
        )
        verification_wall = time.perf_counter() - verification_started
        registry = {row["name"]: row for row in describe_endpoint_fields(handle.config)}
        _require(set(FIELDS) <= set(registry), "required fields absent from registry")
        for field in FIELDS:
            _require(registry[field]["unit"] in {"bps", "1", "trades/s"}, "unexpected registry unit")
        panels = _panels()
        scan_started = time.perf_counter()
        rows = 0
        batches = 0
        for batch in iter_endpoint_batches(
            handle,
            fields=FIELDS,
            selection=selection,
            include_support=False,
            include_run_boundaries=False,
            batch_size=4096,
        ):
            rows += batch.num_rows
            batches += 1
            for panel in panels:
                panel.add(batch)
        scan_wall = time.perf_counter() - scan_started
        _require(rows == 1_382_400, "fixed pilot represented-row count mismatch")
        summaries = [panel.finish() for panel in panels]
        _require(all(summary["selected"] == rows for summary in summaries), "panel selection mismatch")
        _require(all(summary["selected_members"] == 24 for summary in summaries), "fixed pilot member count mismatch")
        _write_json(output / "coverage.json", summaries)
        with (output / "coverage.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(summaries[0]))
            writer.writeheader()
            writer.writerows(summaries)
        histogram_rows = [row for panel, summary in zip(panels, summaries) for row in _histogram_rows(panel, summary)]
        pq.write_table(pa.Table.from_pylist(histogram_rows), output / "histogram_counts.parquet", compression="zstd")
        contributions = []
        for panel in panels:
            for member, (selected, pair_valid, plotted) in sorted(panel.member_counts.items()):
                contributions.append(
                    {
                        "panel_id": panel.panel_id,
                        "member": member,
                        "selected": selected,
                        "pair_valid": pair_valid,
                        "plotted": plotted,
                        "pair_valid_share": pair_valid / panel.counts.sum() if panel.counts.sum() else None,
                    }
                )
        pq.write_table(pa.Table.from_pylist(contributions), output / "member_contributions.parquet", compression="zstd")
        render_wall = _render_from_tables(output / "histogram_counts.parquet", output / "coverage.json", output / "joint_distribution_preview.png")
    blocks_after = resource.getrusage(resource.RUSAGE_SELF).ru_inblock
    artifacts = []
    for path in sorted(output.iterdir()):
        if path.name not in {"metadata.json", "artifacts.json"} and path.is_file():
            artifacts.append({"path": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path)})
    metadata = {
        "schema": "endpoint_joint_preview_run_v1",
        "status": "first_phase4_c_real_data_preview_not_phase4_completion",
        "reference_identity": args.reference_identity,
        "source_baseline": "b3d467dad943d82aca6c861d4e3a3de9199f6841",
        "population": "fixed 24-member stratified development pilot",
        "population_timing_mode": "historical_membership",
        "sessions": list(SESSIONS),
        "represented_rows": rows,
        "projected_fields": list(FIELDS),
        "projected_field_count": len(FIELDS),
        "batch_size": 4096,
        "batches": batches,
        "optional_support": False,
        "run_boundaries": False,
        "worker_count": 1,
        "library_threads": 1,
        "timings_seconds": {
            "file_verification": handle.validation_seconds,
            "open_call_wall": verification_wall,
            "scan_and_aggregation": scan_wall,
            "rendering_from_saved_tables": render_wall,
            "total": time.perf_counter() - total_started,
        },
        "resources": {
            "peak_sampled_process_tree_rss_bytes": rss.peak_bytes,
            "rss_stop_bytes": args.rss_stop_bytes,
            "filesystem_input_bytes_from_ru_inblock": (blocks_after - blocks_before) * 512,
            "cache_state": args.cache_state,
            "output_bytes": None,
        },
        "verification_bytes": handle.validation_bytes,
        "limitations": [
            "retrospective fixed development sample, not a representative corpus estimate",
            "pooled one-second observations are dependent because returns overlap and features are exponentially smoothed",
            "RMS and participation reuse the same return moments; their association is descriptive",
            "no activity gate, ratio threshold, or all-feature complete-case mask was applied",
        ],
    }
    _write_json(output / "artifacts.json", artifacts)
    _write_json(output / "metadata.json", metadata)
    # Include the two small manifest files in the actual owned-output total.
    # A second write is stable because only a fixed-width integer changes.
    for _ in range(2):
        metadata["resources"]["output_bytes"] = sum(path.stat().st_size for path in output.iterdir() if path.is_file())
        _write_json(output / "metadata.json", metadata)
    return metadata


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--reference", required=True)
    result.add_argument("--reference-identity", default=REFERENCE_IDENTITY)
    result.add_argument("--base-root", required=True)
    result.add_argument("--features-root", required=True)
    result.add_argument("--output", required=True)
    result.add_argument("--cache-state", default="uncontrolled shared OS cache; not labeled cold")
    result.add_argument("--rss-stop-bytes", type=int, default=2 * 1024**3)
    return result


def main() -> None:
    args = parser().parse_args()
    print(json.dumps(run(args), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
