"""Expanded fixed-pilot joints with activity gates, bands, and sessions."""
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
import resource
import time

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, Normalize
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ..query import (
    EndpointSelection,
    describe_endpoint_fields,
    iter_endpoint_batches,
    open_endpoint_reference,
)
from .endpoint_joint_preview import (
    Axis,
    DEFAULT_AXES,
    HALF_LIVES,
    REFERENCE_IDENTITY,
    RSSSampler,
    SESSIONS,
    _sha256,
    _write_json,
)


AGE_FIELDS = {
    30: "trade_age_p90_seconds_window60s",
    120: "trade_age_p90_seconds_window300s",
}
RATE_FIELDS = {half_life: f"trade_rate_per_second_hl{half_life}s" for half_life in HALF_LIVES}
RMS_FIELDS = {half_life: f"midpoint_rms_5s_bps_hl{half_life}s" for half_life in HALF_LIVES}
RELATIONSHIP_FIELDS = {
    "spread": {half_life: f"quoted_spread_bps_hl{half_life}s" for half_life in HALF_LIVES},
    "participation": {half_life: f"movement_participation_hl{half_life}s" for half_life in HALF_LIVES},
    "trade_rate": RATE_FIELDS,
}
EXPANDED_FIELDS = tuple(
    dict.fromkeys(
        value
        for half_life in HALF_LIVES
        for value in (
            RMS_FIELDS[half_life],
            RELATIONSHIP_FIELDS["spread"][half_life],
            RELATIONSHIP_FIELDS["participation"][half_life],
            RATE_FIELDS[half_life],
            AGE_FIELDS[half_life],
        )
    )
)
SESSION_SCOPES = ("pooled",) + SESSIONS
FILTER_SCOPES = ("unconditional", "activity_gate")
ACTIVITY_BANDS = (
    ("zero", "exactly 0 trades/s"),
    ("positive_lt_1", "0–<1 trades/s"),
    ("1_to_lt_10", "1–<10 trades/s"),
    ("10_to_lt_30", "10–<30 trades/s"),
    ("30_to_lt_100", "30–<100 trades/s"),
    ("ge_100", "≥100 trades/s"),
)
GATED_DISPLAY_BANDS = tuple(name for name, _ in ACTIVITY_BANDS[2:])
SESSION_BY_SEGMENT = {0: "premarket", 1: "rth", 2: "after_hours"}


class ExpandedError(ValueError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ExpandedError(message)


def activity_band_ids(rate: np.ndarray, valid: np.ndarray) -> np.ndarray:
    """Return -1 for unavailable and 0..5 for exhaustive valid-rate bands."""
    rate = np.asarray(rate, dtype=np.float64)
    valid = np.asarray(valid, dtype=bool)
    result = np.full(rate.shape, -1, dtype=np.int8)
    values = rate[valid]
    _require(np.isfinite(values).all() and (values >= 0).all(), "valid rate must be finite and nonnegative")
    destination = result[valid]
    destination[values == 0.0] = 0
    destination[(values > 0.0) & (values < 1.0)] = 1
    destination[(values >= 1.0) & (values < 10.0)] = 2
    destination[(values >= 10.0) & (values < 30.0)] = 3
    destination[(values >= 30.0) & (values < 100.0)] = 4
    destination[values >= 100.0] = 5
    result[valid] = destination
    return result


def activity_gate(rate: np.ndarray, rate_mask: np.ndarray, age: np.ndarray, age_mask: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    rate_valid = np.asarray(rate_mask, dtype=np.uint16) == 0
    age_valid = np.asarray(age_mask, dtype=np.uint16) == 0
    valid = rate_valid & age_valid
    passed = valid & (np.asarray(rate, dtype=np.float64) >= 1.0) & (np.asarray(age, dtype=np.float64) <= 2.0)
    return valid, passed


class HistogramState:
    def __init__(self, key: tuple, x_field: str, y_field: str, x_axis: Axis, y_axis: Axis):
        self.key = key
        self.x_field = x_field
        self.y_field = y_field
        self.x_axis = x_axis
        self.y_axis = y_axis
        self.counts = np.zeros((x_axis.total_bins, y_axis.total_bins), dtype=np.int64)
        self.selected = self.x_invalid_only = self.y_invalid_only = self.both_invalid = 0
        self.x_zero = self.y_zero = 0
        self.member_selected = self.member_pair_valid = self.member_plotted = 0

    def add(self, values: dict[str, np.ndarray], masks: dict[str, np.ndarray], subset: np.ndarray) -> None:
        selected = int(np.count_nonzero(subset))
        if not selected:
            return
        x_valid = masks[self.x_field] == 0
        y_valid = masks[self.y_field] == 0
        pair = subset & x_valid & y_valid
        self.selected += selected
        self.member_selected += selected
        self.x_invalid_only += int(np.count_nonzero(subset & ~x_valid & y_valid))
        self.y_invalid_only += int(np.count_nonzero(subset & x_valid & ~y_valid))
        self.both_invalid += int(np.count_nonzero(subset & ~x_valid & ~y_valid))
        if not pair.any():
            return
        x = values[self.x_field][pair]
        y = values[self.y_field][pair]
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
        self.member_pair_valid += pair_count
        self.member_plotted += plotted

    def flush_member(self, member: str) -> dict:
        row = {
            **self.labels(),
            "member": member,
            "selected": self.member_selected,
            "pair_valid": self.member_pair_valid,
            "plotted": self.member_plotted,
        }
        self.member_selected = self.member_pair_valid = self.member_plotted = 0
        return row

    def labels(self) -> dict:
        session, filter_scope, band, half_life, relationship = self.key
        return {
            "session": session,
            "filter_scope": filter_scope,
            "activity_band": band,
            "half_life_seconds": half_life,
            "relationship": relationship,
            "panel_id": f"{session}__{filter_scope}__{band}__rms_vs_{relationship}_hl{half_life}s",
        }

    def summary(self) -> dict:
        pair_valid = int(self.counts.sum())
        _require(
            self.selected == pair_valid + self.x_invalid_only + self.y_invalid_only + self.both_invalid,
            f"validity reconciliation failed for {self.key}",
        )
        plotted = int(self.counts[self.x_axis.finite_slice, self.y_axis.finite_slice].sum())
        return {
            **self.labels(),
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
        }

    def histogram_rows(self, pair_valid: int) -> list[dict]:
        result = []
        for x_index in range(self.x_axis.total_bins):
            x_category, x_display_bin, x_low, x_high = self.x_axis.category(x_index)
            for y_index in range(self.y_axis.total_bins):
                y_category, y_display_bin, y_low, y_high = self.y_axis.category(y_index)
                count = int(self.counts[x_index, y_index])
                result.append(
                    {
                        **self.labels(),
                        "x_category": x_category,
                        "x_display_bin": x_display_bin,
                        "x_low": x_low,
                        "x_high": x_high,
                        "y_category": y_category,
                        "y_display_bin": y_display_bin,
                        "y_low": y_low,
                        "y_high": y_high,
                        "count": count,
                        "percent_pair_valid": 100.0 * count / pair_valid if pair_valid else None,
                    }
                )
        return result


class ExpandedAccumulator:
    def __init__(self, contribution_path: Path):
        axes = {
            name: Axis(name, spec["unit"], spec["scale"], spec["edges"])
            for name, spec in DEFAULT_AXES.items()
        }
        self.states: dict[tuple, HistogramState] = {}
        for session in SESSION_SCOPES:
            for filter_scope in FILTER_SCOPES:
                bands = ("all",) + tuple(
                    name for name, _ in ACTIVITY_BANDS if filter_scope == "unconditional" or name in GATED_DISPLAY_BANDS
                )
                for band in bands:
                    relationships = ("spread", "participation", "trade_rate") if band == "all" else ("spread", "participation")
                    for half_life in HALF_LIVES:
                        for relationship in relationships:
                            key = (session, filter_scope, band, half_life, relationship)
                            self.states[key] = HistogramState(
                                key,
                                RELATIONSHIP_FIELDS[relationship][half_life],
                                RMS_FIELDS[half_life],
                                axes[relationship],
                                axes["rms"],
                            )
        self.current_member: str | None = None
        self.gate_global: dict[tuple, np.ndarray] = {}
        self.gate_member: dict[tuple, np.ndarray] = {}
        self._contribution_writer = pq.ParquetWriter(
            contribution_path,
            pa.schema(
                [
                    ("session", pa.string()),
                    ("filter_scope", pa.string()),
                    ("activity_band", pa.string()),
                    ("half_life_seconds", pa.int64()),
                    ("relationship", pa.string()),
                    ("panel_id", pa.string()),
                    ("member", pa.string()),
                    ("selected", pa.int64()),
                    ("pair_valid", pa.int64()),
                    ("plotted", pa.int64()),
                ]
            ),
            compression="zstd",
        )
        self.member_gate_rows: list[dict] = []

    def _flush_member(self) -> None:
        if self.current_member is None:
            return
        rows = [state.flush_member(self.current_member) for state in self.states.values()]
        self._contribution_writer.write_table(pa.Table.from_pylist(rows, schema=self._contribution_writer.schema))
        for (session, half_life), counts in sorted(self.gate_member.items()):
            self.member_gate_rows.append(
                {
                    "member": self.current_member,
                    "session": session,
                    "half_life_seconds": half_life,
                    "represented": int(counts[0]),
                    "gate_valid": int(counts[1]),
                    "gate_pass": int(counts[2]),
                    "gate_fail": int(counts[1] - counts[2]),
                    "gate_unavailable": int(counts[0] - counts[1]),
                }
            )
        self.gate_member.clear()

    @staticmethod
    def _column(batch: pa.RecordBatch, name: str) -> np.ndarray:
        return np.asarray(batch.column(batch.schema.get_field_index(name)).to_numpy(zero_copy_only=False))

    def add(self, batch: pa.RecordBatch) -> None:
        dates = batch.column(batch.schema.get_field_index("session_date")).to_pylist()
        symbols = batch.column(batch.schema.get_field_index("symbol")).to_pylist()
        _require(len(set(dates)) == 1 and len(set(symbols)) == 1, "batch crosses member")
        member = f"{dates[0]}/{symbols[0]}"
        if member != self.current_member:
            self._flush_member()
            self.current_member = member
        values = {field: self._column(batch, field).astype(np.float64, copy=False) for field in EXPANDED_FIELDS}
        masks = {
            field: self._column(batch, field + "_reason_mask").astype(np.uint16, copy=False)
            for field in EXPANDED_FIELDS
        }
        segment_ids = batch.column(batch.schema.get_field_index("selection_segment_id")).to_pylist()
        sessions = np.asarray([SESSION_BY_SEGMENT[int(value.rsplit(":", 1)[1])] for value in segment_ids], dtype=object)
        for half_life in HALF_LIVES:
            rate_field = RATE_FIELDS[half_life]
            age_field = AGE_FIELDS[half_life]
            gate_valid, gate_pass = activity_gate(values[rate_field], masks[rate_field], values[age_field], masks[age_field])
            bands = activity_band_ids(values[rate_field], masks[rate_field] == 0)
            for session in SESSION_SCOPES:
                session_mask = np.ones(batch.num_rows, dtype=bool) if session == "pooled" else sessions == session
                represented = int(np.count_nonzero(session_mask))
                if represented:
                    gate_counts = np.asarray(
                        [represented, np.count_nonzero(session_mask & gate_valid), np.count_nonzero(session_mask & gate_pass)],
                        dtype=np.int64,
                    )
                    self.gate_global.setdefault((session, half_life), np.zeros(3, dtype=np.int64))[:] += gate_counts
                    self.gate_member.setdefault((session, half_life), np.zeros(3, dtype=np.int64))[:] += gate_counts
                for filter_scope, filter_mask in (
                    ("unconditional", session_mask),
                    ("activity_gate", session_mask & gate_pass),
                ):
                    band_names = ["all"] + [
                        name
                        for index, (name, _label) in enumerate(ACTIVITY_BANDS)
                        if (filter_scope == "unconditional" or name in GATED_DISPLAY_BANDS)
                        and np.any(filter_mask & (bands == index))
                    ]
                    for band in band_names:
                        subset = filter_mask if band == "all" else filter_mask & (bands == next(i for i, row in enumerate(ACTIVITY_BANDS) if row[0] == band))
                        relationships = ("spread", "participation", "trade_rate") if band == "all" else ("spread", "participation")
                        for relationship in relationships:
                            self.states[(session, filter_scope, band, half_life, relationship)].add(values, masks, subset)

    def finish(self) -> tuple[list[dict], list[dict], list[dict]]:
        self._flush_member()
        self._contribution_writer.close()
        summaries = [state.summary() for state in self.states.values()]
        histogram_rows = [row for state, summary in zip(self.states.values(), summaries) for row in state.histogram_rows(summary["pair_valid"])]
        gate_rows = []
        for (session, half_life), counts in sorted(self.gate_global.items()):
            gate_rows.append(
                {
                    "session": session,
                    "half_life_seconds": half_life,
                    "represented": int(counts[0]),
                    "gate_valid": int(counts[1]),
                    "gate_pass": int(counts[2]),
                    "gate_fail": int(counts[1] - counts[2]),
                    "gate_unavailable": int(counts[0] - counts[1]),
                    "gate_pass_share_represented": counts[2] / counts[0] if counts[0] else None,
                    "gate_pass_share_valid": counts[2] / counts[1] if counts[1] else None,
                }
            )
        return summaries, histogram_rows, gate_rows


def _matrix(records: list[dict], panel_id: str, x_bins: int, y_bins: int) -> np.ndarray:
    result = np.zeros((x_bins, y_bins), dtype=float)
    for row in records:
        if row["panel_id"] == panel_id and row["x_category"] == "displayed" and row["y_category"] == "displayed":
            result[row["x_display_bin"], row["y_display_bin"]] = row["percent_pair_valid"]
    return result


def _plot_panel(ax, matrix, x_axis: Axis, y_axis: Axis, norm, relationship: str):
    display = np.ma.masked_less_equal(matrix.T, 0.0) if isinstance(norm, LogNorm) else matrix.T
    image = ax.pcolormesh(x_axis.edges, y_axis.edges, display, shading="auto", cmap="magma", norm=norm)
    if x_axis.scale == "log":
        ax.set_xscale("log")
    ax.set_yscale("log")
    ax.grid(alpha=0.10, which="both")
    if relationship == "spread":
        x = np.asarray(x_axis.edges)
        for ratio, style in ((0.1, ":"), (1.0, "-"), (10.0, ":")):
            y = ratio * x
            visible = (y >= y_axis.edges[0]) & (y <= y_axis.edges[-1])
            if visible.any():
                ax.plot(x[visible], y[visible], color="cyan", linewidth=0.9 if ratio == 1 else 0.6, linestyle=style)
    return image


def render_gated_six(records: list[dict], summaries: dict[str, dict], output: Path) -> None:
    axes_config = {name: Axis(name, spec["unit"], spec["scale"], spec["edges"]) for name, spec in DEFAULT_AXES.items()}
    relationships = ("spread", "participation", "trade_rate")
    titles = {"spread": "Mean full quoted spread", "participation": "Movement participation", "trade_rate": "Eligible trade rate"}
    fig, axes = plt.subplots(3, 2, figsize=(14, 16), sharey=True)
    fig.subplots_adjust(left=.09, right=.92, top=.92, bottom=.09, hspace=.30, wspace=.18)
    for row_index, relationship in enumerate(relationships):
        matrices = []
        panel_ids = []
        for half_life in HALF_LIVES:
            panel_id = f"pooled__activity_gate__all__rms_vs_{relationship}_hl{half_life}s"
            panel_ids.append(panel_id)
            matrices.append(_matrix(records, panel_id, axes_config[relationship].finite_bins, axes_config["rms"].finite_bins))
        positives = np.concatenate([matrix[matrix > 0] for matrix in matrices])
        norm = LogNorm(vmin=max(1e-4, float(positives.min(initial=1e-4))), vmax=float(positives.max(initial=1.0)))
        for column_index, (half_life, panel_id, matrix) in enumerate(zip(HALF_LIVES, panel_ids, matrices)):
            ax = axes[row_index, column_index]
            image = _plot_panel(ax, matrix, axes_config[relationship], axes_config["rms"], norm, relationship)
            summary = summaries[panel_id]
            ax.set_title(f"{titles[relationship]} · EW half-life {half_life}s")
            ax.set_xlabel({"spread": "Spread (bps)", "participation": "Participation", "trade_rate": "Trades / second"}[relationship])
            if column_index == 0:
                ax.set_ylabel("RMS five-second midpoint return (bps)")
            ax.text(.01, .99, f"pair-valid {summary['pair_valid']:,}\nplotted {summary['plotted']/summary['pair_valid']:.2%}", va="top", transform=ax.transAxes, fontsize=8, bbox={"facecolor":"white","alpha":.8,"edgecolor":"none"})
        colorbar = fig.colorbar(image, ax=list(axes[row_index, :]), pad=.01, fraction=.025)
        colorbar.set_label("% of pair-valid observations per bin (log color)")
    fig.suptitle("Activity-gated endpoint/EW joint distributions — 24-member pilot", fontsize=16)
    fig.text(.09, .03, "Gate: corresponding EW trade rate ≥1/s and trade-age p90 ≤2s; 30s EW pairs with window60s, 120s EW with window300s. Historical membership, pooled sessions.", fontsize=9)
    fig.savefig(output, dpi=170)
    plt.close(fig)


def render_activity_bands(records: list[dict], summaries: dict[str, dict], output: Path, relationship: str) -> None:
    axes_config = {name: Axis(name, spec["unit"], spec["scale"], spec["edges"]) for name, spec in DEFAULT_AXES.items()}
    band_labels = dict(ACTIVITY_BANDS)
    fig, axes = plt.subplots(2, len(GATED_DISPLAY_BANDS), figsize=(22, 9), sharex=True, sharey=True)
    fig.subplots_adjust(left=.07, right=.94, top=.88, bottom=.14, hspace=.22, wspace=.12)
    all_matrices = []
    identifiers = []
    for half_life in HALF_LIVES:
        for band in GATED_DISPLAY_BANDS:
            panel_id = f"pooled__activity_gate__{band}__rms_vs_{relationship}_hl{half_life}s"
            identifiers.append((half_life, band, panel_id))
            all_matrices.append(_matrix(records, panel_id, axes_config[relationship].finite_bins, axes_config["rms"].finite_bins))
    positives = np.concatenate([matrix[matrix > 0] for matrix in all_matrices if np.any(matrix > 0)])
    norm = LogNorm(vmin=max(1e-4, float(positives.min(initial=1e-4))), vmax=float(positives.max(initial=1.0)))
    for index, ((half_life, band, panel_id), matrix) in enumerate(zip(identifiers, all_matrices)):
        row, column = divmod(index, len(GATED_DISPLAY_BANDS))
        ax = axes[row, column]
        image = _plot_panel(ax, matrix, axes_config[relationship], axes_config["rms"], norm, relationship)
        summary = summaries[panel_id]
        ax.set_title(band_labels[band])
        if column == 0:
            ax.set_ylabel(f"{half_life}s EW RMS return (bps)")
        ax.set_xlabel("Spread (bps)" if relationship == "spread" else "Participation")
        if summary["pair_valid"]:
            ax.text(.01, .99, f"n={summary['pair_valid']:,}", va="top", transform=ax.transAxes, fontsize=8, bbox={"facecolor":"white","alpha":.8,"edgecolor":"none"})
    colorbar = fig.colorbar(image, ax=list(axes.ravel()), pad=.01, fraction=.018)
    colorbar.set_label("% of band pair-valid observations per bin (log color)")
    label = "spread" if relationship == "spread" else "participation"
    fig.suptitle(f"RMS return versus {label}, by activity band — activity-gated pilot", fontsize=16)
    fig.text(.07, .04, "Bands partition gate-passing observations; ≥100 trades/s is retained. Each panel uses its own pair-valid denominator. Cyan lines show RMS/spread ratios 0.1, 1, and 10 on spread panels.", fontsize=9)
    fig.savefig(output, dpi=170)
    plt.close(fig)


def run(args: argparse.Namespace) -> dict:
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    selection = EndpointSelection("historical_membership", sessions=SESSIONS)
    _write_json(output / "selection.json", selection.to_dict())
    _write_json(
        output / "expanded_config.json",
        {
            "schema": "endpoint_joint_expanded_config_v1",
            "gate": {
                "fast": {"half_life_seconds": 30, "trade_age_window_seconds": 60, "trade_rate_minimum_inclusive": 1.0, "trade_age_p90_maximum_inclusive": 2.0},
                "slow": {"half_life_seconds": 120, "trade_age_window_seconds": 300, "trade_rate_minimum_inclusive": 1.0, "trade_age_p90_maximum_inclusive": 2.0},
            },
            "activity_bands": [{"name": name, "label": label} for name, label in ACTIVITY_BANDS],
            "sessions": list(SESSION_SCOPES),
            "filters": list(FILTER_SCOPES),
            "axes": {name: {"unit": spec["unit"], "scale": spec["scale"], "edges": list(spec["edges"])} for name, spec in DEFAULT_AXES.items()},
        },
    )
    total_started = time.perf_counter()
    blocks_before = resource.getrusage(resource.RUSAGE_SELF).ru_inblock
    with RSSSampler(args.rss_stop_bytes) as rss:
        open_started = time.perf_counter()
        handle = open_endpoint_reference(
            args.reference,
            expected_identity=args.reference_identity,
            data_roots={"base": args.base_root, "features": args.features_root},
        )
        open_wall = time.perf_counter() - open_started
        registry = {row["name"]: row for row in describe_endpoint_fields(handle.config)}
        _require(set(EXPANDED_FIELDS) <= set(registry), "expanded fields absent from registry")
        accumulator = ExpandedAccumulator(output / "member_contributions.parquet")
        rows = batches = 0
        scan_started = time.perf_counter()
        for batch in iter_endpoint_batches(
            handle,
            fields=EXPANDED_FIELDS,
            selection=selection,
            include_support=False,
            include_run_boundaries=False,
            batch_size=4096,
        ):
            rows += batch.num_rows
            batches += 1
            accumulator.add(batch)
        scan_wall = time.perf_counter() - scan_started
        expected_rows = handle.manifest["members"]["rows_per_table"]
        expected_members = handle.manifest["members"]["count"]
        _require(rows == expected_rows, "reference row count mismatch")
        if args.expected_members is not None:
            _require(expected_members == args.expected_members, "reference member count mismatch")
        summaries, histogram_rows, gate_rows = accumulator.finish()
        pq.write_table(pa.Table.from_pylist(histogram_rows), output / "histogram_counts.parquet", compression="zstd")
        pq.write_table(pa.Table.from_pylist(accumulator.member_gate_rows), output / "member_gate_accounting.parquet", compression="zstd")
        _write_json(output / "coverage.json", summaries)
        _write_json(output / "gate_accounting.json", gate_rows)
        with (output / "gate_accounting.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(gate_rows[0]))
            writer.writeheader()
            writer.writerows(gate_rows)
        render_started = time.perf_counter()
        summary_map = {row["panel_id"]: row for row in summaries}
        render_gated_six(histogram_rows, summary_map, output / "activity_gated_six_panel.png")
        render_activity_bands(histogram_rows, summary_map, output / "rms_spread_by_activity_band.png", "spread")
        render_activity_bands(histogram_rows, summary_map, output / "rms_participation_by_activity_band.png", "participation")
        render_wall = time.perf_counter() - render_started
    blocks_after = resource.getrusage(resource.RUSAGE_SELF).ru_inblock
    artifacts = []
    for path in sorted(output.iterdir()):
        if path.name not in {"metadata.json", "artifacts.json"} and path.is_file():
            artifacts.append({"path": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path)})
    metadata = {
        "schema": "endpoint_joint_expanded_run_v1",
        "status": "expanded_activity_bounded_reference_not_full_universe",
        "reference_kind": handle.manifest["reference_kind"],
        "members": expected_members,
        "reference_identity": args.reference_identity,
        "represented_rows": rows,
        "projected_fields": list(EXPANDED_FIELDS),
        "projected_field_count": len(EXPANDED_FIELDS),
        "batch_size": 4096,
        "batches": batches,
        "worker_count": 1,
        "library_threads": 1,
        "timings_seconds": {
            "file_verification": handle.validation_seconds,
            "open_call_wall": open_wall,
            "scan_and_expanded_aggregation": scan_wall,
            "rendering_from_saved_tables": render_wall,
            "total": time.perf_counter() - total_started,
        },
        "resources": {
            "peak_sampled_process_tree_rss_bytes": rss.peak_bytes,
            "rss_stop_bytes": args.rss_stop_bytes,
            "filesystem_input_bytes_from_ru_inblock": (blocks_after - blocks_before) * 512,
            "cache_state": args.cache_state,
            "spill_bytes": 0,
            "output_bytes": None,
        },
        "verification_bytes": handle.validation_bytes,
    }
    _write_json(output / "artifacts.json", artifacts)
    _write_json(output / "metadata.json", metadata)
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
    result.add_argument("--expected-members", type=int)
    result.add_argument("--cache-state", default="uncontrolled shared OS cache; not labeled cold")
    result.add_argument("--rss-stop-bytes", type=int, default=2 * 1024**3)
    return result


def main() -> None:
    args = parser().parse_args()
    print(json.dumps(run(args), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
