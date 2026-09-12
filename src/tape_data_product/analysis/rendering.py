"""Offline, reusable rendering of the six tape-data-product report visuals.

Input is a verified ``report_plot_aggregate_v1`` directory: a hash-binding
manifest plus figure-ready JSON. This module never selects rows, computes
features, changes denominators, or accesses the network. Runtime is O(V + J),
where V is retained ECDF vertices and J is fixed joint-histogram cells. Memory is
O(max page pixels + V + J), independent of raw observation count; figures are
rendered and closed one page at a time.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import re
from datetime import date, timedelta
from pathlib import Path
import resource
import socket
import sys
import time

import numpy as np

from . import export_verify as Verify
from . import selection as Selection

SCHEMA = "report_plot_aggregate_v1"
RENDER_SCHEMA = "report_plot_render_v1"
SESSIONS = ("pooled", "premarket", "rth", "after_hours")
FEATURES = (
    "movement_mean_5s_bps",
    "quoted_spread_mean_bps",
    "movement_mean_to_spread",
    "trade_rate",
    "dollar_rate",
    "movement_participation",
    "trade_age_p90_seconds",
    "quote_age_p90_seconds",
    "midpoint_change_age_p90_seconds",
)
TITLES = (
    "Mean five-second movement",
    "Mean quoted spread",
    "Movement / spread",
    "Trade rate*",
    "Dollar rate",
    "Movement participation",
    "Trade-age p90*",
    "Quote-age p90",
    "Midpoint-change-age p90",
)
COLORS = {
    "pooled": "#4b5563",
    "premarket": "#0072b2",
    "rth": "#d55e00",
    "after_hours": "#cc79a7",
}
LINESTYLES = {
    "pooled": "-.",
    "premarket": (0, (6, 3)),
    "rth": "-",
    "after_hours": (0, (1, 2)),
}
LINEWIDTHS = {"pooled": 1.5, "premarket": 1.9, "rth": 1.8, "after_hours": 1.8}
LINE_ZORDERS = {"pooled": 1, "after_hours": 2, "rth": 3, "premarket": 4}
ECDF_XMAX = {
    "movement_mean_5s_bps": 1_000,
    "quoted_spread_mean_bps": 1_000,
    "movement_mean_to_spread": 10,
    "trade_rate": 500,
    "dollar_rate": 5_000_000,
    "movement_participation": 1,
    "trade_age_p90_seconds": 2_000,
    "quote_age_p90_seconds": 120_000,
    "midpoint_change_age_p90_seconds": 1_800_000,
}
SVG_DECIMALS = 6


class ArtifactError(ValueError):
    pass


def default_figure_config(population_share: dict, *, scope: str) -> dict:
    """Return the reviewed report layout as an explicit, identity-bound config."""
    config = {
        "schema": "report_plot_config_v1",
        "scope": scope,
        "population_share": population_share,
        "display_merge_factor": 4,
        "activity_band_display_labels": [
            "1–<10 trades/s",
            "10–<30 trades/s",
            "30–<100 trades/s",
            "≥100 trades/s",
        ],
        "ecdf_disclosure": "Exact ECDF tables; numerical reduction and vector rounding verified before completion. Supported zeros and off-axis mass remain in denominators.",
        "joint_layouts": {
            "movement_spread_participation": {
                "title": "Movement, quoted friction, and concentration",
                "title_x": 0.09,
                "layout": {
                    "left": 0.105,
                    "right": 0.875,
                    "top": 0.825,
                    "bottom": 0.15,
                    "hspace": 0.48,
                    "wspace": 0.27,
                },
                "colorbar_axes": [0.91, 0.24, 0.014, 0.52],
                "footers": [
                    {
                        "x": 0.5,
                        "y": 0.025,
                        "ha": "center",
                        "text": "Each panel uses its own eligible denominator; zeros and saved crop/tail mass remain in normalization.",
                    }
                ],
            },
            "activity_movement_spread": {
                "title": "Activity, movement, and quoted friction",
                "title_x": 0.075,
                "layout": {
                    "left": 0.075,
                    "right": 0.93,
                    "top": 0.78,
                    "bottom": 0.19,
                    "hspace": 0.53,
                    "wspace": 0.25,
                },
                "colorbar_axes": [0.95, 0.29, 0.009, 0.46],
                "footers": [
                    {
                        "x": 0.075,
                        "y": 0.035,
                        "text": "Band shares use the gate-passing movement + spread + trade-rate denominator; ≥100 trades/s remains in coverage.",
                    }
                ],
                "group_headers": [
                    {"x": 0.165, "text": "Movement vs. trade rate"},
                    {"x": 0.63, "text": "Movement vs. spread"},
                ],
            },
            "activity_movement_participation": {
                "title": "Movement and participation, by activity",
                "title_x": 0.09,
                "layout": {
                    "left": 0.09,
                    "right": 0.91,
                    "top": 0.79,
                    "bottom": 0.19,
                    "hspace": 0.53,
                    "wspace": 0.24,
                },
                "colorbar_axes": [0.94, 0.29, 0.012, 0.46],
                "footers": [
                    {
                        "x": 0.09,
                        "y": 0.035,
                        "text": "Participation excludes all-zero movement windows; ≥100 trades/s remains in coverage.",
                    }
                ],
            },
        },
    }
    config["config_identity"] = Verify.canonical_identity(config)
    return config


def _deny_network(*_args, **_kwargs):
    raise RuntimeError("report rendering cannot use network")


@contextlib.contextmanager
def _network_disabled():
    original_create, original_connect = socket.create_connection, socket.socket.connect
    socket.create_connection = _deny_network
    socket.socket.connect = _deny_network
    try:
        yield
    finally:
        socket.create_connection, socket.socket.connect = (
            original_create,
            original_connect,
        )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ArtifactError(message)


def _finite(values, name: str) -> np.ndarray:
    array = np.asarray(values, dtype=np.float64)
    _require(
        array.ndim == 1 and np.all(np.isfinite(array)),
        f"{name} must be a finite vector",
    )
    return array


def ecdf_reduction_error_pp(curve: dict) -> float:
    """Exact supremum error at all left/right atom limits, in percentage points."""
    n = int(curve["denominator"])
    if not n:
        return 0.0
    if "exact_reference" not in curve:
        value = float(curve["verified_reduction_error_pp"])
        _require(
            value >= 0 and curve.get("exact_reference_sha256"),
            "missing exact ECDF verification identity",
        )
        return value
    reduced_x = _finite(curve["atom_values"], "reduced atom values")
    reduced_c = np.asarray(curve["cumulative_counts"], dtype=np.int64)
    exact_x = _finite(curve["exact_reference"]["atom_values"], "exact atom values")
    exact_c = np.asarray(curve["exact_reference"]["cumulative_counts"], dtype=np.int64)
    _require(
        len(exact_x) == len(exact_c) and len(exact_x), "invalid exact ECDF reference"
    )
    _require(
        np.all(np.diff(exact_x) > 0)
        and np.all(np.diff(exact_c) > 0)
        and exact_c[-1] == n,
        "exact ECDF reference does not reconcile to denominator",
    )
    right_index = np.searchsorted(reduced_x, exact_x, side="right") - 1
    reduced_right = np.where(right_index >= 0, reduced_c[np.maximum(right_index, 0)], 0)
    exact_left = np.r_[0, exact_c[:-1]]
    left_index = np.searchsorted(reduced_x, exact_x, side="left") - 1
    reduced_left = np.where(left_index >= 0, reduced_c[np.maximum(left_index, 0)], 0)
    return float(
        max(
            np.max(np.abs(exact_c - reduced_right)),
            np.max(np.abs(exact_left - reduced_left)),
        )
        * 100
        / n
    )


def _load_fixture_aggregate(directory: Path) -> tuple[dict, dict]:
    manifest_path = directory / "aggregate_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    _require(manifest.get("schema") == SCHEMA, "unsupported aggregate schema")
    _require(manifest.get("state") == "verified", "aggregate is not verified")
    # The aggregation layer owns gate/selection/denominator semantics.  Requiring
    # its immutable binding prevents the renderer from accepting look-alike
    # counts produced under a different policy.
    Selection.validate_artifact_binding(manifest["binding"])
    files = manifest.get("files", {})
    _require(
        files == {"figure_data.json": Verify.sha256(directory / "figure_data.json")},
        "aggregate file hash mismatch",
    )
    data = json.loads((directory / "figure_data.json").read_text())
    expected_identity = Verify.canonical_identity(
        {
            "binding_identity": manifest["binding"]["artifact_identity"],
            "files": files,
        }
    )
    _require(
        expected_identity == manifest.get("aggregate_identity"),
        "aggregate identity mismatch",
    )
    validate_aggregate(data)
    return manifest, data


def _read_membership(path: Path) -> list[dict]:
    # Explicit bounded metadata: numerical endpoints never enter this list.
    members = []
    with path.open() as stream:
        for line in stream:
            if len(line) > 1024 * 1024:
                raise ArtifactError("membership record exceeds 1 MiB")
            if line.strip():
                if len(members) >= 10000:
                    raise ArtifactError(
                        "render membership cap is 10,000; split report scope"
                    )
                item = json.loads(line)
                members.append({"date": item["date"], "symbol": item["symbol"]})
    _require(
        len({(m["date"], m["symbol"]) for m in members}) == len(members),
        "duplicate aggregate member",
    )
    return members


def _reduced_exact_curve(
    path: Path, session: str, denominator: int, epsilon: float = 0.0005
) -> dict:
    """Stream one exact table into bounded retained atoms, then verify every exact limit."""
    import pyarrow.parquet as pq

    if denominator == 0:
        return dict(
            session=session,
            denominator=0,
            atom_values=[],
            cumulative_counts=[],
            verified_reduction_error_pp=0,
            exact_reference_sha256=Verify.sha256(path),
            reduction_error_bound_pp=0,
            claimed_final_error_bound_pp=0.05,
        )
    points, last_kept, previous = [], 0.0, None
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=4096, columns=["value", session + "_count", session + "_cumulative"]
    ):
        values = batch["value"].to_numpy()
        counts = batch[session + "_count"].to_numpy()
        cumulative = batch[session + "_cumulative"].to_numpy()
        for value, count, total in zip(values, counts, cumulative):
            if not count:
                continue
            current = (float(value), int(total))
            if not points:
                points.append(current)
                last_kept = total / denominator
            elif total / denominator - last_kept >= epsilon:
                if previous is not None and previous != points[-1]:
                    points.append(previous)
                points.append(current)
                last_kept = total / denominator
            previous = current
    if previous is not None and points[-1] != previous:
        points.append(previous)
    reduced_x = np.asarray([p[0] for p in points])
    reduced_c = np.asarray([p[1] for p in points], dtype=np.int64)
    _require(
        len(reduced_x) and reduced_c[-1] == denominator,
        "reduced ECDF lost terminal atom",
    )
    maximum_count_error = 0
    exact_previous = 0
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=4096, columns=["value", session + "_cumulative"]
    ):
        for value, total in zip(
            batch["value"].to_numpy(), batch[session + "_cumulative"].to_numpy()
        ):
            right = np.searchsorted(reduced_x, value, side="right") - 1
            left = np.searchsorted(reduced_x, value, side="left") - 1
            reduced_right = int(reduced_c[right]) if right >= 0 else 0
            reduced_left = int(reduced_c[left]) if left >= 0 else 0
            maximum_count_error = max(
                maximum_count_error,
                abs(int(total) - reduced_right),
                abs(exact_previous - reduced_left),
            )
            exact_previous = int(total)
    measured = maximum_count_error * 100 / denominator
    _require(measured <= 0.05 + 1e-12, "ECDF reduction exceeds 0.05 percentage points")
    return dict(
        session=session,
        denominator=denominator,
        atom_values=reduced_x.tolist(),
        cumulative_counts=reduced_c.tolist(),
        verified_reduction_error_pp=measured,
        exact_reference_sha256=Verify.sha256(path),
        reduction_error_bound_pp=measured,
        claimed_final_error_bound_pp=0.05 if measured <= 0.04999 else None,
    )


def _display_joint(
    full: np.ndarray, x_edges, y_edges, factor: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Apply the documented zero fold/display merge; tails remain outside the returned grid."""
    ix = np.r_[0, np.arange(2, len(x_edges) + 1)]
    iy = np.r_[0, np.arange(2, len(y_edges) + 1)]
    visible = full[np.ix_(ix, iy)]
    core = visible[1:, 1:].copy()
    core[0, :] += visible[0, 1:]
    core[:, 0] += visible[1:, 0]
    core[0, 0] += visible[0, 0]
    sx, sy = np.arange(0, core.shape[0], factor), np.arange(0, core.shape[1], factor)
    merged = np.add.reduceat(np.add.reduceat(core, sx, axis=0), sy, axis=1)
    ex = np.asarray(x_edges)[np.r_[sx, len(x_edges) - 1]].copy()
    ey = np.asarray(y_edges)[np.r_[sy, len(y_edges) - 1]].copy()
    ex[0] = 0
    ey[0] = 0
    return merged, ex, ey


def _adapt_numerical_bundle(directory: Path, config_path: Path) -> tuple[dict, dict]:
    from . import aggregates as Aggregates

    manifest = json.loads((directory / "manifest.json").read_text())
    Aggregates.validate_bundle(directory, manifest["binding"])
    config = json.loads(config_path.read_text())
    _require(
        config.get("schema") == "report_plot_config_v1",
        "unsupported report figure config",
    )
    candidate = dict(config)
    saved_config_identity = candidate.pop("config_identity", None)
    _require(
        saved_config_identity == Verify.canonical_identity(candidate),
        "report figure config identity mismatch",
    )
    members = _read_membership(directory / "membership.jsonl")
    dates = sorted({m["date"] for m in members})
    symbols = {m["symbol"] for m in members}
    gate = json.loads((directory / "coverage/gate.json").read_text())
    populations = json.loads((directory / "coverage/populations.json").read_text())
    concentration = json.loads((directory / "coverage/concentration.json").read_text())
    metadata = dict(
        dates={"start": dates[0], "end": dates[-1], "count": len(dates)},
        members={"symbol_days": len(members), "symbols": len(symbols)},
        scope=config["scope"],
        gate={
            str(h): {
                "display": f"Trade rate ≥{manifest['binding']['gate_policy']['rate_minimum_inclusive']:g}/s AND trade-age p90 ≤{manifest['binding']['gate_policy']['trade_age_maximum_seconds_inclusive']:g}s · same-horizon eligible measurements"
            }
            for h in Selection.HORIZONS
        },
        coverage={
            str(h): {
                s: {
                    "post_discovery": gate[f"{h}s"][s]["post_discovery"],
                    "passing": gate[f"{h}s"][s]["passing"],
                    "eligible_inputs": gate[f"{h}s"][s]["gate_eligible"],
                    "unavailable": gate[f"{h}s"][s]["unavailable"],
                }
                for s in Selection.SESSIONS
            }
            for h in Selection.HORIZONS
        },
        ecdf_disclosure=config["ecdf_disclosure"],
    )
    unit_map = {
        "seconds": ("milliseconds", 1000, "Time since event"),
        "bps": ("bps", 1, "Basis points"),
        "trades/second": ("trades/s", 1, "Trades / second"),
        "USD/second": ("USD/s", 1, "Reported dollars / second"),
        "dimensionless": ("ratio", 1, "Ratio"),
    }
    ecdf = {}
    for h in Selection.HORIZONS:
        feature_map = {}
        for stem in FEATURES:
            feature = f"{stem}_{h}s"
            exact = manifest["exact_tables"][feature]
            path = directory / exact["path"]
            display_unit, scale, label = unit_map[exact["source_unit"]]
            if stem == "movement_participation":
                label = "Participation (0–1)"
            saved_curves = (
                json.loads(path.read_text())
                if manifest["schema"] == "tape_report_numerical_artifacts_v2"
                else None
            )
            curves = (
                saved_curves["curves"]
                if saved_curves is not None
                else {
                    s: _reduced_exact_curve(path, s, exact["totals"][s])
                    for s in SESSIONS
                }
            )
            panel = dict(
                feature=stem,
                horizon_seconds=h,
                source_unit=exact["source_unit"],
                display_unit=display_unit,
                display_scale=scale,
                display_label=label,
                curves=curves,
            )
            lower = (
                1
                if stem in ("movement_mean_5s_bps", "quoted_spread_mean_bps")
                else 1000 if stem == "dollar_rate" else None
            )
            if lower is not None and saved_curves is not None:
                panel["crop"] = saved_curves["crop"]
            elif lower is not None:
                import pyarrow.parquet as pq

                anchors = {s: 0 for s in SESSIONS}
                for batch in pq.ParquetFile(path).iter_batches(
                    batch_size=4096,
                    columns=["value", *[s + "_cumulative" for s in SESSIONS]],
                ):
                    values = batch["value"].to_numpy()
                    k = np.searchsorted(values, lower, side="right") - 1
                    if k >= 0:
                        anchors.update(
                            {
                                s: int(batch[s + "_cumulative"][k].as_py())
                                for s in SESSIONS
                            }
                        )
                    if len(values) and values[-1] > lower:
                        break
                panel["crop"] = {
                    "lower": lower,
                    "at_or_below_count_by_session": anchors,
                }
            feature_map[stem] = panel
        ecdf[str(h)] = feature_map
    factor = int(config.get("display_merge_factor", 4))
    _require(factor in (1, 2, 4), "invalid display merge factor")

    def panel(h, name, title, *, band_index=None, transpose=False):
        note = json.loads((directory / f"joints/{h}s/{name}.json").read_text())
        with np.load(directory / f"joints/{h}s/{name}.npz") as saved:
            raw = saved["counts"]
            saved_x_edges = saved["x_edges"].copy()
            saved_y_edges = saved["y_edges"].copy()
        full = (
            raw[:, band_index].sum(axis=0)
            if band_index is not None
            else raw.sum(axis=0)
        )
        coverage = (
            note["coverage"][Selection.RATE_BAND_LABELS[band_index]]["pooled"]
            if band_index is not None
            else note["coverage"]["pooled"]
        )
        x_feature, y_feature = note["x_feature"].removesuffix(f"_{h}s"), note[
            "y_feature"
        ].removesuffix(f"_{h}s")
        x_edges, y_edges = saved_x_edges, saved_y_edges
        if transpose:
            full = full.T
            x_feature, y_feature = y_feature, x_feature
            x_edges, y_edges = y_edges, x_edges
        counts, ex, ey = _display_joint(full, x_edges, y_edges, factor)
        n = int(coverage["eligible"])
        key = (
            f"pair:{name}"
            + (
                ":" + Selection.RATE_BAND_LABELS[band_index]
                if band_index is not None
                else ""
            )
            + f"@{h}s"
        )
        contributor = concentration[key]["pooled"]["symbol_day"]["contributors"]
        p = dict(
            horizon_seconds=h,
            title=title,
            x_feature=x_feature,
            y_feature=y_feature,
            x_label={
                "quoted_spread_mean_bps": "Mean quoted spread (bps)",
                "movement_participation": "Movement participation (0–1)",
                "trade_rate": "Trade rate (trades/s)",
            }[x_feature],
            y_label="Mean five-second movement (bps)",
            x_edges=ex.tolist(),
            y_edges=ey.tolist(),
            counts=counts.tolist(),
            axis_order=["x", "y"],
            eligible_denominator=n,
            off_axis_count=n - int(counts.sum()),
            contributors={"symbol_days": contributor},
            coverage={
                "denominator": gate[f"{h}s"]["pooled"]["post_discovery"],
                "denominator_label": "post-discovery time",
            },
            first_bin={
                "treatment": (
                    "small_positives_only"
                    if x_feature == "movement_participation"
                    else "zeros_and_small_positives_combined"
                ),
                "all_zero_movement_excluded": x_feature == "movement_participation",
                "saved_crop_tail_count": n - int(counts.sum()),
            },
            ms_guides=[0.1, 1, 10] if x_feature == "quoted_spread_mean_bps" else [],
        )
        if band_index is not None:
            baseline = note["pair_baseline"]["pooled"]["eligible"]
            p["band"] = {
                "label": Selection.RATE_BAND_LABELS[band_index],
                "display_label": config["activity_band_display_labels"][band_index],
                "numerator": n,
                "denominator": baseline,
                "denominator_label": "gated pair-eligible baseline",
            }
        return p

    page1 = [
        p
        for h in Selection.HORIZONS
        for p in (
            panel(h, "movement_spread", "Movement versus spread"),
            panel(
                h,
                "movement_participation",
                "Movement versus participation",
                transpose=True,
            ),
        )
    ]
    page2 = []
    page3 = []
    for h in Selection.HORIZONS:
        page2.append(panel(h, "movement_trade_rate", "All eligible activity"))
        page2.extend(
            panel(h, "movement_spread_rate_bands", "", band_index=i) for i in range(3)
        )
        page3.extend(
            panel(
                h, "movement_participation_rate_bands", "", band_index=i, transpose=True
            )
            for i in range(3)
        )
    layouts = config["joint_layouts"]
    joints = {
        "movement_spread_participation": dict(
            **layouts["movement_spread_participation"], panels=page1
        ),
        "activity_movement_spread": dict(
            **layouts["activity_movement_spread"], panels=page2
        ),
        "activity_movement_participation": dict(
            **layouts["activity_movement_participation"], panels=page3
        ),
    }
    data = dict(
        metadata=metadata,
        population_share=config["population_share"],
        ecdf=ecdf,
        joints=joints,
        figure_config=config,
    )
    validate_aggregate(data)
    return {
        "aggregate_identity": manifest["artifact_identity"],
        "binding": manifest["binding"],
    }, data


def load_aggregate(
    directory: Path, config_path: Path | None = None
) -> tuple[dict, dict]:
    if (directory / "manifest.json").is_file():
        _require(config_path is not None, "a report_plot_config_v1 file is required")
        return _adapt_numerical_bundle(directory, config_path)
    return _load_fixture_aggregate(directory)


def validate_aggregate(data: dict) -> None:
    """Validate rendering invariants without redefining selection or denominators."""
    metadata = data["metadata"]
    dates = metadata["dates"]
    _require(
        date.fromisoformat(dates["start"]) <= date.fromisoformat(dates["end"]),
        "invalid date range",
    )
    _require(
        dates["count"] >= 0 and metadata["members"]["symbol_days"] >= 0,
        "negative membership count",
    )
    _require(
        sorted(map(int, data["ecdf"])) == [60, 300], "ECDF horizons must be 60 and 300"
    )
    for horizon_text, feature_map in data["ecdf"].items():
        horizon = int(horizon_text)
        _require(
            set(feature_map) == set(FEATURES) and len(feature_map) == len(FEATURES),
            f"wrong feature membership at {horizon}s",
        )
        for feature, panel in feature_map.items():
            _require(panel["horizon_seconds"] == horizon, "ECDF horizon mismatch")
            _require(panel["feature"] == feature, "ECDF feature mismatch")
            _require(
                panel["source_unit"]
                in (
                    "bps",
                    "ratio",
                    "trades/s",
                    "trades/second",
                    "USD/s",
                    "USD/second",
                    "seconds",
                    "dimensionless",
                ),
                "unknown source unit",
            )
            scale = float(panel["display_scale"])
            _require(math.isfinite(scale) and scale > 0, "invalid display scale")
            for session in SESSIONS:
                curve = panel["curves"][session]
                n = int(curve["denominator"])
                values = _finite(curve["atom_values"], "atom values")
                cumulative = np.asarray(curve["cumulative_counts"], dtype=np.int64)
                _require(
                    len(values) == len(cumulative), "ECDF value/count length mismatch"
                )
                _require(n >= 0 and np.all(cumulative >= 0), "negative ECDF count")
                if n == 0:
                    _require(len(values) == 0, "zero denominator must have no ECDF")
                    continue
                _require(
                    len(values) > 0 and np.all(np.diff(values) > 0),
                    "ECDF atoms must be strictly increasing",
                )
                _require(
                    np.all(np.diff(cumulative) > 0) and cumulative[-1] == n,
                    "invalid ECDF cumulative counts",
                )
                measured = ecdf_reduction_error_pp(curve)
                _require(
                    measured <= float(curve["reduction_error_bound_pp"]) + 1e-12,
                    "reduced ECDF exceeds its numerical error bound",
                )
                lower = panel.get("crop", {}).get("lower")
                if lower is not None:
                    saved = int(panel["crop"]["at_or_below_count_by_session"][session])
                    _require(
                        0 <= saved <= n, "cropped ECDF anchor count outside denominator"
                    )
                    if "exact_reference" in curve:
                        exact_x = np.asarray(curve["exact_reference"]["atom_values"])
                        exact_c = np.asarray(
                            curve["exact_reference"]["cumulative_counts"]
                        )
                        expected = (
                            int(
                                exact_c[
                                    np.searchsorted(exact_x, lower, side="right") - 1
                                ]
                            )
                            if np.any(exact_x <= lower)
                            else 0
                        )
                        _require(
                            expected == saved, "cropped ECDF anchor count mismatch"
                        )
            pooled = panel["curves"]["pooled"]["denominator"]
            _require(
                pooled == sum(panel["curves"][s]["denominator"] for s in SESSIONS[1:]),
                "pooled denominator mismatch",
            )
    for page_name, page in data["joints"].items():
        _require(
            page_name
            in (
                "movement_spread_participation",
                "activity_movement_spread",
                "activity_movement_participation",
            ),
            "unknown joint page",
        )
        for panel in page["panels"]:
            x_edges = _finite(panel["x_edges"], "x edges")
            y_edges = _finite(panel["y_edges"], "y edges")
            counts = np.asarray(panel["counts"], dtype=np.int64)
            _require(
                panel["axis_order"] == ["x", "y"],
                "joint counts must have explicit x,y orientation",
            )
            _require(
                np.all(np.diff(x_edges) > 0) and np.all(np.diff(y_edges) > 0),
                "joint edges must increase",
            )
            _require(
                counts.shape == (len(x_edges) - 1, len(y_edges) - 1),
                "joint shape/orientation mismatch",
            )
            _require(np.all(counts >= 0), "negative joint count")
            n = int(panel["eligible_denominator"])
            _require(
                n >= 0 and int(counts.sum()) + int(panel["off_axis_count"]) == n,
                "joint denominator/tail mismatch",
            )
            band = panel.get("band")
            if band:
                _require(
                    0 <= band["numerator"] <= band["denominator"],
                    "invalid activity-band denominator",
                )
            zero = panel["first_bin"]
            _require(
                zero["treatment"]
                in ("zeros_and_small_positives_combined", "small_positives_only"),
                "missing first-bin disclosure",
            )
            if "participation" in panel["x_feature"]:
                _require(
                    zero["treatment"] == "small_positives_only"
                    and zero["all_zero_movement_excluded"],
                    "participation must exclude all-zero movement",
                )


def _metadata_line(metadata: dict) -> str:
    d, m = metadata["dates"], metadata["members"]
    return f"{d['start']} to {d['end']} · {m['symbol_days']:,} symbol-days · {m['symbols']:,} symbols · {d['count']:,} dates"


def _style():
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "svg.fonttype": "none",
            "svg.hashsalt": "report-plot-render-v1",
            "path.simplify": False,
            "axes.spines.top": False,
            "axes.spines.right": False,
        }
    )


def _axis_format(ax, feature: str) -> None:
    from matplotlib.ticker import FuncFormatter, NullLocator

    if feature in ("movement_mean_5s_bps", "quoted_spread_mean_bps"):
        ax.set_xscale("log")
        ax.set_xlim(left=1)
        ax.xaxis.set_major_formatter(
            FuncFormatter(lambda x, _: f"{x/1000:g}k" if x >= 1000 else f"{x:g}")
        )
    elif feature == "movement_participation":
        ax.set_xlim(0, 1)
    elif feature == "dollar_rate":
        ax.set_xscale("log")
        ax.set_xlim(left=1000)
        ax.xaxis.set_major_formatter(
            FuncFormatter(lambda x, _: f"${x/1e6:g}m" if x >= 1e6 else f"${x/1000:g}k")
        )
    elif feature == "trade_rate":
        ax.set_xscale("log")
        ax.set_xlim(left=1)
    else:
        ax.set_xscale("symlog", linthresh=100 if "age" in feature else 0.1)
        ax.set_xlim(left=0)
    if feature == "trade_age_p90_seconds":
        ax.set_xlim(0, 2000)
    if "age_p90" in feature:
        upper = ax.get_xlim()[1]
        candidates = (
            (0, 100, 500, 1000, 2000)
            if feature == "trade_age_p90_seconds"
            else (0, 100, 1000, 10000, 60000, 600000)
        )
        ticks = [v for v in candidates if v <= upper]
        ax.set_xticks(ticks)
        ax.xaxis.set_minor_locator(NullLocator())
        ax.xaxis.set_major_formatter(
            FuncFormatter(
                lambda x, _: (
                    "0"
                    if x == 0
                    else (
                        f"{x:g} ms"
                        if x < 1000
                        else f"{x/1000:g} s" if x < 60000 else f"{x/60000:g} min"
                    )
                )
            )
        )


def _step_vertices(panel: dict, curve: dict) -> tuple[np.ndarray, np.ndarray]:
    values = np.asarray(curve["atom_values"], dtype=float) * float(
        panel["display_scale"]
    )
    cumulative = np.asarray(curve["cumulative_counts"], dtype=float)
    n = int(curve["denominator"])
    if not n:
        return np.empty(0), np.empty(0)
    y = 100 * cumulative / n
    x_vertices = np.repeat(values, 2)
    y_vertices = np.empty(2 * len(values))
    y_vertices[0::2] = np.r_[0.0, y[:-1]]
    y_vertices[1::2] = y
    crop = panel.get("crop", {})
    lower = crop.get("lower")
    if lower is not None:
        lower *= float(panel["display_scale"])
        keep = x_vertices >= lower
        x_vertices, y_vertices = x_vertices[keep], y_vertices[keep]
        anchor = 100 * int(crop["at_or_below_count_by_session"][curve["session"]]) / n
        later = x_vertices > lower
        x_vertices = np.r_[lower, x_vertices[later]]
        y_vertices = np.r_[anchor, y_vertices[later]]
    return x_vertices, y_vertices


def _right_tail_upper_bound_pp(
    panel: dict, curve: dict, displayed_upper: float
) -> float:
    """Conservative tail bound from an exact-at-vertex reduced ECDF."""
    n = int(curve["denominator"])
    if not n:
        return 0.0
    values = np.asarray(curve["atom_values"], dtype=float) * float(
        panel["display_scale"]
    )
    cumulative = np.asarray(curve["cumulative_counts"], dtype=np.int64)
    index = int(np.searchsorted(values, displayed_upper, side="right") - 1)
    known_at_or_below = int(cumulative[index]) if index >= 0 else 0
    return 100.0 * (n - known_at_or_below) / n


def _clip_polyline(points: np.ndarray, bounds) -> list[tuple[float, float]]:
    """Clip a polyline to explicit ``(xmin, ymin, xmax, ymax)`` bounds."""
    xmin, ymin, xmax, ymax = bounds
    result = []
    for first, second in zip(points[:-1], points[1:]):
        x0, y0 = map(float, first)
        x1, y1 = map(float, second)
        dx, dy = x1 - x0, y1 - y0
        lo, hi = 0.0, 1.0
        for p, q in (
            (-dx, x0 - xmin),
            (dx, xmax - x0),
            (-dy, y0 - ymin),
            (dy, ymax - y0),
        ):
            if p == 0:
                if q < 0:
                    lo, hi = 1.0, 0.0
            else:
                t = q / p
                if p < 0:
                    lo = max(lo, t)
                else:
                    hi = min(hi, t)
        if lo <= hi:
            clipped = [(x0 + lo * dx, y0 + lo * dy), (x0 + hi * dx, y0 + hi * dy)]
            if not result or not np.allclose(result[-1], clipped[0], rtol=0, atol=1e-9):
                result.append(clipped[0])
            if not result or not np.allclose(result[-1], clipped[1], rtol=0, atol=1e-9):
                result.append(clipped[1])
    if (
        len(points) == 1
        and xmin <= points[0, 0] <= xmax
        and ymin <= points[0, 1] <= ymax
    ):
        result.append(tuple(map(float, points[0])))
    return result


def _save_page(
    fig,
    output: Path,
    stem: str,
    geometry_curves: list[dict],
    render_config_identity: str,
    dpi: int,
) -> dict:
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    output.mkdir(parents=True, exist_ok=True)
    fig.canvas.draw()
    height_pt = fig.get_size_inches()[1] * 72
    display_to_svg = 72.0 / fig.dpi
    for curve in geometry_curves:
        artist = curve.pop("artist")
        vertices = artist.get_path().vertices
        display = artist.get_transform().transform(vertices)
        curve["svg_points"] = [
            [
                round(float(x * display_to_svg), SVG_DECIMALS + 3),
                round(float(height_pt - y * display_to_svg), SVG_DECIMALS + 3),
            ]
            for x, y in display
        ]
        curve["axes_height_pt"] = float(
            artist.axes.get_window_extent().height * display_to_svg
        )
        curve["y_span_percentage_points"] = 101.0
    sidecar = output / f"{stem}.geometry.json"
    sidecar.write_text(
        json.dumps(
            {
                "schema": Verify.SCHEMA,
                "render_config_identity": render_config_identity,
                "svg_coordinate_decimals": SVG_DECIMALS,
                "curves": geometry_curves,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    with mpl.rc_context({"path.simplify": False}):
        fig.savefig(output / f"{stem}.svg", facecolor="white")
        fig.savefig(output / f"{stem}.png", dpi=dpi, facecolor="white")
    plt.close(fig)
    return {"stem": stem, "geometry": sidecar.name}


def render_ecdf(
    data: dict, horizon: int, output: Path, config_id: str, dpi: int
) -> dict:
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(3, 3, figsize=(16, 12), sharey=True)
    fig.subplots_adjust(
        left=0.08, right=0.975, top=0.85, bottom=0.14, hspace=0.40, wspace=0.16
    )
    geometry = []
    x_ranges = {}
    for panel, feature, title, ax in zip(
        (data["ecdf"][str(horizon)][f] for f in FEATURES), FEATURES, TITLES, axes.flat
    ):
        _axis_format(ax, feature)
        ax.set_xlim(ax.get_xlim()[0], ECDF_XMAX[feature])
        if "age_p90" in feature:
            _axis_format(ax, feature)
        x_ranges[feature] = [float(v) for v in ax.get_xlim()]
        ax.set_ylim(0, 101)
        any_curve = False
        for session in SESSIONS:
            curve = dict(panel["curves"][session], session=session)
            x, y = _step_vertices(panel, curve)
            if not len(x):
                continue
            clipped = _clip_polyline(
                np.column_stack((x, y)),
                (
                    ax.get_xlim()[0],
                    ax.get_ylim()[0],
                    ax.get_xlim()[1],
                    ax.get_ylim()[1],
                ),
            )
            if not clipped:
                continue
            x, y = np.asarray(clipped).T
            any_curve = True
            artist_id = f"ecdf_{horizon}_{feature}_{session}"
            (line,) = ax.plot(
                x,
                y,
                color=COLORS[session],
                linestyle=LINESTYLES[session],
                linewidth=LINEWIDTHS[session],
                zorder=LINE_ZORDERS[session],
                label={
                    "pooled": "Pooled",
                    "premarket": "Premarket",
                    "rth": "RTH",
                    "after_hours": "After-hours",
                }[session],
                gid=artist_id,
            )
            line.get_path().should_simplify = False
            geometry.append(
                {
                    "artist_id": artist_id,
                    "artist": line,
                    "reduction_error_bound_pp": ecdf_reduction_error_pp(curve),
                    "claimed_final_error_bound_pp": curve.get(
                        "claimed_final_error_bound_pp"
                    ),
                    "right_tail_upper_bound_pp": _right_tail_upper_bound_pp(
                        panel, curve, ECDF_XMAX[feature]
                    ),
                }
            )
        ax.set(title=title, xlabel=panel["display_label"], ylim=(0, 101))
        ax.set_yticks([0, 20, 40, 60, 80, 100])
        ax.grid(alpha=0.18)
        ax.tick_params(labelsize=9, labelleft=True)
        if not any_curve:
            ax.text(
                0.5,
                0.5,
                "No eligible observations",
                transform=ax.transAxes,
                ha="center",
                va="center",
            )
    handles, labels = axes[0, 0].get_legend_handles_labels()
    if handles:
        fig.legend(
            handles,
            labels,
            loc="upper center",
            bbox_to_anchor=(0.52, 0.905),
            ncol=4,
            frameon=False,
        )
    gate = data["metadata"]["gate"][str(horizon)]
    fig.suptitle(
        f"{horizon}s Feature ECDFs"
        + (
            " — SYNTHETIC DEMONSTRATION"
            if "SYNTHETIC" in data["metadata"]["scope"]
            else ""
        ),
        fontsize=19,
        y=0.98,
    )
    fig.text(0.5, 0.934, gate["display"], ha="center", fontsize=12)
    fig.supylabel("Eligible observed time at or below value (%)", x=0.025, fontsize=12)
    coverage = data["metadata"]["coverage"][str(horizon)]
    retention = []
    for session in SESSIONS[1:]:
        row = coverage[session]
        retention.append(
            f"{session.replace('_',' ').title()}: {100*row['passing']/row['post_discovery']:.1f}%"
            if row["post_discovery"]
            else f"{session.title()}: unavailable"
        )
    fig.text(
        0.08,
        0.067,
        "Gate retention of post-discovery time — " + " · ".join(retention),
        fontsize=10,
    )
    fig.text(
        0.08,
        0.040,
        _metadata_line(data["metadata"])
        + " · *Selection variables; feature-specific denominators.",
        fontsize=10,
    )
    maximum_tail = max(
        (curve["right_tail_upper_bound_pp"] for curve in geometry), default=0.0
    )
    fig.text(
        0.08,
        0.018,
        f"Exact ECDF reduction/SVG rounding verified; shared x-ranges retain all but ≤{maximum_tail:.3f}% per curve; off-axis mass remains in denominators.",
        fontsize=9,
        color="#475569",
    )
    result = _save_page(
        fig, output, f"transaction_ecdf_{horizon}s", geometry, config_id, dpi
    )
    result.update(
        x_ranges=x_ranges,
        maximum_right_tail_upper_bound_pp=maximum_tail,
        palette=COLORS,
        line_widths=LINEWIDTHS,
        line_zorders=LINE_ZORDERS,
        figure_inches=[16, 12],
    )
    return result


def _joint_mesh(ax, panel: dict):
    import matplotlib.pyplot as plt
    from matplotlib.colors import LogNorm

    counts = np.asarray(panel["counts"], dtype=np.int64)
    n = int(panel["eligible_denominator"])
    if n == 0:
        ax.text(
            0.5,
            0.5,
            "No eligible observations",
            transform=ax.transAxes,
            ha="center",
            va="center",
        )
        return None
    mass = counts.T * 100.0 / n
    cmap = plt.get_cmap("magma").copy()
    cmap.set_bad("#eef1f5")
    cmap.set_under("#24104f")
    return ax.pcolormesh(
        panel["x_edges"],
        panel["y_edges"],
        np.ma.masked_equal(mass, 0),
        norm=LogNorm(0.001, 100),
        cmap=cmap,
        rasterized=True,
    )


def _joint_axes(ax, panel: dict) -> None:
    from matplotlib.ticker import FuncFormatter, NullLocator

    y_lower, y_upper = panel["y_edges"][0], panel["y_edges"][-1]
    ax.set_yscale("symlog", linthresh=0.1)
    ax.set_yticks(
        [value for value in (0, 0.1, 1, 10, 100) if y_lower <= value <= y_upper]
    )
    ax.set_ylim(y_lower, y_upper)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    if panel["x_feature"] == "movement_participation":
        ax.set_xlim(0, 1)
        ax.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])
    elif panel["x_feature"] == "quoted_spread_mean_bps":
        ax.set_xscale("log")
        ax.set_xlim(left=1)
        ax.set_xticks([1, 10, 100, 1000])
        ax.xaxis.set_minor_locator(NullLocator())
    else:
        _require(panel["x_feature"] == "trade_rate", "unknown joint x feature")
        ax.set_xscale("log")
        ax.set_xlim(left=Selection.RATE_MINIMUM)
        ax.set_xticks([1, 10, 100])
        ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(
        FuncFormatter(lambda v, _: f"{v/1000:g}k" if v >= 1000 else f"{v:g}")
    )
    ax.set_xlabel(panel["x_label"])
    ax.set_ylabel(panel.get("y_label", ""))
    if panel.get("ms_guides"):
        _require(
            panel["x_feature"] == "quoted_spread_mean_bps"
            and panel["y_feature"] == "movement_mean_5s_bps",
            "M/S guide orientation is invalid",
        )
        xs = np.geomspace(max(1, panel["x_edges"][0]), panel["x_edges"][-1], 200)
        for ratio in panel["ms_guides"]:
            ax.plot(xs, ratio * xs, color="#24a9b4", lw=1, ls="--", alpha=0.9)
            ax.text(
                1.4,
                ratio * 1.4,
                f"M/S = {ratio:g}",
                fontsize=8,
                va="bottom",
                bbox={
                    "facecolor": "white",
                    "edgecolor": "none",
                    "alpha": 0.8,
                    "pad": 1.3,
                },
            )


def _panel_footer(panel: dict) -> str:
    n = int(panel["eligible_denominator"])
    if panel.get("band"):
        band = panel["band"]
        pct = (
            100 * band["numerator"] / band["denominator"]
            if band["denominator"]
            else None
        )
        share = (
            f"{pct:.2f}% of {band['denominator_label']}"
            if pct is not None
            else f"No {band['denominator_label']}"
        )
    else:
        base = panel["coverage"]
        pct = 100 * n / base["denominator"] if base["denominator"] else None
        share = (
            f"{pct:.1f}% of {base['denominator_label']}"
            if pct is not None
            else f"No {base['denominator_label']}"
        )
    return f"{share}\n{n/3600:,.1f} stock-hours · {panel['contributors']['symbol_days']:,} symbol-days"


def render_joint(
    data: dict, page_name: str, output: Path, config_id: str, dpi: int
) -> dict:
    import matplotlib.pyplot as plt

    page = data["joints"][page_name]
    layouts = {
        "movement_spread_participation": (2, 2, (14, 10.5)),
        "activity_movement_spread": (2, 4, (20, 11)),
        "activity_movement_participation": (2, 3, (16, 11)),
    }
    rows, cols, size = layouts[page_name]
    fig, axes = plt.subplots(rows, cols, figsize=size, squeeze=False)
    fig.subplots_adjust(**page["layout"])
    meshes = []
    for index, (ax, panel) in enumerate(zip(axes.flat, page["panels"])):
        band = (panel.get("band") or {}).get("label", "all")
        artist_suffix = re.sub(r"[^A-Za-z0-9]+", "_", band).strip("_").lower()
        panel_id = f"joint_panel_{page_name}_{panel['horizon_seconds']}s_{index}_{artist_suffix}"
        ax.set_gid(panel_id)
        mesh = _joint_mesh(ax, panel)
        if mesh is not None:
            mesh.set_gid(panel_id + "_mesh")
            meshes.append(mesh)
        _joint_axes(ax, panel)
        title = (panel.get("band") or {}).get("display_label", panel["title"])
        ax.set_title(title if index < cols else "", fontsize=14, weight="bold", pad=14)
        ax.text(
            0.5,
            -0.25,
            _panel_footer(panel),
            transform=ax.transAxes,
            ha="center",
            va="top",
            fontsize=9,
            color="#334155",
            linespacing=1.5,
        )
    for row, horizon in enumerate(Selection.HORIZONS):
        box = axes[row, 0].get_position()
        fig.text(
            0.022,
            (box.y0 + box.y1) / 2,
            f"{horizon}s history",
            fontsize=12,
            weight="bold",
            rotation=90,
            ha="center",
            va="center",
        )
    if meshes:
        cax = fig.add_axes(page["colorbar_axes"])
        cb = fig.colorbar(meshes[-1], cax=cax, extend="min")
        cb.set_label("Eligible time per bin (%)", fontsize=10)
    fig.text(
        page["title_x"],
        0.957,
        page["title"],
        fontsize=page.get("title_size", 22),
        weight="bold",
    )
    fig.text(
        page["title_x"],
        0.917,
        _metadata_line(data["metadata"]) + " · pooled sessions",
        fontsize=11,
    )
    fig.text(
        page["title_x"], 0.883, data["metadata"]["scope"], fontsize=10, color="#475569"
    )
    for header in page.get("group_headers", []):
        fig.text(
            header["x"], 0.839, header["text"], fontsize=14, weight="bold", ha="center"
        )
    for text in page["footers"]:
        fig.text(
            text["x"],
            text["y"],
            text["text"],
            fontsize=text.get("size", 9),
            color="#475569",
            ha=text.get("ha", "left"),
        )
    return _save_page(fig, output, page_name, [], config_id, dpi)


def render_population(data: dict, output: Path, config_id: str, dpi: int) -> dict:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    rows = data["population_share"]["daily"]
    xs = [date.fromisoformat(row["date"]) for row in rows]
    selected = np.asarray([row["selected"] for row in rows], dtype=np.int64)
    eligible = np.asarray([row["eligible"] for row in rows], dtype=np.int64)
    _require(
        len(xs)
        and np.all(eligible > 0)
        and np.all((selected >= 0) & (selected <= eligible)),
        "invalid daily population counts",
    )
    ys = selected / eligible * 100
    period = selected.sum() / eligible.sum() * 100
    fig, ax = plt.subplots(figsize=(10, 4.8))
    fig.subplots_adjust(left=0.085, right=0.975, top=0.84, bottom=0.13)
    ax.set_title(
        "Daily Share of Eligible Stocks Selected"
        + (
            "\nSYNTHETIC DEMONSTRATION"
            if "SYNTHETIC" in data["metadata"]["scope"]
            else ""
        ),
        fontsize=15,
        fontweight="semibold",
        pad=18,
    )
    ax.plot(xs, ys, color="#166b9c", lw=1.7, marker="o", markersize=2.2)
    ax.axhline(
        period,
        color="#a15d16",
        ls=(0, (5, 3)),
        lw=1.2,
        label=f"Period share: {period:.2f}%",
    )
    ax.legend(loc="upper right", frameon=False)
    ax.set_ylabel("Symbols selected (%)")
    ax.set_xlim(
        (xs[0] - timedelta(days=1), xs[-1] + timedelta(days=1))
        if len(xs) == 1
        else (xs[0], xs[-1])
    )
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.grid(axis="y", color="#e5e7eb", linewidth=0.7)
    ax.set_axisbelow(True)
    result = _save_page(fig, output, "population_share", [], config_id, dpi)
    result.update(
        selected_symbol_days=int(selected.sum()),
        eligible_symbol_days=int(eligible.sum()),
        period_share_pct=float(period),
    )
    return result


def render_bundle(
    aggregate_dir: Path, output: Path, dpi: int = 160, config_path: Path | None = None
) -> dict:
    started = time.monotonic()
    source_manifest, data = load_aggregate(aggregate_dir, config_path)
    output.mkdir(parents=True, exist_ok=False)
    code_identity = {
        Path(__file__).name: Verify.sha256(Path(__file__)),
        Path(Verify.__file__).name: Verify.sha256(Path(Verify.__file__)),
    }
    config = {
        "schema": RENDER_SCHEMA,
        "dpi": dpi,
        "formats": ["png", "svg"],
        "path_simplify": False,
        "svg_coordinate_decimals": SVG_DECIMALS,
        "code": code_identity,
        "figure_config": data["figure_config"],
    }
    config_id = Verify.canonical_identity(config)
    source_manifest_path = aggregate_dir / (
        "manifest.json"
        if (aggregate_dir / "manifest.json").is_file()
        else "aggregate_manifest.json"
    )
    pending = {
        "schema": RENDER_SCHEMA,
        "state": "rendering",
        "source_aggregate_identity": source_manifest["aggregate_identity"],
        "source_manifest_sha256": Verify.sha256(source_manifest_path),
        "render_config": config,
        "render_config_identity": config_id,
    }
    manifest_path = output / "render_manifest.json"
    manifest_path.write_text(json.dumps(pending, indent=2, sort_keys=True) + "\n")
    try:
        with _network_disabled():
            _style()
            pages = (
                [render_population(data, output, config_id, dpi)]
                if data["population_share"].get("daily")
                else []
            )
            pages += [
                render_ecdf(data, horizon, output, config_id, dpi)
                for horizon in (60, 300)
            ]
            pages += [
                render_joint(data, name, output, config_id, dpi)
                for name in (
                    "movement_spread_participation",
                    "activity_movement_spread",
                    "activity_movement_participation",
                )
            ]
        verification = Verify.verify_bundle(output, config_id)
        verification_path = output / "export_verification.json"
        verification_path.write_text(
            json.dumps(verification, indent=2, sort_keys=True) + "\n"
        )
        files = {
            path.name: Verify.sha256(path)
            for path in sorted(output.iterdir())
            if path.name != manifest_path.name
        }
        result = {
            **pending,
            "state": "complete",
            "verification": {
                "state": "passed",
                "path": verification_path.name,
                "sha256": Verify.sha256(verification_path),
            },
            "pages": pages,
            "files": files,
            "elapsed_seconds": time.monotonic() - started,
            "peak_process_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            / (1024**2 if sys.platform == "darwin" else 1024),
            "network_requests": 0,
            "numerical_disclosures": {
                "ecdf_crops": {
                    f"{h}s:{f}": data["ecdf"][str(h)][f].get("crop")
                    for h in Selection.HORIZONS
                    for f in FEATURES
                    if data["ecdf"][str(h)][f].get("crop")
                },
                "joint_panels": [
                    {
                        "page": name,
                        "horizon_seconds": p["horizon_seconds"],
                        "x_feature": p["x_feature"],
                        "y_feature": p["y_feature"],
                        "eligible_denominator": p["eligible_denominator"],
                        "off_axis_count": p["off_axis_count"],
                        "first_bin": p["first_bin"],
                        "band": p.get("band"),
                    }
                    for name, page in data["joints"].items()
                    for p in page["panels"]
                ],
            },
        }
    except Exception as exc:
        result = {
            **pending,
            "state": "failed",
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_seconds": time.monotonic() - started,
        }
        manifest_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        raise
    manifest_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aggregate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--config",
        type=Path,
        help="Required report_plot_config_v1 for numerical aggregate bundles",
    )
    parser.add_argument("--dpi", type=int, default=160)
    args = parser.parse_args()
    result = render_bundle(args.aggregate, args.output, args.dpi, args.config)
    print(
        json.dumps(
            {
                key: result[key]
                for key in ("state", "elapsed_seconds", "peak_process_rss_mib")
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
