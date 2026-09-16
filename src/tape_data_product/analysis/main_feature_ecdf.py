"""Bounded one-pass-per-family ECDFs for the active-tape report population."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time
import hashlib
import json
import math
from pathlib import Path
import time
from zoneinfo import ZoneInfo

import numpy as np


SESSIONS = ("pooled", "premarket", "rth", "after_hours")
VIEWS = {
    "fast": ("hl30s", "window60s"),
    "slow": ("hl120s", "window300s"),
}


@dataclass(frozen=True)
class Metric:
    key: str
    label: str
    unit: str
    stem: str
    source: str = "feature"
    transform: str = "log1p"


FAMILIES = {
    "movement_friction": (
        Metric("movement", "Five-second RMS movement", "bps", "midpoint_rms_5s_bps"),
        Metric("spread", "Mean quoted spread", "bps", "quoted_spread_bps"),
        Metric("ratio", "Movement / spread", "ratio", "midpoint_rms_5s_to_spread"),
        Metric("participation", "Movement participation", "0–1", "movement_participation", transform="linear"),
    ),
    "throughput_liquidity": (
        Metric("share_rate", "Eligible share rate", "shares/s", "share_rate_per_second"),
        Metric("dollar_rate", "Eligible dollar rate", "USD/s", "dollar_rate_usd_per_second"),
        Metric("bid_size", "Mean displayed bid size", "shares", "bid_size_mean_shares"),
        Metric("ask_size", "Mean displayed ask size", "shares", "ask_size_mean_shares"),
    ),
    "freshness": (
        Metric("current_trade_age", "Current eligible-trade age", "seconds", "trade_age_seconds", source="base"),
        Metric("current_quote_age", "Current quote-message age", "seconds", "quote_age_seconds", source="base"),
        Metric("current_midpoint_age", "Current midpoint-change age", "seconds", "midpoint_change_age_seconds", source="base"),
        Metric("quote_age_p90", "Trailing quote-age p90", "seconds", "quote_age_p90_seconds"),
        Metric("midpoint_age_p90", "Trailing midpoint-change-age p90", "seconds", "midpoint_change_age_p90_seconds"),
    ),
}


def _write_json(path: Path, value) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _members(inventory_path: Path, maximum_members: int | None) -> tuple[dict, list[dict]]:
    payload = json.loads(inventory_path.read_text())
    members = payload["members"]
    if maximum_members is not None:
        if maximum_members <= 0:
            raise ValueError("maximum_members must be positive")
        members = members[:maximum_members]
    if not members:
        raise ValueError("inventory has no selected members")
    keys = [(row["session_date"], row["symbol"]) for row in members]
    if len(keys) != len(set(keys)):
        raise ValueError("inventory contains duplicate members")
    return payload, members


def _feature_name(metric: Metric, view: str) -> str:
    suffix, window = VIEWS[view]
    if metric.source == "base":
        return metric.stem
    if metric.key.endswith("_p90"):
        return f"{metric.stem}_{window}"
    return f"{metric.stem}_{suffix}"


def _gate_names(view: str) -> tuple[str, str]:
    suffix, window = VIEWS[view]
    return f"trade_rate_per_second_{suffix}", f"trade_age_p90_seconds_{window}"


def _session_ids(interval_end_ns: np.ndarray, session_date: str) -> np.ndarray:
    day = date.fromisoformat(session_date)
    local_noon = datetime.combine(day, datetime_time(12), ZoneInfo("America/New_York"))
    offset_ns = int(local_noon.utcoffset().total_seconds()) * 1_000_000_000
    seconds = ((interval_end_ns.astype(np.int64) - 1_000_000_000 + offset_ns) % 86_400_000_000_000) // 1_000_000_000
    result = np.full(seconds.size, 3, dtype=np.int8)
    result[seconds < 57_600] = 2
    result[seconds < 34_200] = 1
    if np.any(seconds < 14_400):
        raise ValueError("observation precedes represented 04:00 session")
    return result


def _as_numpy(batch, name: str) -> np.ndarray:
    return batch.column(batch.schema.get_field_index(name)).to_numpy(zero_copy_only=False)


class Accumulator:
    def __init__(self, transform: str, width: float):
        self.transform = transform
        self.width = width
        self.counts = np.zeros(128, dtype=np.int64)
        self.valid = 0
        self.zeros = 0
        self.minimum = math.inf
        self.maximum = -math.inf

    def add(self, values: np.ndarray) -> None:
        if values.size == 0:
            return
        values = np.asarray(values, dtype=np.float64)
        if np.any(~np.isfinite(values)) or np.any(values < 0):
            raise ValueError("valid ECDF values must be finite and nonnegative")
        if self.transform == "linear" and np.any(values > 1 + 1e-12):
            raise ValueError("participation exceeds one")
        self.valid += int(values.size)
        zero = values == 0
        self.zeros += int(np.count_nonzero(zero))
        self.minimum = min(self.minimum, float(np.min(values)))
        self.maximum = max(self.maximum, float(np.max(values)))
        positive = values[~zero]
        if not positive.size:
            return
        transformed = np.log1p(positive) if self.transform == "log1p" else positive
        bins = np.floor(transformed / self.width).astype(np.int64) + 1
        update = np.bincount(bins)
        if update.size > self.counts.size:
            self.counts = np.pad(self.counts, (0, update.size - self.counts.size))
        self.counts[: update.size] += update

    def result(self) -> dict:
        if not self.valid:
            return {"valid": 0, "zeros": 0, "minimum": None, "maximum": None, "curve": []}
        cumulative = self.zeros
        curve = [[0.0, self.zeros / self.valid]] if self.zeros else [[self.minimum, 0.0]]
        for bin_id in np.flatnonzero(self.counts):
            cumulative += int(self.counts[bin_id])
            x = bin_id * self.width
            if self.transform == "log1p":
                x = math.expm1(x)
            curve.append([min(float(x), self.maximum), cumulative / self.valid])
        if curve[-1][0] != self.maximum or curve[-1][1] != 1.0:
            curve.append([self.maximum, 1.0])
        return {
            "valid": self.valid,
            "zeros": self.zeros,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "curve": curve,
        }


def _columns(family: str) -> tuple[list[str], list[str]]:
    feature = {"interval_end_ns"}
    base = {"interval_end_ns"}
    for view in VIEWS:
        rate, age = _gate_names(view)
        feature.update((rate, rate + "_reason_mask", age, age + "_reason_mask"))
        for metric in FAMILIES[family]:
            name = _feature_name(metric, view)
            target = base if metric.source == "base" else feature
            target.update((name, name + "_reason_mask"))
    return sorted(feature), sorted(base)


def _batch_rows(feature_file, base_file, feature_columns, base_columns, batch_size):
    feature_batches = feature_file.iter_batches(batch_size=batch_size, columns=feature_columns)
    if base_file is None:
        for feature_batch in feature_batches:
            yield feature_batch, None
        return
    base_batches = base_file.iter_batches(batch_size=batch_size, columns=base_columns)
    sentinel = object()
    while True:
        feature_batch = next(feature_batches, sentinel)
        base_batch = next(base_batches, sentinel)
        if feature_batch is sentinel or base_batch is sentinel:
            if feature_batch is not sentinel or base_batch is not sentinel:
                raise ValueError("paired base/feature batch counts differ")
            return
        yield feature_batch, base_batch


def _calculate_family(payload: dict, members: list[dict], family: str, batch_size: int,
                      log_bin_width: float, linear_bin_width: float) -> dict:
    import pyarrow.parquet as pq

    feature_columns, base_columns = _columns(family)
    needs_base = any(metric.source == "base" for metric in FAMILIES[family])
    accumulators = {
        (metric.key, view, session): Accumulator(metric.transform, linear_bin_width if metric.transform == "linear" else log_bin_width)
        for metric in FAMILIES[family] for view in VIEWS for session in SESSIONS
    }
    gate_counts = {(view, session): 0 for view in VIEWS for session in SESSIONS}
    rows = 0
    started = time.monotonic()
    for member_index, member in enumerate(members, 1):
        day, symbol = member["session_date"], member["symbol"]
        feature_path = Path(payload["feature_root"]) / f"session_date={day}" / f"symbol={symbol}" / "features.parquet"
        base_path = Path(payload["base_root"]) / f"session_date={day}" / f"symbol={symbol}" / "base.parquet"
        feature_file = pq.ParquetFile(feature_path)
        base_file = pq.ParquetFile(base_path) if needs_base else None
        for feature_batch, base_batch in _batch_rows(feature_file, base_file, feature_columns, base_columns, batch_size):
            endpoints = _as_numpy(feature_batch, "interval_end_ns")
            if base_batch is not None and not np.array_equal(endpoints, _as_numpy(base_batch, "interval_end_ns")):
                raise ValueError(f"base/feature endpoint mismatch for {day}/{symbol}")
            session_ids = _session_ids(endpoints, day)
            rows += endpoints.size
            for view in VIEWS:
                rate_name, age_name = _gate_names(view)
                rate = _as_numpy(feature_batch, rate_name)
                age = _as_numpy(feature_batch, age_name)
                gate = (
                    (_as_numpy(feature_batch, rate_name + "_reason_mask") == 0)
                    & (_as_numpy(feature_batch, age_name + "_reason_mask") == 0)
                    & np.isfinite(rate) & np.isfinite(age) & (rate >= 1.0) & (age <= 2.0)
                )
                for session_index, session in enumerate(SESSIONS):
                    selected = gate if session == "pooled" else gate & (session_ids == session_index)
                    gate_counts[(view, session)] += int(np.count_nonzero(selected))
                for metric in FAMILIES[family]:
                    name = _feature_name(metric, view)
                    batch = base_batch if metric.source == "base" else feature_batch
                    values = _as_numpy(batch, name)
                    valid = gate & (_as_numpy(batch, name + "_reason_mask") == 0) & np.isfinite(values)
                    for session_index, session in enumerate(SESSIONS):
                        selected = valid if session == "pooled" else valid & (session_ids == session_index)
                        accumulators[(metric.key, view, session)].add(values[selected])
        if member_index % 250 == 0:
            print(f"{family}: {member_index}/{len(members)} members", flush=True)
    metrics = []
    for metric in FAMILIES[family]:
        entry = {"key": metric.key, "label": metric.label, "unit": metric.unit,
                 "source": metric.source, "transform": metric.transform, "views": {}}
        for view in VIEWS:
            entry["views"][view] = {
                "field": _feature_name(metric, view),
                "gate_selected": {session: gate_counts[(view, session)] for session in SESSIONS},
                "sessions": {session: accumulators[(metric.key, view, session)].result() for session in SESSIONS},
            }
            pooled = entry["views"][view]["sessions"]["pooled"]["valid"]
            split = sum(entry["views"][view]["sessions"][s]["valid"] for s in SESSIONS[1:])
            if pooled != split:
                raise ValueError("pooled field-valid count does not reconcile")
        metrics.append(entry)
    return {
        "family": family,
        "members": len(members),
        "represented_rows": rows,
        "elapsed_seconds": time.monotonic() - started,
        "log_bin_width": log_bin_width,
        "linear_bin_width": linear_bin_width,
        "metrics": metrics,
    }


def calculate(inventory: Path, output: Path, *, families: tuple[str, ...] = tuple(FAMILIES),
              maximum_members: int | None = None, batch_size: int = 25_000,
              log_bin_width: float = 0.0025, linear_bin_width: float = 0.001,
              resume: bool = False) -> dict:
    inventory, output = Path(inventory), Path(output)
    if not 1 <= batch_size <= 25_000:
        raise ValueError("batch_size must be 1..25,000")
    unknown = set(families) - set(FAMILIES)
    if unknown:
        raise ValueError(f"unknown families: {sorted(unknown)}")
    partial = output / "numerical.partial.json"
    if output.exists() and not resume:
        raise ValueError("output already exists")
    output.mkdir(parents=True, exist_ok=resume)
    payload, members = _members(inventory, maximum_members)
    existing = json.loads(partial.read_text()) if resume and partial.exists() else {"families": {}}
    started = time.monotonic()
    for family in families:
        if family in existing["families"]:
            continue
        existing["families"][family] = _calculate_family(
            payload, members, family, batch_size, log_bin_width, linear_bin_width
        )
        _write_json(partial, existing)
    numerical = {
        "schema": "active_tape_main_feature_ecdf_v1",
        "population": "corresponding fast/slow active-tape gate; field-specific validity",
        "weighting": "one valid stock-second per observation",
        "session_assignment": "interval left edge in America/New_York",
        "gate": {"trade_rate_minimum_inclusive": 1.0, "trade_age_p90_maximum_inclusive": 2.0},
        "inventory": {"name": inventory.name, "sha256": _sha256(inventory)},
        "release": payload.get("release"),
        "members": len(members),
        "families": existing["families"],
        "elapsed_seconds": time.monotonic() - started + sum(
            family["elapsed_seconds"] for family in existing["families"].values()
        ),
    }
    _write_json(output / "numerical.json", numerical)
    if partial.exists():
        partial.unlink()
    return numerical


def _format_tick(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:g}m"
    if value >= 1_000:
        return f"{value / 1_000:g}k"
    return f"{value:g}"


def _native_ticks(maximum: float, transform: str) -> list[float]:
    if transform == "linear":
        return [0, .2, .4, .6, .8, 1]
    candidates = [0, .01, .03, .1, .3, 1, 3, 10, 30, 100, 300, 1_000, 3_000,
                  10_000, 30_000, 100_000, 300_000, 1_000_000, 3_000_000]
    ticks = [value for value in candidates if value <= maximum]
    if len(ticks) < 3:
        ticks.append(maximum)
    return sorted(set(ticks))


def render(numerical_path: Path, output: Path, *, dpi: int = 190) -> dict:
    import matplotlib.pyplot as plt

    numerical_path, output = Path(numerical_path), Path(output)
    payload = json.loads(numerical_path.read_text())
    output.mkdir(parents=True, exist_ok=True)
    colors = {"pooled": "#334155", "premarket": "#0284c7", "rth": "#d97706", "after_hours": "#db72b0"}
    styles = {"pooled": "-", "premarket": "--", "rth": "-", "after_hours": ":"}
    outputs = {}
    for family, family_data in payload["families"].items():
        metrics = family_data["metrics"]
        fig, axes = plt.subplots(len(metrics), 2, figsize=(14, 3.05 * len(metrics) + 2.2), sharey=True, squeeze=False)
        title = {
            "movement_friction": "Active-Tape Movement and Friction Distributions",
            "throughput_liquidity": "Active-Tape Throughput and Displayed Liquidity",
            "freshness": "Active-Tape Freshness Distributions",
        }[family]
        fig.suptitle(title, fontsize=20, y=.988)
        for row, metric in enumerate(metrics):
            maxima = [metric["views"][view]["sessions"]["pooled"]["maximum"] or 0 for view in VIEWS]
            maximum = max(maxima)
            for column, view in enumerate(VIEWS):
                axis = axes[row, column]
                for session in SESSIONS:
                    curve = metric["views"][view]["sessions"][session]["curve"]
                    if not curve:
                        continue
                    x = np.asarray([point[0] for point in curve])
                    y = np.asarray([point[1] * 100 for point in curve])
                    if metric["transform"] == "log1p":
                        x = np.log1p(x)
                    axis.step(x, y, where="post", color=colors[session], linestyle=styles[session],
                              linewidth=2.2 if session == "pooled" else 1.7, label=session.replace("_", " ").title())
                ticks = _native_ticks(maximum, metric["transform"])
                axis.set_xticks(np.log1p(ticks) if metric["transform"] == "log1p" else ticks)
                axis.set_xticklabels([_format_tick(value) for value in ticks])
                axis.set_xlim(0, math.log1p(maximum) if metric["transform"] == "log1p" else max(1, maximum))
                axis.set_ylim(0, 100.8)
                axis.grid(axis="y", color="#cbd5e1", alpha=.55, linewidth=.8)
                axis.spines[["top", "right"]].set_visible(False)
                axis.set_title(f"{metric['label']} — {view.title()}", fontsize=12)
                axis.set_xlabel(metric["unit"] + (" · log(1+x) position" if metric["transform"] == "log1p" else ""))
                if column == 0:
                    axis.set_ylabel("Valid observations at or below value (%)")
        handles, labels = axes[0, 0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(.5, .962), ncol=4, frameon=False)
        fig.text(.5, .012, "Each column uses its corresponding active-tape gate. Curves use field-valid stock-seconds; all finite tails and valid zeros remain in the denominator.", ha="center", fontsize=9.5, color="#475569")
        fig.tight_layout(rect=(.04, .035, .995, .94), h_pad=2.0, w_pad=2.0)
        for extension in ("png", "svg"):
            path = output / f"{family}_ecdf.{extension}"
            fig.savefig(path, dpi=dpi if extension == "png" else None, bbox_inches="tight")
            outputs[extension if family == "movement_friction" else family + "_" + extension] = str(path)
        plt.close(fig)
    manifest = {"schema": "active_tape_main_feature_ecdf_render_v1", "source": str(numerical_path), "outputs": outputs}
    _write_json(output / "render.json", manifest)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    calculator = subparsers.add_parser("calculate")
    calculator.add_argument("--inventory", type=Path, required=True)
    calculator.add_argument("--output", type=Path, required=True)
    calculator.add_argument("--families", nargs="+", choices=tuple(FAMILIES), default=list(FAMILIES))
    calculator.add_argument("--maximum-members", type=int)
    calculator.add_argument("--batch-size", type=int, default=25_000)
    calculator.add_argument("--resume", action="store_true")
    renderer = subparsers.add_parser("render")
    renderer.add_argument("--numerical", type=Path, required=True)
    renderer.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "calculate":
        result = calculate(args.inventory, args.output, families=tuple(args.families),
                           maximum_members=args.maximum_members, batch_size=args.batch_size,
                           resume=args.resume)
        print(json.dumps({"members": result["members"], "families": list(result["families"]),
                          "elapsed_seconds": result["elapsed_seconds"]}, sort_keys=True))
    else:
        result = render(args.numerical, args.output)
        print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
