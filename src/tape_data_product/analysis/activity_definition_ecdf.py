"""Exact-rank activity-definition ECDFs for the endpoint/EW report."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import time


SESSIONS = ("pooled", "premarket", "rth", "after_hours")
FIELDS = (
    {"view": "fast", "kind": "trade_rate", "field": "trade_rate_per_second_hl30s", "unit": "trades/s", "threshold": 1.0, "qualifier": ">="},
    {"view": "fast", "kind": "trade_age", "field": "trade_age_p90_seconds_window60s", "unit": "seconds", "threshold": 2.0, "qualifier": "<="},
    {"view": "slow", "kind": "trade_rate", "field": "trade_rate_per_second_hl120s", "unit": "trades/s", "threshold": 1.0, "qualifier": ">="},
    {"view": "slow", "kind": "trade_age", "field": "trade_age_p90_seconds_window300s", "unit": "seconds", "threshold": 2.0, "qualifier": "<="},
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, value) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def _feature_paths(inventory: Path, expected_members: int | None) -> tuple[dict, list[str]]:
    payload = json.loads(inventory.read_text())
    members = payload["members"]
    if expected_members is not None and len(members) != expected_members:
        raise ValueError(f"expected {expected_members:,} members, found {len(members):,}")
    if not members or len(members) > 10_000:
        raise ValueError("inventory must contain 1..10,000 members")
    keys = [(row["session_date"], row["symbol"]) for row in members]
    if len(set(keys)) != len(keys):
        raise ValueError("inventory contains duplicate members")
    root = Path(payload["feature_root"])
    paths = [root / f"session_date={day}" / f"symbol={symbol}" / "features.parquet" for day, symbol in keys]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise ValueError(f"missing {len(missing)} feature files; first: {missing[0]}")
    return payload, [str(path) for path in paths]


def _probabilities(steps: int) -> list[float]:
    if type(steps) is not int or not 100 <= steps <= 20_000:
        raise ValueError("probability steps must be 100..20,000")
    return [i / steps for i in range(steps + 1)]


def _session_expression() -> str:
    # Stored endpoints are exact one-second boundaries. Subtract one second to
    # assign each observation by its interval's left edge in New York time.
    local = (
        "timezone('America/New_York', "
        "make_timestamp_ns(interval_end_ns - 1000000000) AT TIME ZONE 'UTC')"
    )
    seconds = f"hour({local}) * 3600 + minute({local}) * 60 + second({local})"
    return (
        f"CASE WHEN {seconds} < 34200 THEN 'premarket' "
        f"WHEN {seconds} < 57600 THEN 'rth' ELSE 'after_hours' END"
    )


def _collapse_quantiles(values: list[float], steps: int) -> list[list[float]]:
    """Keep both sides of atoms while bounding omitted cumulative rank."""
    if len(values) != steps + 1:
        raise ValueError("quantile result length mismatch")
    points: list[list[float]] = []
    start = 0
    for index in range(1, len(values) + 1):
        if index < len(values) and values[index] == values[start]:
            continue
        low = start / steps
        high = (index - 1) / steps
        value = float(values[start])
        if not points or points[-1] != [value, low]:
            points.append([value, low])
        if points[-1] != [value, high]:
            points.append([value, high])
        start = index
    return points


def calculate(
    inventory: Path,
    output: Path,
    *,
    expected_members: int | None = None,
    probability_steps: int = 10_000,
    memory_limit: str = "1GiB",
    maximum_spill: str = "24GiB",
) -> dict:
    """Calculate four ungated session ECDFs from a verified feature inventory."""
    import duckdb

    inventory, output = Path(inventory), Path(output)
    if output.exists():
        raise ValueError("output already exists")
    output.mkdir(parents=True)
    scratch = output / "scratch"
    scratch.mkdir()
    payload, paths = _feature_paths(inventory, expected_members)
    probabilities = _probabilities(probability_steps)
    started = time.monotonic()
    connection = duckdb.connect()
    connection.execute("SET threads=1")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute("SET TimeZone='UTC'")
    connection.execute(f"SET memory_limit='{memory_limit}'")
    connection.execute(f"SET max_temp_directory_size='{maximum_spill}'")
    connection.execute("SET temp_directory=?", [str(scratch)])
    session_expression = _session_expression()
    results = []
    try:
        for specification in FIELDS:
            field = specification["field"]
            mask = field + "_reason_mask"
            threshold = specification["threshold"]
            threshold_predicate = f"value >= {threshold}" if specification["qualifier"] == ">=" else f"value <= {threshold}"
            query = f"""
                WITH valid AS (
                    SELECT CAST(\"{field}\" AS DOUBLE) AS value,
                           {session_expression} AS session
                    FROM read_parquet(?, hive_partitioning=false, union_by_name=false)
                    WHERE \"{mask}\" = 0
                )
                SELECT CASE WHEN GROUPING(session)=1 THEN 'pooled' ELSE session END AS session,
                       count(*)::BIGINT AS valid,
                       count(*) FILTER (WHERE value=0)::BIGINT AS zeros,
                       min(value)::DOUBLE AS minimum,
                       max(value)::DOUBLE AS maximum,
                       count(*) FILTER (WHERE {threshold_predicate})::BIGINT AS threshold_pass,
                       quantile_disc(value, ?) AS quantiles
                FROM valid
                GROUP BY GROUPING SETS ((session), ())
            """
            tick = time.monotonic()
            rows = connection.execute(query, [paths, probabilities]).fetchall()
            sessions = {}
            for session, valid, zeros, minimum, maximum, threshold_pass, quantiles in rows:
                if session not in SESSIONS or session in sessions:
                    raise ValueError("invalid or duplicate session result")
                if valid <= 0 or quantiles is None:
                    raise ValueError("activity input unexpectedly has no valid observations")
                sessions[session] = {
                    "valid": int(valid), "zeros": int(zeros),
                    "minimum": float(minimum), "maximum": float(maximum),
                    "threshold_pass": int(threshold_pass),
                    "threshold_pass_share_valid": int(threshold_pass) / int(valid),
                    "curve": _collapse_quantiles(quantiles, probability_steps),
                }
            if set(sessions) != set(SESSIONS):
                raise ValueError("missing session result")
            if sessions["pooled"]["valid"] != sum(sessions[name]["valid"] for name in SESSIONS[1:]):
                raise ValueError("pooled valid count does not reconcile")
            result = dict(specification)
            result.update(sessions=sessions, elapsed_seconds=time.monotonic() - tick,
                          display_reduction_max_rank_gap=1 / probability_steps)
            results.append(result)
            _write_json(output / "numerical.partial.json", {"fields": results})
    finally:
        connection.close()
    for path in scratch.iterdir():
        if path.is_file():
            path.unlink()
    scratch.rmdir()
    numerical = {
        "schema": "activity_definition_ecdf_v1",
        "population": "ungated represented full-session endpoints; field-specific validity",
        "session_assignment": "interval left edge in America/New_York",
        "weighting": "one valid stock-second per observation",
        "members": len(payload["members"]),
        "represented_rows": len(payload["members"]) * 57_600,
        "inventory": {"name": inventory.name, "sha256": _sha256(inventory)},
        "release": payload.get("release"),
        "probability_steps": probability_steps,
        "display_reduction_max_percentage_points": 100 / probability_steps,
        "fields": results,
        "elapsed_seconds": time.monotonic() - started,
    }
    _write_json(output / "numerical.json", numerical)
    (output / "numerical.partial.json").unlink()
    return numerical


def _tick_values(kind: str, maximum: float) -> list[float]:
    candidates = (
        [0, 0.1, 0.3, 1, 3, 10, 30, 100, 300, 1000, 3000]
        if kind == "trade_rate"
        else [0, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 300, 1800, 7200, 28800, 57600]
    )
    ticks = [value for value in candidates if value <= maximum]
    if ticks[-1] < maximum and math.log1p(maximum) - math.log1p(ticks[-1]) > 0.5:
        ticks.append(maximum)
    return ticks


def _format_tick(value: float) -> str:
    return f"{value/1000:g}k" if value >= 1000 else f"{value:g}"


def render(numerical_path: Path, output: Path, *, dpi: int = 190) -> dict:
    """Render the reviewed 2x2 activity-definition figure."""
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    import numpy as np

    numerical_path, output = Path(numerical_path), Path(output)
    data = json.loads(numerical_path.read_text())
    if data["schema"] != "activity_definition_ecdf_v1":
        raise ValueError("unsupported numerical artifact")
    by_key = {(row["view"], row["kind"]): row for row in data["fields"]}
    required = {(view, kind) for view in ("fast", "slow") for kind in ("trade_rate", "trade_age")}
    if set(by_key) != required:
        raise ValueError("numerical artifact is missing activity panels")
    output.mkdir(parents=True, exist_ok=True)
    colors = {"pooled": "#374151", "premarket": "#0072B2", "rth": "#D55E00", "after_hours": "#CC79A7"}
    styles = {"pooled": "-", "premarket": "--", "rth": "-", "after_hours": ":"}
    widths = {"pooled": 2.2, "premarket": 1.65, "rth": 1.65, "after_hours": 1.8}
    labels = {"pooled": "Pooled", "premarket": "Premarket", "rth": "RTH", "after_hours": "After-hours"}
    maxima = {kind: max(by_key[(view, kind)]["sessions"][session]["maximum"] for view in ("fast", "slow") for session in SESSIONS) for kind in ("trade_rate", "trade_age")}
    fig, axes = plt.subplots(2, 2, figsize=(15.5, 10.2), sharey=True)
    fig.subplots_adjust(left=.085, right=.975, top=.80, bottom=.17, hspace=.34, wspace=.18)
    for row_index, view in enumerate(("fast", "slow")):
        for column_index, kind in enumerate(("trade_rate", "trade_age")):
            ax, panel, maximum = axes[row_index, column_index], by_key[(view, kind)], maxima[kind]
            for session in SESSIONS:
                curve = np.asarray(panel["sessions"][session]["curve"], dtype=float)
                ax.plot(np.log1p(curve[:, 0]), curve[:, 1] * 100,
                        color=colors[session], linestyle=styles[session], linewidth=widths[session],
                        label=labels[session], zorder=2 if session == "pooled" else 3)
            threshold = panel["threshold"]
            ax.axvline(np.log1p(threshold), color="#111827", linestyle=(0, (4, 3)), linewidth=1.25, zorder=4)
            if kind == "trade_rate":
                ax.axvspan(np.log1p(threshold), np.log1p(maximum), color="#86BFA3", alpha=.10, zorder=0)
            else:
                ax.axvspan(0, np.log1p(threshold), color="#86BFA3", alpha=.10, zorder=0)
            ticks = _tick_values(kind, maximum)
            ax.set_xticks(np.log1p(ticks), [_format_tick(value) for value in ticks])
            ax.set_xlim(0, np.log1p(maximum) * 1.005)
            ax.set_ylim(0, 101)
            ax.set_yticks([0, 20, 40, 60, 80, 100])
            ax.grid(axis="y", color="#E5E7EB", linewidth=.8)
            ax.set_axisbelow(True)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.tick_params(labelsize=9)
            ax.set_xlabel("Eligible trades per second" if kind == "trade_rate" else "Trade-age p90 (seconds)", fontsize=11)
            ax.text(np.log1p(threshold), 3.5, "  ≥1 trade/s" if kind == "trade_rate" else "≤2 seconds  ",
                    ha="left" if kind == "trade_rate" else "right", va="bottom", fontsize=9, color="#374151")
            pooled = panel["sessions"]["pooled"]
            ax.text(.985, .08, f"Pooled valid n={pooled['valid']:,}\nthreshold side={pooled['threshold_pass_share_valid']:.1%}",
                    transform=ax.transAxes, ha="right", va="bottom", fontsize=8.5, color="#475569")
    axes[0, 0].set_title("EW eligible trade rate", fontsize=13, pad=10)
    axes[0, 1].set_title("Trailing trade-age p90", fontsize=13, pad=10)
    axes[0, 0].set_ylabel("Fast · 30s EW / 60s age window\n\nValid observed time at or below value (%)", fontsize=11)
    axes[1, 0].set_ylabel("Slow · 120s EW / 300s age window\n\nValid observed time at or below value (%)", fontsize=11)
    handles, legend_labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, legend_labels, loc="upper center", bbox_to_anchor=(.53, .865), ncol=4, frameon=False, fontsize=10)
    fig.suptitle("Activity Definition — Ungated Input Distributions", fontsize=20, y=.965)
    fig.text(.5, .905, "Full endpoint/EW release · field-valid observations · shaded side satisfies that input threshold", ha="center", fontsize=11, color="#475569")
    fig.text(.09, .085, "Horizontal position uses log(1+x); tick labels remain in native units. All finite tails and valid zeros remain in each ECDF denominator.", fontsize=9.5, color="#475569")
    fig.text(.09, .052, "The report gate requires both inputs simultaneously. Marginal threshold shares are not the combined active-tape retention rate.", fontsize=9.5, color="#475569")
    fig.text(.09, .019, f"{data['members']:,} symbol-days · {data['represented_rows']:,} represented seconds · exact discrete ranks reduced to ≤{data['display_reduction_max_percentage_points']:.3f} percentage-point display gaps", fontsize=9.2, color="#475569")
    png, svg = output / "activity_definition_ecdf.png", output / "activity_definition_ecdf.svg"
    fig.savefig(png, dpi=dpi, facecolor="white")
    with mpl.rc_context({"svg.hashsalt": "activity-definition-ecdf-v1"}):
        fig.savefig(svg, facecolor="white", metadata={"Date": None})
    plt.close(fig)
    summary = {
        "schema": "activity_definition_ecdf_render_v1", "numerical_sha256": _sha256(numerical_path),
        "outputs": {path.name: {"bytes": path.stat().st_size, "sha256": _sha256(path)} for path in (png, svg)},
        "x_transform": "log1p", "shared_x_domain_by_column": True,
        "thresholds": {"trade_rate_minimum_inclusive": 1.0, "trade_age_p90_maximum_inclusive": 2.0},
    }
    _write_json(output / "render.json", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    calculator = subparsers.add_parser("calculate")
    calculator.add_argument("--inventory", type=Path, required=True)
    calculator.add_argument("--output", type=Path, required=True)
    calculator.add_argument("--expected-members", type=int)
    calculator.add_argument("--probability-steps", type=int, default=10_000)
    calculator.add_argument("--memory-limit", default="1GiB")
    calculator.add_argument("--maximum-spill", default="24GiB")
    renderer = subparsers.add_parser("render")
    renderer.add_argument("--numerical", type=Path, required=True)
    renderer.add_argument("--output", type=Path, required=True)
    renderer.add_argument("--dpi", type=int, default=190)
    arguments = parser.parse_args()
    if arguments.command == "calculate":
        result = calculate(arguments.inventory, arguments.output,
                           expected_members=arguments.expected_members,
                           probability_steps=arguments.probability_steps,
                           memory_limit=arguments.memory_limit,
                           maximum_spill=arguments.maximum_spill)
    else:
        result = render(arguments.numerical, arguments.output, dpi=arguments.dpi)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
