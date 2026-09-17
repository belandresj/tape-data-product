"""Small, reproducible population artifacts for the endpoint/EW report."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from datetime import date
from pathlib import Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def render_daily_completed_members(
    inventory: Path,
    output: Path,
    *,
    expected_members: int | None = None,
    session_hours: float = 16.0,
    dpi: int = 190,
) -> dict:
    """Count verified inventory members by date and render a simple bar chart.

    The input is metadata-only. No feature rows are decoded, and the output
    contains no symbol-level membership.
    """

    inventory = Path(inventory)
    output = Path(output)
    if expected_members is not None and (
        type(expected_members) is not int or expected_members < 1
    ):
        raise ValueError("expected_members must be a positive integer")
    if not isinstance(session_hours, (int, float)) or session_hours <= 0:
        raise ValueError("session_hours must be positive")
    if type(dpi) is not int or dpi < 72:
        raise ValueError("dpi must be an integer of at least 72")

    payload = json.loads(inventory.read_text())
    members = payload["members"]
    if not isinstance(members, list) or not members:
        raise ValueError("inventory members must be a nonempty list")
    keys = []
    for row in members:
        day = row["session_date"]
        symbol = row["symbol"]
        date.fromisoformat(day)
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("inventory member symbol must be nonempty")
        keys.append((day, symbol))
    if expected_members is not None and len(keys) != expected_members:
        raise ValueError(
            f"expected {expected_members:,} members, found {len(keys):,}"
        )
    if len(set(keys)) != len(keys):
        raise ValueError("inventory contains duplicate session-date/symbol members")

    counts = Counter(day for day, _ in keys)
    dates = sorted(date.fromisoformat(day) for day in counts)
    values = [counts[day.isoformat()] for day in dates]
    if sum(values) != len(keys):
        raise ValueError("daily counts do not reconcile to inventory membership")

    output.mkdir(parents=True, exist_ok=False)
    csv_path = output / "daily_completed_members.csv"
    with csv_path.open("x", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            [
                "session_date",
                "completed_symbol_days",
                "represented_symbol_hours",
            ]
        )
        for day, count in zip(dates, values):
            writer.writerow(
                [day.isoformat(), count, f"{count * float(session_hours):g}"]
            )

    import matplotlib as mpl
    import matplotlib.pyplot as plt
    import numpy as np

    fig, ax = plt.subplots(figsize=(12.5, 6.4))
    fig.subplots_adjust(left=0.09, right=0.975, top=0.77, bottom=0.18)
    positions = np.arange(len(dates))
    ax.bar(
        positions,
        values,
        width=0.82,
        color="#557A9E",
        edgecolor="white",
        linewidth=0.35,
    )
    fig.suptitle(
        "Completed Symbol-Days by Trading Date",
        fontsize=19,
        fontweight="semibold",
        y=0.955,
    )
    fig.text(
        0.5,
        0.875,
        f"Endpoint/EW release · {len(keys):,} completed symbol-days across {len(dates)} trading dates",
        ha="center",
        fontsize=11,
        color="#475569",
    )
    ax.set_ylabel("Completed symbol-days", fontsize=12)
    ax.set_xlabel("Session date", fontsize=12, labelpad=10)
    month_positions = []
    month_labels = []
    previous_month = None
    for index, day in enumerate(dates):
        if day.month != previous_month:
            month_positions.append(index)
            month_labels.append(day.strftime("%b"))
            previous_month = day.month
    ax.set_xticks(month_positions, month_labels)
    ax.set_xlim(-0.7, len(dates) - 0.3)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color="#e5e7eb", linewidth=0.8)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(axis="both", labelsize=10)
    fig.text(
        0.095,
        0.060,
        "Each bar is one represented trading date; dates are evenly spaced and exclude weekends and market holidays.",
        fontsize=9.5,
        color="#475569",
    )
    fig.text(
        0.095,
        0.027,
        "Historical full-session membership · one symbol-day is one symbol on one date · not a count of trading opportunities",
        fontsize=9.5,
        color="#475569",
    )
    png_path = output / "daily_completed_members.png"
    svg_path = output / "daily_completed_members.svg"
    fig.savefig(png_path, dpi=dpi, facecolor="white")
    with mpl.rc_context({"svg.hashsalt": "endpoint-population-v1"}):
        fig.savefig(svg_path, facecolor="white", metadata={"Date": None})
    plt.close(fig)

    summary = {
        "schema": "daily_completed_members_v1",
        "source_inventory": {
            "name": inventory.name,
            "sha256": _sha256(inventory),
        },
        "completed_symbol_days": len(keys),
        "represented_trading_dates": len(dates),
        "first_date": dates[0].isoformat(),
        "last_date": dates[-1].isoformat(),
        "minimum_daily_count": min(values),
        "maximum_daily_count": max(values),
        "mean_daily_count": sum(values) / len(values),
        "session_hours_per_member": float(session_hours),
        "represented_symbol_hours": len(keys) * float(session_hours),
        "daily_counts_sum": sum(values),
        "membership_unique": True,
    }
    summary_path = output / "daily_completed_members_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    artifacts = {
        "schema": "daily_completed_members_artifacts_v1",
        "source_inventory_sha256": summary["source_inventory"]["sha256"],
        "renderer": {
            "name": Path(__file__).name,
            "sha256": _sha256(Path(__file__)),
        },
        "outputs": {
            path.name: {"bytes": path.stat().st_size, "sha256": _sha256(path)}
            for path in (csv_path, png_path, svg_path, summary_path)
        },
    }
    (output / "artifacts.json").write_text(
        json.dumps(artifacts, indent=2, sort_keys=True) + "\n"
    )
    return summary


def render_daily_active_tape_hours(
    member_gate_accounting: Path,
    config: Path,
    output: Path,
    *,
    half_life_seconds: int = 30,
    session: str = "pooled",
    expected_members: int | None = None,
    expected_represented_seconds: int | None = None,
    dpi: int = 190,
) -> dict:
    """Aggregate verified member gate accounting by date and render stock-hours.

    The input may contain private member identifiers. Outputs contain only daily
    aggregates and source identities, never symbol-level membership.
    """

    member_gate_accounting = Path(member_gate_accounting)
    config = Path(config)
    output = Path(output)
    if type(half_life_seconds) is not int or half_life_seconds < 1:
        raise ValueError("half_life_seconds must be a positive integer")
    if not isinstance(session, str) or not session:
        raise ValueError("session must be nonempty")
    if expected_members is not None and (
        type(expected_members) is not int or expected_members < 1
    ):
        raise ValueError("expected_members must be a positive integer")
    if expected_represented_seconds is not None and (
        type(expected_represented_seconds) is not int
        or expected_represented_seconds < 1
    ):
        raise ValueError("expected_represented_seconds must be a positive integer")
    if type(dpi) is not int or dpi < 72:
        raise ValueError("dpi must be an integer of at least 72")

    config_payload = json.loads(config.read_text())
    matching_gates = [
        (name, gate)
        for name, gate in config_payload["gate"].items()
        if gate["half_life_seconds"] == half_life_seconds
    ]
    if len(matching_gates) != 1:
        raise ValueError(
            f"expected one configured gate for half-life {half_life_seconds}"
        )
    gate_name, gate = matching_gates[0]
    required_gate_fields = {
        "half_life_seconds",
        "trade_rate_minimum_inclusive",
        "trade_age_p90_maximum_inclusive",
        "trade_age_window_seconds",
    }
    if not required_gate_fields.issubset(gate):
        raise ValueError("configured gate is missing required fields")

    import pyarrow.parquet as pq

    columns = [
        "member",
        "session",
        "half_life_seconds",
        "represented",
        "gate_valid",
        "gate_pass",
        "gate_fail",
        "gate_unavailable",
    ]
    rows = pq.read_table(member_gate_accounting, columns=columns).to_pylist()
    selected = [
        row
        for row in rows
        if row["session"] == session
        and row["half_life_seconds"] == half_life_seconds
    ]
    if not selected:
        raise ValueError("member gate accounting contains no selected rows")
    if expected_members is not None and len(selected) != expected_members:
        raise ValueError(
            f"expected {expected_members:,} selected members, found {len(selected):,}"
        )

    seen_members = set()
    daily = {}
    for row in selected:
        member = row["member"]
        if not isinstance(member, str) or "/" not in member:
            raise ValueError("member must use session-date/symbol identity")
        day_text, symbol = member.split("/", 1)
        day = date.fromisoformat(day_text)
        if not symbol:
            raise ValueError("member symbol must be nonempty")
        if member in seen_members:
            raise ValueError("duplicate selected member gate accounting row")
        seen_members.add(member)

        counts = {
            key: row[key]
            for key in (
                "represented",
                "gate_valid",
                "gate_pass",
                "gate_fail",
                "gate_unavailable",
            )
        }
        if any(type(value) is not int or value < 0 for value in counts.values()):
            raise ValueError("gate accounting counts must be nonnegative integers")
        if counts["represented"] != (
            counts["gate_valid"] + counts["gate_unavailable"]
        ):
            raise ValueError("represented count does not reconcile")
        if counts["gate_valid"] != counts["gate_pass"] + counts["gate_fail"]:
            raise ValueError("gate-valid count does not reconcile")

        aggregate = daily.setdefault(
            day,
            {
                "members": 0,
                "represented": 0,
                "gate_valid": 0,
                "gate_pass": 0,
                "gate_fail": 0,
                "gate_unavailable": 0,
            },
        )
        aggregate["members"] += 1
        for key, value in counts.items():
            aggregate[key] += value

    dates = sorted(daily)
    totals = {
        key: sum(daily[day][key] for day in dates)
        for key in (
            "represented",
            "gate_valid",
            "gate_pass",
            "gate_fail",
            "gate_unavailable",
        )
    }
    if expected_represented_seconds is not None and (
        totals["represented"] != expected_represented_seconds
    ):
        raise ValueError(
            "expected "
            f"{expected_represented_seconds:,} represented seconds, "
            f"found {totals['represented']:,}"
        )
    if totals["represented"] != totals["gate_valid"] + totals["gate_unavailable"]:
        raise ValueError("aggregate represented count does not reconcile")
    if totals["gate_valid"] != totals["gate_pass"] + totals["gate_fail"]:
        raise ValueError("aggregate gate-valid count does not reconcile")

    output.mkdir(parents=True, exist_ok=False)
    csv_path = output / "daily_active_tape_hours.csv"

    def ratio_text(numerator: int, denominator: int) -> str:
        return f"{numerator / denominator:.12g}" if denominator else ""

    with csv_path.open("x", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            [
                "session_date",
                "completed_symbol_days",
                "represented_seconds",
                "gate_valid_seconds",
                "gate_pass_seconds",
                "gate_fail_seconds",
                "gate_unavailable_seconds",
                "active_stock_hours",
                "gate_pass_share_represented",
                "gate_pass_share_valid",
            ]
        )
        for day in dates:
            row = daily[day]
            writer.writerow(
                [
                    day.isoformat(),
                    row["members"],
                    row["represented"],
                    row["gate_valid"],
                    row["gate_pass"],
                    row["gate_fail"],
                    row["gate_unavailable"],
                    f"{row['gate_pass'] / 3600:.10g}",
                    ratio_text(row["gate_pass"], row["represented"]),
                    ratio_text(row["gate_pass"], row["gate_valid"]),
                ]
            )

    import matplotlib as mpl
    import matplotlib.pyplot as plt
    import numpy as np

    active_hours = [daily[day]["gate_pass"] / 3600 for day in dates]
    fig, ax = plt.subplots(figsize=(12.5, 6.4))
    fig.subplots_adjust(left=0.09, right=0.975, top=0.77, bottom=0.18)
    positions = np.arange(len(dates))
    ax.bar(
        positions,
        active_hours,
        width=0.82,
        color="#786F91",
        edgecolor="white",
        linewidth=0.35,
    )
    fig.suptitle(
        "Active-Tape Stock-Hours by Trading Date",
        fontsize=19,
        fontweight="semibold",
        y=0.955,
    )
    rate = gate["trade_rate_minimum_inclusive"]
    age = gate["trade_age_p90_maximum_inclusive"]
    window = gate["trade_age_window_seconds"]
    fig.text(
        0.5,
        0.875,
        f"Fast gate: trade rate ≥ {rate:g}/s and {window}s trade-age p90 ≤ {age:g}s"
        f" · {totals['gate_pass'] / 3600:,.1f} active stock-hours",
        ha="center",
        fontsize=11,
        color="#475569",
    )
    ax.set_ylabel("Active-tape stock-hours", fontsize=12)
    ax.set_xlabel("Session date", fontsize=12, labelpad=10)
    month_positions = []
    month_labels = []
    previous_month = None
    for index, day in enumerate(dates):
        if day.month != previous_month:
            month_positions.append(index)
            month_labels.append(day.strftime("%b"))
            previous_month = day.month
    ax.set_xticks(month_positions, month_labels)
    ax.set_xlim(-0.7, len(dates) - 0.3)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color="#e5e7eb", linewidth=0.8)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(axis="both", labelsize=10)
    fig.text(
        0.095,
        0.060,
        "Each bar sums gate-passing one-second observations across symbols; represented trading dates are evenly spaced.",
        fontsize=9.5,
        color="#475569",
    )
    fig.text(
        0.095,
        0.027,
        "Historical full-session description · one stock-hour is 3,600 passing symbol-seconds · not executable opportunity time",
        fontsize=9.5,
        color="#475569",
    )
    png_path = output / "daily_active_tape_hours.png"
    svg_path = output / "daily_active_tape_hours.svg"
    fig.savefig(png_path, dpi=dpi, facecolor="white")
    with mpl.rc_context({"svg.hashsalt": "endpoint-active-population-v1"}):
        fig.savefig(svg_path, facecolor="white", metadata={"Date": None})
    plt.close(fig)

    summary = {
        "schema": "daily_active_tape_hours_v1",
        "source_member_gate_accounting": {
            "name": member_gate_accounting.name,
            "sha256": _sha256(member_gate_accounting),
        },
        "source_config": {"name": config.name, "sha256": _sha256(config)},
        "gate_name": gate_name,
        "gate": {key: gate[key] for key in sorted(required_gate_fields)},
        "session": session,
        "completed_symbol_days": len(selected),
        "represented_trading_dates": len(dates),
        "first_date": dates[0].isoformat(),
        "last_date": dates[-1].isoformat(),
        "represented_seconds": totals["represented"],
        "gate_valid_seconds": totals["gate_valid"],
        "gate_pass_seconds": totals["gate_pass"],
        "gate_fail_seconds": totals["gate_fail"],
        "gate_unavailable_seconds": totals["gate_unavailable"],
        "active_stock_hours": totals["gate_pass"] / 3600,
        "gate_pass_share_represented": (
            totals["gate_pass"] / totals["represented"]
            if totals["represented"]
            else None
        ),
        "gate_pass_share_valid": (
            totals["gate_pass"] / totals["gate_valid"]
            if totals["gate_valid"]
            else None
        ),
        "minimum_daily_active_stock_hours": min(active_hours),
        "maximum_daily_active_stock_hours": max(active_hours),
        "mean_daily_active_stock_hours": sum(active_hours) / len(active_hours),
        "member_rows_unique": True,
    }
    summary_path = output / "daily_active_tape_hours_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    artifacts = {
        "schema": "daily_active_tape_hours_artifacts_v1",
        "source_member_gate_accounting_sha256": summary[
            "source_member_gate_accounting"
        ]["sha256"],
        "source_config_sha256": summary["source_config"]["sha256"],
        "renderer": {
            "name": Path(__file__).name,
            "sha256": _sha256(Path(__file__)),
        },
        "outputs": {
            path.name: {"bytes": path.stat().st_size, "sha256": _sha256(path)}
            for path in (csv_path, png_path, svg_path, summary_path)
        },
    }
    (output / "artifacts.json").write_text(
        json.dumps(artifacts, indent=2, sort_keys=True) + "\n"
    )
    return summary
