"""Exact five-minute tape examples, saved independently of offline rendering.

The published GPUS/CAST intervals were chosen retrospectively. This adapter
preserves their SIP-event semantics and endpoint convention; it is a new bounded
implementation of the original plotting recipe. Time O(T+Q) to each slice end;
RAM is a 4,096-row source batch, 600 one-second counters, 300 feature endpoints,
and at most 250,000 compact plot events per stream per pair. Exceeding the display
cap fails explicitly instead of silently sampling prints or quote states.
"""

from __future__ import annotations

from datetime import datetime
import json
import math
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

from tape_data_product import stages

NS = 1_000_000_000
SECONDS = 300
MAX_PLOT_EVENTS = 250_000
FIELDS = (
    "movement_mean_5s_bps_300s",
    "quoted_spread_mean_bps_300s",
    "movement_mean_to_spread_300s",
    "trade_rate_300s",
    "dollar_rate_300s",
)
HISTORICAL_SLICES = (
    {"symbol": "GPUS", "date": "2026-06-18", "start": "09:50:00", "end": "09:55:00"},
    {"symbol": "CAST", "date": "2026-06-18", "start": "10:03:00", "end": "10:08:00"},
)


def _ns(day, time):
    return (
        int(
            datetime.fromisoformat(day + "T" + time)
            .replace(tzinfo=ZoneInfo("America/New_York"))
            .timestamp()
        )
        * NS
    )


def _quote_tuple(ts, row, start):
    valid = row["price_state_valid"]
    return [
        (ts - start) / NS,
        row["bid"] if valid else None,
        row["ask"] if valid else None,
        row["midpoint"] if valid else None,
    ]


def _quotes(path, day, start, end, remaining):
    from tape_data_product.features.economic_tape_state_v3 import events

    prior = None
    selected = []
    scanned = 0
    for timestamp, row in events(path, "quote", day, batch_size=4096):
        scanned += 1
        if timestamp < start:
            prior = _quote_tuple(start, row, start)
            continue
        if timestamp >= end:
            break
        item = _quote_tuple(timestamp, row, start)
        if selected and selected[-1][0] == item[0]:
            selected[-1] = item  # last stable sequence wins at an identical SIP time
        else:
            if len(selected) >= remaining:
                raise ValueError(
                    "tape quote display cap exceeded; use a smaller explicit scope"
                )
            selected.append(item)
    if not selected or selected[0][0] != 0:
        if prior is None:
            raise ValueError("tape slice has no causal quote state at its start")
        selected.insert(0, prior)
    selected.append([SECONDS, *selected[-1][1:]])
    return selected, scanned


def _trades(path, day, start, end, remaining):
    from tape_data_product.features.economic_tape_state_v3 import events

    counts = np.zeros(600, dtype=np.int64)
    dollars = np.zeros(600, dtype=np.float64)
    correction = np.zeros(600, dtype=np.float64)
    selected = []
    scanned = 0
    history = start - SECONDS * NS
    for timestamp, row in events(path, "trade", day, batch_size=4096):
        scanned += 1
        if timestamp < history:
            continue
        if timestamp >= end:
            break
        if not row["eligible"]:
            continue
        index = (timestamp - history) // NS
        counts[index] += 1
        # Kahan accumulation avoids retaining all values for math.fsum.
        value = float(row["price"]) * float(row["shares"]) - correction[index]
        total = dollars[index] + value
        correction[index] = (total - dollars[index]) - value
        dollars[index] = total
        if timestamp >= start:
            if len(selected) >= remaining:
                raise ValueError(
                    "tape trade display cap exceeded; prints are never silently sampled"
                )
            selected.append([(timestamp - start) / NS, float(row["price"])])
    return selected, counts, dollars, scanned


def _feature_summary(partition, spec, start, end, counts, dollars, identities):
    from tape_data_product.features.compact_product import verify_complete, joined_rows

    manifest = verify_complete(partition)
    metadata = manifest["metadata"]
    if (metadata["symbol"], metadata["session_date"]) != (spec["symbol"], spec["date"]):
        raise ValueError("tape source partition does not match configured symbol/date")
    for stream in ("quotes", "trades"):
        saved = metadata["inputs"][stream]
        if (
            saved.get("sha256") != identities[stream]["sha256"]
            or saved.get("size_bytes", saved.get("bytes"))
            != identities[stream]["bytes"]
            or saved.get("rows", identities[stream]["rows"])
            != identities[stream]["rows"]
        ):
            raise ValueError(
                "tape T/Q identities differ from feature calculation inputs"
            )
    rows = []
    for row in joined_rows(partition, metadata):
        if row["interval_end_ns"] < start:
            continue
        if row["interval_end_ns"] >= end:
            break
        rows.append(
            {
                key: row[key]
                for key in (
                    "interval_end_ns",
                    *FIELDS,
                    *(f + "_eda_eligible" for f in FIELDS),
                )
            }
        )
        if len(rows) > SECONDS:
            raise ValueError("tape endpoint count exceeds five-minute grid")
    if [r["interval_end_ns"] for r in rows] != [start + i * NS for i in range(SECONDS)]:
        raise ValueError("tape compact feature endpoints are incomplete or misaligned")
    for index, row in enumerate(rows):
        for field, raw in (
            ("trade_rate_300s", int(counts[index : index + 300].sum()) / 300),
            ("dollar_rate_300s", math.fsum(dollars[index : index + 300]) / 300),
        ):
            if not row[field + "_eda_eligible"]:
                raise ValueError(
                    "tape example requires eligible 300s activity for reconciliation"
                )
            if not math.isclose(
                row[field],
                raw,
                rel_tol=1e-12,
                abs_tol=1e-8 if field.startswith("dollar") else 1e-12,
            ):
                raise ValueError(
                    "tape raw/stored trailing activity reconciliation failed: " + field
                )
        if all(row[f + "_eda_eligible"] for f in FIELDS[:3]):
            expected = row[FIELDS[0]] / row[FIELDS[1]]
            if not math.isclose(row[FIELDS[2]], expected, rel_tol=1e-10, abs_tol=1e-12):
                raise ValueError("stored tape endpoint movement/spread ratio mismatch")
    common = [r for r in rows if all(r[f + "_eda_eligible"] for f in FIELDS)]
    specific = {}
    for field in FIELDS:
        values = [r[field] for r in rows if r[field + "_eda_eligible"]]
        specific[field] = {
            "count": len(values),
            "mean": math.fsum(values) / len(values) if values else None,
        }
    return {
        "common_five_feature_eligible_count": len(common),
        "common_five_feature_means": {
            f: math.fsum(r[f] for r in common) / len(common) if common else None
            for f in FIELDS
        },
        "feature_specific": specific,
        "displayed_eligible_trades_per_second": int(counts[300:].sum()) / 300,
        "activity_reconciliation": "passed",
        "endpoint_grid": "[start,end), 300 stored trailing endpoints",
        "partition_manifest": stages.file_identity(Path(partition) / "manifest.json"),
    }


def aggregate_tape(config, output, *, synthetic=False):
    output = Path(output)
    slices = config.get("slices", [])
    if len(slices) != 2:
        raise ValueError(
            "tape comparison needs exactly two explicit five-minute slices"
        )
    output.mkdir(parents=True, exist_ok=False)
    pair = []
    quote_budget = trade_budget = MAX_PLOT_EVENTS
    for spec in slices:
        start, end = _ns(spec["date"], spec["start"]), _ns(spec["date"], spec["end"])
        if end - start != SECONDS * NS:
            raise ValueError("tape intervals must be exactly 300 seconds")
        identities = {
            name: stages.file_identity(spec[name]) for name in ("quotes", "trades")
        }
        for name, expected in spec.get("historical_tq_identities", {}).items():
            if any(
                identities[name].get(key) != value for key, value in expected.items()
            ):
                raise ValueError(
                    "tape input differs from configured historical identity"
                )
        quotes, quote_scanned = _quotes(
            spec["quotes"], spec["date"], start, end, quote_budget
        )
        trades, counts, dollars, trade_scanned = _trades(
            spec["trades"], spec["date"], start, end, trade_budget
        )
        quote_budget -= len(quotes)
        trade_budget -= len(trades)
        summary = _feature_summary(
            spec["partition"], spec, start, end, counts, dollars, identities
        )
        # Reject source mutation during extraction; this is independent of filenames.
        if identities != {
            name: stages.file_identity(spec[name]) for name in identities
        }:
            raise ValueError("canonical tape input changed during extraction")
        first = next(
            (
                q[3]
                for q in quotes
                if q[3] is not None and math.isfinite(q[3]) and q[3] > 0
            ),
            None,
        )
        if first is None:
            raise ValueError("tape has no valid reference midpoint")
        pair.append(
            {
                "spec": {key: spec[key] for key in ("date", "symbol", "start", "end")},
                "quotes": quotes,
                "trades": trades,
                "reference_midpoint": first,
                "trade_count_1s": counts[300:].tolist(),
                "summary": summary,
                "source_identities": identities,
                "source_rows_scanned": {
                    "quotes": quote_scanned,
                    "trades": trade_scanned,
                },
            }
        )
    payload = {
        "schema": "report_tape_v1",
        "synthetic": bool(synthetic),
        "pair": pair,
        "selection": (
            "Invented offline examples"
            if synthetic
            else "Explicit retrospective illustrative intervals; no deterministic original selection rule"
        ),
        "display": {
            "price": "10000*(price/first_valid_midpoint - 1) bps",
            "quotes": "causal SIP states, last sequence at tied timestamp; invalid state is gap",
            "trades": "all frozen-eligible prints in [start,end)",
            "x": "elapsed seconds 0..300",
        },
        "resource_contract": {
            "source_batch_rows": 4096,
            "maximum_plot_events_per_stream": MAX_PLOT_EVENTS,
        },
    }
    with (output / "tape.json").open("x") as stream:
        json.dump(payload, stream, allow_nan=False)
    with (output / "table.json").open("x") as stream:
        json.dump(
            [{**item["spec"], **item["summary"]} for item in pair],
            stream,
            indent=2,
            allow_nan=False,
        )
    return {
        "state": "complete",
        "synthetic": bool(synthetic),
        "slices": len(pair),
        "activity_reconciliation": "passed",
    }


def render_tape(source, output, *, dpi=120):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    source, output = Path(source), Path(output)
    if (source / "tape.json").stat().st_size > 128 * 1024**2:
        raise ValueError("saved tape exceeds 128 MiB render cap")
    data = json.loads((source / "tape.json").read_text())
    if data["schema"] != "report_tape_v1" or len(data["pair"]) != 2:
        raise ValueError("invalid saved tape comparison")
    fig, axes = plt.subplots(
        2, 2, figsize=(13, 7), sharex="col", gridspec_kw={"height_ratios": [3, 1]}
    )
    low = high = 0.0
    for column, item in enumerate(data["pair"]):
        quotes = np.array(item["quotes"], dtype=float)
        trades = np.array(item["trades"], dtype=float).reshape((-1, 2))
        if len(quotes) > MAX_PLOT_EVENTS + 2 or len(trades) > MAX_PLOT_EVENTS:
            raise ValueError("saved tape event cap exceeded")
        baseline = item["reference_midpoint"]
        quote_bps = 10000 * (quotes[:, 1:] / baseline - 1)
        trade_bps = 10000 * (trades[:, 1] / baseline - 1)
        for index, (label, color) in enumerate(
            (("Bid", "#00866A"), ("Ask", "#D84A5B"), ("Midpoint", "#68737E"))
        ):
            axes[0, column].step(
                quotes[:, 0],
                quote_bps[:, index],
                where="post",
                color=color,
                linewidth=0.65,
                label=label,
            )
        axes[0, column].scatter(
            trades[:, 0],
            trade_bps,
            s=2,
            alpha=0.15,
            color="#3679C5",
            linewidths=0,
            rasterized=True,
        )
        finite = quote_bps[np.isfinite(quote_bps)]
        if len(finite):
            low, high = min(low, finite.min()), max(high, finite.max())
        if len(trade_bps):
            low, high = min(low, trade_bps.min()), max(high, trade_bps.max())
        counts = item["trade_count_1s"]
        if len(counts) != 300 or sum(counts) != len(trades):
            raise ValueError("tape count bars do not account for all plotted prints")
        axes[1, column].bar(np.arange(300) + 0.5, counts, width=1, color="#3679C5")
        spec = item["spec"]
        axes[0, column].set_title(
            f"{spec['symbol']} · {spec['date']} · [{spec['start']}, {spec['end']}) ET"
        )
        axes[1, column].set_xlabel("Elapsed seconds")
    padding = max(12.0, 0.055 * (high - low))
    for ax in axes[0]:
        ax.set_ylim(low - padding, high + padding)
        ax.legend(loc="upper right", fontsize=8)
    for ax in axes.flat:
        ax.set_xlim(0, 300)
        ax.grid(alpha=0.15)
    axes[0, 0].set_ylabel("Price from first midpoint (bps)")
    axes[1, 0].set_ylabel("Eligible trades / 1s")
    fig.suptitle(
        "SYNTHETIC OFFLINE DEMONSTRATION"
        if data["synthetic"]
        else "Retrospective five-minute tape comparison"
    )
    fig.tight_layout(rect=(0, 0.06, 1, 0.96))
    fig.text(
        0.03,
        0.018,
        "Causal SIP quote states; all eligible prints. Trailing feature means include prior history. Descriptive comparison only.",
        fontsize=8,
    )
    path = output / "tape_comparison.png"
    if path.exists():
        raise ValueError("tape figure exists")
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    with (output / "tape_verification.json").open("x") as stream:
        json.dump(
            {
                "schema": "tape_export_v1",
                "state": "passed",
                "source": stages.file_identity(source / "tape.json"),
                "figure": stages.file_identity(path),
                "axes": data["display"],
                "trade_bar_accounting": "exact",
                "synthetic": data["synthetic"],
            },
            stream,
            indent=2,
        )
    return path
