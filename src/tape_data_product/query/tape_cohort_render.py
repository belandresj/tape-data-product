"""Offline-only rendering from committed cohort result artifacts."""

from pathlib import Path
import json


def render(run):
    root = Path(run)
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest.get("state") != "complete":
        raise ValueError("cannot render incomplete run")
    summary = json.loads((root / "summary.json").read_text())
    text = (
        "# Tape cohort supply summary\n\n"
        f"This descriptive retrieval covered {summary['dates']} acquired dates and {summary['members']} symbol-days. "
        f"It returned {summary['windows']} hysteresis windows and {summary['active_seconds']/60:.2f} active minutes. "
        f"Mean distinct qualifying stocks per acquired date was {summary['mean_stocks_per_date']:.6g}.\n\n"
        "The cohort is causal at one-second endpoint resolution, but this is not a trade model: future returns, costs, latency, capacity, and executable expectancy remain untested.\n"
    )
    path = root / "supply_summary.md"
    path.write_text(text)
    return {"summary": str(path), "network_requests": 0}
