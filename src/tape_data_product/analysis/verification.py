"""Independent integer accounting of saved report numerical intermediates."""

import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from . import aggregates, selection


def verify_numerical(directory):
    """Stream exact atoms and reconcile zeros, sessions, joint tails and bands.

    This proves the saved distributions agree with saved population counts. It
    does not independently reconstruct T/Q features or historical selection.
    """
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    aggregates.validate_bundle(directory, manifest["binding"])
    populations = json.loads((directory / "coverage/populations.json").read_text())
    gate = json.loads((directory / "coverage/gate.json").read_text())
    status = json.loads((directory / "coverage/midpoint_status.json").read_text())
    sessions = (*selection.SESSIONS, "pooled")
    for horizon in selection.HORIZONS:
        for session in sessions:
            counts = gate[f"{horizon}s"][session]
            if (
                counts["passing"] + counts["failing"] != counts["gate_eligible"]
                or counts["gate_eligible"] + counts["unavailable"]
                != counts["post_discovery"]
            ):
                raise ValueError("gate partition accounting mismatch")
            for scope, expected in (
                ("post_discovery", counts["post_discovery"]),
                ("gate_passing", counts["passing"]),
            ):
                item = status[f"{horizon}s"][scope][session]
                if (
                    sum(item[s] for s in aggregates.MIDPOINT_STATUSES)
                    != item["support_rows_provided"]
                    or item["support_rows_provided"] + item["support_rows_missing"]
                    != expected
                ):
                    raise ValueError("midpoint observation accounting mismatch")
    for feature, table in manifest["exact_tables"].items():
        last_value = None
        cumulative = np.zeros(4, dtype=np.int64)
        zero = np.zeros(4, dtype=np.int64)
        distinct = 0
        with pq.ParquetFile(directory / table["path"]) as parquet:
            for batch in parquet.iter_batches(batch_size=4096, use_threads=False):
                values = batch["value"].to_numpy()
                counts = np.column_stack(
                    [batch[s + "_count"].to_numpy() for s in sessions]
                )
                saved = np.column_stack(
                    [batch[s + "_cumulative"].to_numpy() for s in sessions]
                )
                if (
                    not np.isfinite(values).all()
                    or np.any(values < 0)
                    or np.any(np.diff(values) <= 0)
                    or (
                        len(values)
                        and last_value is not None
                        and values[0] <= last_value
                    )
                ):
                    raise ValueError(
                        "exact ECDF values are not ordered distinct nonnegative atoms"
                    )
                if np.any(counts < 0) or not np.array_equal(
                    counts[:, :3].sum(axis=1), counts[:, 3]
                ):
                    raise ValueError("exact ECDF session count mismatch")
                if not np.array_equal(np.cumsum(counts, axis=0) + cumulative, saved):
                    raise ValueError("exact ECDF cumulative count mismatch")
                if len(values):
                    cumulative = saved[-1].copy()
                    last_value = values[-1]
                zero += counts[values == 0].sum(axis=0)
                distinct += len(values)
        expected = populations["features"][feature]["sessions"]
        if distinct != table["distinct_values"]:
            raise ValueError("exact ECDF distinct count mismatch")
        for index, session in enumerate(sessions):
            if (
                cumulative[index] != table["totals"][session]
                or cumulative[index] != expected[session]["eligible"]
                or zero[index] != expected[session]["zero"]
            ):
                raise ValueError("exact ECDF denominator or zero mismatch")
    for horizon in selection.HORIZONS:
        saved_joints = {}
        for name in (*aggregates.JOINT_STEMS, *aggregates.BANDED_JOINT_STEMS):
            path = directory / f"joints/{horizon}s/{name}"
            note = json.loads(path.with_suffix(".json").read_text())
            with np.load(path.with_suffix(".npz")) as arrays:
                counts = arrays["counts"]
                if np.any(counts < 0) or counts.shape != tuple(note["counts_shape"]):
                    raise ValueError("invalid saved joint shape/counts")
                if not np.array_equal(
                    arrays["x_edges"], note["x_edges"]
                ) or not np.array_equal(arrays["y_edges"], note["y_edges"]):
                    raise ValueError("joint axis identity mismatch")
            saved_joints[name] = counts
            banded = name in aggregates.BANDED_JOINT_STEMS
            for band in range(4) if banded else (None,):
                grid = counts[:, band] if banded else counts
                coverage = (
                    note["coverage"][selection.RATE_BAND_LABELS[band]]
                    if banded
                    else note["coverage"]
                )
                for index, session in enumerate(sessions):
                    cells = grid[index] if index < 3 else grid.sum(axis=0)
                    expected = coverage[session]
                    inside = cells.copy()
                    inside[[1, -1], :] = 0
                    inside[:, [1, -1]] = 0
                    actual = {
                        "eligible": int(cells.sum()),
                        "off_axis": int(cells.sum() - inside.sum()),
                        "x_zero": int(cells[0].sum()),
                        "y_zero": int(cells[:, 0].sum()),
                        "zero_zero": int(cells[0, 0]),
                        "x_below": int(cells[1].sum()),
                        "x_above": int(cells[-1].sum()),
                        "y_below": int(cells[:, 1].sum()),
                        "y_above": int(cells[:, -1].sum()),
                    }
                    if actual != expected:
                        raise ValueError(
                            "joint zero/tail/denominator accounting mismatch"
                        )
        for base in ("movement_spread", "movement_participation"):
            if not np.array_equal(
                saved_joints[base], saved_joints[base + "_rate_bands"].sum(axis=1)
            ):
                raise ValueError(
                    "activity-band mass does not reconcile to pair baseline"
                )
    return {
        "state": "passed",
        "exact_tables": len(manifest["exact_tables"]),
        "horizons": [60, 300],
        "accounting": [
            "gate",
            "midpoint status",
            "ECDF counts and zeros",
            "joint zero/tail mass",
            "all four activity bands",
        ],
    }
