"""Hand-calculated checks for the six-panel fixed-bin reducer."""
import numpy as np
import pyarrow as pa

from tape_data_product.analysis.endpoint_joint_preview import Axis, PanelAccumulator


def _batch(rows):
    return pa.RecordBatch.from_pylist(
        rows,
        schema=pa.schema(
            [
                ("session_date", pa.string()),
                ("symbol", pa.string()),
                ("x", pa.float64()),
                ("x_reason_mask", pa.uint16()),
                ("y", pa.float64()),
                ("y_reason_mask", pa.uint16()),
            ]
        ),
    )


def _panel():
    return PanelAccumulator(
        "fixture",
        30,
        "spread",
        "x",
        "y",
        Axis("x", "bps", "log", (1.0, 2.0, 4.0)),
        Axis("y", "bps", "log", (10.0, 20.0, 40.0)),
    )


ROWS = [
    {"session_date": "2026-01-02", "symbol": "A", "x": 0.0, "x_reason_mask": 0, "y": 0.0, "y_reason_mask": 0},
    {"session_date": "2026-01-02", "symbol": "A", "x": 0.5, "x_reason_mask": 0, "y": 5.0, "y_reason_mask": 0},
    {"session_date": "2026-01-02", "symbol": "A", "x": 1.0, "x_reason_mask": 0, "y": 10.0, "y_reason_mask": 0},
    {"session_date": "2026-01-02", "symbol": "A", "x": 2.0, "x_reason_mask": 0, "y": 20.0, "y_reason_mask": 0},
    {"session_date": "2026-01-02", "symbol": "A", "x": 4.0, "x_reason_mask": 0, "y": 40.0, "y_reason_mask": 0},
    {"session_date": "2026-01-02", "symbol": "A", "x": 5.0, "x_reason_mask": 0, "y": 50.0, "y_reason_mask": 0},
    {"session_date": "2026-01-02", "symbol": "A", "x": None, "x_reason_mask": 8, "y": 15.0, "y_reason_mask": 0},
    {"session_date": "2026-01-02", "symbol": "A", "x": 3.0, "x_reason_mask": 0, "y": None, "y_reason_mask": 16},
    {"session_date": "2026-01-02", "symbol": "A", "x": None, "x_reason_mask": 8, "y": None, "y_reason_mask": 16},
]


def test_boundaries_zeros_unavailable_and_off_axis_reconcile():
    panel = _panel()
    panel.add(_batch(ROWS))
    summary = panel.finish()
    assert (summary["selected"], summary["pair_valid"], summary["unavailable"]) == (9, 6, 3)
    assert (summary["x_invalid_only"], summary["y_invalid_only"], summary["both_invalid"]) == (1, 1, 1)
    assert (summary["plotted"], summary["off_axis_or_log_zero"]) == (3, 3)
    assert (summary["x_zero"], summary["y_zero"]) == (1, 1)
    # zero/zero, underflow/underflow, exact lower edges, internal edges,
    # exact final edges, and overflow/overflow each land in a distinct cell.
    assert np.count_nonzero(panel.counts) == 5
    assert panel.counts[0, 0] == 1
    assert panel.counts[1, 1] == 1
    assert panel.counts[2, 2] == 1
    assert panel.counts[3, 3] == 2
    assert panel.counts[-1, -1] == 1


def test_batch_divisions_are_identical():
    single = _panel()
    single.add(_batch(ROWS))
    divided = _panel()
    divided.add(_batch(ROWS[:4]))
    divided.add(_batch(ROWS[4:]))
    assert single.finish() == divided.finish()
    np.testing.assert_array_equal(single.counts, divided.counts)


def test_linear_participation_keeps_zero_in_displayed_first_bin():
    axis = Axis("participation", "1", "linear", (0.0, 0.5, 1.0))
    assert axis.classify(np.asarray([0.0, 0.5, 1.0, 1.1])).tolist() == [1, 2, 2, 3]
