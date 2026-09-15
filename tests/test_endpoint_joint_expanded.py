"""Literal activity gate, band, session, and restart checks."""
import numpy as np
import pyarrow as pa

from tape_data_product.analysis.endpoint_joint_expanded import (
    EXPANDED_FIELDS,
    ExpandedAccumulator,
    activity_band_ids,
    activity_gate,
)


def test_activity_band_boundaries_and_unavailable():
    values = np.asarray([0.0, 0.5, 1.0, 9.999, 10.0, 29.999, 30.0, 99.999, 100.0, 500.0, np.nan])
    valid = np.asarray([True] * 10 + [False])
    assert activity_band_ids(values, valid).tolist() == [0, 1, 2, 2, 3, 3, 4, 4, 5, 5, -1]


def test_activity_gate_inclusive_edges_and_pair_specific_unavailable():
    rate = np.asarray([1.0, 1.0, 0.999, 10.0, np.nan, 5.0])
    rate_mask = np.asarray([0, 0, 0, 0, 8, 0], dtype=np.uint16)
    age = np.asarray([2.0, 2.001, 1.0, np.nan, 1.0, 1.0])
    age_mask = np.asarray([0, 0, 0, 16, 0, 0], dtype=np.uint16)
    valid, passed = activity_gate(rate, rate_mask, age, age_mask)
    assert valid.tolist() == [True, True, True, False, False, True]
    assert passed.tolist() == [True, False, False, False, False, True]


def test_band_and_gate_results_are_batch_division_invariant():
    rate = np.asarray([0.0, 0.2, 1.0, 10.0, 30.0, 100.0, 5.0, np.nan])
    rate_mask = np.asarray([0, 0, 0, 0, 0, 0, 0, 8], dtype=np.uint16)
    age = np.asarray([0.0, 0.0, 2.0, 2.0, 2.001, 1.0, 1.0, 1.0])
    age_mask = np.asarray([0, 0, 0, 0, 0, 0, 0, 0], dtype=np.uint16)
    full_bands = activity_band_ids(rate, rate_mask == 0)
    full_gate = activity_gate(rate, rate_mask, age, age_mask)
    split_bands = np.concatenate([activity_band_ids(rate[:3], rate_mask[:3] == 0), activity_band_ids(rate[3:], rate_mask[3:] == 0)])
    split_valid = []
    split_passed = []
    for slc in (slice(None, 3), slice(3, None)):
        valid, passed = activity_gate(rate[slc], rate_mask[slc], age[slc], age_mask[slc])
        split_valid.append(valid)
        split_passed.append(passed)
    np.testing.assert_array_equal(full_bands, split_bands)
    np.testing.assert_array_equal(full_gate[0], np.concatenate(split_valid))
    np.testing.assert_array_equal(full_gate[1], np.concatenate(split_passed))


def _expanded_batch(rows):
    values = []
    rates = [0.0, 0.5, 1.0, 10.0, 30.0, 100.0]
    ages = [1.0, 1.0, 2.0, 2.0, 2.1, 1.0]
    for index in rows:
        row = {
            "session_date": "2026-01-02",
            "symbol": "A",
            "selection_segment_id": f"2026-01-02/A:{index // 2}",
        }
        for field in EXPANDED_FIELDS:
            if field.startswith("trade_rate"):
                value = rates[index]
            elif field.startswith("trade_age"):
                value = ages[index]
            elif field.startswith("movement_participation"):
                value = 0.5
            elif field.startswith("quoted_spread"):
                value = 10.0
            else:
                value = 2.0
            mask = 0
            if index == 3 and field.startswith("midpoint_rms"):
                value, mask = None, 8
            if index == 5 and field.startswith("movement_participation"):
                value, mask = None, 1024
            row[field] = value
            row[field + "_reason_mask"] = mask
        values.append(row)
    return pa.RecordBatch.from_pylist(values)


def _close(accumulator):
    accumulator._flush_member()
    accumulator._contribution_writer.close()


def test_expanded_accumulator_reconciles_sessions_gate_bands_and_pair_validity(tmp_path):
    accumulator = ExpandedAccumulator(tmp_path / "contributions.parquet")
    accumulator.add(_expanded_batch(range(6)))
    _close(accumulator)
    spread = accumulator.states[("pooled", "activity_gate", "all", 30, "spread")].summary()
    participation = accumulator.states[("pooled", "activity_gate", "all", 30, "participation")].summary()
    assert (spread["selected"], spread["pair_valid"], spread["unavailable"]) == (3, 2, 1)
    assert (participation["selected"], participation["pair_valid"], participation["unavailable"]) == (3, 1, 2)
    assert accumulator.states[("pooled", "activity_gate", "1_to_lt_10", 30, "spread")].summary()["pair_valid"] == 1
    assert accumulator.states[("pooled", "activity_gate", "10_to_lt_30", 30, "spread")].summary()["pair_valid"] == 0
    assert accumulator.states[("pooled", "activity_gate", "30_to_lt_100", 30, "spread")].summary()["selected"] == 0
    assert accumulator.states[("pooled", "activity_gate", "ge_100", 30, "spread")].summary()["pair_valid"] == 1
    assert accumulator.gate_global[("pooled", 30)].tolist() == [6, 6, 3]
    assert accumulator.gate_global[("premarket", 30)].tolist() == [2, 2, 0]
    assert accumulator.gate_global[("rth", 30)].tolist() == [2, 2, 2]
    assert accumulator.gate_global[("after_hours", 30)].tolist() == [2, 2, 1]


def test_expanded_accumulator_is_batch_invariant(tmp_path):
    whole = ExpandedAccumulator(tmp_path / "whole.parquet")
    whole.add(_expanded_batch(range(6)))
    _close(whole)
    divided = ExpandedAccumulator(tmp_path / "divided.parquet")
    divided.add(_expanded_batch(range(3)))
    divided.add(_expanded_batch(range(3, 6)))
    _close(divided)
    assert whole.gate_global.keys() == divided.gate_global.keys()
    for key in whole.gate_global:
        np.testing.assert_array_equal(whole.gate_global[key], divided.gate_global[key])
    for key in whole.states:
        assert whole.states[key].summary() == divided.states[key].summary()
        np.testing.assert_array_equal(whole.states[key].counts, divided.states[key].counts)
