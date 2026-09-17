"""Differential and literal-oracle coverage for the columnar batch validator."""
from dataclasses import replace
from decimal import Decimal

import pyarrow as pa
import pytest

from scalar_validation_reference import validate_batch as scalar_validate_batch
from tape_data_product.contracts import (
    BASE_SCHEMA, DEFAULT_CONFIG, EWView, FeatureConfig, Reason,
    feature_registry,
)
from tape_data_product.contracts.config import ContractError
from tape_data_product.contracts.schemas import feature_schema, support_schema
from tape_data_product.contracts.validation import validate_batch
from test_endpoint_contracts import base_row, feature_row


def _batch(rows, schema):
    return pa.RecordBatch.from_pylist(rows, schema=schema)


def _support_row(config=DEFAULT_CONFIG, second=1):
    schema = support_schema(config)
    row = {field.name: 0 for field in schema}
    row.update({name: base_row(second)[name] for name in
                ("session_date", "symbol", "interval_end_ns")})
    return row


def _outcome(validator, batch, kind, **kwargs):
    try:
        return "accepted", validator(batch, kind, **kwargs)
    except ContractError:
        return "rejected", None


def _assert_differential(batch, kind, expected, **kwargs):
    scalar = _outcome(scalar_validate_batch, batch, kind, **kwargs)
    columnar = _outcome(validate_batch, batch, kind, **kwargs)
    assert scalar[0] == columnar[0] == expected
    if expected == "accepted":
        assert scalar[1] == columnar[1]


@pytest.mark.parametrize(
    ("kind", "rows", "schema"),
    [
        ("base", [base_row(1), base_row(2)], BASE_SCHEMA),
        ("features", [
            feature_row(),
            {**feature_row(), "interval_end_ns": base_row(2)["interval_end_ns"]},
        ], feature_schema()),
        ("support", [_support_row(second=1), _support_row(second=2)], support_schema()),
    ],
)
def test_valid_all_table_kinds_match_scalar_and_return_literal_final_key(kind, rows, schema):
    arrow_batch = _batch(rows, schema)
    _assert_differential(arrow_batch, kind, "accepted")
    assert validate_batch(arrow_batch, kind) == (
        "2026-07-01", "TEST", base_row(2)["interval_end_ns"]
    )


@pytest.mark.parametrize("kind,schema", [
    ("base", BASE_SCHEMA), ("features", feature_schema()), ("support", support_schema())
])
def test_empty_batches_preserve_previous_key(kind, schema):
    previous = ("2026-07-01", "TEST", base_row(1)["interval_end_ns"])
    empty = _batch([], schema)
    _assert_differential(empty, kind, "accepted", previous_key=previous)
    assert validate_batch(empty, kind, previous_key=previous) == previous


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ({"bid_end_usd": None}, "rejected"),
        ({"bid_end_usd": None, "ask_end_usd": None, "price_end_reason_mask": 4096}, "rejected"),
        ({"ask_end_usd": float("nan")}, "rejected"),
        ({"ask_end_usd": float("inf")}, "rejected"),
        ({"price_valid_duration_ns": 0}, "rejected"),
        ({"quote_source_status": 2}, "rejected"),
        ({"midpoint_age_status": 2}, "rejected"),
        ({"halt_active": True}, "rejected"),
    ],
)
def test_base_representative_corruptions_have_literal_outcomes(mutation, expected):
    row = base_row()
    row.update(mutation)
    _assert_differential(_batch([row], BASE_SCHEMA), "base", expected)


def test_exact_decimal_quantity_survives_columnar_validation():
    row = base_row()
    exact = Decimal("12345678901234567890123456789.123456789")
    row["share_volume_1s"] = exact
    arrow_batch = _batch([row], BASE_SCHEMA)
    _assert_differential(arrow_batch, "base", "accepted")
    assert arrow_batch.column(BASE_SCHEMA.get_field_index("share_volume_1s"))[0].as_py() == exact


def test_twap_and_ratio_tolerance_boundaries_are_literal_oracles():
    row = base_row()
    row["midpoint_twap_usd"] = 101.0 + 0.9e-10
    _assert_differential(_batch([row], BASE_SCHEMA), "base", "accepted")
    row["midpoint_twap_usd"] = 101.0 + 1.1e-10
    _assert_differential(_batch([row], BASE_SCHEMA), "base", "rejected")

    row = feature_row()
    row["midpoint_rms_5s_to_spread_hl30s"] = 0.2 + 1.9e-11
    _assert_differential(_batch([row], feature_schema()), "features", "accepted")
    row["midpoint_rms_5s_to_spread_hl30s"] = 0.2 + 2.1e-11
    _assert_differential(_batch([row], feature_schema()), "features", "rejected")


def test_nullable_feature_predicates_cannot_disappear_from_reduction():
    row = feature_row()
    for stem in ("midpoint_rms_5s_bps", "movement_participation",
                 "midpoint_rms_5s_to_spread"):
        name = stem + "_hl30s"
        row[name] = None
        row[name + "_reason_mask"] = int(Reason.NO_SUPPORTED_DATA)
    _assert_differential(_batch([row], feature_schema()), "features", "accepted")
    row["midpoint_rms_5s_to_spread_hl30s_reason_mask"] = 0
    _assert_differential(_batch([row], feature_schema()), "features", "rejected")


def test_grid_gaps_duplicates_member_changes_and_cross_batch_keys():
    first = _batch([base_row(1)], BASE_SCHEMA)
    previous = validate_batch(first, "base")
    _assert_differential(_batch([base_row(2)], BASE_SCHEMA), "base", "accepted",
                         previous_key=previous)
    _assert_differential(_batch([base_row(3)], BASE_SCHEMA), "base", "rejected",
                         previous_key=previous)
    _assert_differential(_batch([base_row(1), base_row(1)], BASE_SCHEMA), "base", "rejected")
    changed = base_row(2)
    changed["symbol"] = "OTHER"
    _assert_differential(_batch([base_row(1), changed], BASE_SCHEMA), "base", "rejected")


def test_support_nonfinite_ordering_and_bounds():
    row = _support_row()
    row["return_possible_weight_hl30s"] = float("inf")
    _assert_differential(_batch([row], support_schema()), "support", "rejected")
    row = _support_row()
    row["return_usable_weight_hl30s"] = 1.0
    _assert_differential(_batch([row], support_schema()), "support", "rejected")
    row = _support_row()
    row["trade_age_sample_count_window60s"] = 2
    row["trade_age_elapsed_slots_window60s"] = 1
    _assert_differential(_batch([row], support_schema()), "support", "rejected")


def test_alternate_configuration_is_precomputed_without_changing_semantics():
    config = FeatureConfig(
        views=(EWView(45, 90),),
        age_windows_seconds=(30,),
        spread_min_coverage=.9,
        other_min_coverage=.8,
        age_min_coverage=.9,
        max_trade_reporting_age_ns=1_000_000_000,
    )
    schema = feature_schema(config)
    row = {field.name: 0 for field in schema}
    row.update({name: base_row()[name] for name in ("session_date", "symbol", "interval_end_ns")})
    for feature in feature_registry(config):
        row[feature.name] = (
            .5 if feature.family == "participation"
            else .2 if feature.family == "ratio"
            else 10.0 if feature.family == "spread"
            else 2.0
        )
    _assert_differential(_batch([row], schema), "features", "accepted", config=config)
    row["movement_participation_hl45s"] = 1.0000000001
    _assert_differential(_batch([row], schema), "features", "rejected", config=config)


def test_batch_limit_is_exact():
    schema = support_schema()
    rows = [_support_row(second=i + 1) for i in range(25000)]
    _assert_differential(_batch(rows, schema), "support", "accepted")
    extra = pa.concat_batches([_batch(rows, schema), _batch([_support_row(second=25001)], schema)])
    _assert_differential(extra, "support", "rejected")
