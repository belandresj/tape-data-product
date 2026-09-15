import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tape_data_product.query.endpoint_predicates import compile_predicates
from tape_data_product.query.endpoint_query_core import (
    EndpointQueryReducer,
    query_descriptor,
    query_identity,
)
from tape_data_product.query.endpoint_query_outputs import EndpointQueryExport
from tape_data_product.query.endpoint_runs import NS, RunBoundary


DESCRIPTORS = (
    {"name": "quote_metric", "reason_mask": "quote_metric_reason_mask", "sources": ["quote"]},
    {"name": "trade_metric", "reason_mask": "trade_metric_reason_mask", "sources": ["trade"]},
    {"name": "display_metric", "reason_mask": "display_metric_reason_mask", "sources": ["trade"]},
)


def predicates(raw=None):
    return compile_predicates(
        raw or [{"field": "quote_metric", "operator": ">", "value": 2}],
        DESCRIPTORS,
    )


def row(offset, quote, *, mask=0, day="2026-03-02", symbol="AAA", segment="s0", **extra):
    result = {
        "session_date": day,
        "symbol": symbol,
        "selection_segment_id": segment,
        "interval_end_ns": 1_000 * NS + offset * NS,
        "quote_metric": quote,
        "quote_metric_reason_mask": mask,
        "quote_continuity_id": 0,
        "quote_continuity_break_in_second": False,
        "trade_continuity_id": 0,
        "trade_continuity_break_in_second": False,
        "halt_active": False,
    }
    result.update(extra)
    return result


def test_predicate_boundaries_equality_intersection_and_validation():
    inclusive = predicates(
        [{
            "field": "quote_metric",
            "range": {
                "lower": 0,
                "lower_inclusive": True,
                "upper": 2,
                "upper_inclusive": False,
            },
        }]
    )
    assert inclusive.evaluate(row(0, 0)).status == "matching"
    assert inclusive.evaluate(row(1, 2)).status == "nonmatching"

    exact = predicates([
        {"field": "quote_metric", "operator": ">=", "value": 2},
        {"field": "quote_metric", "operator": "<=", "value": 2},
    ])
    assert exact.evaluate(row(0, 2)).status == "matching"
    assert exact.evaluate(row(1, 2.0000001)).status == "nonmatching"
    assert exact.to_dict()["conjunction"][0]["lower"] == 2.0
    assert exact.to_dict()["conjunction"][0]["upper"] == 2.0

    bad = [
        [{"field": "missing", "operator": ">", "value": 0}],
        [{"field": "quote_metric", "operator": ">", "value": math.inf}],
        [
            {"field": "quote_metric", "operator": ">", "value": 2},
            {"field": "quote_metric", "operator": "<=", "value": 2},
        ],
        [{"field": "quote_metric", "range": {
            "lower": None, "lower_inclusive": True,
            "upper": None, "upper_inclusive": True,
        }}],
    ]
    for raw in bad:
        with pytest.raises(ValueError):
            predicates(raw)


def test_eligibility_uses_only_predicate_dependencies_and_preserves_zero():
    selected = predicates([
        {"field": "quote_metric", "operator": ">=", "value": 0},
        {"field": "trade_metric", "operator": "<", "value": 5},
    ])
    value = row(0, 0, trade_metric=4, trade_metric_reason_mask=0,
                display_metric=None, display_metric_reason_mask=64)
    assert selected.evaluate(value).status == "matching"
    value["trade_metric"] = None
    value["trade_metric_reason_mask"] = 8
    assert selected.evaluate(value).status == "unavailable"
    value["trade_metric"] = 5
    value["trade_metric_reason_mask"] = 0
    assert selected.evaluate(value).status == "nonmatching"


def test_strict_runs_cross_batches_but_not_false_unavailable_or_relevant_breaks():
    emitted = []
    reducer = EndpointQueryReducer(predicates(), "query-1", run_sink=emitted.append)
    reducer.consume_rows([row(0, 3), row(1, 3)])
    reducer.consume_rows([row(2, 1), row(3, 3), row(4, None, mask=8)])
    reducer.consume_rows([
        row(5, 3),
        row(6, 3, quote_continuity_id=1, quote_continuity_break_in_second=True),
        row(7, 3, quote_continuity_id=1, trade_continuity_id=9,
            trade_continuity_break_in_second=True),
    ])
    summary = reducer.finish(RunBoundary("member_boundary", False))

    assert [(r["first_endpoint_ns"] // NS, r["last_endpoint_ns"] // NS,
             r["closure_reason"]) for r in emitted] == [
        (1000, 1001, "nonmatching"),
        (1003, 1003, "unavailable"),
        (1005, 1005, "source_continuity"),
        (1006, 1007, "member_boundary"),
    ]
    last = emitted[-1]
    assert last["match_count"] == 2
    assert last["represented_start_ns"] == 1005 * NS
    assert last["represented_end_ns"] == 1007 * NS
    assert last["represented_duration_seconds"] == 2
    assert last["endpoint_elapsed_seconds"] == 1
    assert summary["totals"] == {
        "selected": 8, "eligible": 7, "matching": 6,
        "nonmatching": 1, "unavailable": 1,
    }


def test_halt_and_selection_boundaries_close_and_censor_runs():
    emitted = []
    reducer = EndpointQueryReducer(predicates(), "query-2", run_sink=emitted.append)
    reducer.consume(row(0, 3, boundary_before_reason="selection_boundary",
                        boundary_before_censored=True))
    reducer.consume(row(1, 3, halt_active=True))
    reducer.consume(row(2, 3, boundary_after_reason="selection_boundary",
                        boundary_after_censored=True))
    reducer.finish()
    assert [(item["closure_reason"], item["left_censored"], item["right_censored"])
            for item in emitted] == [
        ("halt", True, False),
        ("selection_boundary", False, True),
    ]


def test_member_accounting_distinguishes_no_eligible_and_valid_zero_match():
    reducer = EndpointQueryReducer(
        predicates(), "query-3",
        planned_members=(("2026-03-02", "NONE"), ("2026-03-02", "ZERO")),
    )
    reducer.consume(row(0, None, mask=8, symbol="NONE"))
    reducer.consume(row(0, 1, symbol="ZERO"))
    summary = reducer.finish(RunBoundary("member_boundary", False))
    members = {item["symbol"]: item for item in summary["members"]}
    assert members["NONE"]["eligibility_class"] == "no_eligible_rows"
    assert members["NONE"]["match_fraction"] is None
    assert members["ZERO"]["eligibility_class"] == "valid_zero_match"
    assert members["ZERO"]["match_fraction"] == 0.0
    assert summary["match_fraction"] == 0.0
    assert all(item["matching_share"] is None for item in summary["contributions"])


def test_query_identity_is_canonical_and_extra_display_does_not_change_matches():
    selected = predicates()
    base = dict(
        reference_identity="ref",
        selection={"version": "synthetic-selection-v1", "session": "rth"},
        predicates=selected,
        display_fields=[],
        field_descriptors=DESCRIPTORS,
        contract_identity="contract",
        implementation_identity="implementation",
    )
    first = query_descriptor(**base)
    assert query_identity(first) == query_identity(query_descriptor(**base))
    with_display = query_descriptor(**{**base, "display_fields": ["display_metric"]})
    assert query_identity(first) != query_identity(with_display)
    test_row = row(0, 3, display_metric=None, display_metric_reason_mask=64)
    assert selected.evaluate(test_row).status == "matching"


def test_empty_export_retains_schemas_and_complete_accounting(tmp_path):
    selected = predicates()
    descriptor = query_descriptor(
        reference_identity="ref",
        selection={"version": "synthetic-selection-v1"},
        predicates=selected,
        display_fields=[],
        field_descriptors=DESCRIPTORS,
        contract_identity="contract",
        implementation_identity="implementation",
    )
    identity = query_identity(descriptor)
    observation_schema = pa.schema([
        pa.field("session_date", pa.string(), False),
        pa.field("symbol", pa.string(), False),
        pa.field("interval_end_ns", pa.int64(), False),
        pa.field("quote_metric", pa.float64(), True),
        pa.field("quote_metric_reason_mask", pa.uint16(), False),
    ])
    export = EndpointQueryExport(
        tmp_path / "query", observation_schema=observation_schema,
        descriptor=descriptor, identity=identity, buffer_rows=1,
    )
    reducer = EndpointQueryReducer(
        selected, identity,
        observation_sink=export.add_observation,
        run_sink=export.add_run,
        planned_members=(("2026-03-02", "NONE"),),
    )
    reducer.consume(row(0, None, mask=8, symbol="NONE"))
    summary = reducer.finish(RunBoundary("member_boundary", False))
    receipt = export.finish(summary)
    assert receipt["complete"] is True
    assert pq.read_table(tmp_path / "query/matching_observations.parquet").schema == observation_schema
    assert pq.read_table(tmp_path / "query/matching_observations.parquet").num_rows == 0
    assert pq.read_table(tmp_path / "query/strict_runs.parquet").num_rows == 0
    accounting = pq.read_table(tmp_path / "query/member_accounting.parquet").to_pylist()
    assert accounting[0]["eligibility_class"] == "no_eligible_rows"
