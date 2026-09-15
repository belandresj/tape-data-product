"""Focused representation checks for the bounded raw replay hot path."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tape_data_product.contracts.config import ContractError, DEFAULT_CONFIG
from tape_data_product.contracts.source import MAX_UNITS
from tape_data_product.replay.builder import (
    TRADE_COLUMNS,
    _IntervalIndex,
    _add_admitted_share_units,
    _event_rows,
    _trade_class,
)


TRADE_SCHEMA = pa.schema([
    pa.field("sip_timestamp", pa.int64()),
    pa.field("sequence_number", pa.int64()),
    pa.field("participant_timestamp", pa.int64()),
    pa.field("price", pa.float64()),
    pa.field("decimal_size", pa.string()),
    pa.field("size", pa.float64()),
    pa.field("conditions", pa.list_(pa.int64())),
    pa.field("correction", pa.int64()),
])


def _reference_rows(path, stop_ns):
    result = []
    for batch in pq.ParquetFile(path).iter_batches(batch_size=1, columns=list(TRADE_COLUMNS), use_threads=False):
        names = batch.schema.names
        for values in zip(*(batch.column(i).to_pylist() for i in range(batch.num_columns))):
            row = dict(zip(names, values))
            if row["sip_timestamp"] >= stop_ns:
                return result
            result.append(tuple(row[name] for name in TRADE_COLUMNS))
    return result


def test_primitive_event_columns_match_row_reference_with_nulls_and_ties(tmp_path):
    rows = [
        {"sip_timestamp": 100, "sequence_number": 1, "participant_timestamp": 99,
         "price": 10.0, "decimal_size": "0.100000001", "size": None,
         "conditions": [], "correction": 0},
        {"sip_timestamp": 100, "sequence_number": 2, "participant_timestamp": 100,
         "price": None, "decimal_size": None, "size": 2.0,
         "conditions": [1], "correction": None},
        {"sip_timestamp": 101, "sequence_number": 3, "participant_timestamp": 101,
         "price": 11.0, "decimal_size": "1", "size": None,
         "conditions": None, "correction": 7},
    ]
    path = tmp_path / "trades.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=TRADE_SCHEMA), path, row_group_size=1)
    expected = _reference_rows(path, 102)
    for batch_size in (1, 2, 4096):
        actual = list(_event_rows(path, TRADE_COLUMNS, batch_size, 102))
        assert actual == expected
        assert type(actual[0][0]) is int and type(actual[0][1]) is int


def test_interval_index_matches_literal_boundary_durations():
    index = _IntervalIndex.from_values(((10, 20), (30, 40)))
    assert [index.contains(point) for point in (9, 10, 19, 20, 30, 39, 40)] == [False, True, True, False, True, True, False]
    assert index.duration(5, 45) == 20
    assert index.duration(12, 35) == 13
    assert index.duration(20, 30) == 0
    assert _IntervalIndex.from_values(()).duration(0, 100) == 0


def test_checked_integer_share_accumulation_preserves_decimal128_limit():
    assert _add_admitted_share_units(MAX_UNITS - 1, 1) == MAX_UNITS
    with pytest.raises(ContractError, match="share total overflow"):
        _add_admitted_share_units(MAX_UNITS, 1)


def test_numeric_column_scalar_preserves_size_fallback_admission():
    row = (100, 1, 100, 10.0, None, 2.0, [], 0)
    assert _trade_class(row, DEFAULT_CONFIG, 0) == ("eligible", (10.0, 2_000_000_000))
