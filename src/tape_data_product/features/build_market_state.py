#!/usr/bin/env python3
"""Reusable causal SIP-time preprocessing/replay for U.S. equity tick data.

This module is the semantic boundary between immutable Massive.com SIP Level 1
trades/quotes and downstream feature construction.  Its normal production use
is import-based: it reconstructs one symbol-day of quote state in memory and
streams semantically processed trade batches to a caller such as
``build_tape_primitives.py``. It does *not* persist event-level copies by default.

Expected raw layout::

    project/
      features/build_market_state.py
      data/raw/tick_data/
        sessions/YYYY-MM-DD/SYMBOL/trades.parquet
        sessions/YYYY-MM-DD/SYMBOL/quotes.parquet

Optional audit outputs, enabled explicitly with ``--write-intermediates``::

    data/audit/market_state/YYYY-MM-DD/SYMBOL.parquet
    data/audit/trade_events/YYYY-MM-DD/SYMBOL.parquet
    quality/market_state/YYYY-MM-DD/SYMBOL.json

Core methodology
----------------
1. ``sip_timestamp`` is the causal information-arrival clock.
2. A trade at SIP time T uses the latest quote with quote SIP time strictly
   less than T.  Equal cross-stream timestamps are never ordered by sequence.
3. ``participant_timestamp`` is diagnostic/execution time only; it never
   backdates information.
4. Current-pressure trade evidence additionally requires reporting latency
   ``0 <= sip_timestamp - participant_timestamp <= 1 second`` by default.
   Late or negative-latency rows remain observable and auditable; they are not
   silently deleted.
5. Quote validity is explicit.  A previous valid midpoint never substitutes
   for a currently invalid/one-sided/crossed state.  Locked markets may have a
   valid midpoint but are unusable for aggressor classification.
6. Aggressor classification uses only an eligible trade and the strictly prior
   usable NBBO.  There is no tick-rule fallback and no probability score.
7. Corrections/cancellations follow observed-as-known semantics.  Historical
   annotations on an original execution do not retroactively mutate prior
   pressure observations; correction/cancel action rows are not new executions.
8. Trade eligibility remains feature-family specific; there is no universal
   ``valid_trade`` flag.

The high-volume path is NumPy + PyArrow.  Quotes are processed one symbol-day
at a time; trades are streamed in batches.  No pandas DataFrames, Python
row-by-row event loops, or giant merged trade/quote event table are used.
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import os
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

try:
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover - startup guard
    raise SystemExit(
        "Missing dependency. Install with: python -m pip install numpy pyarrow"
    ) from exc


SCRIPT_VERSION = "0.2.0"
NY_TZ = ZoneInfo("America/New_York")
NS_PER_MS = 1_000_000
NS_PER_SECOND = 1_000_000_000
DEFAULT_MAX_REPORTING_LATENCY_NS = NS_PER_SECOND
PRICE_ATOL = 1e-9
QA_SAMPLE_MAX_VALUES = 200_000


def _data_root(project_root: Path) -> Path:
    configured = os.getenv("TAPE_DATA_ROOT") or os.getenv("CAPITULATION_DATA_ROOT")
    if configured:
        configured_path = Path(configured).expanduser()
        if not configured_path.is_absolute():
            configured_path = project_root / configured_path
        return configured_path.resolve()
    return project_root / "data"


# ---------------------------------------------------------------------------
# Massive semantic policy
# ---------------------------------------------------------------------------
# Conditions considered ordinary/continuous enough for short-horizon pressure
# and aggressor research. Empty/null condition lists are treated as regular.
# 0  Regular Sale
# 3  Automatic Execution
# 14 Intermarket Sweep
# 36 Yellow Flag Regular Trade
# 37 Odd Lot Trade
# 41 Trade Thru Exempt
# 60 SSR in Effect
SAFE_PRESSURE_TRADE_CONDITIONS = frozenset({0, 3, 14, 36, 37, 41, 60})

# Rolling Tape State V2 spans [04:00,20:00) ET.  Form T (12) is the ordinary
# SIP marker for an extended-hours execution, so extended-session consumers
# need a separately named population that accepts it.  The legacy pressure
# population above remains unchanged for existing RTH/V1 callers.
SAFE_EXTENDED_HOURS_ACTIVITY_TRADE_CONDITIONS = frozenset(
    {*SAFE_PRESSURE_TRADE_CONDITIONS, 12}
)

# Auxiliary trade-price use excludes odd lots because Massive's published
# condition table marks them as not updating consolidated last/high-low fields.
# Midpoint remains the primary price process for the derived dataset.
SAFE_PRICE_TRADE_CONDITIONS = frozenset({0, 3, 14, 36, 41, 60})

KNOWN_TRADE_CONDITIONS = frozenset(
    {
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
        23,
        24,
        25,
        26,
        27,
        28,
        29,
        30,
        32,
        33,
        34,
        35,
        36,
        37,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        45,
        46,
        47,
        48,
        49,
        50,
        51,
        52,
        53,
        54,
        55,
        56,
        59,
        60,
    }
)

KNOWN_QUOTE_CONDITIONS = frozenset(
    {
        -1,
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
        23,
        25,
        26,
        27,
        28,
        29,
        30,
        31,
        32,
        33,
        34,
        35,
        36,
        37,
        38,
        39,
        40,
        41,
        42,
        43,
        71,
        80,
        81,
        82,
        83,
        84,
        85,
        86,
        87,
        88,
        89,
        90,
        91,
        92,
        # Massive reference ID 94 is the CTA ``S`` CQS-generated flag.  It is
        # provenance on an otherwise complete NBBO, not an invalidating quote
        # condition.  The feed places it in the conditions list alongside the
        # actual quote condition.
        94,
    }
)

QUOTE_ONE_SIDED_CODES = frozenset({2, 34})
QUOTE_NONFIRM_CODES = frozenset({20})
QUOTE_CLOSED_OR_NO_QUOTE_CODES = frozenset({15, 19, 32})
QUOTE_INVALID_CODES = frozenset({-1, 80, 83})
QUOTE_EXPLICIT_CROSSED_CODES = frozenset({84})
QUOTE_EXPLICIT_LOCKED_CODES = frozenset({85})

# Indicators are preserved/diagnosed but do not invalidate the base midpoint.
KNOWN_QUOTE_INDICATORS = frozenset(
    {
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
        23,
        24,
        25,
        26,
        27,
        28,
        29,
        30,
        # Massive indicator 304: Short Sales Restriction in Effect.  This is
        # security-status metadata, not a halt or an invalidating quote flag.
        304,
        501,
        502,
        503,
        504,
        505,
        506,
        507,
        508,
        509,
        601,
        602,
        603,
        604,
        605,
        901,
        902,
        903,
        904,
        905,
        906,
        907,
        908,
    }
)

CORRECTION_NAMES = {
    0: "regular_or_unspecified",
    1: "original_late_corrected",
    7: "original_later_errored",
    8: "original_later_cancelled",
    10: "cancel_record",
    11: "error_record",
    12: "correction_record",
}

# Observed-as-known policy:
# - 0: ordinary event.
# - 7/8: original event later annotated as errored/cancelled.  It remains
#   eligible at its original information time because the later action was not
#   known then.
# - 1: row contains corrected data with original time; conservatively excluded
#   until explicit linkage/replay semantics are validated.
# - 10/11/12: action records, not current executions.
CAUSAL_TRADE_PAYLOAD_CORRECTIONS = frozenset({0, 7, 8})
KNOWN_CORRECTION_CODES = frozenset(CORRECTION_NAMES)

SIDE_LABELS = ["unclassified", "buyer", "seller"]
CONFIDENCE_LABELS = ["unclassified", "medium", "high"]

# Keep the prior dictionary ordering for audit-output compatibility.  The
# stale_quote reason is retained as a reserved value but is not emitted by the
# current methodology: quote age is metadata, not a hard eligibility cutoff.
METHOD_LABELS = [
    "trade_ineligible",
    "no_prior_quote",
    "quote_unusable",
    "stale_quote",
    "at_ask",
    "at_bid",
    "inside_above_mid",
    "inside_below_mid",
    "at_midpoint",
    "outside_nbbo",
    "other_unclassified",
]
METHOD_TRADE_INELIGIBLE = 0
METHOD_NO_PRIOR_QUOTE = 1
METHOD_QUOTE_UNUSABLE = 2
METHOD_STALE_QUOTE = 3  # reserved; intentionally unused
METHOD_AT_ASK = 4
METHOD_AT_BID = 5
METHOD_INSIDE_ABOVE_MID = 6
METHOD_INSIDE_BELOW_MID = 7
METHOD_AT_MIDPOINT = 8
METHOD_OUTSIDE_NBBO = 9
METHOD_OTHER = 10

QUOTE_COLUMNS = [
    "ask_exchange",
    "ask_price",
    "ask_size",
    "bid_exchange",
    "bid_price",
    "bid_size",
    "conditions",
    "indicators",
    "participant_timestamp",
    "sequence_number",
    "sip_timestamp",
    "tape",
    "trf_timestamp",
]

TRADE_COLUMNS = [
    "conditions",
    "correction",
    "decimal_size",
    "exchange",
    "id",
    "participant_timestamp",
    "price",
    "sequence_number",
    "sip_timestamp",
    "size",
    "tape",
    "trf_id",
    "trf_timestamp",
]


# ---------------------------------------------------------------------------
# Public data structures / API types
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Job:
    """One immutable raw symbol-day input pair."""

    date: str
    symbol: str
    trade_path: Path
    quote_path: Path


@dataclass(frozen=True)
class PreprocessConfig:
    """Methodology/performance configuration for causal preprocessing.

    ``max_reporting_latency_ns`` applies only to feature families intended to
    represent contemporaneous trade pressure/activity.  It intentionally does
    not erase raw condition/correction semantics or auxiliary price eligibility.
    """

    batch_size: int = 250_000
    max_reporting_latency_ns: int = DEFAULT_MAX_REPORTING_LATENCY_NS
    compression: str | None = "zstd"
    compression_level: int | None = 3
    memory_map_inputs: bool = False
    parquet_use_threads: bool = False


@dataclass(frozen=True)
class AuditPaths:
    """Optional large audit outputs.  Pass None in normal production use."""

    market_state: Path
    trade_events: Path


@dataclass
class QuoteState:
    """Causally ordered quote-event semantics for one symbol-day.

    Arrays have one element per raw quote row and remain in raw SIP order.  This
    is deliberately not a Python object per event.  ``state_change`` marks
    semantic NBBO-state transitions; raw quote refreshes remain present so a
    downstream aggregator can separately count quote updates.

    ``build_tape_primitives.py`` can use:
    - every ``sip_timestamp`` for quote-update counts;
    - ``state_change`` plus state values for time-weighted spread/depth and
      locked/crossed/valid-state durations;
    - prices/sizes/midpoint/validity for end-of-second state snapshots;
    - ``state_start_sip_timestamp`` and quote timestamps for age metadata.
    """

    sip_timestamp: np.ndarray
    sequence_number: np.ndarray
    bid_price: np.ndarray
    ask_price: np.ndarray
    bid_size: np.ndarray
    ask_size: np.ndarray
    midpoint: np.ndarray
    spread: np.ndarray
    spread_bps: np.ndarray
    price_state_valid: np.ndarray
    depth_state_valid: np.ndarray
    one_sided: np.ndarray
    nonfirm: np.ndarray
    closed: np.ndarray
    condition_invalid: np.ndarray
    locked: np.ndarray
    crossed: np.ndarray
    unknown_quote_condition: np.ndarray
    unknown_quote_indicator: np.ndarray
    aggressor_context_usable: np.ndarray
    state_change: np.ndarray
    state_id: np.ndarray
    state_start_sip_timestamp: np.ndarray

    @classmethod
    def empty(cls) -> "QuoteState":
        i64 = lambda: np.empty(0, dtype=np.int64)
        f64 = lambda: np.empty(0, dtype=np.float64)
        b = lambda: np.empty(0, dtype=np.bool_)
        return cls(
            sip_timestamp=i64(),
            sequence_number=i64(),
            bid_price=f64(),
            ask_price=f64(),
            bid_size=f64(),
            ask_size=f64(),
            midpoint=f64(),
            spread=f64(),
            spread_bps=f64(),
            price_state_valid=b(),
            depth_state_valid=b(),
            one_sided=b(),
            nonfirm=b(),
            closed=b(),
            condition_invalid=b(),
            locked=b(),
            crossed=b(),
            unknown_quote_condition=b(),
            unknown_quote_indicator=b(),
            aggressor_context_usable=b(),
            state_change=b(),
            state_id=i64(),
            state_start_sip_timestamp=i64(),
        )

    @property
    def num_rows(self) -> int:
        return int(self.sip_timestamp.size)

    def transition_indices(self) -> np.ndarray:
        """Indices of semantic state transitions, in deterministic SIP order."""
        return np.flatnonzero(self.state_change)


@dataclass
class ProcessedTradeBatch:
    """Vectorized semantics for one raw trade batch.

    The original Arrow table is retained only for this batch.  Derived numeric
    arrays are exposed directly so a feature aggregator need not decode a large
    audit table.  ``to_audit_table`` materializes the legacy-style enriched
    event table only when explicitly requested.
    """

    raw_table: pa.Table
    raw_row_start: int
    analytic_size_arrow: pa.Array
    analytic_size: np.ndarray
    sip_timestamp: np.ndarray
    participant_timestamp: np.ndarray
    sequence_number: np.ndarray
    price: np.ndarray
    reporting_latency_ns: np.ndarray
    reporting_latency_usable: np.ndarray
    late_for_current_pressure: np.ndarray
    current_pressure_semantic_eligible: np.ndarray
    eligible_price_trade: np.ndarray
    eligible_activity_trade: np.ndarray
    eligible_extended_hours_activity_trade: np.ndarray
    eligible_activity_shares: np.ndarray
    eligible_aggressor_trade: np.ndarray
    is_trf: np.ndarray
    is_odd_lot: np.ndarray
    unknown_trade_condition: np.ndarray
    pressure_condition_policy_ok: np.ndarray
    price_condition_policy_ok: np.ndarray
    correction_code: np.ndarray
    quote_state_id: np.ndarray
    has_prior_quote: np.ndarray
    prior_quote_sip_timestamp: np.ndarray
    prior_quote_sequence_number: np.ndarray
    quote_age_ns: np.ndarray
    quote_state_age_ns: np.ndarray
    reference_midpoint: np.ndarray
    reference_price_state_valid: np.ndarray
    reference_locked: np.ndarray
    reference_crossed: np.ndarray
    aggressor_side_code: np.ndarray
    aggressor_confidence_code: np.ndarray
    aggressor_method_code: np.ndarray
    qa: dict

    @property
    def num_rows(self) -> int:
        return int(self.raw_table.num_rows)

    def to_audit_table(self) -> pa.Table:
        """Materialize enriched trade rows for explicit debugging/auditing."""
        n = self.num_rows
        correction = self.correction_code
        out = self.raw_table
        out = out.append_column(
            "raw_trade_index",
            pa.array(
                np.arange(self.raw_row_start, self.raw_row_start + n, dtype=np.int64),
                type=pa.int64(),
            ),
        )
        out = out.append_column("analytic_size", self.analytic_size_arrow)
        out = out.append_column(
            "participant_to_sip_latency_ns",
            pa.array(self.reporting_latency_ns, type=pa.int64()),
        )
        trf_latency = pc.subtract(
            self.raw_table["sip_timestamp"].combine_chunks(),
            self.raw_table["trf_timestamp"].combine_chunks(),
        )
        out = out.append_column("trf_to_sip_latency_ns", trf_latency)
        out = out.append_column("is_trf", pa.array(self.is_trf, type=pa.bool_()))
        out = out.append_column(
            "is_odd_lot", pa.array(self.is_odd_lot, type=pa.bool_())
        )
        out = out.append_column(
            "unknown_trade_condition",
            pa.array(self.unknown_trade_condition, type=pa.bool_()),
        )
        out = out.append_column(
            "pressure_condition_policy_ok",
            pa.array(self.pressure_condition_policy_ok, type=pa.bool_()),
        )
        out = out.append_column(
            "price_condition_policy_ok",
            pa.array(self.price_condition_policy_ok, type=pa.bool_()),
        )
        out = out.append_column(
            "correction_code", pa.array(correction, type=pa.int16())
        )
        out = out.append_column(
            "correction_is_ordinary", pa.array(correction == 0, type=pa.bool_())
        )
        out = out.append_column(
            "correction_is_original_late_corrected",
            pa.array(correction == 1, type=pa.bool_()),
        )
        out = out.append_column(
            "correction_is_original_later_errored",
            pa.array(correction == 7, type=pa.bool_()),
        )
        out = out.append_column(
            "correction_is_original_later_cancelled",
            pa.array(correction == 8, type=pa.bool_()),
        )
        action = np.isin(correction, np.array([10, 11, 12], dtype=np.int16))
        out = out.append_column(
            "correction_is_action_record", pa.array(action, type=pa.bool_())
        )
        out = out.append_column(
            "correction_is_cancel_record", pa.array(correction == 10, type=pa.bool_())
        )
        out = out.append_column(
            "correction_is_error_record", pa.array(correction == 11, type=pa.bool_())
        )
        out = out.append_column(
            "correction_is_correction_record",
            pa.array(correction == 12, type=pa.bool_()),
        )
        out = out.append_column(
            "reporting_latency_usable",
            pa.array(self.reporting_latency_usable, type=pa.bool_()),
        )
        out = out.append_column(
            "late_for_current_pressure",
            pa.array(self.late_for_current_pressure, type=pa.bool_()),
        )
        out = out.append_column(
            "current_pressure_semantic_eligible",
            pa.array(self.current_pressure_semantic_eligible, type=pa.bool_()),
        )
        out = out.append_column(
            "eligible_price_trade", pa.array(self.eligible_price_trade, type=pa.bool_())
        )
        out = out.append_column(
            "eligible_activity_trade",
            pa.array(self.eligible_activity_trade, type=pa.bool_()),
        )
        out = out.append_column(
            "eligible_extended_hours_activity_trade",
            pa.array(self.eligible_extended_hours_activity_trade, type=pa.bool_()),
        )
        out = out.append_column(
            "eligible_activity_shares",
            pa.array(self.eligible_activity_shares, type=pa.bool_()),
        )
        out = out.append_column(
            "eligible_aggressor_trade",
            pa.array(self.eligible_aggressor_trade, type=pa.bool_()),
        )

        out = out.append_column(
            "quote_state_id", _nullable_int64(self.quote_state_id, self.has_prior_quote)
        )
        out = out.append_column(
            "prior_quote_sip_timestamp",
            _nullable_int64(self.prior_quote_sip_timestamp, self.has_prior_quote),
        )
        out = out.append_column(
            "prior_quote_sequence_number",
            _nullable_int64(self.prior_quote_sequence_number, self.has_prior_quote),
        )
        out = out.append_column(
            "quote_age_ns", _nullable_int64(self.quote_age_ns, self.has_prior_quote)
        )
        out = out.append_column(
            "quote_state_age_ns",
            _nullable_int64(self.quote_state_age_ns, self.has_prior_quote),
        )
        out = out.append_column(
            "reference_midpoint",
            pa.array(
                self.reference_midpoint,
                mask=~(self.has_prior_quote & self.reference_price_state_valid),
                type=pa.float64(),
            ),
        )
        out = out.append_column(
            "reference_price_state_valid",
            pa.array(self.reference_price_state_valid, type=pa.bool_()),
        )
        out = out.append_column(
            "reference_locked", pa.array(self.reference_locked, type=pa.bool_())
        )
        out = out.append_column(
            "reference_crossed", pa.array(self.reference_crossed, type=pa.bool_())
        )
        # Compatibility field: there is intentionally no quote-age cutoff.
        out = out.append_column(
            "quote_stale_for_aggressor", pa.array(np.zeros(n, dtype=np.bool_))
        )
        out = out.append_column(
            "aggressor_side",
            _dict_array(self.aggressor_side_code, SIDE_LABELS),
        )
        out = out.append_column(
            "aggressor_confidence",
            _dict_array(self.aggressor_confidence_code, CONFIDENCE_LABELS),
        )
        out = out.append_column(
            "aggressor_method",
            _dict_array(self.aggressor_method_code, METHOD_LABELS),
        )
        return out


@dataclass(frozen=True)
class ActivityTradeSemantics:
    """Canonical activity-trade eligibility independent of quote state.

    Historical scanners use this compact result so the condition, correction,
    analytic-size, and reporting-latency policy cannot drift from market-state
    preprocessing.  Quote alignment and aggressor classification deliberately
    remain outside this object.
    """

    analytic_size_arrow: pa.Array
    analytic_size: np.ndarray
    correction_code: np.ndarray
    unknown_trade_condition: np.ndarray
    pressure_condition_policy_ok: np.ndarray
    extended_hours_activity_condition_policy_ok: np.ndarray
    price_condition_policy_ok: np.ndarray
    is_odd_lot: np.ndarray
    base_numeric_valid: np.ndarray
    current_pressure_semantic_eligible: np.ndarray
    reporting_latency_ns: np.ndarray
    reporting_latency_usable: np.ndarray
    eligible_activity_trade: np.ndarray
    eligible_extended_hours_activity_trade: np.ndarray
    eligible_price_trade: np.ndarray


@dataclass
class SymbolDayResult:
    """Result of processing one symbol-day without materializing all trades."""

    job: Job
    quote_state: QuoteState
    quote_qa: dict
    trade_qa: dict
    qa_manifest: dict


# A caller such as build_tape_primitives.py can aggregate each processed batch and
# immediately release it.  No processed trade-day table is retained here.
TradeBatchConsumer = Callable[[ProcessedTradeBatch], None]


# ---------------------------------------------------------------------------
# General utilities
# ---------------------------------------------------------------------------
class CodeListView:
    """Efficient row-level membership for Arrow list<int> columns."""

    def __init__(self, lists: pa.Array | pa.ChunkedArray, n_rows: int):
        if isinstance(lists, pa.ChunkedArray):
            lists = lists.combine_chunks()
        self.n_rows = int(n_rows)
        if len(lists) == 0:
            self.flat = np.empty(0, dtype=np.int64)
            self.parents = np.empty(0, dtype=np.int64)
            return

        flat = pc.list_flatten(lists)
        parents = pc.list_parent_indices(lists)
        self.flat = np.asarray(flat.to_numpy(zero_copy_only=False), dtype=np.int64)
        self.parents = np.asarray(
            parents.to_numpy(zero_copy_only=False), dtype=np.int64
        )

    def any_in(self, codes: Iterable[int]) -> np.ndarray:
        out = np.zeros(self.n_rows, dtype=np.bool_)
        if self.flat.size == 0:
            return out
        code_array = np.fromiter(codes, dtype=np.int64)
        if code_array.size == 0:
            return out
        hit = np.isin(self.flat, code_array)
        if np.any(hit):
            out[self.parents[hit]] = True
        return out

    def any_not_in(self, allowed: Iterable[int]) -> np.ndarray:
        out = np.zeros(self.n_rows, dtype=np.bool_)
        if self.flat.size == 0:
            return out
        allowed_array = np.fromiter(allowed, dtype=np.int64)
        bad = ~np.isin(self.flat, allowed_array)
        if np.any(bad):
            out[self.parents[bad]] = True
        return out

    def all_in(self, allowed: Iterable[int]) -> np.ndarray:
        return ~self.any_not_in(allowed)

    def counts(self) -> Counter[int]:
        if self.flat.size == 0:
            return Counter()
        values, counts = np.unique(self.flat, return_counts=True)
        return Counter({int(v): int(c) for v, c in zip(values, counts)})

    def unique_not_in(self, allowed: Iterable[int]) -> set[int]:
        if self.flat.size == 0:
            return set()
        allowed_array = np.fromiter(allowed, dtype=np.int64)
        bad = ~np.isin(self.flat, allowed_array)
        if not np.any(bad):
            return set()
        return {int(x) for x in np.unique(self.flat[bad]).tolist()}


class _StrideSampler:
    """Deterministic bounded sample for QA percentiles.

    Exact event counts/volumes are always maintained separately.  This sampler
    avoids retaining a full extra day of latency/age arrays merely to report QA
    percentiles.  If the sample grows too large it is deterministically thinned.
    """

    def __init__(self, max_values: int = QA_SAMPLE_MAX_VALUES):
        self.max_values = int(max_values)
        self._stride = 1
        self._offset = 0
        self._parts: list[np.ndarray] = []
        self._size = 0

    def update(self, values: np.ndarray) -> None:
        if values.size == 0:
            return
        values = np.asarray(values)
        first = (-self._offset) % self._stride
        selected = values[first :: self._stride]
        self._offset += values.size
        if selected.size:
            self._parts.append(selected.astype(np.float64, copy=True))
            self._size += selected.size
        while self._size > self.max_values:
            data = self.values()
            data = data[::2]
            self._stride *= 2
            self._parts = [data]
            self._size = data.size

    def values(self) -> np.ndarray:
        if not self._parts:
            return np.empty(0, dtype=np.float64)
        if len(self._parts) == 1:
            return self._parts[0]
        data = np.concatenate(self._parts)
        self._parts = [data]
        self._size = data.size
        return data

    @property
    def stride(self) -> int:
        return self._stride


def _col_numpy(
    table: pa.Table,
    name: str,
    dtype: np.dtype | type | None = None,
) -> np.ndarray:
    arr = table[name].combine_chunks()
    out = np.asarray(arr.to_numpy(zero_copy_only=False))
    return out.astype(dtype, copy=False) if dtype is not None else out


def _nullable_int_numpy(table: pa.Table, name: str, fill_value: int = -1) -> np.ndarray:
    arr = table[name].combine_chunks()
    if arr.null_count:
        arr = pc.fill_null(arr, fill_value)
    arr = pc.cast(arr, pa.int64(), safe=False)
    return np.asarray(arr.to_numpy(zero_copy_only=False), dtype=np.int64)


def _require_non_null(table: pa.Table, names: Iterable[str], path: Path) -> None:
    bad = [name for name in names if table[name].null_count]
    if bad:
        raise ValueError(f"{path}: null values in required columns: {bad}")


def _dict_array(codes: np.ndarray, labels: Sequence[str]) -> pa.DictionaryArray:
    return pa.DictionaryArray.from_arrays(
        pa.array(codes, type=pa.int8()),
        pa.array(labels, type=pa.string()),
    )


def _nullable_int64(values: np.ndarray, valid: np.ndarray) -> pa.Array:
    return pa.array(values, mask=~valid, type=pa.int64())


def _float_changed(a: np.ndarray) -> np.ndarray:
    if a.size <= 1:
        return np.empty(0, dtype=np.bool_)
    left = a[:-1]
    right = a[1:]
    both_nan = np.isnan(left) & np.isnan(right)
    return (left != right) & ~both_nan


def _scalar_changed(current: object, previous: object) -> bool:
    try:
        if isinstance(current, (float, np.floating)) and isinstance(
            previous, (float, np.floating)
        ):
            if math.isnan(float(current)) and math.isnan(float(previous)):
                return False
    except TypeError:
        pass
    return bool(current != previous)


def _counter_to_json(counter: Counter[int] | Counter[str]) -> dict[str, int]:
    return {
        str(k): int(v) for k, v in sorted(counter.items(), key=lambda kv: str(kv[0]))
    }


def _sample_percentiles(
    sampler: _StrideSampler,
    scale: float = 1.0,
    exact_max: float | None = None,
) -> dict[str, float | int | str | None]:
    data = sampler.values()
    if data.size == 0:
        return {
            "p50": None,
            "p90": None,
            "p99": None,
            "max": None if exact_max is None else float(exact_max / scale),
            "sample_size": 0,
            "sample_stride": sampler.stride,
            "percentile_method": "deterministic_stride_sample",
        }
    data = data / scale
    return {
        "p50": float(np.percentile(data, 50)),
        "p90": float(np.percentile(data, 90)),
        "p99": float(np.percentile(data, 99)),
        "max": float(np.max(data)) if exact_max is None else float(exact_max / scale),
        "sample_size": int(data.size),
        "sample_stride": sampler.stride,
        "percentile_method": "deterministic_stride_sample",
    }


def _session_boundary_ns(date_str: str, hour: int, minute: int = 0) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(
        hour=hour,
        minute=minute,
        second=0,
        microsecond=0,
        tzinfo=NY_TZ,
    )
    return int(dt.timestamp()) * NS_PER_SECOND


def _tmp_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.tmp-{os.getpid()}")


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(path)
    try:
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _writer_kwargs(config: PreprocessConfig) -> dict:
    kwargs = {
        "compression": config.compression,
        "use_dictionary": True,
        "write_statistics": True,
    }
    if config.compression_level is not None and config.compression in {
        "zstd",
        "gzip",
        "brotli",
    }:
        kwargs["compression_level"] = config.compression_level
    return kwargs


def _validate_config(config: PreprocessConfig) -> None:
    if config.batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    if config.max_reporting_latency_ns < 0:
        raise ValueError("max_reporting_latency_ns must be >= 0")


# ---------------------------------------------------------------------------
# Quote reconstruction
# ---------------------------------------------------------------------------
def _quote_batch_semantics(table: pa.Table) -> dict[str, np.ndarray | CodeListView]:
    n = table.num_rows
    bid = _col_numpy(table, "bid_price", np.float64)
    ask = _col_numpy(table, "ask_price", np.float64)
    bid_size = _col_numpy(table, "bid_size", np.float64)
    ask_size = _col_numpy(table, "ask_size", np.float64)
    bid_exchange = _nullable_int_numpy(table, "bid_exchange")
    ask_exchange = _nullable_int_numpy(table, "ask_exchange")
    sip = _col_numpy(table, "sip_timestamp", np.int64)
    seq = _col_numpy(table, "sequence_number", np.int64)

    conditions = CodeListView(table["conditions"], n)
    indicators = CodeListView(table["indicators"], n)

    numeric_two_sided = np.isfinite(bid) & np.isfinite(ask) & (bid > 0.0) & (ask > 0.0)
    one_sided = conditions.any_in(QUOTE_ONE_SIDED_CODES)
    nonfirm = conditions.any_in(QUOTE_NONFIRM_CODES)
    closed = conditions.any_in(QUOTE_CLOSED_OR_NO_QUOTE_CODES)
    condition_invalid = conditions.any_in(QUOTE_INVALID_CODES)
    unknown_condition = conditions.any_not_in(KNOWN_QUOTE_CONDITIONS)
    unknown_indicator = indicators.any_not_in(KNOWN_QUOTE_INDICATORS)

    numeric_crossed = numeric_two_sided & (bid > ask)
    numeric_locked = numeric_two_sided & (bid == ask)
    crossed = numeric_crossed | conditions.any_in(QUOTE_EXPLICIT_CROSSED_CODES)
    locked = numeric_locked | conditions.any_in(QUOTE_EXPLICIT_LOCKED_CODES)

    # A locked state may still have a valid midpoint.  Crossed/missing/one-sided
    # states do not.  No previous good quote is substituted here.
    price_state_valid = (
        numeric_two_sided
        & ~one_sided
        & ~nonfirm
        & ~closed
        & ~condition_invalid
        & ~unknown_condition
        & ~crossed
    )
    depth_state_valid = (
        price_state_valid
        & np.isfinite(bid_size)
        & np.isfinite(ask_size)
        & (bid_size > 0.0)
        & (ask_size > 0.0)
    )

    midpoint = np.full(n, np.nan, dtype=np.float64)
    spread = np.full(n, np.nan, dtype=np.float64)
    spread_bps = np.full(n, np.nan, dtype=np.float64)
    midpoint[price_state_valid] = (
        bid[price_state_valid] + ask[price_state_valid]
    ) / 2.0
    spread[price_state_valid] = ask[price_state_valid] - bid[price_state_valid]
    good_mid = price_state_valid & (midpoint > 0.0)
    spread_bps[good_mid] = spread[good_mid] / midpoint[good_mid] * 10_000.0

    aggressor_usable = price_state_valid & ~locked

    return {
        "bid": bid,
        "ask": ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "bid_exchange": bid_exchange,
        "ask_exchange": ask_exchange,
        "sip": sip,
        "seq": seq,
        "conditions": conditions,
        "indicators": indicators,
        "one_sided": one_sided,
        "nonfirm": nonfirm,
        "closed": closed,
        "condition_invalid": condition_invalid,
        "unknown_condition": unknown_condition,
        "unknown_indicator": unknown_indicator,
        "locked": locked,
        "crossed": crossed,
        "price_state_valid": price_state_valid,
        "depth_state_valid": depth_state_valid,
        "midpoint": midpoint,
        "spread": spread,
        "spread_bps": spread_bps,
        "aggressor_usable": aggressor_usable,
    }


def _state_change_mask(
    sem: dict[str, np.ndarray | CodeListView],
    previous_signature: dict[str, object] | None,
) -> tuple[np.ndarray, dict[str, object]]:
    n = len(sem["sip"])  # type: ignore[arg-type]
    if n == 0:
        return np.empty(0, dtype=np.bool_), previous_signature or {}

    # State changes represent economically/semantically different current NBBO
    # states, not every repeated quote refresh.  Conditions that affect the
    # validity flags therefore enter indirectly through those flags.
    fields = [
        "bid",
        "ask",
        "bid_size",
        "ask_size",
        "bid_exchange",
        "ask_exchange",
        "one_sided",
        "nonfirm",
        "closed",
        "condition_invalid",
        "locked",
        "crossed",
        "price_state_valid",
        "depth_state_valid",
    ]

    change = np.zeros(n, dtype=np.bool_)
    if previous_signature is None:
        change[0] = True
    else:
        change[0] = any(
            _scalar_changed(sem[name][0], previous_signature[name])  # type: ignore[index]
            for name in fields
        )

    if n > 1:
        for name in fields:
            arr = sem[name]  # type: ignore[assignment]
            if np.issubdtype(arr.dtype, np.floating):  # type: ignore[union-attr]
                change[1:] |= _float_changed(arr)  # type: ignore[arg-type]
            else:
                change[1:] |= arr[1:] != arr[:-1]  # type: ignore[index]

    signature = {
        name: sem[name][-1].item() for name in fields  # type: ignore[index,union-attr]
    }
    return change, signature


def _empty_market_state_table() -> pa.Table:
    return pa.table(
        {
            "raw_quote_index": pa.array([], type=pa.int64()),
            "quote_state_id": pa.array([], type=pa.int64()),
            "sip_timestamp": pa.array([], type=pa.int64()),
            "sequence_number": pa.array([], type=pa.int64()),
            "price_state_valid": pa.array([], type=pa.bool_()),
            "depth_state_valid": pa.array([], type=pa.bool_()),
            "one_sided": pa.array([], type=pa.bool_()),
            "nonfirm": pa.array([], type=pa.bool_()),
            "closed": pa.array([], type=pa.bool_()),
            "condition_invalid": pa.array([], type=pa.bool_()),
            "locked": pa.array([], type=pa.bool_()),
            "crossed": pa.array([], type=pa.bool_()),
            "unknown_quote_condition": pa.array([], type=pa.bool_()),
            "unknown_quote_indicator": pa.array([], type=pa.bool_()),
            "aggressor_context_usable": pa.array([], type=pa.bool_()),
            "midpoint": pa.array([], type=pa.float64()),
            "spread": pa.array([], type=pa.float64()),
            "spread_bps": pa.array([], type=pa.float64()),
        }
    )


def _allocate_quote_state(n: int) -> QuoteState:
    return QuoteState(
        sip_timestamp=np.empty(n, dtype=np.int64),
        sequence_number=np.empty(n, dtype=np.int64),
        bid_price=np.empty(n, dtype=np.float64),
        ask_price=np.empty(n, dtype=np.float64),
        bid_size=np.empty(n, dtype=np.float64),
        ask_size=np.empty(n, dtype=np.float64),
        midpoint=np.empty(n, dtype=np.float64),
        spread=np.empty(n, dtype=np.float64),
        spread_bps=np.empty(n, dtype=np.float64),
        price_state_valid=np.empty(n, dtype=np.bool_),
        depth_state_valid=np.empty(n, dtype=np.bool_),
        one_sided=np.empty(n, dtype=np.bool_),
        nonfirm=np.empty(n, dtype=np.bool_),
        closed=np.empty(n, dtype=np.bool_),
        condition_invalid=np.empty(n, dtype=np.bool_),
        locked=np.empty(n, dtype=np.bool_),
        crossed=np.empty(n, dtype=np.bool_),
        unknown_quote_condition=np.empty(n, dtype=np.bool_),
        unknown_quote_indicator=np.empty(n, dtype=np.bool_),
        aggressor_context_usable=np.empty(n, dtype=np.bool_),
        state_change=np.empty(n, dtype=np.bool_),
        state_id=np.empty(n, dtype=np.int64),
        state_start_sip_timestamp=np.empty(n, dtype=np.int64),
    )


def _quote_duration_qa(state: QuoteState, date_str: str) -> tuple[dict, dict]:
    rth_start = _session_boundary_ns(date_str, 9, 30)
    rth_end = _session_boundary_ns(date_str, 16, 0)
    rth_duration = rth_end - rth_start

    duration_qa = {
        "rth_total_seconds": rth_duration / NS_PER_SECOND,
        "observed_state_seconds": 0.0,
        "no_state_seconds": rth_duration / NS_PER_SECOND,
        "price_valid_seconds": 0.0,
        "locked_seconds": 0.0,
        "crossed_seconds": 0.0,
        "one_sided_seconds": 0.0,
        "nonfirm_seconds": 0.0,
        "closed_seconds": 0.0,
        "price_valid_fraction": 0.0,
    }

    idx = state.transition_indices()
    if idx.size:
        t = state.sip_timestamp[idx]
        next_t = np.empty_like(t)
        if t.size > 1:
            next_t[:-1] = t[1:]
        next_t[-1] = rth_end
        start = np.maximum(t, rth_start)
        end = np.minimum(next_t, rth_end)
        dur = np.maximum(0, end - start)
        observed = int(np.sum(dur, dtype=np.int64))
        duration_qa["observed_state_seconds"] = observed / NS_PER_SECOND
        duration_qa["no_state_seconds"] = (
            max(0, rth_duration - observed) / NS_PER_SECOND
        )

        flag_map = {
            "price_state_valid": "price_valid_seconds",
            "locked": "locked_seconds",
            "crossed": "crossed_seconds",
            "one_sided": "one_sided_seconds",
            "nonfirm": "nonfirm_seconds",
            "closed": "closed_seconds",
        }
        for flag_name, out_name in flag_map.items():
            flags = getattr(state, flag_name)[idx]
            duration_qa[out_name] = (
                int(np.sum(dur[flags], dtype=np.int64)) / NS_PER_SECOND
            )
        duration_qa["price_valid_fraction"] = (
            duration_qa["price_valid_seconds"] / duration_qa["rth_total_seconds"]
            if duration_qa["rth_total_seconds"]
            else 0.0
        )

    open_state = {
        "has_quote_before_open": False,
        "price_state_valid_before_open": False,
        "quote_age_at_open_ms": None,
        "quote_state_id_before_open": None,
    }
    if state.sip_timestamp.size:
        # Strictly prior to the RTH boundary is consistent with the causal
        # cross-stream tie policy used for trade classification.
        i = int(np.searchsorted(state.sip_timestamp, rth_start, side="left") - 1)
        if i >= 0:
            open_state = {
                "has_quote_before_open": True,
                "price_state_valid_before_open": bool(state.price_state_valid[i]),
                "quote_age_at_open_ms": float(
                    (rth_start - int(state.sip_timestamp[i])) / NS_PER_MS
                ),
                "quote_state_id_before_open": int(state.state_id[i]),
            }
    return duration_qa, open_state


def prepare_quote_state(
    quote_path: Path,
    date_str: str,
    config: PreprocessConfig | None = None,
    *,
    audit_output_path: Path | None = None,
) -> tuple[QuoteState, dict]:
    """Load/validate one quote symbol-day and reconstruct causal NBBO semantics.

    Normal production use passes no ``audit_output_path``.  The returned
    ``QuoteState`` is the reusable in-memory state that both trade alignment and
    downstream second aggregation consume.  If an audit path is supplied, only
    semantic state-transition rows are written there; raw quotes remain the
    canonical immutable source.
    """

    config = config or PreprocessConfig()
    _validate_config(config)
    quote_path = Path(quote_path)
    qpf = pq.ParquetFile(str(quote_path), memory_map=config.memory_map_inputs)
    missing = [name for name in QUOTE_COLUMNS if name not in qpf.schema_arrow.names]
    if missing:
        raise ValueError(f"{quote_path}: missing quote columns: {missing}")

    total_rows = int(qpf.metadata.num_rows)
    state = _allocate_quote_state(total_rows) if total_rows else QuoteState.empty()

    qa = {
        "input_rows": 0,
        "state_rows": 0,
        "sip_timestamp_monotonic": True,
        "equal_sip_sequence_inversions": 0,
        "sequence_inversions": 0,
        "event_counts": Counter(),
        "condition_counts": Counter(),
        "indicator_counts": Counter(),
        "unknown_condition_codes": set(),
        "unknown_indicator_codes": set(),
    }

    writer: pq.ParquetWriter | None = None
    previous_signature: dict[str, object] | None = None
    previous_sip: int | None = None
    previous_seq: int | None = None
    previous_state_start: int | None = None
    next_state_id = 0
    row_start = 0

    try:
        for batch in qpf.iter_batches(
            batch_size=config.batch_size,
            columns=QUOTE_COLUMNS,
            use_threads=config.parquet_use_threads,
        ):
            table = pa.Table.from_batches([batch])
            n = table.num_rows
            if n == 0:
                continue
            _require_non_null(
                table,
                ["sip_timestamp", "sequence_number"],
                quote_path,
            )
            sem = _quote_batch_semantics(table)
            sip: np.ndarray = sem["sip"]  # type: ignore[assignment]
            seq: np.ndarray = sem["seq"]  # type: ignore[assignment]

            if np.any(sip[1:] < sip[:-1]):
                raise ValueError(
                    f"{quote_path}: sip_timestamp is not monotonic within a batch; "
                    "streaming replay refuses to guess an order"
                )
            if previous_sip is not None and int(sip[0]) < previous_sip:
                raise ValueError(
                    f"{quote_path}: sip_timestamp decreases across Parquet batches"
                )

            same = sip[1:] == sip[:-1]
            qa["equal_sip_sequence_inversions"] += int(
                np.count_nonzero(same & (seq[1:] < seq[:-1]))
            )
            qa["sequence_inversions"] += int(np.count_nonzero(seq[1:] < seq[:-1]))
            if previous_sip is not None and int(sip[0]) == previous_sip:
                if previous_seq is not None and int(seq[0]) < previous_seq:
                    qa["equal_sip_sequence_inversions"] += 1
            if previous_seq is not None and int(seq[0]) < previous_seq:
                qa["sequence_inversions"] += 1

            if qa["equal_sip_sequence_inversions"]:
                raise ValueError(
                    f"{quote_path}: quote sequence decreases inside an equal-SIP-"
                    "timestamp tie group; deterministic within-stream state "
                    "requires a documented tie order"
                )

            change, previous_signature = _state_change_mask(sem, previous_signature)
            local_count = np.cumsum(change, dtype=np.int64)
            state_ids = (next_state_id - 1) + local_count
            transitions_this_batch = int(local_count[-1])
            next_state_id += transitions_this_batch

            seed = int(sip[0]) if previous_state_start is None else previous_state_start
            starts = np.where(change, sip, seed).astype(np.int64, copy=False)
            state_start = np.maximum.accumulate(starts)
            previous_state_start = int(state_start[-1])

            sl = slice(row_start, row_start + n)
            state.sip_timestamp[sl] = sip
            state.sequence_number[sl] = seq
            state.bid_price[sl] = sem["bid"]  # type: ignore[index]
            state.ask_price[sl] = sem["ask"]  # type: ignore[index]
            state.bid_size[sl] = sem["bid_size"]  # type: ignore[index]
            state.ask_size[sl] = sem["ask_size"]  # type: ignore[index]
            state.midpoint[sl] = sem["midpoint"]  # type: ignore[index]
            state.spread[sl] = sem["spread"]  # type: ignore[index]
            state.spread_bps[sl] = sem["spread_bps"]  # type: ignore[index]
            state.price_state_valid[sl] = sem["price_state_valid"]  # type: ignore[index]
            state.depth_state_valid[sl] = sem["depth_state_valid"]  # type: ignore[index]
            state.one_sided[sl] = sem["one_sided"]  # type: ignore[index]
            state.nonfirm[sl] = sem["nonfirm"]  # type: ignore[index]
            state.closed[sl] = sem["closed"]  # type: ignore[index]
            state.condition_invalid[sl] = sem["condition_invalid"]  # type: ignore[index]
            state.locked[sl] = sem["locked"]  # type: ignore[index]
            state.crossed[sl] = sem["crossed"]  # type: ignore[index]
            state.unknown_quote_condition[sl] = sem["unknown_condition"]  # type: ignore[index]
            state.unknown_quote_indicator[sl] = sem["unknown_indicator"]  # type: ignore[index]
            state.aggressor_context_usable[sl] = sem["aggressor_usable"]  # type: ignore[index]
            state.state_change[sl] = change
            state.state_id[sl] = state_ids
            state.state_start_sip_timestamp[sl] = state_start

            state_idx = np.flatnonzero(change)
            if audit_output_path is not None and state_idx.size:
                raw_idx = row_start + state_idx
                out = pa.table(
                    {
                        "raw_quote_index": pa.array(raw_idx, type=pa.int64()),
                        "quote_state_id": pa.array(
                            state_ids[state_idx], type=pa.int64()
                        ),
                        "sip_timestamp": pa.array(sip[state_idx], type=pa.int64()),
                        "sequence_number": pa.array(seq[state_idx], type=pa.int64()),
                        "price_state_valid": pa.array(
                            sem["price_state_valid"][state_idx], type=pa.bool_()  # type: ignore[index]
                        ),
                        "depth_state_valid": pa.array(
                            sem["depth_state_valid"][state_idx], type=pa.bool_()  # type: ignore[index]
                        ),
                        "one_sided": pa.array(
                            sem["one_sided"][state_idx], type=pa.bool_()  # type: ignore[index]
                        ),
                        "nonfirm": pa.array(
                            sem["nonfirm"][state_idx], type=pa.bool_()  # type: ignore[index]
                        ),
                        "closed": pa.array(
                            sem["closed"][state_idx], type=pa.bool_()  # type: ignore[index]
                        ),
                        "condition_invalid": pa.array(
                            sem["condition_invalid"][state_idx], type=pa.bool_()  # type: ignore[index]
                        ),
                        "locked": pa.array(
                            sem["locked"][state_idx], type=pa.bool_()  # type: ignore[index]
                        ),
                        "crossed": pa.array(
                            sem["crossed"][state_idx], type=pa.bool_()  # type: ignore[index]
                        ),
                        "unknown_quote_condition": pa.array(
                            sem["unknown_condition"][state_idx], type=pa.bool_()  # type: ignore[index]
                        ),
                        "unknown_quote_indicator": pa.array(
                            sem["unknown_indicator"][state_idx], type=pa.bool_()  # type: ignore[index]
                        ),
                        "aggressor_context_usable": pa.array(
                            sem["aggressor_usable"][state_idx], type=pa.bool_()  # type: ignore[index]
                        ),
                        "midpoint": pa.array(
                            sem["midpoint"][state_idx], type=pa.float64()  # type: ignore[index]
                        ),
                        "spread": pa.array(
                            sem["spread"][state_idx], type=pa.float64()  # type: ignore[index]
                        ),
                        "spread_bps": pa.array(
                            sem["spread_bps"][state_idx], type=pa.float64()  # type: ignore[index]
                        ),
                    }
                )
                if writer is None:
                    audit_output_path.parent.mkdir(parents=True, exist_ok=True)
                    writer = pq.ParquetWriter(
                        str(audit_output_path), out.schema, **_writer_kwargs(config)
                    )
                writer.write_table(out, row_group_size=config.batch_size)

            conditions: CodeListView = sem["conditions"]  # type: ignore[assignment]
            indicators: CodeListView = sem["indicators"]  # type: ignore[assignment]
            qa["condition_counts"].update(conditions.counts())
            qa["indicator_counts"].update(indicators.counts())
            qa["unknown_condition_codes"].update(
                conditions.unique_not_in(KNOWN_QUOTE_CONDITIONS)
            )
            qa["unknown_indicator_codes"].update(
                indicators.unique_not_in(KNOWN_QUOTE_INDICATORS)
            )
            for key in [
                "price_state_valid",
                "depth_state_valid",
                "one_sided",
                "nonfirm",
                "closed",
                "condition_invalid",
                "locked",
                "crossed",
                "unknown_condition",
                "unknown_indicator",
            ]:
                qa["event_counts"][key] += int(
                    np.count_nonzero(sem[key])  # type: ignore[arg-type]
                )

            qa["input_rows"] += n
            qa["state_rows"] += int(state_idx.size)
            row_start += n
            previous_sip = int(sip[-1])
            previous_seq = int(seq[-1])

        if row_start != total_rows:
            raise AssertionError(
                f"{quote_path}: Parquet metadata rows={total_rows} but replayed {row_start}"
            )

        if audit_output_path is not None and writer is None:
            audit_output_path.parent.mkdir(parents=True, exist_ok=True)
            empty = _empty_market_state_table()
            writer = pq.ParquetWriter(
                str(audit_output_path), empty.schema, **_writer_kwargs(config)
            )
            writer.write_table(empty)
    finally:
        if writer is not None:
            writer.close()

    duration_qa, open_state = _quote_duration_qa(state, date_str)
    unknown_conditions = sorted(qa["unknown_condition_codes"])
    unknown_indicators = sorted(qa["unknown_indicator_codes"])
    alerts: list[str] = []
    if unknown_conditions:
        alerts.append(f"unknown_quote_condition_codes:{unknown_conditions}")
    if unknown_indicators:
        alerts.append(f"unknown_quote_indicator_codes:{unknown_indicators}")

    qa_out = {
        "input_rows": int(qa["input_rows"]),
        "state_rows": int(qa["state_rows"]),
        "compression_ratio_state_rows_over_raw": (
            float(qa["state_rows"] / qa["input_rows"]) if qa["input_rows"] else None
        ),
        "sip_timestamp_monotonic": bool(qa["sip_timestamp_monotonic"]),
        "sequence_inversions": int(qa["sequence_inversions"]),
        "equal_sip_sequence_inversions": int(qa["equal_sip_sequence_inversions"]),
        "event_counts": _counter_to_json(qa["event_counts"]),
        "condition_occurrence_counts": _counter_to_json(qa["condition_counts"]),
        "indicator_occurrence_counts": _counter_to_json(qa["indicator_counts"]),
        "unknown_condition_codes": unknown_conditions,
        "unknown_indicator_codes": unknown_indicators,
        "rth_state_durations": duration_qa,
        "open_initialization": open_state,
        "quality_alerts": alerts,
    }
    return state, qa_out


# ---------------------------------------------------------------------------
# Trade semantics and aggressor classification
# ---------------------------------------------------------------------------
def _normalize_correction(table: pa.Table) -> np.ndarray:
    arr = table["correction"].combine_chunks()
    arr = pc.fill_null(arr, 0)
    arr = pc.cast(arr, pa.int16())
    return np.asarray(arr.to_numpy(zero_copy_only=False), dtype=np.int16)


def _analytic_trade_size(table: pa.Table) -> tuple[pa.Array, np.ndarray]:
    decimal = table["decimal_size"].combine_chunks()
    raw_size = table["size"].combine_chunks()
    decimal_float = pc.cast(decimal, pa.float64(), safe=False)
    raw_size_float = pc.cast(raw_size, pa.float64(), safe=False)
    analytic = pc.if_else(pc.is_null(decimal), raw_size_float, decimal_float)
    analytic_np = np.asarray(analytic.to_numpy(zero_copy_only=False), dtype=np.float64)
    return analytic, analytic_np


def activity_trade_semantics(
    table: pa.Table,
    *,
    max_reporting_latency_ns: int = DEFAULT_MAX_REPORTING_LATENCY_NS,
) -> ActivityTradeSemantics:
    """Evaluate the frozen common activity and auxiliary price populations.

    The input may contain only the eight fields used here.  This function is
    the single reusable implementation for both market-state preprocessing and
    historical interruption detection.
    """

    required = {
        "sip_timestamp",
        "participant_timestamp",
        "conditions",
        "correction",
        "price",
        "size",
        "decimal_size",
    }
    missing = sorted(required - set(table.column_names))
    if missing:
        raise ValueError(f"activity trade semantics missing columns: {missing}")

    n = table.num_rows
    sip = _col_numpy(table, "sip_timestamp", np.int64)
    participant = _col_numpy(table, "participant_timestamp", np.int64)
    price = _col_numpy(table, "price", np.float64)
    analytic_size_arr, analytic_size = _analytic_trade_size(table)
    correction = _normalize_correction(table)

    conditions = CodeListView(table["conditions"], n)
    unknown_condition = conditions.any_not_in(KNOWN_TRADE_CONDITIONS)
    pressure_condition_ok = conditions.all_in(SAFE_PRESSURE_TRADE_CONDITIONS)
    extended_hours_activity_condition_ok = conditions.all_in(
        SAFE_EXTENDED_HOURS_ACTIVITY_TRADE_CONDITIONS
    )
    price_condition_ok = conditions.all_in(SAFE_PRICE_TRADE_CONDITIONS)
    is_odd_lot = conditions.any_in({37})

    base_numeric_valid = (
        np.isfinite(price)
        & (price > 0.0)
        & np.isfinite(analytic_size)
        & (analytic_size > 0.0)
    )
    causal_payload_codes = np.fromiter(CAUSAL_TRADE_PAYLOAD_CORRECTIONS, dtype=np.int16)
    known_correction_codes = np.fromiter(KNOWN_CORRECTION_CODES, dtype=np.int16)
    correction_payload_usable = np.isin(correction, causal_payload_codes)
    known_correction = np.isin(correction, known_correction_codes)
    current_pressure_semantic_eligible = (
        base_numeric_valid
        & pressure_condition_ok
        & ~unknown_condition
        & correction_payload_usable
        & known_correction
    )
    reporting_latency = sip - participant
    reporting_latency_usable = (reporting_latency >= 0) & (
        reporting_latency <= max_reporting_latency_ns
    )
    eligible_activity_trade = (
        current_pressure_semantic_eligible & reporting_latency_usable
    )
    eligible_extended_hours_activity_trade = (
        base_numeric_valid
        & extended_hours_activity_condition_ok
        & ~unknown_condition
        & correction_payload_usable
        & known_correction
        & reporting_latency_usable
    )
    eligible_price_trade = (
        base_numeric_valid
        & price_condition_ok
        & ~unknown_condition
        & correction_payload_usable
        & known_correction
    )
    return ActivityTradeSemantics(
        analytic_size_arrow=analytic_size_arr,
        analytic_size=analytic_size,
        correction_code=correction,
        unknown_trade_condition=unknown_condition,
        pressure_condition_policy_ok=pressure_condition_ok,
        extended_hours_activity_condition_policy_ok=extended_hours_activity_condition_ok,
        price_condition_policy_ok=price_condition_ok,
        is_odd_lot=is_odd_lot,
        base_numeric_valid=base_numeric_valid,
        current_pressure_semantic_eligible=current_pressure_semantic_eligible,
        reporting_latency_ns=reporting_latency,
        reporting_latency_usable=reporting_latency_usable,
        eligible_activity_trade=eligible_activity_trade,
        eligible_extended_hours_activity_trade=eligible_extended_hours_activity_trade,
        eligible_price_trade=eligible_price_trade,
    )


def _aggressor_labels(
    trade_price: np.ndarray,
    trade_eligible: np.ndarray,
    quote_state: QuoteState,
    trade_sip: np.ndarray,
) -> dict[str, np.ndarray]:
    """Classify trades from strictly prior SIP-known quote state.

    ``searchsorted(..., side='left') - 1`` is the causality-sensitive core:
    a quote stamped exactly T is not available to classify a trade stamped T.
    Cross-stream ``sequence_number`` is never used to invent an order.
    """

    n = trade_price.size
    side = np.zeros(n, dtype=np.int8)
    confidence = np.zeros(n, dtype=np.int8)
    method = np.full(n, METHOD_OTHER, dtype=np.int8)

    has_quote = np.zeros(n, dtype=np.bool_)
    ref_idx = np.full(n, -1, dtype=np.int64)
    ref_state_id = np.full(n, -1, dtype=np.int64)
    ref_quote_ts = np.zeros(n, dtype=np.int64)
    ref_quote_seq = np.zeros(n, dtype=np.int64)
    quote_age_ns = np.zeros(n, dtype=np.int64)
    state_age_ns = np.zeros(n, dtype=np.int64)
    ref_price_valid = np.zeros(n, dtype=np.bool_)
    ref_midpoint = np.full(n, np.nan, dtype=np.float64)
    ref_locked = np.zeros(n, dtype=np.bool_)
    ref_crossed = np.zeros(n, dtype=np.bool_)

    method[~trade_eligible] = METHOD_TRADE_INELIGIBLE

    if quote_state.sip_timestamp.size:
        ref_idx = (
            np.searchsorted(quote_state.sip_timestamp, trade_sip, side="left").astype(
                np.int64
            )
            - 1
        )
        has_quote = ref_idx >= 0
        valid_positions = np.flatnonzero(has_quote)
        qi = ref_idx[valid_positions]

        ref_state_id[valid_positions] = quote_state.state_id[qi]
        ref_quote_ts[valid_positions] = quote_state.sip_timestamp[qi]
        ref_quote_seq[valid_positions] = quote_state.sequence_number[qi]
        quote_age_ns[valid_positions] = (
            trade_sip[valid_positions] - quote_state.sip_timestamp[qi]
        )
        state_age_ns[valid_positions] = (
            trade_sip[valid_positions] - quote_state.state_start_sip_timestamp[qi]
        )
        ref_price_valid[valid_positions] = quote_state.price_state_valid[qi]
        ref_midpoint[valid_positions] = quote_state.midpoint[qi]
        ref_locked[valid_positions] = quote_state.locked[qi]
        ref_crossed[valid_positions] = quote_state.crossed[qi]

        if np.any(ref_quote_ts[valid_positions] >= trade_sip[valid_positions]):
            raise AssertionError("strict-prior quote alignment invariant violated")

        quote_usable = np.zeros(n, dtype=np.bool_)
        quote_usable[valid_positions] = quote_state.aggressor_context_usable[qi]
    else:
        quote_usable = np.zeros(n, dtype=np.bool_)

    method[trade_eligible & ~has_quote] = METHOD_NO_PRIOR_QUOTE
    method[trade_eligible & has_quote & ~quote_usable] = METHOD_QUOTE_UNUSABLE

    # No quote-age/staleness cutoff is applied.  Quote age remains observable
    # metadata for downstream feature/model decisions.
    classifiable = trade_eligible & has_quote & quote_usable
    pos = np.flatnonzero(classifiable)
    if pos.size:
        qi = ref_idx[pos]
        p = trade_price[pos]
        b = quote_state.bid_price[qi]
        a = quote_state.ask_price[qi]
        m = quote_state.midpoint[qi]

        at_ask = np.isclose(p, a, rtol=0.0, atol=PRICE_ATOL)
        at_bid = np.isclose(p, b, rtol=0.0, atol=PRICE_ATOL)
        at_mid = np.isclose(p, m, rtol=0.0, atol=PRICE_ATOL)

        idx = pos[at_ask]
        side[idx] = 1
        confidence[idx] = 2
        method[idx] = METHOD_AT_ASK

        idx = pos[at_bid]
        side[idx] = 2
        confidence[idx] = 2
        method[idx] = METHOD_AT_BID

        undecided = ~(at_ask | at_bid)
        idx = pos[undecided & at_mid]
        method[idx] = METHOD_AT_MIDPOINT

        inside_above = undecided & ~at_mid & (p > m + PRICE_ATOL) & (p < a - PRICE_ATOL)
        idx = pos[inside_above]
        side[idx] = 1
        confidence[idx] = 1
        method[idx] = METHOD_INSIDE_ABOVE_MID

        inside_below = undecided & ~at_mid & (p < m - PRICE_ATOL) & (p > b + PRICE_ATOL)
        idx = pos[inside_below]
        side[idx] = 2
        confidence[idx] = 1
        method[idx] = METHOD_INSIDE_BELOW_MID

        outside = (p < b - PRICE_ATOL) | (p > a + PRICE_ATOL)
        idx = pos[outside]
        side[idx] = 0
        confidence[idx] = 0
        method[idx] = METHOD_OUTSIDE_NBBO

    return {
        "side": side,
        "confidence": confidence,
        "method": method,
        "has_quote": has_quote,
        "ref_idx": ref_idx,
        "ref_state_id": ref_state_id,
        "ref_quote_ts": ref_quote_ts,
        "ref_quote_seq": ref_quote_seq,
        "quote_age_ns": quote_age_ns,
        "state_age_ns": state_age_ns,
        "ref_midpoint": ref_midpoint,
        "ref_price_valid": ref_price_valid,
        "ref_locked": ref_locked,
        "ref_crossed": ref_crossed,
    }


def _process_trade_table(
    table: pa.Table,
    raw_row_start: int,
    quote_state: QuoteState,
    config: PreprocessConfig,
) -> ProcessedTradeBatch:
    n = table.num_rows
    sip = _col_numpy(table, "sip_timestamp", np.int64)
    price = _col_numpy(table, "price", np.float64)
    seq = _col_numpy(table, "sequence_number", np.int64)
    semantics = activity_trade_semantics(
        table, max_reporting_latency_ns=config.max_reporting_latency_ns
    )
    participant = _col_numpy(table, "participant_timestamp", np.int64)
    analytic_size_arr = semantics.analytic_size_arrow
    analytic_size = semantics.analytic_size
    correction = semantics.correction_code
    unknown_condition = semantics.unknown_trade_condition
    pressure_condition_ok = semantics.pressure_condition_policy_ok
    price_condition_ok = semantics.price_condition_policy_ok
    is_odd_lot = semantics.is_odd_lot
    current_pressure_semantic_eligible = semantics.current_pressure_semantic_eligible
    reporting_latency = semantics.reporting_latency_ns
    reporting_latency_usable = semantics.reporting_latency_usable
    late_for_current_pressure = ~reporting_latency_usable
    eligible_activity_trade = semantics.eligible_activity_trade
    eligible_extended_hours_activity_trade = (
        semantics.eligible_extended_hours_activity_trade
    )
    # Keep separate masks even where the current policy is identical; future
    # feature families may intentionally diverge.
    eligible_activity_shares = eligible_activity_trade.copy()
    eligible_aggressor_trade = eligible_activity_trade.copy()

    # Auxiliary trade-price eligibility is not a contemporaneous pressure mask,
    # so the 1-second reporting gate does not erase it.
    eligible_price_trade = semantics.eligible_price_trade

    conditions = CodeListView(table["conditions"], n)
    known_correction_codes = np.fromiter(KNOWN_CORRECTION_CODES, dtype=np.int16)

    labels = _aggressor_labels(
        trade_price=price,
        trade_eligible=eligible_aggressor_trade,
        quote_state=quote_state,
        trade_sip=sip,
    )

    is_trf = np.asarray(
        pc.is_valid(table["trf_id"].combine_chunks()).to_numpy(zero_copy_only=False),
        dtype=np.bool_,
    )

    side = labels["side"]
    confidence = labels["confidence"]
    method = labels["method"]
    fail_latency_after_semantics = (
        current_pressure_semantic_eligible & ~reporting_latency_usable
    )

    correction_counts = (
        Counter(
            {int(k): int(v) for k, v in zip(*np.unique(correction, return_counts=True))}
        )
        if correction.size
        else Counter()
    )
    method_counts = (
        Counter(
            {
                METHOD_LABELS[int(k)]: int(v)
                for k, v in zip(*np.unique(method, return_counts=True))
            }
        )
        if method.size
        else Counter()
    )
    eligible_side = side[eligible_aggressor_trade]
    eligible_confidence = confidence[eligible_aggressor_trade]
    side_counts = (
        Counter(
            {
                SIDE_LABELS[int(k)]: int(v)
                for k, v in zip(*np.unique(eligible_side, return_counts=True))
            }
        )
        if eligible_side.size
        else Counter()
    )
    confidence_counts = (
        Counter(
            {
                CONFIDENCE_LABELS[int(k)]: int(v)
                for k, v in zip(*np.unique(eligible_confidence, return_counts=True))
            }
        )
        if eligible_confidence.size
        else Counter()
    )

    unknown_correction_mask = ~np.isin(correction, known_correction_codes)
    has_quote: np.ndarray = labels["has_quote"]
    quote_ages = labels["quote_age_ns"][has_quote]

    qa = {
        "rows": n,
        "condition_counts": conditions.counts(),
        "unknown_condition_codes": conditions.unique_not_in(KNOWN_TRADE_CONDITIONS),
        "correction_counts": correction_counts,
        "unknown_correction_codes": {
            int(x) for x in np.unique(correction[unknown_correction_mask]).tolist()
        },
        "eligible_price_count": int(np.count_nonzero(eligible_price_trade)),
        "pressure_semantic_eligible_count": int(
            np.count_nonzero(current_pressure_semantic_eligible)
        ),
        "pressure_semantic_eligible_shares": float(
            np.sum(analytic_size[current_pressure_semantic_eligible])
        ),
        "eligible_activity_count": int(np.count_nonzero(eligible_activity_trade)),
        "eligible_activity_shares": float(
            np.sum(analytic_size[eligible_activity_shares])
        ),
        "eligible_aggressor_count": int(np.count_nonzero(eligible_aggressor_trade)),
        "aggressor_eligible_before_latency_count": int(
            np.count_nonzero(current_pressure_semantic_eligible)
        ),
        "aggressor_lost_to_latency_count": int(
            np.count_nonzero(fail_latency_after_semantics)
        ),
        "side_counts": side_counts,
        "confidence_counts": confidence_counts,
        "method_counts": method_counts,
        "buyer_shares": float(
            np.sum(analytic_size[eligible_aggressor_trade & (side == 1)])
        ),
        "seller_shares": float(
            np.sum(analytic_size[eligible_aggressor_trade & (side == 2)])
        ),
        "unclassified_shares": float(
            np.sum(analytic_size[eligible_aggressor_trade & (side == 0)])
        ),
        "trf_count": int(np.count_nonzero(is_trf)),
        "odd_lot_count": int(np.count_nonzero(is_odd_lot)),
        "negative_latency_count": int(np.count_nonzero(reporting_latency < 0)),
        "over_latency_threshold_count": int(
            np.count_nonzero(reporting_latency > config.max_reporting_latency_ns)
        ),
        "otherwise_eligible_failed_latency_count": int(
            np.count_nonzero(fail_latency_after_semantics)
        ),
        "otherwise_eligible_failed_latency_shares": float(
            np.sum(analytic_size[fail_latency_after_semantics])
        ),
        "trf_otherwise_eligible_failed_latency_count": int(
            np.count_nonzero(fail_latency_after_semantics & is_trf)
        ),
        "trf_otherwise_eligible_failed_latency_shares": float(
            np.sum(analytic_size[fail_latency_after_semantics & is_trf])
        ),
        "exchange_otherwise_eligible_failed_latency_count": int(
            np.count_nonzero(fail_latency_after_semantics & ~is_trf)
        ),
        "exchange_otherwise_eligible_failed_latency_shares": float(
            np.sum(analytic_size[fail_latency_after_semantics & ~is_trf])
        ),
        "trf_negative_latency_count": int(
            np.count_nonzero((reporting_latency < 0) & is_trf)
        ),
        "exchange_negative_latency_count": int(
            np.count_nonzero((reporting_latency < 0) & ~is_trf)
        ),
        "trf_over_latency_threshold_count": int(
            np.count_nonzero(
                (reporting_latency > config.max_reporting_latency_ns) & is_trf
            )
        ),
        "exchange_over_latency_threshold_count": int(
            np.count_nonzero(
                (reporting_latency > config.max_reporting_latency_ns) & ~is_trf
            )
        ),
        "quote_ages": quote_ages,
        "reporting_latencies": reporting_latency,
    }

    return ProcessedTradeBatch(
        raw_table=table,
        raw_row_start=raw_row_start,
        analytic_size_arrow=analytic_size_arr,
        analytic_size=analytic_size,
        sip_timestamp=sip,
        participant_timestamp=participant,
        sequence_number=seq,
        price=price,
        reporting_latency_ns=reporting_latency,
        reporting_latency_usable=reporting_latency_usable,
        late_for_current_pressure=late_for_current_pressure,
        current_pressure_semantic_eligible=current_pressure_semantic_eligible,
        eligible_price_trade=eligible_price_trade,
        eligible_activity_trade=eligible_activity_trade,
        eligible_extended_hours_activity_trade=eligible_extended_hours_activity_trade,
        eligible_activity_shares=eligible_activity_shares,
        eligible_aggressor_trade=eligible_aggressor_trade,
        is_trf=is_trf,
        is_odd_lot=is_odd_lot,
        unknown_trade_condition=unknown_condition,
        pressure_condition_policy_ok=pressure_condition_ok,
        price_condition_policy_ok=price_condition_ok,
        correction_code=correction,
        quote_state_id=labels["ref_state_id"],
        has_prior_quote=has_quote,
        prior_quote_sip_timestamp=labels["ref_quote_ts"],
        prior_quote_sequence_number=labels["ref_quote_seq"],
        quote_age_ns=labels["quote_age_ns"],
        quote_state_age_ns=labels["state_age_ns"],
        reference_midpoint=labels["ref_midpoint"],
        reference_price_state_valid=labels["ref_price_valid"],
        reference_locked=labels["ref_locked"],
        reference_crossed=labels["ref_crossed"],
        aggressor_side_code=side,
        aggressor_confidence_code=confidence,
        aggressor_method_code=method,
        qa=qa,
    )


def iter_processed_trade_batches(
    trade_path: Path,
    quote_state: QuoteState,
    config: PreprocessConfig | None = None,
) -> Iterator[ProcessedTradeBatch]:
    """Yield processed trade batches without materializing a full trade day.

    This is the lowest-level streaming API.  For most callers,
    ``process_trade_batches(..., consumer=...)`` is more convenient because it
    also aggregates and returns final QA.
    """

    config = config or PreprocessConfig()
    _validate_config(config)
    trade_path = Path(trade_path)
    tpf = pq.ParquetFile(str(trade_path), memory_map=config.memory_map_inputs)
    missing = [name for name in TRADE_COLUMNS if name not in tpf.schema_arrow.names]
    if missing:
        raise ValueError(f"{trade_path}: missing trade columns: {missing}")

    raw_row_start = 0
    previous_sip: int | None = None
    previous_seq: int | None = None

    for batch in tpf.iter_batches(
        batch_size=config.batch_size,
        columns=TRADE_COLUMNS,
        use_threads=config.parquet_use_threads,
    ):
        table = pa.Table.from_batches([batch])
        if table.num_rows == 0:
            continue
        _require_non_null(
            table,
            ["sip_timestamp", "participant_timestamp", "sequence_number"],
            trade_path,
        )
        sip = _col_numpy(table, "sip_timestamp", np.int64)
        seq = _col_numpy(table, "sequence_number", np.int64)

        if np.any(sip[1:] < sip[:-1]):
            raise ValueError(
                f"{trade_path}: sip_timestamp is not monotonic within a batch; "
                "streaming replay refuses to guess an order"
            )
        if previous_sip is not None and int(sip[0]) < previous_sip:
            raise ValueError(
                f"{trade_path}: sip_timestamp decreases across Parquet batches"
            )

        processed = _process_trade_table(
            table,
            raw_row_start=raw_row_start,
            quote_state=quote_state,
            config=config,
        )
        # Sequence validation is attached here so it covers batch boundaries
        # without affecting cross-stream alignment semantics.
        inversions = int(np.count_nonzero(seq[1:] < seq[:-1]))
        equal_sip_inversions = int(
            np.count_nonzero((sip[1:] == sip[:-1]) & (seq[1:] < seq[:-1]))
        )
        if previous_seq is not None and int(seq[0]) < previous_seq:
            inversions += 1
            if previous_sip is not None and int(sip[0]) == previous_sip:
                equal_sip_inversions += 1
        processed.qa["sequence_inversions"] = inversions
        processed.qa["equal_sip_sequence_inversions"] = equal_sip_inversions

        yield processed

        raw_row_start += table.num_rows
        previous_sip = int(sip[-1])
        previous_seq = int(seq[-1])


def _empty_trade_output_table(schema: pa.Schema, quote_state: QuoteState) -> pa.Table:
    base = pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in schema], names=schema.names
    )
    processed = _process_trade_table(
        base,
        raw_row_start=0,
        quote_state=quote_state,
        config=PreprocessConfig(),
    )
    return processed.to_audit_table()


def _new_trade_qa_aggregate() -> dict:
    return {
        "input_rows": 0,
        "condition_counts": Counter(),
        "correction_counts": Counter(),
        "side_counts": Counter(),
        "confidence_counts": Counter(),
        "method_counts": Counter(),
        "unknown_condition_codes": set(),
        "unknown_correction_codes": set(),
        "eligible_price_count": 0,
        "pressure_semantic_eligible_count": 0,
        "pressure_semantic_eligible_shares": 0.0,
        "eligible_activity_count": 0,
        "eligible_activity_shares": 0.0,
        "eligible_aggressor_count": 0,
        "aggressor_eligible_before_latency_count": 0,
        "aggressor_lost_to_latency_count": 0,
        "buyer_shares": 0.0,
        "seller_shares": 0.0,
        "unclassified_shares": 0.0,
        "trf_count": 0,
        "odd_lot_count": 0,
        "negative_latency_count": 0,
        "over_latency_threshold_count": 0,
        "otherwise_eligible_failed_latency_count": 0,
        "otherwise_eligible_failed_latency_shares": 0.0,
        "trf_otherwise_eligible_failed_latency_count": 0,
        "trf_otherwise_eligible_failed_latency_shares": 0.0,
        "exchange_otherwise_eligible_failed_latency_count": 0,
        "exchange_otherwise_eligible_failed_latency_shares": 0.0,
        "trf_negative_latency_count": 0,
        "exchange_negative_latency_count": 0,
        "trf_over_latency_threshold_count": 0,
        "exchange_over_latency_threshold_count": 0,
        "sequence_inversions": 0,
        "equal_sip_sequence_inversions": 0,
        "quote_age_sampler": _StrideSampler(),
        "latency_sampler": _StrideSampler(),
        "max_quote_age_ns": None,
        "max_latency_ns": None,
    }


def _update_trade_qa_aggregate(agg: dict, qa: dict) -> None:
    agg["input_rows"] += qa["rows"]
    agg["condition_counts"].update(qa["condition_counts"])
    agg["correction_counts"].update(qa["correction_counts"])
    agg["side_counts"].update(qa["side_counts"])
    agg["confidence_counts"].update(qa["confidence_counts"])
    agg["method_counts"].update(qa["method_counts"])
    agg["unknown_condition_codes"].update(qa["unknown_condition_codes"])
    agg["unknown_correction_codes"].update(qa["unknown_correction_codes"])

    int_keys = [
        "eligible_price_count",
        "pressure_semantic_eligible_count",
        "eligible_activity_count",
        "eligible_aggressor_count",
        "aggressor_eligible_before_latency_count",
        "aggressor_lost_to_latency_count",
        "trf_count",
        "odd_lot_count",
        "negative_latency_count",
        "over_latency_threshold_count",
        "otherwise_eligible_failed_latency_count",
        "trf_otherwise_eligible_failed_latency_count",
        "exchange_otherwise_eligible_failed_latency_count",
        "trf_negative_latency_count",
        "exchange_negative_latency_count",
        "trf_over_latency_threshold_count",
        "exchange_over_latency_threshold_count",
        "sequence_inversions",
        "equal_sip_sequence_inversions",
    ]
    for key in int_keys:
        agg[key] += qa[key]

    float_keys = [
        "pressure_semantic_eligible_shares",
        "eligible_activity_shares",
        "buyer_shares",
        "seller_shares",
        "unclassified_shares",
        "otherwise_eligible_failed_latency_shares",
        "trf_otherwise_eligible_failed_latency_shares",
        "exchange_otherwise_eligible_failed_latency_shares",
    ]
    for key in float_keys:
        agg[key] += qa[key]

    quote_ages = qa["quote_ages"]
    latencies = qa["reporting_latencies"]
    agg["quote_age_sampler"].update(quote_ages)
    agg["latency_sampler"].update(latencies)
    if quote_ages.size:
        qmax = int(np.max(quote_ages))
        agg["max_quote_age_ns"] = (
            qmax
            if agg["max_quote_age_ns"] is None
            else max(agg["max_quote_age_ns"], qmax)
        )
    if latencies.size:
        lmax = int(np.max(latencies))
        agg["max_latency_ns"] = (
            lmax if agg["max_latency_ns"] is None else max(agg["max_latency_ns"], lmax)
        )


def _finalize_trade_qa(agg: dict, config: PreprocessConfig) -> dict:
    classified = int(
        agg["side_counts"].get("buyer", 0) + agg["side_counts"].get("seller", 0)
    )
    eligible_aggressor = int(agg["eligible_aggressor_count"])
    pre_latency = int(agg["pressure_semantic_eligible_count"])
    failed = int(agg["otherwise_eligible_failed_latency_count"])

    unknown_conditions = sorted(agg["unknown_condition_codes"])
    unknown_corrections = sorted(agg["unknown_correction_codes"])
    alerts: list[str] = []
    if unknown_conditions:
        alerts.append(f"unknown_trade_condition_codes:{unknown_conditions}")
    if unknown_corrections:
        alerts.append(f"unknown_trade_correction_codes:{unknown_corrections}")

    return {
        "input_rows": int(agg["input_rows"]),
        "sip_timestamp_monotonic": True,
        "sequence_inversions": int(agg["sequence_inversions"]),
        "equal_sip_sequence_inversions": int(agg["equal_sip_sequence_inversions"]),
        "condition_occurrence_counts": _counter_to_json(agg["condition_counts"]),
        "correction_counts": {
            CORRECTION_NAMES.get(int(k), f"unknown_{k}"): int(v)
            for k, v in sorted(agg["correction_counts"].items())
        },
        "unknown_condition_codes": unknown_conditions,
        "unknown_correction_codes": unknown_corrections,
        "eligibility": {
            "price_trade_count": int(agg["eligible_price_count"]),
            "current_pressure_semantic_trade_count_before_latency": pre_latency,
            "current_pressure_semantic_shares_before_latency": float(
                agg["pressure_semantic_eligible_shares"]
            ),
            "activity_trade_count": int(agg["eligible_activity_count"]),
            "activity_shares": float(agg["eligible_activity_shares"]),
            "aggressor_trade_count": eligible_aggressor,
        },
        "reporting_latency_gate": {
            "max_reporting_latency_ms": config.max_reporting_latency_ns / NS_PER_MS,
            "rule": "0 <= sip_timestamp - participant_timestamp <= max_reporting_latency",
            "otherwise_eligible_failed_count": failed,
            "otherwise_eligible_failed_fraction": (
                failed / pre_latency if pre_latency else None
            ),
            "otherwise_eligible_failed_shares": float(
                agg["otherwise_eligible_failed_latency_shares"]
            ),
            "negative_latency_count": int(agg["negative_latency_count"]),
            "latency_over_threshold_count": int(agg["over_latency_threshold_count"]),
            "aggressor_eligible_before_latency_count": int(
                agg["aggressor_eligible_before_latency_count"]
            ),
            "aggressor_lost_to_latency_count": int(
                agg["aggressor_lost_to_latency_count"]
            ),
            "trf": {
                "otherwise_eligible_failed_count": int(
                    agg["trf_otherwise_eligible_failed_latency_count"]
                ),
                "otherwise_eligible_failed_shares": float(
                    agg["trf_otherwise_eligible_failed_latency_shares"]
                ),
                "negative_latency_count": int(agg["trf_negative_latency_count"]),
                "latency_over_threshold_count": int(
                    agg["trf_over_latency_threshold_count"]
                ),
            },
            "exchange": {
                "otherwise_eligible_failed_count": int(
                    agg["exchange_otherwise_eligible_failed_latency_count"]
                ),
                "otherwise_eligible_failed_shares": float(
                    agg["exchange_otherwise_eligible_failed_latency_shares"]
                ),
                "negative_latency_count": int(agg["exchange_negative_latency_count"]),
                "latency_over_threshold_count": int(
                    agg["exchange_over_latency_threshold_count"]
                ),
            },
        },
        "aggressor": {
            "side_counts": _counter_to_json(agg["side_counts"]),
            "confidence_counts": _counter_to_json(agg["confidence_counts"]),
            "method_counts": _counter_to_json(agg["method_counts"]),
            "classified_fraction_of_eligible_aggressor_trades": (
                classified / eligible_aggressor if eligible_aggressor else None
            ),
            "buyer_shares": float(agg["buyer_shares"]),
            "seller_shares": float(agg["seller_shares"]),
            "unclassified_shares": float(agg["unclassified_shares"]),
            "quote_age_ms": _sample_percentiles(
                agg["quote_age_sampler"],
                NS_PER_MS,
                agg["max_quote_age_ns"],
            ),
            "quote_age_cutoff_policy": "none; quote age is metadata only",
        },
        "trf_count": int(agg["trf_count"]),
        "odd_lot_count": int(agg["odd_lot_count"]),
        "participant_to_sip_latency_ms": _sample_percentiles(
            agg["latency_sampler"], NS_PER_MS, agg["max_latency_ns"]
        ),
        "quality_alerts": alerts,
    }


def process_trade_batches(
    trade_path: Path,
    quote_state: QuoteState,
    config: PreprocessConfig | None = None,
    *,
    consumer: TradeBatchConsumer | None = None,
    audit_output_path: Path | None = None,
) -> dict:
    """Stream processed trade batches, optionally consuming/writing each batch.

    Parameters
    ----------
    consumer:
        Callback invoked once per ``ProcessedTradeBatch``.  A future
        ``build_tape_primitives.py`` should pass its per-batch aggregator here. The
        callback must not retain batches unless it intentionally wants the
        memory cost.
    audit_output_path:
        Optional enriched event-level Parquet for debugging.  Omit in normal
        production.

    Returns
    -------
    dict
        Final trade QA for the symbol-day.
    """

    config = config or PreprocessConfig()
    _validate_config(config)
    trade_path = Path(trade_path)
    tpf = pq.ParquetFile(str(trade_path), memory_map=config.memory_map_inputs)
    missing = [name for name in TRADE_COLUMNS if name not in tpf.schema_arrow.names]
    if missing:
        raise ValueError(f"{trade_path}: missing trade columns: {missing}")

    agg = _new_trade_qa_aggregate()
    writer: pq.ParquetWriter | None = None
    saw_batch = False

    try:
        for processed in iter_processed_trade_batches(trade_path, quote_state, config):
            saw_batch = True
            _update_trade_qa_aggregate(agg, processed.qa)

            if consumer is not None:
                consumer(processed)

            if audit_output_path is not None:
                out = processed.to_audit_table()
                if writer is None:
                    audit_output_path.parent.mkdir(parents=True, exist_ok=True)
                    writer = pq.ParquetWriter(
                        str(audit_output_path), out.schema, **_writer_kwargs(config)
                    )
                writer.write_table(out, row_group_size=config.batch_size)

            # The consumer has completed; release this batch before reading the
            # next one.  This is the normal GiB-scale production behavior.
            del processed

        if audit_output_path is not None and not saw_batch:
            trade_schema = pa.schema(
                [tpf.schema_arrow.field(name) for name in TRADE_COLUMNS]
            )
            empty = _empty_trade_output_table(trade_schema, quote_state)
            audit_output_path.parent.mkdir(parents=True, exist_ok=True)
            writer = pq.ParquetWriter(
                str(audit_output_path), empty.schema, **_writer_kwargs(config)
            )
            writer.write_table(empty)
    finally:
        if writer is not None:
            writer.close()

    return _finalize_trade_qa(agg, config)


# ---------------------------------------------------------------------------
# Symbol-day orchestration / importable production API
# ---------------------------------------------------------------------------
def _build_qa_manifest(
    job: Job, config: PreprocessConfig, quote_qa: dict, trade_qa: dict
) -> dict:
    return {
        "build_version": SCRIPT_VERSION,
        "date": job.date,
        "symbol": job.symbol,
        "inputs": {
            "trades": str(job.trade_path),
            "quotes": str(job.quote_path),
            "trade_file_bytes": job.trade_path.stat().st_size,
            "quote_file_bytes": job.quote_path.stat().st_size,
        },
        "policy": {
            "causal_clock": "sip_timestamp",
            "participant_timestamp_role": "diagnostic/execution time only; never backdates information",
            "trade_quote_alignment": "latest quote with quote.sip_timestamp < trade.sip_timestamp",
            "equal_cross_stream_timestamp_policy": "trade cannot use quote at identical SIP timestamp",
            "cross_stream_sequence_policy": "sequence_number is not used to order trades versus quotes",
            "safe_pressure_trade_conditions": sorted(SAFE_PRESSURE_TRADE_CONDITIONS),
            "safe_price_trade_conditions": sorted(SAFE_PRICE_TRADE_CONDITIONS),
            "causal_trade_payload_correction_codes": sorted(
                CAUSAL_TRADE_PAYLOAD_CORRECTIONS
            ),
            "correction_policy": "observed_as_known; no retrospective pressure-feature mutation",
            "correction_code_1_policy": "excluded_pending_linkage_validation",
            "current_pressure_reporting_latency_ms": config.max_reporting_latency_ns
            / NS_PER_MS,
            "current_pressure_reporting_latency_rule": "0 <= participant_to_sip_latency <= threshold",
            "locked_midpoint_policy": "midpoint_valid_but_aggressor_unusable",
            "crossed_midpoint_policy": "midpoint_invalid",
            "missing_or_one_sided_midpoint_policy": "midpoint_invalid; previous valid midpoint is not substituted",
            "quote_age_policy": "measured metadata only; no hard staleness cutoff",
            "aggressor_labels": {
                "at_ask": "buyer/high",
                "inside_above_mid": "buyer/medium",
                "at_bid": "seller/high",
                "inside_below_mid": "seller/medium",
                "midpoint_or_outside_or_unusable": "unclassified/unclassified",
            },
        },
        "quotes": quote_qa,
        "trades": trade_qa,
    }


def process_symbol_day(
    job: Job,
    config: PreprocessConfig | None = None,
    *,
    trade_batch_consumer: TradeBatchConsumer | None = None,
    audit_paths: AuditPaths | None = None,
    qa_output_path: Path | None = None,
    overwrite: bool = False,
) -> SymbolDayResult:
    """Process one symbol-day and return reusable state + QA.

    This is the main API intended for ``build_tape_primitives.py``. In normal use:

    - ``audit_paths=None``
    - ``qa_output_path=None`` (or a small QA path if desired)
    - ``trade_batch_consumer`` points at the 1-second feature accumulator

    No full processed trade day is retained or written.  Quote semantics are
    returned as ``QuoteState`` so interval/time-weighted quote features can be
    computed without rereading or reinterpreting the raw quote file.
    """

    config = config or PreprocessConfig()
    _validate_config(config)
    t0 = time.perf_counter()

    if not job.trade_path.exists():
        raise FileNotFoundError(job.trade_path)
    if not job.quote_path.exists():
        raise FileNotFoundError(job.quote_path)

    final_paths: list[Path] = []
    if audit_paths is not None:
        final_paths.extend([audit_paths.market_state, audit_paths.trade_events])
    if qa_output_path is not None:
        final_paths.append(qa_output_path)

    if final_paths and not overwrite:
        existing = [p for p in final_paths if p.exists()]
        if existing:
            raise FileExistsError(
                "Refusing to overwrite existing audit/QA output(s): "
                + ", ".join(str(p) for p in existing)
            )

    state_tmp: Path | None = None
    trade_tmp: Path | None = None
    qa_tmp: Path | None = None
    if audit_paths is not None:
        audit_paths.market_state.parent.mkdir(parents=True, exist_ok=True)
        audit_paths.trade_events.parent.mkdir(parents=True, exist_ok=True)
        state_tmp = _tmp_path(audit_paths.market_state)
        trade_tmp = _tmp_path(audit_paths.trade_events)
    if qa_output_path is not None:
        qa_output_path.parent.mkdir(parents=True, exist_ok=True)
        qa_tmp = _tmp_path(qa_output_path)

    for tmp in [state_tmp, trade_tmp, qa_tmp]:
        if tmp is not None:
            tmp.unlink(missing_ok=True)

    try:
        quote_state, quote_qa = prepare_quote_state(
            job.quote_path,
            job.date,
            config,
            audit_output_path=state_tmp,
        )
        gc.collect()

        trade_qa = process_trade_batches(
            job.trade_path,
            quote_state,
            config,
            consumer=trade_batch_consumer,
            audit_output_path=trade_tmp,
        )
        gc.collect()

        qa_manifest = _build_qa_manifest(job, config, quote_qa, trade_qa)
        qa_manifest["runtime_seconds"] = time.perf_counter() - t0

        if qa_tmp is not None:
            with qa_tmp.open("w", encoding="utf-8") as fh:
                json.dump(qa_manifest, fh, indent=2, sort_keys=True)
                fh.write("\n")

        # Commit audit artifacts only after the whole symbol-day succeeds.
        if audit_paths is not None:
            assert state_tmp is not None and trade_tmp is not None
            os.replace(state_tmp, audit_paths.market_state)
            os.replace(trade_tmp, audit_paths.trade_events)
        if qa_output_path is not None:
            assert qa_tmp is not None
            os.replace(qa_tmp, qa_output_path)

        return SymbolDayResult(
            job=job,
            quote_state=quote_state,
            quote_qa=quote_qa,
            trade_qa=trade_qa,
            qa_manifest=qa_manifest,
        )
    finally:
        # Only private temporary files are cleaned.  Existing final/raw files
        # are never deleted by this module.
        for tmp in [state_tmp, trade_tmp, qa_tmp]:
            if tmp is not None:
                tmp.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Discovery / optional audit CLI
# ---------------------------------------------------------------------------
def discover_jobs(
    project_root: Path,
    symbols: set[str] | None,
    exact_date: str | None,
    start_date: str | None,
    end_date: str | None,
) -> list[Job]:
    tick_data_root = _data_root(project_root) / "raw" / "tick_data"
    sessions_root = tick_data_root / "sessions"
    if not sessions_root.exists():
        raise FileNotFoundError(f"Missing sessions root: {sessions_root}")

    jobs: list[Job] = []
    for date_dir in sorted(p for p in sessions_root.iterdir() if p.is_dir()):
        date_str = date_dir.name
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            continue
        if exact_date is not None and date_str != exact_date:
            continue
        if start_date is not None and date_str < start_date:
            continue
        if end_date is not None and date_str > end_date:
            continue

        for symbol_dir in sorted(p for p in date_dir.iterdir() if p.is_dir()):
            symbol = symbol_dir.name.upper()
            if symbols is not None and symbol not in symbols:
                continue
            trade_path = symbol_dir / "trades.parquet"
            quote_path = symbol_dir / "quotes.parquet"
            if not trade_path.exists():
                raise FileNotFoundError(
                    f"Missing trade file for {date_str} {symbol}: {trade_path}"
                )
            if not quote_path.exists():
                raise FileNotFoundError(
                    f"Missing quote file for {date_str} {symbol}: {quote_path}"
                )
            jobs.append(
                Job(
                    date=date_str,
                    symbol=symbol,
                    trade_path=trade_path,
                    quote_path=quote_path,
                )
            )
    return jobs


def _audit_paths(project_root: Path, job: Job) -> AuditPaths:
    audit_root = _data_root(project_root) / "audit"
    return AuditPaths(
        market_state=audit_root / "market_state" / job.date / f"{job.symbol}.parquet",
        trade_events=audit_root / "trade_events" / job.date / f"{job.symbol}.parquet",
    )


def _qa_path(project_root: Path, job: Job) -> Path:
    return project_root / "quality" / "market_state" / job.date / f"{job.symbol}.json"


def _run_cli_job(
    job: Job,
    project_root: Path,
    config: PreprocessConfig,
    write_intermediates: bool,
    write_qa: bool,
    overwrite: bool,
) -> dict:
    t0 = time.perf_counter()
    audits = _audit_paths(project_root, job) if write_intermediates else None
    qa_path = _qa_path(project_root, job) if (write_qa or write_intermediates) else None
    result = process_symbol_day(
        job,
        config,
        audit_paths=audits,
        qa_output_path=qa_path,
        overwrite=overwrite,
    )
    elapsed = time.perf_counter() - t0
    return {
        "date": job.date,
        "symbol": job.symbol,
        "status": "processed",
        "seconds": elapsed,
        "quote_rows": result.quote_qa["input_rows"],
        "state_rows": result.quote_qa["state_rows"],
        "trade_rows": result.trade_qa["input_rows"],
        "classified_fraction": result.trade_qa["aggressor"][
            "classified_fraction_of_eligible_aggressor_trades"
        ],
        "latency_failed_fraction": result.trade_qa["reporting_latency_gate"][
            "otherwise_eligible_failed_fraction"
        ],
        "wrote_intermediates": write_intermediates,
        "wrote_qa": qa_path is not None,
    }


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate/replay deterministic SIP-time market state and trade "
            "semantics. Full intermediates are opt-in."
        )
    )
    default_root = Path(__file__).resolve().parents[1]
    parser.add_argument(
        "--project-root",
        type=Path,
        default=default_root,
        help=f"Project root (default: {default_root})",
    )
    parser.add_argument(
        "--symbols", nargs="+", help="Optional symbols, e.g. --symbols AAPL MU NVDA"
    )
    parser.add_argument("--symbol", help="Convenience alias for one symbol")
    parser.add_argument("--date", help="One date, YYYY-MM-DD")
    parser.add_argument("--start-date", help="Inclusive YYYY-MM-DD")
    parser.add_argument("--end-date", help="Inclusive YYYY-MM-DD")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help=(
            "Parallel independent symbol-days. Default 1 because each worker "
            "retains one symbol-day of quote state in RAM."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250_000,
        help="Arrow trade/quote rows per processing batch (default 250000)",
    )
    parser.add_argument(
        "--max-reporting-latency-ms",
        type=float,
        default=1000.0,
        help=(
            "Maximum nonnegative participant->SIP latency usable for current "
            "trade pressure (default 1000 ms)."
        ),
    )
    parser.add_argument(
        "--write-intermediates",
        action="store_true",
        help=(
            "Write data/audit/market_state/ and data/audit/trade_events/ "
            "audit Parquets. Off by "
            "default; normal production should aggregate downstream directly."
        ),
    )
    parser.add_argument(
        "--write-qa",
        action="store_true",
        help="Write small quality/market_state/YYYY-MM-DD/SYMBOL.json manifests.",
    )
    parser.add_argument(
        "--compression",
        default="zstd",
        choices=["zstd", "snappy", "gzip", "brotli", "lz4", "none"],
        help="Audit Parquet compression (default zstd)",
    )
    parser.add_argument(
        "--compression-level",
        type=int,
        default=3,
        help="Audit Parquet compression level when supported (default 3)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacement of explicitly requested audit/QA outputs",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")
    if args.batch_size < 1:
        raise SystemExit("--batch-size must be >= 1")
    if args.max_reporting_latency_ms < 0:
        raise SystemExit("--max-reporting-latency-ms must be >= 0")

    compression = None if args.compression == "none" else args.compression
    compression_level = args.compression_level if compression is not None else None

    symbols: set[str] | None = None
    supplied_symbols: list[str] = []
    if args.symbols:
        supplied_symbols.extend(args.symbols)
    if args.symbol:
        supplied_symbols.append(args.symbol)
    if supplied_symbols:
        symbols = {x.upper() for x in supplied_symbols}

    project_root = args.project_root.expanduser().resolve()
    load_dotenv(project_root / ".env")
    jobs = discover_jobs(
        project_root=project_root,
        symbols=symbols,
        exact_date=args.date,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    if not jobs:
        print("No matching symbol-days found.", file=sys.stderr)
        return 1

    config = PreprocessConfig(
        batch_size=args.batch_size,
        max_reporting_latency_ns=int(args.max_reporting_latency_ms * NS_PER_MS),
        compression=compression,
        compression_level=compression_level,
    )

    mode = (
        "audit intermediates + QA"
        if args.write_intermediates
        else "QA manifest only" if args.write_qa else "validation only (no outputs)"
    )
    print(
        f"Processing {len(jobs)} symbol-day(s) with {args.workers} worker(s); "
        f"batch_size={args.batch_size:,}; mode={mode}; "
        f"current_pressure_latency<={args.max_reporting_latency_ms:g}ms"
    )

    failures = 0
    if args.workers == 1:
        for i, job in enumerate(jobs, 1):
            try:
                result = _run_cli_job(
                    job,
                    project_root,
                    config,
                    args.write_intermediates,
                    args.write_qa,
                    args.overwrite,
                )
                frac = result["classified_fraction"]
                frac_text = "n/a" if frac is None else f"{frac:.1%}"
                late = result["latency_failed_fraction"]
                late_text = "n/a" if late is None else f"{late:.2%}"
                print(
                    f"[{i}/{len(jobs)}] {job.date} {job.symbol}: "
                    f"{result['trade_rows']:,} trades, "
                    f"{result['quote_rows']:,} quotes -> "
                    f"{result['state_rows']:,} states, "
                    f"classified={frac_text}, latency_rejected={late_text}, "
                    f"{result['seconds']:.2f}s"
                )
            except Exception as exc:
                failures += 1
                print(
                    f"[{i}/{len(jobs)}] {job.date} {job.symbol}: ERROR: {exc}",
                    file=sys.stderr,
                )
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            future_to_job = {
                executor.submit(
                    _run_cli_job,
                    job,
                    project_root,
                    config,
                    args.write_intermediates,
                    args.write_qa,
                    args.overwrite,
                ): job
                for job in jobs
            }
            completed = 0
            for future in as_completed(future_to_job):
                completed += 1
                job = future_to_job[future]
                try:
                    result = future.result()
                    frac = result["classified_fraction"]
                    frac_text = "n/a" if frac is None else f"{frac:.1%}"
                    late = result["latency_failed_fraction"]
                    late_text = "n/a" if late is None else f"{late:.2%}"
                    print(
                        f"[{completed}/{len(jobs)}] {job.date} {job.symbol}: "
                        f"{result['trade_rows']:,} trades, "
                        f"{result['quote_rows']:,} quotes -> "
                        f"{result['state_rows']:,} states, "
                        f"classified={frac_text}, latency_rejected={late_text}, "
                        f"{result['seconds']:.2f}s"
                    )
                except Exception as exc:
                    failures += 1
                    print(
                        f"[{completed}/{len(jobs)}] {job.date} {job.symbol}: "
                        f"ERROR: {exc}",
                        file=sys.stderr,
                    )

    if failures:
        print(f"Completed with {failures} failure(s).", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
