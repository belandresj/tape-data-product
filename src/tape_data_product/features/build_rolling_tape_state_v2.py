#!/usr/bin/env python3
"""Pure primitive-table-to-feature-table builder for Rolling Tape State V2."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import sys
from bisect import bisect_left, insort
from collections import Counter, deque
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = PROJECT_ROOT / "resources/feature_semantics.json"
from tape_data_product.features import rolling_tape_state_v2_halt_clock as CLOCK

NS = 1_000_000_000
NS_PER_MS = 1_000_000
MODEL_VERSION = "rolling_tape_state_v2"
IMPLEMENTATION_PROFILE = "extended_0400_2000_et_frozen_v2"
ELIGIBLE_TRADE_POPULATION_VERSION = "market_state_rth_plus_form_t_extended_v1"
SOURCE_ACCEPTANCE_POLICY_VERSION = "accepted_symbol_day_file_v1"
HALT_TREATMENT_VERSION = "accepted_halt_paused_state_wall_cost_v2"
FEATURE_SPEC_SHA256 = hashlib.sha256(SPEC_PATH.read_bytes()).hexdigest()
HORIZONS = (60, 300)
CONFIGURATION = {
    "state_horizons_observable_seconds": list(HORIZONS),
    "execution_cost_wall_seconds": 60,
    "movement_phase_seconds": 5,
    "movement_rate_unit_seconds": 30,
    "movement_concentration_seconds": 1,
    "movement_concentration_tail_fraction": 0.10,
    "state_support_fraction": 0.80,
    "cost_trust_fraction": 0.90,
    "halt_treatment": "pause_state_clocks_rebuild_wall_cost",
}
FEATURE_CONFIG_HASH = hashlib.sha256(
    json.dumps(CONFIGURATION, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()

REQUIRED_INPUT_FIELDS = (
    "session_date",
    "symbol",
    "interval_end",
    "continuity_segment_id",
    "midpoint_twap",
    "spread_twap_bps",
    "midpoint_valid_frac",
    "spread_valid_frac",
    "locked_frac",
    "quote_age_end_ms",
    "displayed_bid_notional_twap",
    "displayed_ask_notional_twap",
    "displayed_nbbo_notional_valid_frac",
    "quote_source_file_accepted",
    "eligible_trade_count",
    "eligible_share_volume",
    "eligible_dollar_volume",
    "eligible_trade_age_end_ms",
    "effective_spread_share_bps_sum",
    "effective_spread_contributing_shares",
    "trade_source_file_accepted",
    "halt_interval_active",
    "halt_interval_id",
    "halt_resume_boundary",
    "historical_ex_post_overlay",
    "halt_registry_version",
    "halt_registry_config_hash",
)


def _horizon_fields(horizon: int) -> tuple[list[str], list[str], list[str]]:
    suffix = f"{horizon}s"
    floats = [
        f"midpoint_movement_bps_per_30s_{suffix}",
        f"midpoint_movement_in_spreads_per_30s_{suffix}",
        f"midpoint_movement_top_10pct_share_{suffix}",
        f"trade_rate_{suffix}",
        f"dollar_rate_{suffix}",
        f"mean_displayed_bid_notional_{suffix}",
        f"mean_displayed_ask_notional_{suffix}",
        f"flow_to_displayed_depth_{suffix}",
        f"quote_age_p90_{suffix}",
        f"trade_age_p90_{suffix}",
        f"usable_nbbo_fraction_{suffix}",
        f"displayed_notional_valid_fraction_{suffix}",
        f"state_observed_second_fraction_{suffix}",
        f"movement_valid_5s_fraction_{suffix}",
        f"movement_valid_1s_fraction_{suffix}",
        f"activity_valid_second_fraction_{suffix}",
        f"state_pre_halt_observation_fraction_{suffix}",
    ]
    integers = [
        f"state_observed_second_count_{suffix}",
        f"movement_valid_5s_count_{suffix}",
        f"movement_valid_1s_count_{suffix}",
        f"activity_valid_second_count_{suffix}",
        f"quote_age_observation_count_{suffix}",
        f"trade_age_observation_count_{suffix}",
        f"state_post_halt_observed_seconds_{suffix}",
    ]
    booleans = [
        f"state_mature_{suffix}",
        f"movement_support_valid_{suffix}",
        f"activity_support_valid_{suffix}",
        f"displayed_notional_support_valid_{suffix}",
        f"state_contains_pre_halt_history_{suffix}",
        f"state_fully_post_halt_{suffix}",
        f"state_carried_forward_during_halt_{suffix}",
    ]
    return floats, integers, booleans


HORIZON_FLOAT_FIELDS = sum((_horizon_fields(h)[0] for h in HORIZONS), [])
HORIZON_INTEGER_FIELDS = sum((_horizon_fields(h)[1] for h in HORIZONS), [])
HORIZON_BOOL_FIELDS = sum((_horizon_fields(h)[2] for h in HORIZONS), [])
COST_FLOAT_FIELDS = [
    "quoted_spread_bps_60s",
    "effective_spread_bps_60s",
    "effective_to_quoted_spread_60s",
    "quoted_spread_valid_fraction_60s",
    "effective_spread_share_coverage_60s",
]
RATIO_FIELDS = [
    "midpoint_movement_fast_to_slow",
    "trade_rate_fast_to_slow",
    "dollar_rate_fast_to_slow",
    "flow_to_depth_fast_to_slow",
]
FLOAT_OUTPUT_FIELDS = tuple(HORIZON_FLOAT_FIELDS + COST_FLOAT_FIELDS + RATIO_FIELDS)
INTEGER_OUTPUT_FIELDS = tuple(HORIZON_INTEGER_FIELDS)
NULLABLE_INTEGER_OUTPUT_FIELDS = (
    "seconds_since_halt_resume",
    "current_cost_post_halt_observed_seconds_60s",
)
BOOLEAN_OUTPUT_FIELDS = tuple(
    HORIZON_BOOL_FIELDS
    + [
        "quoted_spread_valid_60s",
        "effective_spread_valid_60s",
    ]
)
VERSION_FIELDS = (
    "feature_model_version",
    "feature_spec_sha256",
    "feature_config_hash",
    "implementation_profile",
    "eligible_trade_population_version",
    "source_acceptance_policy_version",
    "halt_treatment_version",
)
OUTPUT_FIELDS = (
    FLOAT_OUTPUT_FIELDS
    + INTEGER_OUTPUT_FIELDS
    + NULLABLE_INTEGER_OUTPUT_FIELDS
    + BOOLEAN_OUTPUT_FIELDS
    + VERSION_FIELDS
)


def _as_table(seconds: pa.Table | pd.DataFrame) -> pa.Table:
    return (
        seconds
        if isinstance(seconds, pa.Table)
        else pa.Table.from_pandas(seconds, preserve_index=False)
    )


def _numeric(frame: Any, name: str) -> np.ndarray:
    values = frame[name]
    if isinstance(values, (pa.Array, pa.ChunkedArray)):
        return np.asarray(
            values.combine_chunks().to_numpy(zero_copy_only=False), dtype=np.float64
        )
    return np.asarray(values, dtype=np.float64)


def _validate(table: pa.Table) -> tuple[dict[str, np.ndarray], list[tuple[int, int]]]:
    missing = sorted(set(REQUIRED_INPUT_FIELDS) - set(table.column_names))
    if missing:
        raise ValueError(f"rolling_tape_state_v2 missing required fields: {missing}")
    overlap = sorted(
        (set(OUTPUT_FIELDS) - set(VERSION_FIELDS)) & set(table.column_names)
    )
    if overlap:
        raise ValueError(
            f"rolling_tape_state_v2 output fields already exist: {overlap}"
        )
    identities = {
        "feature_model_version": MODEL_VERSION,
        "feature_spec_sha256": FEATURE_SPEC_SHA256,
        "feature_config_hash": FEATURE_CONFIG_HASH,
        "implementation_profile": IMPLEMENTATION_PROFILE,
        "eligible_trade_population_version": ELIGIBLE_TRADE_POPULATION_VERSION,
        "source_acceptance_policy_version": SOURCE_ACCEPTANCE_POLICY_VERSION,
        "halt_treatment_version": HALT_TREATMENT_VERSION,
    }
    for name, expected in identities.items():
        if name in table.column_names and any(
            value != expected for value in table[name].combine_chunks().to_pylist()
        ):
            raise ValueError(f"pre-existing {name} does not match V2 identity")
    n = table.num_rows
    data: dict[str, np.ndarray] = {}
    for name in REQUIRED_INPUT_FIELDS:
        column = table[name].combine_chunks()
        if name == "interval_end":
            data[name] = np.asarray(
                pc.cast(column, pa.int64()).to_numpy(), dtype=np.int64
            )
        elif pa.types.is_boolean(column.type):
            data[name] = np.asarray(
                column.to_numpy(zero_copy_only=False),
                dtype=object if column.null_count else np.bool_,
            )
        elif pa.types.is_integer(column.type) or pa.types.is_floating(column.type):
            data[name] = np.asarray(column.to_numpy(zero_copy_only=False))
        else:
            data[name] = np.asarray(column.to_pylist(), dtype=object)
    partitions: list[tuple[int, int]] = []
    start = 0
    for i in range(1, n + 1):
        if (
            i == n
            or data["session_date"][i] != data["session_date"][start]
            or data["symbol"][i] != data["symbol"][start]
        ):
            partitions.append((start, i))
            ends = data["interval_end"][start:i]
            if np.any(ends % NS) or (ends.size > 1 and np.any(np.diff(ends) != NS)):
                raise ValueError("each partition must be an exact one-second grid")
            start = i
    keys = list(zip(data["session_date"], data["symbol"], data["interval_end"]))
    if any(keys[i] <= keys[i - 1] for i in range(1, len(keys))):
        if any(keys[i] == keys[i - 1] for i in range(1, len(keys))):
            raise ValueError("duplicate rolling-tape input key")
        raise ValueError("rolling-tape input keys are not stably sorted")
    for name in (
        "quote_source_file_accepted",
        "trade_source_file_accepted",
        "halt_interval_active",
        "halt_resume_boundary",
        "historical_ex_post_overlay",
    ):
        if any(value is None for value in data[name]):
            raise ValueError(f"{name} must be non-null")
    for name in (
        "midpoint_valid_frac",
        "spread_valid_frac",
        "locked_frac",
        "displayed_nbbo_notional_valid_frac",
    ):
        values = _numeric(data, name)
        if np.any(~np.isfinite(values)) or np.any(
            (values < -1e-12) | (values > 1.0 + 1e-12)
        ):
            raise ValueError(f"{name} must be finite and inside [0,1]")
        data[name] = np.clip(values, 0.0, 1.0)
    for fraction, value in (
        ("midpoint_valid_frac", "midpoint_twap"),
        ("spread_valid_frac", "spread_twap_bps"),
        ("displayed_nbbo_notional_valid_frac", "displayed_bid_notional_twap"),
        ("displayed_nbbo_notional_valid_frac", "displayed_ask_notional_twap"),
    ):
        f, v = _numeric(data, fraction), _numeric(data, value)
        if np.any((f > 0.0) & ~np.isfinite(v)):
            raise ValueError(f"positive {fraction} requires finite {value}")
    for name in (
        "eligible_trade_count",
        "eligible_share_volume",
        "eligible_dollar_volume",
        "eligible_trade_age_end_ms",
        "effective_spread_share_bps_sum",
        "effective_spread_contributing_shares",
        "quote_age_end_ms",
        "displayed_bid_notional_twap",
        "displayed_ask_notional_twap",
    ):
        values = _numeric(data, name)
        if np.any(np.isinf(values)) or np.any(np.isfinite(values) & (values < 0.0)):
            raise ValueError(f"{name} must be nonnegative or native null")
    counts = _numeric(data, "eligible_trade_count")
    if np.any(np.isfinite(counts) & (np.abs(counts - np.rint(counts)) > 1e-9)):
        raise ValueError("eligible_trade_count must be integral")
    eligible = _numeric(data, "eligible_share_volume")
    contributing = _numeric(data, "effective_spread_contributing_shares")
    tolerance = 1e-9 * np.maximum(1.0, eligible)
    if np.any(
        np.isfinite(eligible)
        & np.isfinite(contributing)
        & (contributing - eligible > tolerance)
    ):
        raise ValueError("effective-spread contributing shares exceed eligible shares")
    active = np.asarray(data["halt_interval_active"], dtype=bool)
    if np.any(active):
        if any(value is None for value in data["halt_interval_id"][active]):
            raise ValueError("active halt rows require halt_interval_id")
        if not np.asarray(data["historical_ex_post_overlay"][active], dtype=bool).all():
            raise ValueError("active halt rows require historical ex-post provenance")
        metadata = table.schema.metadata or {}
        if any(
            key not in metadata
            for key in (
                b"halt_registry_version",
                b"halt_registry_config_hash",
                b"halt_registry_sha256",
            )
        ):
            raise ValueError("active halt requires complete registry metadata")
    return data, partitions


def _top_share(values: np.ndarray) -> float:
    if values.size == 0:
        return np.nan
    total = float(np.sum(values))
    if not np.isfinite(total) or total <= 0.0:
        return np.nan
    k = max(1, math.ceil(0.10 * values.size))
    return float(np.sum(np.partition(values, values.size - k)[-k:]) / total)


def _ratio(numerator: float, denominator: float) -> float:
    if np.isfinite(numerator) and np.isfinite(denominator) and denominator > 0.0:
        return float(numerator / denominator)
    return np.nan


class _FiniteWindow:
    """Fixed-capacity finite-value window with bounded exact order statistics."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.queue: deque[float] = deque()
        self.sorted: list[float] = []
        self.total = 0.0

    def clear(self) -> None:
        self.queue.clear()
        self.sorted.clear()
        self.total = 0.0

    def append(self, value: float) -> None:
        if len(self.queue) == self.capacity:
            old = self.queue.popleft()
            if np.isfinite(old):
                position = bisect_left(self.sorted, old)
                self.sorted.pop(position)
                self.total -= old
        value = float(value)
        self.queue.append(value)
        if np.isfinite(value):
            insort(self.sorted, value)
            self.total += value

    @property
    def count(self) -> int:
        return len(self.sorted)

    def mean(self) -> float:
        return self.total / self.count if self.count else np.nan

    def quantile(self, probability: float) -> float:
        if not self.sorted:
            return np.nan
        position = (len(self.sorted) - 1) * probability
        lower = int(math.floor(position))
        upper = int(math.ceil(position))
        weight = position - lower
        return self.sorted[lower] * (1.0 - weight) + self.sorted[upper] * weight

    def top_share(self, fraction: float) -> float:
        if not self.sorted or self.total <= 0.0:
            return np.nan
        count = max(1, math.ceil(fraction * len(self.sorted)))
        return sum(self.sorted[-count:]) / self.total


class _SumWindow:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.queue: deque[float] = deque()
        self.total = 0.0

    def clear(self) -> None:
        self.queue.clear()
        self.total = 0.0

    def append(self, value: float) -> None:
        if len(self.queue) == self.capacity:
            self.total -= self.queue.popleft()
        value = float(value)
        self.queue.append(value)
        self.total += value


class _HorizonWindow:
    def __init__(self, horizon: int):
        self.horizon = horizon
        self.local_position = -1
        self.generations: deque[int] = deque()
        self.generation_counts: Counter[int] = Counter()
        self.move5 = _FiniteWindow(horizon - 4)
        self.move1 = _FiniteWindow(horizon)
        self.trade_count = _FiniteWindow(horizon)
        self.dollars = _FiniteWindow(horizon)
        self.quote_age = _FiniteWindow(horizon)
        self.trade_age = _FiniteWindow(horizon)
        self.midpoint_duration = _SumWindow(horizon)
        self.depth_duration = _SumWindow(horizon)
        self.bid_weight = _SumWindow(horizon)
        self.ask_weight = _SumWindow(horizon)

    def clear(self) -> None:
        self.local_position = -1
        self.generations.clear()
        self.generation_counts.clear()
        for window in (
            self.move5,
            self.move1,
            self.trade_count,
            self.dollars,
            self.quote_age,
            self.trade_age,
            self.midpoint_duration,
            self.depth_duration,
            self.bid_weight,
            self.ask_weight,
        ):
            window.clear()

    def append(
        self,
        *,
        generation: int,
        move5: float,
        move1: float,
        activity_valid: bool,
        trade_count: float,
        dollars: float,
        quote_age: float,
        trade_age: float,
        midpoint_duration: float,
        depth_duration: float,
        bid: float,
        ask: float,
    ) -> None:
        self.local_position += 1
        if len(self.generations) == self.horizon:
            old = self.generations.popleft()
            self.generation_counts[old] -= 1
            if not self.generation_counts[old]:
                del self.generation_counts[old]
        self.generations.append(generation)
        self.generation_counts[generation] += 1
        if self.local_position >= 4:
            self.move5.append(move5)
        self.move1.append(move1)
        self.trade_count.append(trade_count if activity_valid else np.nan)
        self.dollars.append(dollars if activity_valid else np.nan)
        self.quote_age.append(quote_age)
        self.trade_age.append(trade_age)
        self.midpoint_duration.append(midpoint_duration)
        self.depth_duration.append(depth_duration)
        self.bid_weight.append(bid * depth_duration if depth_duration > 0.0 else 0.0)
        self.ask_weight.append(ask * depth_duration if depth_duration > 0.0 else 0.0)


def _partition_features(frame: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    n = len(frame["interval_end"])
    floats = {name: np.full(n, np.nan) for name in FLOAT_OUTPUT_FIELDS}
    integers = {
        name: np.full(n, -1, dtype=np.int64)
        for name in INTEGER_OUTPUT_FIELDS + NULLABLE_INTEGER_OUTPUT_FIELDS
    }
    booleans = {name: np.zeros(n, dtype=bool) for name in BOOLEAN_OUTPUT_FIELDS}
    ends = np.asarray(frame["interval_end"], dtype=np.int64)
    continuity = _numeric(frame, "continuity_segment_id").astype(np.int64)
    active = np.asarray(frame["halt_interval_active"], dtype=bool)
    resume = np.asarray(frame["halt_resume_boundary"], dtype=bool)
    clock = CLOCK.build_halt_clock(ends, continuity, active, resume)
    observed_rows = clock.observed_row_indices
    m = observed_rows.size

    if m:
        obs = {name: values[observed_rows] for name, values in frame.items()}
        midpoint = _numeric(obs, "midpoint_twap")
        midpoint_fraction = _numeric(obs, "midpoint_valid_frac")
        quote_age_raw = _numeric(obs, "quote_age_end_ms")
        trade_age_raw = _numeric(obs, "eligible_trade_age_end_ms")
        bid = _numeric(obs, "displayed_bid_notional_twap")
        ask = _numeric(obs, "displayed_ask_notional_twap")
        depth_fraction = _numeric(obs, "displayed_nbbo_notional_valid_frac")
        trade_count = _numeric(obs, "eligible_trade_count")
        dollars = _numeric(obs, "eligible_dollar_volume")
        quote_ok = np.asarray(obs["quote_source_file_accepted"], dtype=bool)
        trade_ok = np.asarray(obs["trade_source_file_accepted"], dtype=bool)
        activity_valid = (
            trade_ok
            & np.isfinite(trade_count)
            & (trade_count >= 0.0)
            & np.isfinite(dollars)
            & (dollars >= 0.0)
        )

        move5 = np.full(m, np.nan)
        move1 = np.full(m, np.nan)
        for u in range(m):
            if (
                u >= 5
                and clock.state_epoch_id[u] == clock.state_epoch_id[u - 5]
                and clock.halt_generation[u] == clock.halt_generation[u - 5]
                and np.all(quote_ok[u - 4 : u + 1])
                and np.isfinite(midpoint[u])
                and midpoint[u] > 0.0
                and np.isfinite(midpoint[u - 5])
                and midpoint[u - 5] > 0.0
            ):
                move5[u] = abs(10_000.0 * math.log(midpoint[u] / midpoint[u - 5]))
            if (
                u >= 1
                and clock.state_epoch_id[u] == clock.state_epoch_id[u - 1]
                and clock.halt_generation[u] == clock.halt_generation[u - 1]
                and quote_ok[u]
                and quote_ok[u - 1]
                and np.isfinite(midpoint[u])
                and midpoint[u] > 0.0
                and np.isfinite(midpoint[u - 1])
                and midpoint[u - 1] > 0.0
            ):
                move1[u] = abs(10_000.0 * math.log(midpoint[u] / midpoint[u - 1]))

        quote_age = np.full(m, np.nan)
        trade_age = np.full(m, np.nan)
        generation_starts = np.zeros(m, dtype=np.int64)
        for u in range(1, m):
            generation_starts[u] = generation_starts[u - 1]
            if (
                clock.state_epoch_id[u] != clock.state_epoch_id[u - 1]
                or clock.halt_generation[u] != clock.halt_generation[u - 1]
            ):
                generation_starts[u] = u
        for u in range(m):
            boundary = int(
                max(clock.state_epoch_start_position[u], generation_starts[u])
            )
            require_new_origin = boundary > 0 or clock.halt_generation[u] > 0
            boundary_ns = ends[int(observed_rows[boundary])] - NS
            for raw, target in ((quote_age_raw, quote_age), (trade_age_raw, trade_age)):
                if np.isfinite(raw[u]):
                    event_ns = ends[int(observed_rows[u])] - int(
                        round(raw[u] * NS_PER_MS)
                    )
                    if not require_new_origin or event_ns >= boundary_ns:
                        target[u] = raw[u]

        windows = {horizon: _HorizonWindow(horizon) for horizon in HORIZONS}
        latest_resume = -1
        for j in range(m):
            if clock.resume_boundary_on_observed_clock[j]:
                latest_resume = j
            if j == 0 or clock.state_epoch_id[j] != clock.state_epoch_id[j - 1]:
                for window in windows.values():
                    window.clear()
            wall = int(observed_rows[j])
            for horizon in HORIZONS:
                suffix = f"{horizon}s"
                window = windows[horizon]
                window.append(
                    generation=int(clock.halt_generation[j]),
                    move5=move5[j],
                    move1=move1[j],
                    activity_valid=bool(activity_valid[j]),
                    trade_count=trade_count[j],
                    dollars=dollars[j],
                    quote_age=quote_age[j],
                    trade_age=trade_age[j],
                    midpoint_duration=midpoint_fraction[j],
                    depth_duration=depth_fraction[j],
                    bid=bid[j],
                    ask=ask[j],
                )
                state_count = len(window.generations)
                integers[f"state_observed_second_count_{suffix}"][wall] = state_count
                floats[f"state_observed_second_fraction_{suffix}"][wall] = (
                    state_count / horizon
                )
                booleans[f"state_mature_{suffix}"][wall] = state_count == horizon
                integers[f"movement_valid_5s_count_{suffix}"][wall] = window.move5.count
                floats[f"movement_valid_5s_fraction_{suffix}"][wall] = (
                    window.move5.count / (horizon - 4)
                )
                integers[f"movement_valid_1s_count_{suffix}"][wall] = window.move1.count
                floats[f"movement_valid_1s_fraction_{suffix}"][wall] = (
                    window.move1.count / horizon
                )
                integers[f"activity_valid_second_count_{suffix}"][
                    wall
                ] = window.trade_count.count
                floats[f"activity_valid_second_fraction_{suffix}"][wall] = (
                    window.trade_count.count / horizon
                )
                integers[f"quote_age_observation_count_{suffix}"][
                    wall
                ] = window.quote_age.count
                integers[f"trade_age_observation_count_{suffix}"][
                    wall
                ] = window.trade_age.count
                booleans[f"movement_support_valid_{suffix}"][wall] = (
                    state_count >= math.ceil(0.80 * horizon)
                    and window.move5.count >= math.ceil(0.80 * (horizon - 4))
                    and window.move1.count >= math.ceil(0.80 * horizon)
                )
                booleans[f"activity_support_valid_{suffix}"][wall] = (
                    window.trade_count.count >= math.ceil(0.80 * horizon)
                )
                if window.move5.count:
                    floats[f"midpoint_movement_bps_per_30s_{suffix}"][wall] = (
                        6.0 * window.move5.mean()
                    )
                floats[f"midpoint_movement_top_10pct_share_{suffix}"][wall] = (
                    window.move1.top_share(0.10)
                )
                if window.trade_count.count:
                    floats[f"trade_rate_{suffix}"][wall] = window.trade_count.mean()
                    floats[f"dollar_rate_{suffix}"][wall] = window.dollars.mean()
                depth_duration = window.depth_duration.total
                depth_coverage = depth_duration / horizon
                floats[f"displayed_notional_valid_fraction_{suffix}"][
                    wall
                ] = depth_coverage
                booleans[f"displayed_notional_support_valid_{suffix}"][wall] = (
                    state_count >= math.ceil(0.80 * horizon) and depth_coverage >= 0.80
                )
                if depth_duration > 0.0:
                    bid_mean = window.bid_weight.total / depth_duration
                    ask_mean = window.ask_weight.total / depth_duration
                    floats[f"mean_displayed_bid_notional_{suffix}"][wall] = bid_mean
                    floats[f"mean_displayed_ask_notional_{suffix}"][wall] = ask_mean
                    dollar_rate = floats[f"dollar_rate_{suffix}"][wall]
                    if np.isfinite(dollar_rate) and bid_mean + ask_mean > 0.0:
                        floats[f"flow_to_displayed_depth_{suffix}"][wall] = (
                            dollar_rate / (bid_mean + ask_mean)
                        )
                floats[f"usable_nbbo_fraction_{suffix}"][wall] = (
                    window.midpoint_duration.total / horizon
                )
                floats[f"quote_age_p90_{suffix}"][wall] = window.quote_age.quantile(
                    0.90
                )
                floats[f"trade_age_p90_{suffix}"][wall] = window.trade_age.quantile(
                    0.90
                )
                if latest_resume < 0:
                    pre_count, post_total, fully = 0, 0, True
                else:
                    pre_count = (
                        state_count
                        - window.generation_counts[int(clock.halt_generation[j])]
                    )
                    post_total = j - latest_resume + 1
                    fully = state_count == horizon and post_total >= horizon + 1
                floats[f"state_pre_halt_observation_fraction_{suffix}"][wall] = (
                    pre_count / state_count
                )
                integers[f"state_post_halt_observed_seconds_{suffix}"][wall] = min(
                    post_total, horizon
                )
                booleans[f"state_contains_pre_halt_history_{suffix}"][wall] = (
                    pre_count > 0
                )
                booleans[f"state_fully_post_halt_{suffix}"][wall] = fully
            if latest_resume >= 0:
                integers["seconds_since_halt_resume"][wall] = j - latest_resume + 1

        for wall in range(n):
            pos = int(clock.wall_row_to_latest_observed_position[wall])
            if pos < 0:
                continue
            if active[wall]:
                source_wall = int(observed_rows[pos])
                for name in HORIZON_FLOAT_FIELDS:
                    floats[name][wall] = floats[name][source_wall]
                for name in HORIZON_INTEGER_FIELDS:
                    integers[name][wall] = integers[name][source_wall]
                for name in HORIZON_BOOL_FIELDS:
                    booleans[name][wall] = booleans[name][source_wall]
                for horizon in HORIZONS:
                    suffix = f"{horizon}s"
                    floats[f"midpoint_movement_in_spreads_per_30s_{suffix}"][
                        wall
                    ] = np.nan
                    floats[f"state_pre_halt_observation_fraction_{suffix}"][wall] = 1.0
                    integers[f"state_post_halt_observed_seconds_{suffix}"][wall] = 0
                    booleans[f"state_contains_pre_halt_history_{suffix}"][wall] = True
                    booleans[f"state_fully_post_halt_{suffix}"][wall] = False
                    booleans[f"state_carried_forward_during_halt_{suffix}"][wall] = True

    quote_source = np.asarray(frame["quote_source_file_accepted"], dtype=bool)
    trade_source = np.asarray(frame["trade_source_file_accepted"], dtype=bool)
    spread_wall = _numeric(frame, "spread_twap_bps")
    spread_fraction_wall = _numeric(frame, "spread_valid_frac")
    locked_wall = _numeric(frame, "locked_frac")
    shares_wall = _numeric(frame, "eligible_share_volume")
    effective_sum_wall = _numeric(frame, "effective_spread_share_bps_sum")
    effective_shares_wall = _numeric(frame, "effective_spread_contributing_shares")
    cost_epoch = np.zeros(n, dtype=np.int64)
    for i in range(1, n):
        cost_epoch[i] = cost_epoch[i - 1] + int(
            resume[i]
            or continuity[i] != continuity[i - 1]
            or ends[i] - ends[i - 1] != NS
        )
    latest_resume_wall = -1
    for i in range(n):
        if active[i]:
            integers["current_cost_post_halt_observed_seconds_60s"][i] = 0
            continue
        if resume[i]:
            latest_resume_wall = i
        integers["current_cost_post_halt_observed_seconds_60s"][i] = (
            min(i - latest_resume_wall + 1, 60) if latest_resume_wall >= 0 else 0
        )
        if i < 59:
            continue
        left = i - 59
        structural = (
            np.all(cost_epoch[left : i + 1] == cost_epoch[i])
            and not np.any(active[left : i + 1])
            and np.all(np.diff(ends[left : i + 1]) == NS)
        )
        if not structural:
            continue
        if np.all(quote_source[left : i + 1]):
            local_fraction = spread_fraction_wall[left : i + 1]
            unlocked = np.maximum(local_fraction - locked_wall[left : i + 1], 0.0)
            denominator = float(np.sum(unlocked))
            coverage = denominator / 60.0
            floats["quoted_spread_valid_fraction_60s"][i] = coverage
            booleans["quoted_spread_valid_60s"][i] = coverage >= 0.90
            if denominator > 0.0:
                numerator = float(
                    np.sum(
                        np.where(
                            local_fraction > 0.0,
                            spread_wall[left : i + 1] * local_fraction,
                            0.0,
                        )
                    )
                )
                value = numerator / denominator
                if np.isfinite(value) and value >= 0.0:
                    floats["quoted_spread_bps_60s"][i] = value
        if np.all(quote_source[left : i + 1]) and np.all(trade_source[left : i + 1]):
            eligible_shares = float(np.sum(shares_wall[left : i + 1]))
            contributing = float(np.sum(effective_shares_wall[left : i + 1]))
            if eligible_shares > 0.0:
                coverage = contributing / eligible_shares
                floats["effective_spread_share_coverage_60s"][i] = coverage
                booleans["effective_spread_valid_60s"][i] = coverage >= 0.90
                if contributing > 0.0:
                    value = float(
                        np.sum(effective_sum_wall[left : i + 1]) / contributing
                    )
                    if np.isfinite(value) and value >= 0.0:
                        floats["effective_spread_bps_60s"][i] = value
        floats["effective_to_quoted_spread_60s"][i] = _ratio(
            floats["effective_spread_bps_60s"][i], floats["quoted_spread_bps_60s"][i]
        )

    for i in range(n):
        for horizon in HORIZONS:
            suffix = f"{horizon}s"
            floats[f"midpoint_movement_in_spreads_per_30s_{suffix}"][i] = _ratio(
                floats[f"midpoint_movement_bps_per_30s_{suffix}"][i],
                floats["quoted_spread_bps_60s"][i],
            )
        floats["midpoint_movement_fast_to_slow"][i] = _ratio(
            floats["midpoint_movement_in_spreads_per_30s_60s"][i],
            floats["midpoint_movement_in_spreads_per_30s_300s"][i],
        )
        floats["trade_rate_fast_to_slow"][i] = _ratio(
            floats["trade_rate_60s"][i], floats["trade_rate_300s"][i]
        )
        floats["dollar_rate_fast_to_slow"][i] = _ratio(
            floats["dollar_rate_60s"][i], floats["dollar_rate_300s"][i]
        )
        floats["flow_to_depth_fast_to_slow"][i] = _ratio(
            floats["flow_to_displayed_depth_60s"][i],
            floats["flow_to_displayed_depth_300s"][i],
        )
    return floats | integers | booleans


def build_rolling_tape_state_v2(seconds: pa.Table | pd.DataFrame) -> pa.Table:
    """Append the complete frozen causal V2 state to sorted primitives."""
    table = _as_table(seconds)
    frame, partitions = _validate(table)
    n = table.num_rows
    combined: dict[str, np.ndarray] = {
        name: np.full(n, np.nan) for name in FLOAT_OUTPUT_FIELDS
    }
    combined.update(
        {
            name: np.full(n, -1, dtype=np.int64)
            for name in INTEGER_OUTPUT_FIELDS + NULLABLE_INTEGER_OUTPUT_FIELDS
        }
    )
    combined.update({name: np.zeros(n, dtype=bool) for name in BOOLEAN_OUTPUT_FIELDS})
    for start, stop in partitions:
        partition = {name: values[start:stop] for name, values in frame.items()}
        values = _partition_features(partition)
        for name, array in values.items():
            combined[name][start:stop] = array
    output = table
    for name in FLOAT_OUTPUT_FIELDS:
        values = combined[name]
        output = output.append_column(
            name, pa.array(values, mask=~np.isfinite(values), type=pa.float64())
        )
    for name in INTEGER_OUTPUT_FIELDS + NULLABLE_INTEGER_OUTPUT_FIELDS:
        values = combined[name]
        output = output.append_column(
            name, pa.array(values, mask=values < 0, type=pa.int64())
        )
    for name in BOOLEAN_OUTPUT_FIELDS:
        output = output.append_column(name, pa.array(combined[name], type=pa.bool_()))
    constants = {
        "feature_model_version": MODEL_VERSION,
        "feature_spec_sha256": FEATURE_SPEC_SHA256,
        "feature_config_hash": FEATURE_CONFIG_HASH,
        "implementation_profile": IMPLEMENTATION_PROFILE,
        "eligible_trade_population_version": ELIGIBLE_TRADE_POPULATION_VERSION,
        "source_acceptance_policy_version": SOURCE_ACCEPTANCE_POLICY_VERSION,
        "halt_treatment_version": HALT_TREATMENT_VERSION,
    }
    for name, value in constants.items():
        array = pa.array([value] * n, type=pa.string())
        output = (
            output.set_column(output.schema.get_field_index(name), name, array)
            if name in output.column_names
            else output.append_column(name, array)
        )
    metadata = dict(table.schema.metadata or {})
    metadata.update(
        {name.encode(): value.encode() for name, value in constants.items()}
    )
    return output.replace_schema_metadata(metadata)


def build_parquet_file(input_path: Path, output_path: Path) -> Path:
    output = build_rolling_tape_state_v2(pq.read_table(input_path))
    pq.write_table(output, output_path, compression="zstd", compression_level=3)
    return output_path
