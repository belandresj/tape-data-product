"""Compiled scaled-arithmetic kernel for endpoint-EW batch updates.

The public feature builder owns Arrow I/O and age windows.  This module owns the
bounded numerical recurrence and publication gates, with one Numba dispatch per
input batch.  ``fastmath`` is deliberately not enabled: the contract depends on
IEEE-754 ordering and exact zero/null decisions.
"""
from __future__ import annotations

import math

import numba
import numpy as np


KERNEL_IMPLEMENTATION = {
    "backend": "numba_njit",
    "compiler_version": numba.__version__,
    "version": "endpoint_ew_scaled_batch_v1",
}

# Scaled-sum state indices.  Each state is (mantissa, exponent), representing
# mantissa * 2**exponent, with a zero mantissa as the sole zero representation.
U1 = 0
U2 = 1
CENTRAL = 2
RETURN_USABLE = 3
RETURN_POSSIBLE = 4
EXPOSURE_POSSIBLE = 5
ACTIVITY_COUNT = 6
ACTIVITY_SHARE = 7
ACTIVITY_DOLLAR = 8
ACTIVITY_USABLE = 9
SPREAD_NUMERATOR = 10
SPREAD_USABLE = 11
BID_SIZE_NUMERATOR = 12
BID_SIZE_USABLE = 13
ASK_SIZE_NUMERATOR = 14
ASK_SIZE_USABLE = 15
STATE_COUNT = 16

# Feature and support result indices.
RMS = 0
PARTICIPATION = 1
SPREAD = 2
TRADE_RATE = 3
SHARE_RATE = 4
DOLLAR_RATE = 5
BID_SIZE = 6
ASK_SIZE = 7
RMS_TO_SPREAD = 8
FEATURE_COUNT = 9
SUPPORT_COUNT = 10

# Frozen public reason/status values.  Kept scalar here so the compiled loop
# does not depend on Python Enum objects.
SOURCE_UNVERIFIED = 0
SOURCE_UNAVAILABLE = 2
REASON_SOURCE_UNAVAILABLE = 1
REASON_SOURCE_UNVERIFIED = 2
REASON_HALT = 4
REASON_STARTUP = 8
REASON_LOW_COVERAGE = 16
REASON_NO_SUPPORTED_DATA = 32
REASON_ZERO_RETURN_VARIATION = 1024
REASON_ZERO_SPREAD = 2048


@numba.njit(cache=True, nogil=True)
def _set(mantissas, exponents, view, state, mantissa, exponent):
    if mantissa == 0.0:
        mantissas[view, state] = 0.0
        exponents[view, state] = 0
    else:
        normalized, adjustment = math.frexp(mantissa)
        mantissas[view, state] = normalized
        exponents[view, state] = exponent + adjustment


@numba.njit(cache=True, nogil=True)
def _add_parts(mantissas, exponents, view, state, mantissa, exponent):
    if mantissa == 0.0:
        return
    mantissa, adjustment = math.frexp(mantissa)
    exponent += adjustment
    current = mantissas[view, state]
    current_exponent = exponents[view, state]
    if current == 0.0:
        mantissas[view, state] = mantissa
        exponents[view, state] = exponent
    elif exponent > current_exponent:
        _set(
            mantissas,
            exponents,
            view,
            state,
            mantissa + math.ldexp(current, current_exponent - exponent),
            exponent,
        )
    else:
        _set(
            mantissas,
            exponents,
            view,
            state,
            current + math.ldexp(mantissa, exponent - current_exponent),
            current_exponent,
        )


@numba.njit(cache=True, nogil=True)
def _add_float(mantissas, exponents, view, state, value):
    if value != 0.0:
        mantissa, exponent = math.frexp(value)
        _add_parts(mantissas, exponents, view, state, mantissa, exponent)


@numba.njit(cache=True, nogil=True)
def _add_square(mantissas, exponents, view, state, value):
    if value != 0.0:
        mantissa, exponent = math.frexp(abs(value))
        _add_parts(mantissas, exponents, view, state, mantissa * mantissa, 2 * exponent)


@numba.njit(cache=True, nogil=True)
def _decay(mantissas, exponents, view, state, factor):
    mantissa = mantissas[view, state]
    if mantissa != 0.0:
        _set(mantissas, exponents, view, state, mantissa * factor, exponents[view, state])


@numba.njit(cache=True, nogil=True)
def _ratio(mantissas, exponents, view, numerator, denominator):
    return math.ldexp(
        mantissas[view, numerator] / mantissas[view, denominator],
        exponents[view, numerator] - exponents[view, denominator],
    )


@numba.njit(cache=True, nogil=True)
def _sqrt_ratio(mantissas, exponents, view, numerator, denominator):
    if mantissas[view, numerator] == 0.0:
        return 0.0
    exponent = exponents[view, numerator] - exponents[view, denominator]
    ratio = mantissas[view, numerator] / mantissas[view, denominator]
    if exponent & 1:
        ratio *= 2.0
        exponent -= 1
    return math.ldexp(math.sqrt(ratio), exponent // 2)


@numba.njit(cache=True, nogil=True)
def _value(mantissas, exponents, view, state):
    return math.ldexp(mantissas[view, state], exponents[view, state])


@numba.njit(cache=True, nogil=True)
def _scaled_less(mantissas, exponents, view, usable, fraction, possible):
    if mantissas[view, usable] == 0.0:
        return mantissas[view, possible] != 0.0
    if mantissas[view, possible] == 0.0:
        return False
    fraction_m, fraction_e = math.frexp(fraction)
    rhs_m, rhs_adjustment = math.frexp(mantissas[view, possible] * fraction_m)
    rhs_e = exponents[view, possible] + fraction_e + rhs_adjustment
    usable_e = exponents[view, usable]
    return usable_e < rhs_e or (
        usable_e == rhs_e and mantissas[view, usable] < rhs_m
    )


@numba.njit(cache=True, nogil=True)
def _history_mask(
    mantissas,
    exponents,
    view,
    source,
    halted,
    elapsed,
    startup,
    usable,
    possible,
    minimum,
):
    reason = 0
    if source == SOURCE_UNVERIFIED:
        reason |= REASON_SOURCE_UNVERIFIED
    elif source == SOURCE_UNAVAILABLE:
        reason |= REASON_SOURCE_UNAVAILABLE
    if halted:
        reason |= REASON_HALT
    if elapsed < startup:
        reason |= REASON_STARTUP
    if mantissas[view, usable] == 0.0:
        reason |= REASON_NO_SUPPORTED_DATA
    elif elapsed >= startup and _scaled_less(
        mantissas, exponents, view, usable, minimum, possible
    ):
        reason |= REASON_LOW_COVERAGE
    return reason


@numba.njit(cache=True, nogil=True)
def _endpoint_return(start_midpoint, end_midpoint):
    ratio = end_midpoint / start_midpoint
    if 0.5 <= ratio <= 2.0:
        return 10000.0 * math.log1p((end_midpoint - start_midpoint) / start_midpoint)
    return 10000.0 * (math.log(end_midpoint) - math.log(start_midpoint))


@numba.njit(cache=True, nogil=True)
def run_ew_batch(
    mantissas,
    exponents,
    ever_positive,
    ring_midpoints,
    ring_valid,
    ring_continuity,
    ring_length,
    quote_elapsed,
    trade_elapsed,
    decay_factors,
    startup_seconds,
    spread_min_coverage,
    other_min_coverage,
    halt,
    quote_status,
    trade_status,
    bid,
    bid_valid,
    ask,
    ask_valid,
    price_reason,
    continuity,
    spread_numerator,
    spread_exposure,
    activity_count,
    activity_share,
    activity_dollar,
    activity_exposure,
    bid_size_numerator,
    bid_size_exposure,
    ask_size_numerator,
    ask_size_exposure,
):
    """Update all configured views and publish one result row per base row."""
    rows = halt.shape[0]
    views = decay_factors.shape[0]
    values = np.empty((rows, views, FEATURE_COUNT), dtype=np.float64)
    values.fill(np.nan)
    masks = np.zeros((rows, views, FEATURE_COUNT), dtype=np.int64)
    supports = np.empty((rows, views, SUPPORT_COUNT), dtype=np.float64)
    quote_elapsed_rows = np.empty(rows, dtype=np.int64)
    trade_elapsed_rows = np.empty(rows, dtype=np.int64)
    underflows = 0

    for row in range(rows):
        halted = halt[row] != 0
        if halted:
            mantissas.fill(0.0)
            exponents.fill(0)
            ever_positive.fill(False)
            ring_length = 0
            quote_elapsed = 0
            trade_elapsed = 0
        else:
            quote_elapsed += 1
            trade_elapsed += 1
            if ring_length < 6:
                slot = ring_length
                ring_length += 1
            else:
                for slot_index in range(5):
                    ring_midpoints[slot_index] = ring_midpoints[slot_index + 1]
                    ring_valid[slot_index] = ring_valid[slot_index + 1]
                    ring_continuity[slot_index] = ring_continuity[slot_index + 1]
                slot = 5
            valid_midpoint = (
                price_reason[row] == 0 and bid_valid[row] and ask_valid[row]
            )
            ring_valid[slot] = valid_midpoint
            ring_continuity[slot] = continuity[row]
            if valid_midpoint:
                ring_midpoints[slot] = bid[row] / 2.0 + ask[row] / 2.0

            return_valid = (
                ring_length == 6
                and ring_valid[0]
                and ring_valid[5]
                and ring_continuity[0] == ring_continuity[5]
            )
            return_value = 0.0
            if return_valid:
                return_value = _endpoint_return(ring_midpoints[0], ring_midpoints[5])

            for view in range(views):
                factor = decay_factors[view]
                for state in range(STATE_COUNT):
                    _decay(mantissas, exponents, view, state, factor)
                _add_float(mantissas, exponents, view, EXPOSURE_POSSIBLE, 1.0)
                if ring_length == 6:
                    _add_float(mantissas, exponents, view, RETURN_POSSIBLE, 1.0)
                if return_valid:
                    magnitude = abs(return_value)
                    if mantissas[view, RETURN_USABLE] != 0.0:
                        mean = _ratio(mantissas, exponents, view, U1, RETURN_USABLE)
                        denominator_m = mantissas[view, RETURN_USABLE]
                        denominator_e = exponents[view, RETURN_USABLE]
                        # Add one to the already-decayed usable return weight.
                        if denominator_e > 1:
                            denominator_m += math.ldexp(0.5, 1 - denominator_e)
                        else:
                            denominator_m = 0.5 + math.ldexp(
                                denominator_m, denominator_e - 1
                            )
                            denominator_e = 1
                        denominator_m, adjustment = math.frexp(denominator_m)
                        denominator_e += adjustment
                        factor_m, factor_e = math.frexp(
                            mantissas[view, RETURN_USABLE] / denominator_m
                        )
                        factor_e += exponents[view, RETURN_USABLE] - denominator_e
                        delta_m, delta_e = math.frexp(abs(magnitude - mean))
                        _add_parts(
                            mantissas,
                            exponents,
                            view,
                            CENTRAL,
                            factor_m * delta_m * delta_m,
                            factor_e + 2 * delta_e,
                        )
                    _add_float(mantissas, exponents, view, U1, magnitude)
                    _add_square(mantissas, exponents, view, U2, return_value)
                    _add_float(mantissas, exponents, view, RETURN_USABLE, 1.0)
                    if return_value != 0.0:
                        ever_positive[view] = True

                if spread_exposure[row] > 0.0:
                    _add_float(
                        mantissas, exponents, view, SPREAD_NUMERATOR, spread_numerator[row]
                    )
                    _add_float(
                        mantissas, exponents, view, SPREAD_USABLE, spread_exposure[row]
                    )
                if activity_exposure[row] > 0.0:
                    _add_float(
                        mantissas, exponents, view, ACTIVITY_COUNT, activity_count[row]
                    )
                    _add_float(
                        mantissas, exponents, view, ACTIVITY_SHARE, activity_share[row]
                    )
                    _add_float(
                        mantissas, exponents, view, ACTIVITY_DOLLAR, activity_dollar[row]
                    )
                    _add_float(
                        mantissas, exponents, view, ACTIVITY_USABLE, activity_exposure[row]
                    )
                if bid_size_exposure[row] > 0.0:
                    _add_float(
                        mantissas, exponents, view, BID_SIZE_NUMERATOR, bid_size_numerator[row]
                    )
                    _add_float(
                        mantissas, exponents, view, BID_SIZE_USABLE, bid_size_exposure[row]
                    )
                if ask_size_exposure[row] > 0.0:
                    _add_float(
                        mantissas, exponents, view, ASK_SIZE_NUMERATOR, ask_size_numerator[row]
                    )
                    _add_float(
                        mantissas, exponents, view, ASK_SIZE_USABLE, ask_size_exposure[row]
                    )

        quote_elapsed_rows[row] = quote_elapsed
        trade_elapsed_rows[row] = trade_elapsed
        for view in range(views):
            return_reason = _history_mask(
                mantissas,
                exponents,
                view,
                quote_status[row],
                halted,
                quote_elapsed,
                startup_seconds[view],
                RETURN_USABLE,
                RETURN_POSSIBLE,
                other_min_coverage,
            )
            masks[row, view, RMS] = return_reason
            if return_reason == 0:
                values[row, view, RMS] = _sqrt_ratio(
                    mantissas, exponents, view, U2, RETURN_USABLE
                )
            participation_reason = return_reason
            if return_reason == 0 and not ever_positive[view]:
                participation_reason |= REASON_ZERO_RETURN_VARIATION
            masks[row, view, PARTICIPATION] = participation_reason
            if participation_reason == 0:
                # K = U1**2/W, retained as normalized parts.
                k_m = (
                    mantissas[view, U1]
                    * mantissas[view, U1]
                    / mantissas[view, RETURN_USABLE]
                )
                k_e = 2 * exponents[view, U1] - exponents[view, RETURN_USABLE]
                k_m, adjustment = math.frexp(k_m)
                k_e += adjustment
                denominator_m = k_m
                denominator_e = k_e
                central_m = mantissas[view, CENTRAL]
                if central_m != 0.0:
                    central_e = exponents[view, CENTRAL]
                    if central_e > denominator_e:
                        denominator_m = central_m + math.ldexp(
                            denominator_m, denominator_e - central_e
                        )
                        denominator_e = central_e
                    else:
                        denominator_m += math.ldexp(
                            central_m, central_e - denominator_e
                        )
                    denominator_m, adjustment = math.frexp(denominator_m)
                    denominator_e += adjustment
                participation = math.ldexp(k_m / denominator_m, k_e - denominator_e)
                if participation < 0.0 or participation > 1.0:
                    raise ValueError("participation invariant failed")
                values[row, view, PARTICIPATION] = participation

            family_numerators = (
                SPREAD_NUMERATOR,
                ACTIVITY_COUNT,
                ACTIVITY_SHARE,
                ACTIVITY_DOLLAR,
                BID_SIZE_NUMERATOR,
                ASK_SIZE_NUMERATOR,
            )
            family_usable = (
                SPREAD_USABLE,
                ACTIVITY_USABLE,
                ACTIVITY_USABLE,
                ACTIVITY_USABLE,
                BID_SIZE_USABLE,
                ASK_SIZE_USABLE,
            )
            family_outputs = (SPREAD, TRADE_RATE, SHARE_RATE, DOLLAR_RATE, BID_SIZE, ASK_SIZE)
            spread_value = 0.0
            spread_reason = 0
            for family_index in range(6):
                is_trade = 1 <= family_index <= 3
                reason = _history_mask(
                    mantissas,
                    exponents,
                    view,
                    trade_status[row] if is_trade else quote_status[row],
                    halted,
                    trade_elapsed if is_trade else quote_elapsed,
                    startup_seconds[view],
                    family_usable[family_index],
                    EXPOSURE_POSSIBLE,
                    other_min_coverage if family_index else spread_min_coverage,
                )
                output = family_outputs[family_index]
                masks[row, view, output] = reason
                if reason == 0:
                    value = _ratio(
                        mantissas,
                        exponents,
                        view,
                        family_numerators[family_index],
                        family_usable[family_index],
                    )
                    values[row, view, output] = value
                    if family_index == 0:
                        spread_value = value
                if family_index == 0:
                    spread_reason = reason

            ratio_reason = return_reason | spread_reason
            if ratio_reason == 0 and spread_value == 0.0:
                ratio_reason |= REASON_ZERO_SPREAD
            masks[row, view, RMS_TO_SPREAD] = ratio_reason
            if ratio_reason == 0:
                values[row, view, RMS_TO_SPREAD] = (
                    values[row, view, RMS] / spread_value
                )

            supports[row, view, 0] = _value(
                mantissas, exponents, view, RETURN_USABLE
            )
            supports[row, view, 1] = _value(
                mantissas, exponents, view, RETURN_POSSIBLE
            )
            supports[row, view, 2] = _value(
                mantissas, exponents, view, SPREAD_USABLE
            )
            supports[row, view, 3] = _value(
                mantissas, exponents, view, EXPOSURE_POSSIBLE
            )
            supports[row, view, 4] = _value(
                mantissas, exponents, view, ACTIVITY_USABLE
            )
            supports[row, view, 5] = supports[row, view, 3]
            supports[row, view, 6] = _value(
                mantissas, exponents, view, BID_SIZE_USABLE
            )
            supports[row, view, 7] = supports[row, view, 3]
            supports[row, view, 8] = _value(
                mantissas, exponents, view, ASK_SIZE_USABLE
            )
            supports[row, view, 9] = supports[row, view, 3]
            for support_index in (0, 2, 4, 6, 8):
                state_index = (
                    RETURN_USABLE,
                    SPREAD_USABLE,
                    ACTIVITY_USABLE,
                    BID_SIZE_USABLE,
                    ASK_SIZE_USABLE,
                )[support_index // 2]
                if (
                    mantissas[view, state_index] != 0.0
                    and supports[row, view, support_index] == 0.0
                ):
                    underflows += 1
    return (
        values,
        masks,
        supports,
        quote_elapsed_rows,
        trade_elapsed_rows,
        underflows,
        ring_length,
        quote_elapsed,
        trade_elapsed,
    )


class EndpointEWKernel:
    """Persistent bounded state around the compiled batch function."""

    def __init__(self, views, spread_min_coverage, other_min_coverage):
        self.decay_factors = np.array(
            [2.0 ** (-1.0 / view.half_life_seconds) for view in views],
            dtype=np.float64,
        )
        self.startup_seconds = np.array(
            [view.startup_seconds for view in views], dtype=np.int64
        )
        view_count = len(views)
        self.mantissas = np.zeros((view_count, STATE_COUNT), dtype=np.float64)
        self.exponents = np.zeros((view_count, STATE_COUNT), dtype=np.int64)
        self.ever_positive = np.zeros(view_count, dtype=np.bool_)
        self.ring_midpoints = np.zeros(6, dtype=np.float64)
        self.ring_valid = np.zeros(6, dtype=np.bool_)
        self.ring_continuity = np.zeros(6, dtype=np.int64)
        self.ring_length = 0
        self.quote_elapsed = 0
        self.trade_elapsed = 0
        self.spread_min_coverage = float(spread_min_coverage)
        self.other_min_coverage = float(other_min_coverage)

    def process(self, **inputs):
        result = run_ew_batch(
            self.mantissas,
            self.exponents,
            self.ever_positive,
            self.ring_midpoints,
            self.ring_valid,
            self.ring_continuity,
            self.ring_length,
            self.quote_elapsed,
            self.trade_elapsed,
            self.decay_factors,
            self.startup_seconds,
            self.spread_min_coverage,
            self.other_min_coverage,
            **inputs,
        )
        (
            values,
            masks,
            supports,
            quote_elapsed_rows,
            trade_elapsed_rows,
            underflows,
            self.ring_length,
            self.quote_elapsed,
            self.trade_elapsed,
        ) = result
        return (
            values,
            masks,
            supports,
            quote_elapsed_rows,
            trade_elapsed_rows,
            underflows,
        )
