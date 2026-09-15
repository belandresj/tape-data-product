"""Bounded columnar schema/domain checks; numerical reconstruction is separate."""
from functools import lru_cache
import re
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from .config import ContractError, DEFAULT_CONFIG
from .policy import NS, session_bounds
from .registry import feature_registry, AGES
from .reasons import Reason, HISTORY_REASONS
from .schemas import BASE_SCHEMA, feature_schema, support_schema

BASE_GROUPS = {
    "price_twap": ("bid_twap_usd", "ask_twap_usd", "midpoint_twap_usd"),
    "price_end": ("bid_end_usd", "ask_end_usd"),
    "spread_integral": ("spread_integral_bps_seconds",),
    "bid_size_integral": ("bid_size_integral_shares_seconds",),
    "ask_size_integral": ("ask_size_integral_shares_seconds",),
    "bid_size_end": ("bid_size_end_shares",), "ask_size_end": ("ask_size_end_shares",),
    "activity": ("trade_count_1s", "share_volume_1s", "dollar_volume_1s_usd"),
    **{a + "_age": (a + "_age_seconds",) for a in AGES},
}
EXPOSURES = {
    "price_twap": "price_valid_duration_ns",
    "spread_integral": "spread_valid_duration_ns",
    "bid_size_integral": "bid_size_valid_duration_ns",
    "ask_size_integral": "ask_size_valid_duration_ns",
    "activity": "activity_valid_duration_ns",
}
BASE_ALLOWED = int(Reason.SOURCE_UNAVAILABLE | Reason.SOURCE_UNVERIFIED | Reason.HALT |
                   Reason.NO_SUPPORTED_DATA | Reason.INVALID_CURRENT_VALUE | Reason.CONTINUITY_BREAK)
_U16 = pa.uint16()


@lru_cache(maxsize=32)
def _plan(kind, config):
    if kind == "base":
        return BASE_SCHEMA, None
    if kind == "features":
        return feature_schema(config), feature_registry(config)
    if kind == "support":
        return support_schema(config), None
    raise ContractError("expected a known table kind and Arrow RecordBatch")


def _all(condition):
    """Null predicates fail instead of disappearing from Arrow reductions."""
    return bool(pc.all(pc.fill_null(condition, False)).as_py())


def _require(condition, message):
    if not _all(condition):
        raise ContractError(message)


def _scalar(value, array):
    return pa.scalar(value, type=array.type)


def _bit(mask, value):
    return pc.not_equal(pc.bit_wise_and(mask, pa.scalar(int(value), _U16)),
                        pa.scalar(0, _U16))


def _or(left, right):
    return pc.or_kleene(left, right)


def _implies(condition, consequence):
    return _or(pc.invert(condition), consequence)


def _isclose(left, right, rtol, atol):
    difference = pc.abs(pc.subtract(left, right))
    scale = pc.max_element_wise(pc.abs(left), pc.abs(right))
    tolerance = pc.max_element_wise(pc.multiply(scale, rtol), atol)
    return pc.less_equal(difference, tolerance)


def _validate_value(c, name, mask_name, allowed, maximum=None, positive=False):
    value, mask = c[name], c[mask_name]
    zero_mask = pa.scalar(0, _U16)
    _require(pc.equal(pc.bit_wise_and(mask, pa.scalar((~allowed) & 0xffff, _U16)), zero_mask),
             "reason does not apply to this measurement")
    _require(pc.equal(pc.is_null(value), pc.not_equal(mask, zero_mask)),
             "value/null and reason mask disagree")
    domain = pc.greater(value, _scalar(0, value)) if positive else pc.greater_equal(value, _scalar(0, value))
    _require(_or(pc.is_null(value), domain),
             "measurement outside domain" if positive else "measurement must be finite and nonnegative")
    if maximum is not None:
        _require(_or(pc.is_null(value), pc.less_equal(value, _scalar(maximum, value))),
                 "measurement outside domain")


def _validate_finite(c, schema):
    for field in schema:
        if pa.types.is_floating(field.type):
            value = c[field.name]
            _require(_or(pc.is_null(value), pc.is_finite(value)), "nonfinite stored number")


def _validate_base(c, session_start):
    for group, names in BASE_GROUPS.items():
        allowed = BASE_ALLOWED
        if group.endswith("_age"):
            allowed |= int(Reason.NO_OBSERVED_EVENT)
        if group == "midpoint_change_age":
            allowed |= int(Reason.MIDPOINT_AGE_LOWER_BOUND_ONLY)
        for name in names:
            _validate_value(c, name, group + "_reason_mask", allowed,
                            positive=group in ("price_twap", "price_end", "bid_size_end", "ask_size_end"))

    zero_mask = pa.scalar(0, _U16)
    for group, duration_name in EXPOSURES.items():
        duration, mask = c[duration_name], c[group + "_reason_mask"]
        empty = pc.equal(duration, _scalar(0, duration))
        _require(pc.equal(empty, pc.not_equal(mask, zero_mask)), "base numerator/exposure disagreement")
        _require(_implies(empty, _bit(mask, Reason.NO_SUPPORTED_DATA)),
                 "zero exposure requires NO_SUPPORTED_DATA")

    for source in ("quote", "trade"):
        status, continuity = c[source + "_source_status"], c[source + "_continuity_id"]
        _require(pc.is_in(status, value_set=pa.array([0, 1, 2], type=status.type)),
                 "invalid source context")
        _require(pc.greater_equal(continuity, _scalar(0, continuity)), "invalid source context")

    for name, value in c.items():
        if name.endswith("_duration_ns"):
            _require(pc.and_(pc.greater_equal(value, _scalar(0, value)),
                             pc.less_equal(value, _scalar(NS, value))), "duration outside second")

    _require(pc.less_equal(c["price_valid_duration_ns"], c["quote_observed_duration_ns"]),
             "price exposure exceeds observation")
    for name in ("spread", "bid_size", "ask_size"):
        _require(pc.less_equal(c[name + "_valid_duration_ns"], c["price_valid_duration_ns"]),
                 "semantic exposure exceeds valid prices")
    _require(pc.less_equal(c["activity_valid_duration_ns"], c["trade_observed_duration_ns"]),
             "activity exposure exceeds observation")

    source_groups = (
        ("quote", ("price_end", "bid_size_end", "ask_size_end", "quote_age", "midpoint_change_age")),
        ("trade", ("trade_age",)),
    )
    for source, groups in source_groups:
        status = c[source + "_source_status"]
        for code, reason in ((0, Reason.SOURCE_UNVERIFIED), (2, Reason.SOURCE_UNAVAILABLE)):
            selected = pc.equal(status, _scalar(code, status))
            for group in groups:
                _require(_implies(selected, _bit(c[group + "_reason_mask"], reason)),
                         "current measurement contradicts source status")

    bid = c["bid_end_usd"]
    for side in ("bid", "ask"):
        _require(_implies(pc.is_valid(c[side + "_size_end_shares"]), pc.is_valid(bid)),
                 "endpoint size requires valid two-sided prices")
    _require(_or(pc.is_null(bid), pc.less_equal(bid, c["ask_end_usd"])), "crossed endpoint price")

    midpoint, bid_twap, ask_twap = c["midpoint_twap_usd"], c["bid_twap_usd"], c["ask_twap_usd"]
    calculated = pc.add(pc.divide(bid_twap, 2.0), pc.divide(ask_twap, 2.0))
    consistent = pc.and_(pc.less_equal(bid_twap, ask_twap),
                         _isclose(midpoint, calculated, 1e-12, 0.0))
    _require(_or(pc.is_null(midpoint), consistent), "inconsistent common-support TWAPs")

    status = c["midpoint_age_status"]
    age, origin, bound = (c[n] for n in ("midpoint_change_age_seconds",
                                         "midpoint_observation_start_ns",
                                         "midpoint_age_lower_bound_seconds"))
    _require(pc.is_in(status, value_set=pa.array([0, 1, 2], type=status.type)),
             "invalid midpoint age status")
    _require(_implies(pc.not_equal(status, _scalar(0, status)), pc.is_valid(bid)),
             "observable midpoint requires valid endpoint price")
    state0 = pc.equal(status, _scalar(0, status))
    all_null = pc.and_kleene(pc.is_null(age), pc.and_kleene(pc.is_null(origin), pc.is_null(bound)))
    _require(_implies(state0, all_null), "unobservable midpoint has observation values")
    state1 = pc.equal(status, _scalar(1, status))
    lower = pc.and_kleene(
        pc.is_null(age),
        pc.and_kleene(pc.is_valid(origin),
                      pc.and_kleene(pc.is_valid(bound),
                                    _bit(c["midpoint_change_age_reason_mask"],
                                         Reason.MIDPOINT_AGE_LOWER_BOUND_ONLY))),
    )
    _require(_implies(state1, lower), "invalid lower-bound state")
    expected = pc.divide(pc.cast(pc.subtract(c["interval_end_ns"], origin), pa.float64()), float(NS))
    _require(_implies(state1, _isclose(bound, expected, 1e-12, 0.0)),
             "midpoint lower bound mismatch")
    state2 = pc.equal(status, _scalar(2, status))
    known = pc.and_kleene(pc.is_valid(age), pc.and_kleene(pc.is_valid(origin), pc.is_null(bound)))
    _require(_implies(state2, known), "invalid known-age state")
    origin_range = pc.and_kleene(pc.greater_equal(origin, _scalar(session_start, origin)),
                                 pc.less(origin, c["interval_end_ns"]))
    _require(_or(pc.is_null(origin), origin_range), "invalid midpoint observation origin")

    halt, halt_id = c["halt_active"], c["halt_id"]
    _require(pc.equal(pc.is_valid(halt_id), halt), "halt identity disagreement")
    for name in EXPOSURES.values():
        value = c[name]
        _require(_implies(halt, pc.equal(value, _scalar(0, value))),
                 "closed halt second has semantic exposure")
    for group in BASE_GROUPS:
        _require(_implies(halt, _bit(c[group + "_reason_mask"], Reason.HALT)),
                 "closed halt second lacks halt reasons")


def _validate_features(c, config, registry):
    for feature in registry:
        allowed = HISTORY_REASONS
        if feature.family == "participation":
            allowed |= int(Reason.ZERO_RETURN_VARIATION)
        if feature.family == "ratio":
            allowed |= int(Reason.ZERO_SPREAD)
        _validate_value(c, feature.name, feature.name + "_reason_mask", allowed,
                        maximum=1 if feature.family == "participation" else None)

    history, zero = pa.scalar(HISTORY_REASONS, _U16), pa.scalar(0, _U16)
    for view in config.views:
        suffix = f"_hl{view.half_life_seconds}s"
        rms, part, spread, ratio = [
            name + suffix for name in ("midpoint_rms_5s_bps", "movement_participation",
                                       "quoted_spread_bps", "midpoint_rms_5s_to_spread")
        ]
        rr, pr = c[rms + "_reason_mask"], c[part + "_reason_mask"]
        sr, xr = c[spread + "_reason_mask"], c[ratio + "_reason_mask"]
        _require(pc.equal(pc.bit_wise_and(pr, history), rr),
                 "RMS and participation history reasons differ")
        zero_return = _bit(pr, Reason.ZERO_RETURN_VARIATION)
        rms_zero = pc.and_(pc.equal(rr, zero), pc.equal(c[rms], _scalar(0.0, c[rms])))
        _require(_implies(zero_return, rms_zero), "zero-return reason contradicts RMS")
        available = pc.and_(pc.equal(rr, zero), pc.equal(sr, zero))
        _require(pc.equal(pc.bit_wise_and(xr, history), pc.bit_wise_or(rr, sr)),
                 "ratio component reasons differ")
        spread_zero = pc.equal(c[spread], _scalar(0.0, c[spread]))
        zero_spread = pc.equal(xr, pa.scalar(int(Reason.ZERO_SPREAD), _U16))
        _require(_implies(pc.and_kleene(available, spread_zero), zero_spread),
                 "zero spread requires undefined ratio")
        expected = pc.divide(c[rms], c[spread])
        valid = pc.and_(pc.equal(xr, zero), _isclose(c[ratio], expected, 1e-10, 1e-12))
        _require(_implies(pc.and_kleene(available, pc.invert(spread_zero)), valid), "ratio value mismatch")
        _require(_implies(pc.invert(available), pc.invert(_bit(xr, Reason.ZERO_SPREAD))),
                 "zero-spread reason without available components")


def _validate_support(c, config):
    for view in config.views:
        suffix = f"_hl{view.half_life_seconds}s"
        pairs = [("return_usable_weight", "return_possible_weight")]
        pairs += [(f"{family}_usable_exposure_seconds", f"{family}_possible_exposure_seconds")
                  for family in ("spread", "activity", "bid_size", "ask_size")]
        for usable_name, possible_name in pairs:
            usable, possible = c[usable_name + suffix], c[possible_name + suffix]
            _require(pc.and_(pc.greater_equal(usable, _scalar(0.0, usable)),
                             pc.less_equal(usable, possible)),
                     "usable/possible support disagreement")
    for source in ("quote", "trade"):
        elapsed = c[source + "_ew_startup_elapsed_seconds"]
        _require(pc.and_(pc.greater_equal(elapsed, _scalar(0, elapsed)),
                         pc.less_equal(elapsed, _scalar(57600, elapsed))),
                 "invalid startup clock")
    for age in AGES:
        for window in config.age_windows_seconds:
            count = c[f"{age}_age_sample_count_window{window}s"]
            elapsed = c[f"{age}_age_elapsed_slots_window{window}s"]
            _require(pc.and_(pc.greater_equal(count, _scalar(0, count)),
                             pc.and_(pc.less_equal(count, elapsed),
                                     pc.less_equal(elapsed, _scalar(window, elapsed)))),
                     "age count/elapsed disagreement")


def validate_batch(batch, kind, *, config=DEFAULT_CONFIG, previous_key=None):
    """Validate <=25k rows and return the final cross-batch grid key.

    Valid input stays in bounded Arrow/NumPy columnar operations. It does not
    create per-row dictionaries, Python column lists, or per-row Arrow scalars.
    """
    if not isinstance(batch, pa.RecordBatch) or kind not in ("base", "features", "support"):
        raise ContractError("expected a known table kind and Arrow RecordBatch")
    schema, registry = _plan(kind, config)
    if batch.num_rows > 25000 or not batch.schema.equals(schema, check_metadata=True):
        raise ContractError("batch/schema mismatch")
    if any(not field.nullable and batch.column(i).null_count for i, field in enumerate(schema)):
        raise ContractError("null in required field")
    if not batch.num_rows:
        return previous_key

    c = {name: batch.column(i) for i, name in enumerate(schema.names)}
    _validate_finite(c, schema)
    date, symbol = c["session_date"][0].as_py(), c["symbol"][0].as_py()
    _require(pc.equal(c["session_date"], pa.scalar(date)), "member changed or grid gap/duplicate")
    _require(pc.equal(c["symbol"], pa.scalar(symbol)), "member changed or grid gap/duplicate")
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9._-]{0,31}", symbol):
        raise ContractError("invalid symbol")
    start, end = session_bounds(date)
    timestamps = c["interval_end_ns"].to_numpy(zero_copy_only=True)
    outside = (timestamps <= start) | (timestamps > end) | (timestamps % NS != 0)
    gap = len(timestamps) > 1 and np.any(np.diff(timestamps) != NS)
    if np.any(outside) or gap:
        raise ContractError("timestamp outside session grid" if np.any(outside)
                            else "member changed or grid gap/duplicate")
    if previous_key is not None and (
        previous_key[:2] != (date, symbol) or int(timestamps[0]) != previous_key[2] + NS
    ):
        raise ContractError("member changed or grid gap/duplicate")

    if kind == "base":
        _validate_base(c, start)
    elif kind == "features":
        _validate_features(c, config, registry)
    else:
        _validate_support(c, config)
    return date, symbol, int(timestamps[-1])
