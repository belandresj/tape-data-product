"""Bounded schema/domain checks; independent numerical reconstruction is separate."""
import math
import re
import pyarrow as pa
from .config import ContractError, DEFAULT_CONFIG
from .policy import NS, session_bounds
from .registry import feature_registry, AGES
from .reasons import Reason, HISTORY_REASONS, validate_value
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
EXPOSURES = {"price_twap": "price_valid_duration_ns", "spread_integral": "spread_valid_duration_ns",
             "bid_size_integral": "bid_size_valid_duration_ns", "ask_size_integral": "ask_size_valid_duration_ns",
             "activity": "activity_valid_duration_ns"}
BASE_ALLOWED = int(Reason.SOURCE_UNAVAILABLE | Reason.SOURCE_UNVERIFIED | Reason.HALT |
                   Reason.NO_SUPPORTED_DATA | Reason.INVALID_CURRENT_VALUE | Reason.CONTINUITY_BREAK)


def _base(row, session_start=None):
    for group, names in BASE_GROUPS.items():
        allowed = BASE_ALLOWED
        if group.endswith('_age'):
            allowed |= int(Reason.NO_OBSERVED_EVENT)
        if group == 'midpoint_change_age':
            allowed |= int(Reason.MIDPOINT_AGE_LOWER_BOUND_ONLY)
        for name in names:
            validate_value(row[name], row[group + '_reason_mask'], allowed=allowed,
                           positive=group in ('price_twap', 'price_end', 'bid_size_end', 'ask_size_end'))
    for group, duration in EXPOSURES.items():
        if (row[duration] == 0) != (row[group + '_reason_mask'] != 0):
            raise ContractError("base numerator/exposure disagreement")
        if row[duration] == 0 and not row[group + '_reason_mask'] & Reason.NO_SUPPORTED_DATA:
            raise ContractError("zero exposure requires NO_SUPPORTED_DATA")
    for source in ('quote', 'trade'):
        if row[source + '_source_status'] not in (0, 1, 2) or row[source + '_continuity_id'] < 0:
            raise ContractError("invalid source context")
    for name, value in row.items():
        if name.endswith('_duration_ns') and not 0 <= value <= NS:
            raise ContractError("duration outside second")
    if row['price_valid_duration_ns'] > row['quote_observed_duration_ns']:
        raise ContractError("price exposure exceeds observation")
    for n in ('spread', 'bid_size', 'ask_size'):
        if row[n + '_valid_duration_ns'] > row['price_valid_duration_ns']:
            raise ContractError("semantic exposure exceeds valid prices")
    if row['activity_valid_duration_ns'] > row['trade_observed_duration_ns']:
        raise ContractError("activity exposure exceeds observation")
    for source, groups in (('quote', ('price_end', 'bid_size_end', 'ask_size_end', 'quote_age', 'midpoint_change_age')),
                           ('trade', ('trade_age',))):
        status = row[source + '_source_status']
        if status != 1:
            required = Reason.SOURCE_UNVERIFIED if status == 0 else Reason.SOURCE_UNAVAILABLE
            if any(not row[g + '_reason_mask'] & required for g in groups):
                raise ContractError("current measurement contradicts source status")
    for side in ('bid', 'ask'):
        if row[side + '_size_end_shares'] is not None and row['bid_end_usd'] is None:
            raise ContractError("endpoint size requires valid two-sided prices")
    if row['bid_end_usd'] is not None and row['bid_end_usd'] > row['ask_end_usd']:
        raise ContractError("crossed endpoint price")
    if row['midpoint_twap_usd'] is not None:
        b, a, m = (row[n + '_twap_usd'] for n in ('bid', 'ask', 'midpoint'))
        if b > a or not math.isclose(m, b / 2 + a / 2, rel_tol=1e-12, abs_tol=0):
            raise ContractError("inconsistent common-support TWAPs")
    status = row['midpoint_age_status']
    age, origin, bound = (row[n] for n in ('midpoint_change_age_seconds',
                                         'midpoint_observation_start_ns', 'midpoint_age_lower_bound_seconds'))
    if status != 0 and row['bid_end_usd'] is None:
        raise ContractError("observable midpoint requires valid endpoint price")
    if status not in (0, 1, 2):
        raise ContractError("invalid midpoint age status")
    if status == 0 and any(v is not None for v in (age, origin, bound)):
        raise ContractError("unobservable midpoint has observation values")
    if status == 1:
        if age is not None or origin is None or bound is None or not row['midpoint_change_age_reason_mask'] & 256:
            raise ContractError("invalid lower-bound state")
        if not math.isclose(bound, (row['interval_end_ns'] - origin) / NS, rel_tol=1e-12, abs_tol=0):
            raise ContractError("midpoint lower bound mismatch")
    if status == 2 and (age is None or origin is None or bound is not None):
        raise ContractError("invalid known-age state")
    if session_start is None:
        session_start = session_bounds(row['session_date'])[0]
    if origin is not None and not session_start <= origin < row['interval_end_ns']:
        raise ContractError("invalid midpoint observation origin")
    if (row['halt_id'] is not None) != row['halt_active']:
        raise ContractError("halt identity disagreement")
    if row['halt_active']:
        if any(row[n] != 0 for n in EXPOSURES.values()):
            raise ContractError("closed halt second has semantic exposure")
        if any(not row[g + '_reason_mask'] & Reason.HALT for g in BASE_GROUPS):
            raise ContractError("closed halt second lacks halt reasons")


def _features(row, config, registry=None):
    for f in feature_registry(config) if registry is None else registry:
        allowed = HISTORY_REASONS
        if f.family == 'participation':
            allowed |= int(Reason.ZERO_RETURN_VARIATION)
        if f.family == 'ratio':
            allowed |= int(Reason.ZERO_SPREAD)
        validate_value(row[f.name], row[f.name + '_reason_mask'], allowed=allowed,
                       maximum=1 if f.family == 'participation' else None)
    for v in config.views:
        suffix = f'_hl{v.half_life_seconds}s'
        rms, part, spread, ratio = [n + suffix for n in ('midpoint_rms_5s_bps', 'movement_participation',
                                                       'quoted_spread_bps', 'midpoint_rms_5s_to_spread')]
        rr = row[rms + '_reason_mask']
        pr = row[part + '_reason_mask']
        sr = row[spread + '_reason_mask']
        xr = row[ratio + '_reason_mask']
        if pr & HISTORY_REASONS != rr:
            raise ContractError("RMS and participation history reasons differ")
        if pr & Reason.ZERO_RETURN_VARIATION and (rr or row[rms] != 0):
            raise ContractError("zero-return reason contradicts RMS")
        if xr & HISTORY_REASONS != rr | sr:
            raise ContractError("ratio component reasons differ")
        if not rr and not sr:
            if row[spread] == 0:
                if xr != Reason.ZERO_SPREAD:
                    raise ContractError("zero spread requires undefined ratio")
            elif xr or not math.isclose(row[ratio], row[rms] / row[spread], rel_tol=1e-10, abs_tol=1e-12):
                raise ContractError("ratio value mismatch")
        elif xr & Reason.ZERO_SPREAD:
            raise ContractError("zero-spread reason without available components")


def _support(row, config):
    for v in config.views:
        suffix = f'_hl{v.half_life_seconds}s'
        pairs = [('return_usable_weight', 'return_possible_weight')]
        pairs += [(f'{f}_usable_exposure_seconds', f'{f}_possible_exposure_seconds')
                  for f in ('spread', 'activity', 'bid_size', 'ask_size')]
        for u, p in pairs:
            usable, possible = row[u + suffix], row[p + suffix]
            if not 0 <= usable <= possible:
                raise ContractError("usable/possible support disagreement")
    for source in ('quote', 'trade'):
        if not 0 <= row[source + '_ew_startup_elapsed_seconds'] <= 57600:
            raise ContractError("invalid startup clock")
    for age in AGES:
        for w in config.age_windows_seconds:
            count = row[f'{age}_age_sample_count_window{w}s']
            elapsed = row[f'{age}_age_elapsed_slots_window{w}s']
            if not 0 <= count <= elapsed <= w:
                raise ContractError("age count/elapsed disagreement")


def validate_batch(batch, kind, *, config=DEFAULT_CONFIG, previous_key=None):
    """Validate <=25k rows, returning final key for cross-batch grid validation.

    Callers still reconcile full session/member counts and cross-table joins.
    Work O(rows*fields), auxiliary memory O(batch_rows*fields), bounded by
    the 25,000-row admission limit (default callers use 4,096). Python column
    lists preserve integer/Decimal/null types; no floating coercion is used.
    """
    if kind not in ('base', 'features', 'support') or not isinstance(batch, pa.RecordBatch):
        raise ContractError("expected a known table kind and Arrow RecordBatch")
    schema = BASE_SCHEMA if kind == 'base' else feature_schema(config) if kind == 'features' else support_schema(config)
    if batch.num_rows > 25000 or not batch.schema.equals(schema, check_metadata=True):
        raise ContractError("batch/schema mismatch")
    if any(not f.nullable and batch.column(i).null_count for i, f in enumerate(schema)):
        raise ContractError("null in required field")
    names = schema.names
    columns = [column.to_pylist() for column in batch.columns]
    registry = feature_registry(config) if kind == 'features' else None
    previous = previous_key
    cached_member = None
    for values in zip(*columns):
        row = dict(zip(names, values))
        for value in values:
            if isinstance(value, float) and not math.isfinite(value):
                raise ContractError("nonfinite stored number")
        member = (row['session_date'], row['symbol'])
        if member != cached_member:
            if not re.fullmatch(r'[A-Z0-9][A-Z0-9._-]{0,31}', row['symbol']):
                raise ContractError("invalid symbol")
            start, end = session_bounds(row['session_date'])
            cached_member = member
        t = row['interval_end_ns']
        key = (*member, t)
        if not start < t <= end or t % NS:
            raise ContractError("timestamp outside session grid")
        if previous is not None and (previous[:2] != key[:2] or t != previous[2] + NS):
            raise ContractError("member changed or grid gap/duplicate")
        if kind == 'base':
            _base(row, session_start=start)
        elif kind == 'features':
            _features(row, config, registry)
        else:
            _support(row, config)
        previous = key
    return previous
