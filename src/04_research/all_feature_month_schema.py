"""Single public V2 registry. Types never depend on first-row values."""
import json
import pyarrow as pa

VERSION = 'tape_data_product_eda_v2'
HORIZONS = (60, 300)
REGISTRY = {
    'movement_mean_5s_bps': ('midpoint_movement_bps_per_30s', 1/6, 'bps', 'movement'),
    'movement_participation': (None, 1., 'dimensionless', 'movement'),
    'quoted_spread_mean_bps': ('quoted_spread_bps', 1., 'bps', 'spread'),
    'trade_rate': ('trade_rate', 1., 'trades/second', 'activity'),
    'dollar_rate': ('dollar_rate', 1., 'USD/second', 'activity'),
    'trade_age_p90_seconds': ('trade_age_p90', .001, 'seconds', 'trade_age'),
    'quote_age_p90_seconds': ('quote_age_p90', .001, 'seconds', 'quote_age'),
    'midpoint_change_age_p90_seconds': (None, 1., 'seconds', 'mid_age'),
    'movement_mean_to_spread': (None, 1., 'dimensionless', 'ratio'),
}
MAPPINGS = {k:v for k,v in REGISTRY.items() if v[0] is not None}
FEATURES = tuple(f'{name}_{h}s' for h in HORIZONS for name in REGISTRY)
REASONS = dict(undefined=1, source_unaccepted=2, immature=4, insufficient_support=8,
               active_halt=16, carried_history=32, nonpositive_spread=64, zero_total_movement=128)
NUMERICAL_POLICY = dict(rtol=1e-10, atol=1e-12, final_bound_correction_limit=1e-12,
    moments='exact binary64 fixed-point integer sums and squares; no rounding on eviction',
    diagnostic_recomputations=0, bound_corrections=0)
PRIMITIVES = {
    'midpoint': ('price', 'midpoint'),
    'midpoint_valid_duration_ns': ('nanoseconds', 'quote replay midpoint_duration_ns'),
    'movement_5s_bps': ('bps', 'supported absolute five-second log midpoint change'),
    'movement_5s_valid': ('boolean', 'six endpoint source and boundary eligibility'),
    'movement_1s_valid': ('boolean', 'two endpoint source and boundary eligibility'),
    'quoted_spread_integral_bps_seconds': ('bps seconds', 'quote replay spread_mass'),
    'quoted_spread_valid_duration_ns': ('nanoseconds', 'quote replay unlocked_duration_ns'),
    'trade_count_1s': ('count', 'primitive_trade_count'),
    'dollar_volume_1s': ('USD', 'primitive_dollars'),
    'trade_age_end_seconds': ('seconds', 'primitive_trade_age / 1000'),
    'quote_age_end_seconds': ('seconds', 'primitive_quote_age / 1000'),
    'midpoint_change_age_end_seconds': ('seconds', 'exact eligible quote changes'),
    'midpoint_age_observation_status': ('enum', 'known/no_change_observed/unobservable'),
    'midpoint_observation_start_ns': ('UTC nanoseconds', 'current continuous valid observation origin'),
    'midpoint_no_change_observed_seconds': ('seconds', 'lower bound; excluded from age quantiles'),
}

def field_metadata():
    result = {}
    for h in HORIZONS:
        for name, (source, scale, unit, family) in REGISTRY.items():
            result[f'{name}_{h}s'] = dict(source=f'{source}_{h}s' if source else 'retained primitives',
                source_unit='milliseconds' if family in ('trade_age','quote_age') else
                'bps per 30s normalization' if name=='movement_mean_5s_bps' else unit,
                unit=unit, scale=scale, family=family)
    return result

STRINGS = {'session_date','symbol','halt_interval_id','session_segment','discovery_timing_basis',
           'discovery_provenance_hash','midpoint_age_observation_status'}
BOOLEANS = {'halt_interval_active','halt_resume_boundary','historical_ex_post_overlay','live_reproducible',
            'primitive_quote_source_file_accepted','primitive_trade_source_file_accepted','post_discovery_eligible',
            'movement_5s_valid','movement_1s_valid'}
BOOL_STEMS = ('state_mature','movement_support_valid','activity_support_valid','quoted_spread_valid',
    'state_fully_post_halt','state_contains_pre_halt_history','state_carried_forward_during_halt',
    'midpoint_change_age_mature','midpoint_change_age_support_valid','movement_zero_total')

def dtype(k):
    if k in STRINGS: return pa.string()
    if k in BOOLEANS or k.endswith(('_analysis_valid','_eda_eligible')) or any(k==f'{s}_{h}s' for s in BOOL_STEMS for h in HORIZONS): return pa.bool_()
    if k.endswith('_ns') or k in ('continuity_segment_id','seconds_since_halt_resume','halt_generation','trade_count_1s') or '_count_' in k or k.endswith('_reason_mask') or 'observed_seconds_' in k: return pa.int64()
    return pa.float64()

def schema(columns, metadata):
    meta = metadata | dict(version=VERSION, contract='tape_data_product_v1', fields=field_metadata(),
        primitives={k:dict(unit=v[0],source=v[1]) for k,v in PRIMITIVES.items()},reason_bits=REASONS,
        numerical_policy=NUMERICAL_POLICY,halt_generation_provenance='reconstructed cumulative transitions from active halt to open second')
    return pa.schema([pa.field(k,dtype(k)) for k in columns], metadata={b'eda':json.dumps(meta,sort_keys=True).encode()})


def capabilities(schema):
    """Reject unsupported reuse explicitly; never silently replay raw trades."""
    from all_feature_month_core import BASE_COLUMNS
    missing=set(BASE_COLUMNS)-set(schema.names)
    if missing:raise ValueError('required base reuse capability absent: '+str(sorted(missing)))
    for name in BASE_COLUMNS:
        actual=schema.field(name).type
        if name in ('session_date','symbol','halt_interval_id'):
            if not pa.types.is_string(actual):raise ValueError('base string capability mismatch: '+name)
        elif name in BOOLEANS or any(name==f'{stem}_{h}s' for stem in BOOL_STEMS for h in HORIZONS):
            if not pa.types.is_boolean(actual):raise ValueError('base boolean capability mismatch: '+name)
        elif not (pa.types.is_integer(actual) or pa.types.is_floating(actual)):
            raise ValueError('base numeric capability mismatch: '+name)
    return dict(version=VERSION,transactions='published primitives',quote_integrals='one quote reducer',
                raw_trades_replayed=False,projected_columns=BASE_COLUMNS)
