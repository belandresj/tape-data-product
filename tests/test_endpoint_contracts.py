"""Contract invariants and tiny independent numerical examples, entirely offline."""
from dataclasses import replace
from decimal import Decimal, localcontext
import json
import math

import pyarrow as pa
import pytest
from tape_data_product.contracts import (
    BASE_SCHEMA, FEATURE_SCHEMA, SUPPORT_SCHEMA, DEFAULT_CONFIG, ContractError,
    EWView, FeatureConfig, Reason, contract_identity, query_registry, feature_registry,
)
from tape_data_product.contracts.config import canonical_json
from tape_data_product.contracts.policy import (
    NS, Event, transition, session_bounds, timely_trade, activity_endpoint, endpoint_return,
)
from tape_data_product.contracts.reasons import history_reasons, validate_value, decode_reasons
from tape_data_product.contracts.source import share_units, shares_from_units, add_share_units, SourceUnits
from tape_data_product.contracts.validation import validate_batch
from tape_data_product.contracts.interfaces import CompletedMember


def base_row(second=1):
    start, _ = session_bounds('2026-07-01')
    row = {f.name: 0 for f in BASE_SCHEMA}
    row.update(session_date='2026-07-01', symbol='TEST', interval_end_ns=start + second*NS,
               bid_twap_usd=100., ask_twap_usd=102., midpoint_twap_usd=101.,
               bid_end_usd=100., ask_end_usd=102.,
               spread_integral_bps_seconds=20000/101,
               bid_size_integral_shares_seconds=10., ask_size_integral_shares_seconds=20.,
               bid_size_end_shares=10., ask_size_end_shares=20., share_volume_1s=Decimal('0'),
               midpoint_age_status=1, midpoint_observation_start_ns=start,
               midpoint_age_lower_bound_seconds=float(second), midpoint_change_age_seconds=None,
               midpoint_change_age_reason_mask=256, quote_age_seconds=.5,
               trade_age_seconds=None, trade_age_reason_mask=128,
               quote_source_status=1, trade_source_status=1,
               quote_continuity_break_in_second=False, trade_continuity_break_in_second=False,
               halt_active=False, halt_id=None)
    for n in row:
        if n.endswith('_duration_ns'):
            row[n] = NS
    return row


def feature_row():
    row = {f.name: 0 for f in FEATURE_SCHEMA}
    row.update({k: base_row()[k] for k in ('session_date', 'symbol', 'interval_end_ns')})
    for f in feature_registry():
        row[f.name] = .5 if f.family == 'participation' else .2 if f.family == 'ratio' else 10. if f.family == 'spread' else 2.
    return row


def batch(row, schema):
    return pa.RecordBatch.from_pylist([row], schema=schema)


def test_schema_registry_names_types_and_dependencies():
    assert len(feature_registry()) == 24 and len(query_registry()) == 27
    assert len(FEATURE_SCHEMA) == 3 + 24*2
    assert BASE_SCHEMA.field('share_volume_1s').type == pa.decimal128(38, 9)
    assert FEATURE_SCHEMA.field('midpoint_rms_5s_bps_hl30s').metadata[b'unit'] == b'bps'
    assert all(f.sources == ('quote',) for f in query_registry() if f.family in ('return', 'spread', 'ratio'))
    assert len(BASE_SCHEMA.names) == len(set(BASE_SCHEMA.names))
    assert all(f.name not in BASE_SCHEMA.names for f in feature_registry())
    assert not any('carried_history' in n for n in FEATURE_SCHEMA.names)


@pytest.mark.parametrize('changes', [dict(spread_min_coverage=float('nan')), dict(other_min_coverage=True),
    dict(age_min_coverage=0), dict(views=(EWView(120, 300), EWView(30,60))),
    dict(age_windows_seconds=(301,)), dict(max_trade_reporting_age_ns=True)])
def test_bad_configuration(changes):
    with pytest.raises(ContractError):
        replace(DEFAULT_CONFIG, **changes)


def test_config_roundtrip_and_semantic_identity():
    raw = json.loads(canonical_json(DEFAULT_CONFIG.to_dict()))
    assert FeatureConfig.from_dict(raw) == DEFAULT_CONFIG
    assert contract_identity() == contract_identity(FeatureConfig.from_dict(raw))
    raw['surprise'] = True
    with pytest.raises(ContractError): FeatureConfig.from_dict(raw)
    variants = [replace(DEFAULT_CONFIG, **{name: value}) for name, value in (
        ('spread_min_coverage', .89), ('other_min_coverage', .79), ('age_min_coverage', .79),
        ('max_trade_reporting_age_ns', 999), ('age_windows_seconds', (60,)),
        ('views', (EWView(30, 61), EWView(120, 300))))]
    assert len({contract_identity(c) for c in variants} | {contract_identity()}) == len(variants)+1


def test_decimal_quantity_is_lossless_independent_of_decimal_context():
    with localcontext() as ctx:
        ctx.prec = 3
        total = add_share_units(share_units('123456789.123456789'), '0.000000001')
        assert shares_from_units(total) == Decimal('123456789.123456790')
        assert share_units('.5') == share_units(.5) == 500000000
        assert share_units('1.0000000000') == NS
        assert share_units('0e-999999') == 0
    for value in ('.0000000001', .1, 'NaN', '-1', '1e100000', '1e-100000', True):
        with pytest.raises(ContractError): share_units(value)
    with pytest.raises(ContractError): add_share_units(10**38-1, '.000000001')


def test_units_evidence_prevents_blanket_lot_conversion():
    evidence = 'a'*64
    assert SourceUnits('shares', evidence, evidence).multiplier == 1
    assert SourceUnits('round_lots', evidence, evidence, 100).multiplier == 100
    with pytest.raises(ContractError): SourceUnits('unknown', evidence, evidence)
    with pytest.raises(ContractError): SourceUnits('shares', evidence, evidence, 100)
    with pytest.raises(ContractError): SourceUnits('round_lots', evidence, evidence)


def test_strict_endpoint_and_reporting_age_inclusive_boundary():
    t = session_bounds('2026-07-01')[0]
    assert activity_endpoint(t-1) == t
    assert activity_endpoint(t) == t+NS
    assert timely_trade(t+NS, t)
    assert not timely_trade(t+NS+1, t)
    assert not timely_trade(t, t+1)
    assert activity_endpoint(t+NS+100) == t+2*NS
    assert timely_trade(t+NS+100, t+NS-100)
    for day in ('2026-03-08', '2026-11-01'):
        a, b = session_bounds(day)
        assert b-a == 57600*NS


def test_endpoint_return_does_not_require_valid_intermediate_quotes():
    assert endpoint_return(100.01,100.05,continuity_same=True,crosses_halt=False) == pytest.approx(3.9988004, rel=1e-7)
    assert endpoint_return(None,100.05,continuity_same=True,crosses_halt=False) is None
    assert endpoint_return(100.,101.,continuity_same=False,crosses_halt=False) is None
    assert endpoint_return(100.,101.,continuity_same=True,crosses_halt=True) is None
    assert math.isfinite(endpoint_return(5e-324,1e308,continuity_same=True,crosses_halt=False))


def test_gap_and_local_defects_do_not_reset_ew_or_other_sources():
    gap = transition(Event.GAP_ENTER)
    assert (gap.ew, gap.lag, gap.event_origins, gap.age_windows, gap.source_scope) == ('decay','clear','clear','reset','affected')
    assert transition(Event.GAP_RECOVER).ew == 'retain'
    assert transition(Event.HALT_ENTER).ew == 'reset'
    assert transition(Event.INVALID_QUOTE).lag == 'retain'
    assert transition(Event.INVALID_QUOTE).event_origins == 'clear_midpoint_only'
    for event in (Event.BAD_SIZE, Event.LATE_TRADE, Event.DISCOVERY, Event.STRATUM, Event.BATCH, Event.COVERAGE_FAILURE):
        assert transition(event).ew == 'retain'


def test_historical_publication_and_independent_exposure_examples():
    kwargs = dict(source=1, halted=False, elapsed=60, startup=60, possible=60, minimum=.8)
    assert history_reasons(usable=48, **kwargs) == 0
    assert history_reasons(usable=47, **kwargs) == Reason.LOW_COVERAGE
    assert history_reasons(usable=0, **kwargs) == Reason.NO_SUPPORTED_DATA
    assert history_reasons(usable=48, **{**kwargs, 'source':2}) == Reason.SOURCE_UNAVAILABLE
    assert history_reasons(usable=54, **{**kwargs, 'minimum':.9}) == 0
    # Independent algebraic fixtures: unequal exposure, magnitude participation, linear p90.
    assert (Decimal('.5')*10+10)/(Decimal('.5')*1+Decimal('.5')) == 15
    assert ((Decimal(0)+2)/2)**2 / ((Decimal(0)**2+2**2)/2) == Decimal('.5')
    ages = [0,1,2,3]; position=.9*(len(ages)-1)
    assert ages[2]+(position-2)*(ages[3]-ages[2]) == pytest.approx(2.7)
    # A gap decays observed numerator/denominator equally but reduces coverage.
    decay = 2**(-1/30)
    assert (10*decay)/(2*decay) == 5
    assert decay**5 < .9 < decay**4


def test_base_partial_exposure_known_zero_and_lower_bound():
    row = base_row()
    row.update(spread_integral_bps_seconds=9., spread_valid_duration_ns=900000000)
    validate_batch(batch(row, BASE_SCHEMA), 'base')
    assert row['activity_reason_mask'] == 0 and row['trade_count_1s'] == 0
    row['bid_size_end_shares'] = None
    row['bid_size_end_reason_mask'] = 64
    validate_batch(batch(row, BASE_SCHEMA), 'base')
    row['price_valid_duration_ns'] = 0
    with pytest.raises(ContractError): validate_batch(batch(row, BASE_SCHEMA),'base')


@pytest.mark.parametrize('mutation', ['null_mask','nan','duration','midpoint','halt','size','age_status','key','source'])
def test_base_rejects_contradictions(mutation):
    row = base_row()
    updates = {'null_mask': {'bid_end_usd':None}, 'nan':{'ask_end_usd':float('nan')},
               'duration':{'price_valid_duration_ns':NS+1}, 'midpoint':{'midpoint_twap_usd':150.},
               'halt':{'halt_active':True}, 'size':{'bid_size_end_shares':-1.},
               'age_status':{'midpoint_age_status':2}, 'key':{'interval_end_ns':0}, 'source':{'quote_source_status':2}}
    row.update(updates[mutation])
    with pytest.raises(ContractError): validate_batch(batch(row,BASE_SCHEMA),'base')


def test_batch_boundary_validation_and_schema_corruption():
    last = validate_batch(batch(base_row(1), BASE_SCHEMA),'base')
    validate_batch(batch(base_row(2),BASE_SCHEMA),'base',previous_key=last)
    with pytest.raises(ContractError): validate_batch(batch(base_row(3),BASE_SCHEMA),'base',previous_key=last)
    with pytest.raises(ContractError): validate_batch(batch(base_row(),BASE_SCHEMA).replace_schema_metadata(None),'base')


def test_feature_domain_and_composite_reason_rules():
    row = feature_row()
    validate_batch(batch(row,FEATURE_SCHEMA),'features')
    for h in (30,120):
        row[f'quoted_spread_bps_hl{h}s'] = 0.
        row[f'midpoint_rms_5s_to_spread_hl{h}s'] = None
        row[f'midpoint_rms_5s_to_spread_hl{h}s_reason_mask'] = 2048
    validate_batch(batch(row,FEATURE_SCHEMA),'features')
    row['midpoint_rms_5s_bps_hl30s_reason_mask'] = 64
    row['midpoint_rms_5s_bps_hl30s'] = None
    with pytest.raises(ContractError): validate_batch(batch(row,FEATURE_SCHEMA),'features')
    assert decode_reasons(8|16) == ('STARTUP','LOW_COVERAGE')
    with pytest.raises(ContractError): decode_reasons(4096)
    with pytest.raises(ContractError): validate_value(0., 32)


def test_support_schema_and_completed_member_restart_identity():
    row = {f.name:0 for f in SUPPORT_SCHEMA}
    row.update({k:base_row()[k] for k in ('session_date','symbol','interval_end_ns')})
    validate_batch(batch(row,SUPPORT_SCHEMA),'support')
    row['return_usable_weight_hl30s'] = 1.
    with pytest.raises(ContractError): validate_batch(batch(row,SUPPORT_SCHEMA),'support')
    receipt = CompletedMember(*('a'*64 for _ in range(5)), rows=60)
    receipt.require_match(receipt)
    with pytest.raises(ContractError): receipt.require_match(replace(receipt, contract_identity='b'*64))


def test_contract_layer_has_no_legacy_or_local_document_dependency():
    import ast
    from pathlib import Path
    import tape_data_product.contracts as contracts
    for path in Path(contracts.__file__).parent.glob('*.py'):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and node.module:
                assert not node.module.startswith(('tape_data_product.features', 'tape_data_product.query'))
        assert 'local_docs/' not in path.read_text()
