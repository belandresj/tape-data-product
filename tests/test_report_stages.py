"""Small independent report oracles: eligibility, ties, zeros, tails and bands."""
import json
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tape_data_product.analysis import aggregates, selection, rendering
from tape_data_product.analysis.axes import bin_ids
from tape_data_product.analysis.ecdf import exact_table
from tape_data_product.analysis.verification import verify_numerical


def _axes():
    return {'axes': {stem: {'edges': [0, .5, 1] if stem == 'movement_participation' else [1, 10, 100]}
            for stem in ('movement_mean_5s_bps', 'quoted_spread_mean_bps', 'movement_participation', 'trade_rate')}}


def _values():
    values = {name: np.ones(8) for name in aggregates.FEATURES}
    for horizon in (60, 300):
        values[f'trade_rate_{horizon}s'] = np.array([1, 10, 30, 100, .5, 2, 2, np.nan])
        values[f'trade_age_p90_seconds_{horizon}s'] = np.array([2, 2, 2, 2, 1, 3, 1, np.nan])
        values[f'movement_mean_5s_bps_{horizon}s'] = np.array([0, .5, 100, 101, 2, 2, np.nan, np.nan])
        values[f'quoted_spread_mean_bps_{horizon}s'] = np.array([0, 2, 2, 200, 2, 2, 2, np.nan])
        values[f'movement_participation_{horizon}s'] = np.array([np.nan, .5, 1, .2, .5, .5, .5, np.nan])
        # Stored report projections must be unavailable before discovery.
        for stem in aggregates.FEATURE_STEMS:
            values[f'{stem}_{horizon}s'][-1] = np.nan
    # Distinct horizon population; no universal complete-case selection.
    values['trade_age_p90_seconds_300s'][1] = 3
    return values


def test_horizon_specific_activity_and_pair_masks():
    values = _values()
    post = np.array([True]*7 + [False])
    short = selection.transaction_gate(values, post, 60)
    long = selection.transaction_gate(values, post, 300)
    assert np.flatnonzero(short.passing).tolist() == [0, 1, 2, 3, 6]
    assert np.flatnonzero(long.passing).tolist() == [0, 2, 3, 6]
    assert selection.rate_band_ids(short, values).tolist() == [0, 1, 2, 3, -1, -1, 0, -1]
    assert np.flatnonzero(selection.feature_population(short, values, 'quoted_spread_mean_bps_60s')).tolist() == [0, 1, 2, 3, 6]
    assert np.flatnonzero(selection.pair_population(short, values, 'movement_mean_5s_bps_60s', 'movement_participation_60s')).tolist() == [1, 2, 3]


def test_bins_preserve_zero_atom_tails_and_final_edge():
    axis = {'feature': 'movement_mean_5s_bps', 'edges': [1, 10, 100], 'bins': 5}
    assert bin_ids(np.array([0., .5, 1, 10, 100, 101, np.nan]), axis).tolist() == [0, 1, 2, 3, 3, 4, -1]
    joint = aggregates.FixedJoint(axis, axis)
    joint.add(np.array([0., .5, 100, 101]), np.array([0., 2, 2, 200]), np.zeros(4, dtype=np.int8), np.ones(4, dtype=bool))
    assert joint.summary()['pooled'] == {'eligible': 4, 'off_axis': 2, 'x_below': 1, 'x_above': 1, 'y_below': 0, 'y_above': 1, 'x_zero': 1, 'y_zero': 1, 'zero_zero': 1}
    grid, x, y = rendering._display_joint(joint.counts.sum(axis=0), [1, 10, 100], [1, 10, 100], 1)
    assert grid.sum() == 2  # zero folded in; positive underflow/overflow omitted explicitly


def test_exact_ties_cross_batch_boundary_and_zero(tmp_path):
    source = tmp_path/'source.parquet'
    pq.write_table(pa.table({'value': [2., 0., 1., 1., 1., 1., 3.], 'session': [0, 1, 0, 1, 2, 0, 2]}), source)
    connection = duckdb.connect()
    connection.execute("SET memory_limit='64MB'")
    connection.execute('SET threads=1')
    try:
        result = exact_table(connection, [source], tmp_path/'exact.parquet', batch_size=2)
    finally:
        connection.close()
    rows = pq.read_table(tmp_path/'exact.parquet').to_pylist()
    assert [r['value'] for r in rows] == [0, 1, 2, 3]
    assert [r['pooled_count'] for r in rows] == [1, 4, 1, 1]
    assert [r['pooled_cumulative'] for r in rows] == [1, 5, 6, 7]
    assert result['totals'] == {'premarket': 3, 'rth': 2, 'after_hours': 2, 'pooled': 7}
    curve = rendering._reduced_exact_curve(tmp_path/'exact.parquet', 'pooled', 7)
    assert curve['atom_values'][0] == 0
    assert curve['cumulative_counts'][-1] == 7
    assert curve['verified_reduction_error_pp'] == 0


def test_numerical_bundle_accounting_and_corruption(tmp_path):
    axes = _axes()
    metadata = {f: {'unit': 'dimensionless'} for f in aggregates.FEATURES}
    binding = selection.artifact_binding(release_identity='fixture-release', inventory_identity='fixture-members',
        source_identities={'synthetic': True}, feature_definitions=metadata, bin_configuration=axes,
        units={f: 'dimensionless' for f in metadata}, code_identities={})
    accumulator = aggregates.ReportPlotAggregates(tmp_path/'work', axes, batch_size=8, feature_metadata=metadata)
    values = _values()
    reason = {f: np.zeros(8, dtype=np.uint32) for f in aggregates.FEATURES}
    for horizon in (60, 300):
        reason[f'movement_participation_{horizon}s'][0] = 128
    try:
        accumulator.add_batch({'date': '2026-06-18', 'symbol': 'SYNTH', 'partition_identity': 'synthetic-partition', 'source_identity': {'synthetic': True}},
            values, np.array([True]*7+[False]), np.array([0, 0, 1, 2, 0, 1, 2, 2], dtype=np.int8),
            reason_masks=reason, midpoint_status=np.array(['known', 'no_change_observed', 'known', 'known', 'known', 'known', 'unobservable', 'known']))
        accumulator.finalize(tmp_path/'numerical', binding)
    finally:
        accumulator.close()
        accumulator.database.close()
    assert verify_numerical(tmp_path/'numerical')['exact_tables'] == 18
    gate = json.loads((tmp_path/'numerical/coverage/gate.json').read_text())
    assert gate['60s']['pooled']['passing'] == 5
    assert gate['300s']['pooled']['passing'] == 4
    path = tmp_path/'numerical/coverage/gate.json'
    path.write_text('{}')
    with pytest.raises(ValueError, match='artifact changed'):
        verify_numerical(tmp_path/'numerical')


def test_rejects_infinity_and_bad_batch():
    values = _values()
    values['trade_rate_60s'][0] = np.inf
    with pytest.raises(ValueError, match='infinity'):
        selection.transaction_gate(values, np.array([True]*7+[False]), 60)
    with pytest.raises(ValueError, match='batch size'):
        aggregates.ReportPlotAggregates(Path('not-created'), _axes(), batch_size=25001)


def test_screen_reconciliation_rejects_incomplete_release(tmp_path):
    import sqlite3
    from tape_data_product.stages import write_stage
    from tape_data_product.analysis.pipeline import reconcile_screen_membership, population_from_screen
    screen = tmp_path/'screen'
    screen.mkdir()
    (screen/'denominators.json').write_text(json.dumps({'session_date': '2026-06-18', 'reference_symbols': 2, 'candidates': 1, 'population_coverage_complete': True, 'missing_minute_sources': 0}))
    (screen/'selection.jsonl').write_text(json.dumps({'session_date': '2026-06-18', 'symbol': 'SELECTED'})+'\n')
    connection = sqlite3.connect(screen/'screen.sqlite')
    connection.execute('CREATE TABLE reference(symbol TEXT)')
    connection.executemany('INSERT INTO reference VALUES (?)', [('SELECTED',), ('NO_BARS',)])
    connection.commit()
    connection.close()
    write_stage(screen, 'screen', {}, {}, ['denominators.json', 'selection.jsonl', 'screen.sqlite'], {'passed': True}, synthetic=True)
    population = population_from_screen(screen)
    assert population['period_share_pct'] == 50
    assert population['reference_symbol_days'] == 2
    release = tmp_path/'release'
    release.mkdir()
    (release/'members.jsonl').write_text('')
    with pytest.raises(ValueError, match='missing 1'):
        reconcile_screen_membership(screen, release, tmp_path/'missing.sqlite')
    (release/'members.jsonl').write_text(json.dumps({'session_date': '2026-06-18', 'symbol': 'SELECTED', 'path': 'unused'})+'\n')
    counts = reconcile_screen_membership(screen, release, tmp_path/'matched.sqlite')
    assert counts['distinct_reference_symbols'] == 2
    assert counts['distinct_selected_symbols'] == 1
    assert counts['missing_members'] == 0


def test_quote_carry_and_ties_are_causal(monkeypatch):
    from tape_data_product.features import economic_tape_state_v3
    from tape_data_product.analysis.tape import _quotes
    def quote(mid):
        return {'price_state_valid': True, 'bid': mid-1, 'ask': mid+1, 'midpoint': mid}
    rows = [(5, quote(10)), (10, quote(11)), (10, quote(12)), (20, quote(13)), (30, quote(99))]
    monkeypatch.setattr(economic_tape_state_v3, 'events', lambda *args, **kwargs: iter(rows))
    selected, scanned = _quotes('unused', '2026-06-18', 10, 30, 20)
    assert selected[0][1:] == [11, 13, 12]  # final stable timestamp tie wins
    assert selected[-2][1:] == [12, 14, 13]
    assert all(99 not in row[1:] for row in selected)  # event exactly end excluded


def test_historical_population_rejects_incomplete_source_coverage(tmp_path):
    from tape_data_product.analysis.pipeline import population_from_screen
    from tape_data_product.stages import write_stage
    root = tmp_path/'screen'
    root.mkdir()
    (root/'denominators.json').write_text(json.dumps({'session_date': '2026-06-18', 'reference_symbols': 2, 'candidates': 1,
        'population_coverage_complete': False, 'missing_minute_sources': 1}))
    write_stage(root, 'screen', {}, {}, ['denominators.json'], {'complete': True}, synthetic=False)
    with pytest.raises(ValueError, match='complete verified minute-source coverage'):
        population_from_screen(root)


def test_extract_metadata_caps_fail_before_unbounded_growth(tmp_path):
    accumulator = aggregates.ReportPlotAggregates(tmp_path/'work', _axes(), batch_size=8)
    member = {'date': '2026-06-18', 'symbol': 'SYNTH', 'partition_identity': 'fixture', 'source_identity': {}}
    try:
        accumulator._next_ordinal = aggregates.MAX_MEMBERS
        with pytest.raises(ValueError, match='member cap'):
            accumulator._register_member(member)
        accumulator._next_ordinal = 0
        accumulator._register_member(member)
        accumulator._extract_row_groups = aggregates.MAX_EXTRACT_ROW_GROUPS
        masks = {f: np.ones(8, dtype=bool) for f in aggregates.FEATURES}
        with pytest.raises(ValueError, match='row groups'):
            accumulator._write_extract(_values(), masks, np.zeros(8, dtype=np.int8))
    finally:
        accumulator.close()
        accumulator.database.close()
