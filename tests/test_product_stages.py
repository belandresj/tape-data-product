"""Connected production calculator/release/query checks on tiny invented tapes."""
import json
from pathlib import Path
import pytest
from tape_data_product.features.api import build_partition
from tape_data_product.features import all_feature_month_core as core
from tape_data_product.query.release import build_release, verify_release
from tape_data_product.query.api import run_query
from tape_data_product.experiments.threshold_pilot import variants
from test_economic_tape_v3 import quote, trade, raw_pair


def fixture(tmp_path, symbol='TEST'):
    day='2026-07-01'
    start=core.session_start(day)
    quotes,trades=raw_pair(tmp_path,day,[quote(start-1),quote(start+core.NS)], [trade(start+core.NS,conditions=[12])])
    discovery={'endpoint_ns':start+core.NS,'verified':True,'provenance_hash':'synthetic'}
    path=tmp_path/'partition'
    build_partition(quotes,trades,day,symbol,discovery,path,seconds=8,synthetic=True)
    expected=tmp_path/'expected.jsonl'
    expected.write_text(json.dumps({'session_date':day,'symbol':symbol})+'\n')
    partitions=tmp_path/'partitions.jsonl'
    partitions.write_text(json.dumps({'session_date':day,'symbol':symbol,'path':str(path)})+'\n')
    return expected,partitions,path


def test_connected_release_query_accounts_unavailable_and_zero_matches(tmp_path):
    expected,partitions,path=fixture(tmp_path)
    release=tmp_path/'release'
    built=build_release(expected,partitions,release,allow_partial=True)
    assert verify_release(release)['release_identity']==built['release_identity']
    summary=run_query(release,dict(variants())['C'],tmp_path/'query')
    assert summary['members']==1 and summary['zero_match_members']==1
    assert summary['observed_endpoints']==8 and summary['observed_unavailable']==8
    assert summary['synthetic'] and summary['partial_release']
    assert (tmp_path/'query/windows.jsonl').read_text()==''
    assert json.loads((tmp_path/'query/members.jsonl').read_text())['completion_state']=='checkpoint'
    with pytest.raises(ValueError,match='Historical expected'):
        verify_release(release,'wrong')
    with (path/'features.parquet').open('ab') as stream:stream.write(b'corrupt')
    with pytest.raises(ValueError,match='identity'):
        verify_release(release)


def test_release_rejects_missing_duplicate_and_implicit_partial(tmp_path):
    expected,partitions,_=fixture(tmp_path)
    with pytest.raises(ValueError,match='Partial'):
        build_release(expected,partitions,tmp_path/'full')
    with expected.open('a') as stream:stream.write(json.dumps({'session_date':'2026-07-01','symbol':'MISSING'})+'\n')
    with pytest.raises(ValueError,match='1 missing'):
        build_release(expected,partitions,tmp_path/'missing',allow_partial=True)
    with partitions.open('a') as stream:stream.write(partitions.read_text())
    with pytest.raises(ValueError,match='duplicate'):
        build_release(expected,partitions,tmp_path/'duplicate',allow_partial=True)


def test_production_features_require_admitted_source_receipt(tmp_path):
    with pytest.raises(ValueError,match='canonical pair_manifest'):
        build_partition('missing','missing','2026-07-01','TEST',{},tmp_path/'partition')


def test_historical_pilot_variants_preserve_selected_config():
    configs=dict(variants())
    assert len(configs['A']['conditions'])==5
    assert len(configs['C']['conditions'])==6
    for name,entry,continuation in [('B',.4,.32),('C',.5,.4),('D',.6,.48)]:
        condition=next(row for row in configs[name]['conditions'] if row['feature']=='movement_participation_300s')
        assert condition['entry']['lower']==entry and condition['continuation']['lower']==continuation


def admitted_discovery_fixture(tmp_path, monkeypatch):
    """Isolate feature admission using a fake canonical-verification collaborator.

    These eight-second invented sources are not real full-session coverage.
    Canonical receipt verification itself is covered by acquisition tests.
    """
    from tape_data_product.features import compact_product
    from tape_data_product.storage import catalog
    import pyarrow.parquet as parquet

    day = '2026-07-01'
    start = core.session_start(day)
    quotes, trades = raw_pair(tmp_path, day, [quote(start-1)], [trade(start)])
    discovery = {
        'session_date': day, 'symbol': 'TEST', 'endpoint_ns': start+core.NS,
        'discovery_endpoint_ns': start+core.NS,
        'verified': True, 'discovery_verified': True,
        'timing_basis': 'two_minute_bar_close', 'synthetic': False,
        'provenance_hash': 'screen-input-identity',
        'minute_source_stage_identity': 'complete-minute-stage',
    }
    streams = {}
    for name, path in [('quotes', quotes), ('trades', trades)]:
        identity = compact_product.file_identity(path)
        streams[name] = {
            'sha256': identity['sha256'], 'bytes': identity['size_bytes'],
            'rows': parquet.ParquetFile(path).metadata.num_rows,
        }
    receipt = {
        'session_date': day, 'symbol': 'TEST', 'streams': streams,
        'selection_record': dict(discovery),
        'selection_record_sha256': compact_product.digest(discovery),
        'selection_sha256': 'complete-selection-file-identity',
        'selection_stage_identity': 'verified-screen-stage',
    }
    manifest = tmp_path/'pair.json'
    manifest.write_text(json.dumps(receipt))
    monkeypatch.setattr(catalog, 'verify_pair', lambda path, allow_synthetic=False: receipt)
    return quotes, trades, day, discovery, manifest, receipt


@pytest.mark.parametrize('change', [
    {'symbol': 'OTHER'}, {'session_date': '2026-07-02'},
    {'endpoint_ns': 1}, {'discovery_endpoint_ns': 1},
    {'provenance_hash': 'another-screen'},
])
def test_discovery_must_equal_admitted_selection_record(tmp_path, monkeypatch, change):
    quotes, trades, day, discovery, manifest, _ = admitted_discovery_fixture(tmp_path, monkeypatch)
    output = tmp_path/'features'
    with pytest.raises(ValueError, match='Discovery'):
        build_partition(quotes, trades, day, 'TEST', discovery | change,
                        output, seconds=8, pair_manifest=manifest)
    assert not (output/'manifest.json').exists()


@pytest.mark.parametrize('field', ['selection_record_sha256', 'selection_sha256', 'selection_stage_identity'])
def test_discovery_requires_bound_selection_provenance(tmp_path, monkeypatch, field):
    quotes, trades, day, discovery, manifest, receipt = admitted_discovery_fixture(tmp_path, monkeypatch)
    receipt[field] = None
    with pytest.raises(ValueError, match='selection provenance'):
        build_partition(quotes, trades, day, 'TEST', discovery,
                        tmp_path/'features', seconds=8, pair_manifest=manifest)


def test_admitted_discovery_identity_is_persisted(tmp_path, monkeypatch):
    quotes, trades, day, discovery, manifest, receipt = admitted_discovery_fixture(tmp_path, monkeypatch)
    result = build_partition(quotes, trades, day, 'TEST', discovery,
                             tmp_path/'features', seconds=8, pair_manifest=manifest)
    admission = result['metadata']['source_admission']
    assert admission['selection_record_sha256'] == receipt['selection_record_sha256']
    assert admission['selection_sha256'] == receipt['selection_sha256']
    assert result['metadata']['discovery']['first_discovery_endpoint_ns'] == discovery['endpoint_ns']


@pytest.mark.parametrize('mutation', ['quotes', 'pair_receipt'])
def test_source_mutation_before_completion_never_publishes_manifest(tmp_path, monkeypatch, mutation):
    from tape_data_product.features import api

    quotes, trades, day, discovery, manifest, _ = admitted_discovery_fixture(tmp_path, monkeypatch)
    original = api.direct_frozen_product.product_pairs

    def changing_pairs(*args, **kwargs):
        yield from original(*args, **kwargs)
        target = quotes if mutation == 'quotes' else manifest
        with target.open('ab') as stream:
            stream.write(b'changed')

    monkeypatch.setattr(api.direct_frozen_product, 'product_pairs', changing_pairs)
    output = tmp_path/'features'
    with pytest.raises(ValueError, match='changed during'):
        build_partition(quotes, trades, day, 'TEST', discovery,
                        output, seconds=8, pair_manifest=manifest)
    assert not (output/'manifest.json').exists()
    assert not (output/'stage.json').exists()


@pytest.mark.parametrize('change', [
    {'verified': False}, {'discovery_verified': False},
    {'minute_source_stage_identity': None}, {'timing_basis': 'unavailable'},
    {'synthetic': True},
])
def test_production_discovery_requires_verified_earliest_source_timing(tmp_path, monkeypatch, change):
    from tape_data_product.features import compact_product

    quotes, trades, day, discovery, manifest, receipt = admitted_discovery_fixture(tmp_path, monkeypatch)
    discovery.update(change)
    receipt['selection_record'] = dict(discovery)
    receipt['selection_record_sha256'] = compact_product.digest(discovery)
    with pytest.raises(ValueError, match='earliest-source timing'):
        build_partition(quotes, trades, day, 'TEST', discovery,
                        tmp_path/'features', seconds=8, pair_manifest=manifest)


def inventory_fixture(tmp_path, monkeypatch):
    """Tiny calculated tapes; canonical admission collaborator remains explicit fake."""
    from tape_data_product.features import compact_product
    from tape_data_product.storage import catalog

    inventory = tmp_path/'inventory.jsonl'
    with inventory.open('x') as stream:
        for symbol in ('ONE', 'TWO'):
            folder = tmp_path/symbol
            _, _, _, discovery, pair, receipt = admitted_discovery_fixture(folder, monkeypatch)
            discovery.update(symbol=symbol, synthetic=True)
            receipt.update(symbol=symbol, synthetic=True,
                           selection_record=dict(discovery),
                           selection_record_sha256=compact_product.digest(discovery))
            pair.write_text(json.dumps(receipt))
            stream.write(json.dumps(discovery | {
                'quotes_path': str(Path(symbol)/'quotes.parquet'),
                'trades_path': str(Path(symbol)/'trades.parquet'),
                'pair_manifest_path': str(Path(symbol)/'pair.json'),
                'pair_manifest_sha256': compact_product.file_identity(pair)['sha256'],
            })+'\n')
    monkeypatch.setattr(catalog, 'verify_pair',
                        lambda path, allow_synthetic=False: json.loads((Path(path)/'pair.json').read_text()))
    return inventory


def test_inventory_cli_emits_complete_release_index_without_hand_editing(tmp_path, monkeypatch):
    from tape_data_product.cli import main
    from tape_data_product.stages import verify_stage

    inventory = inventory_fixture(tmp_path, monkeypatch)
    output = tmp_path/'features'
    assert main(['features', 'build', '--inventory', str(inventory), '--output', str(output),
                 '--synthetic', '--seconds', '8']) == 0
    stage = verify_stage(output)
    assert stage['validation']['completed_members'] == 2
    members = [json.loads(line) for line in (output/'partitions.jsonl').read_text().splitlines()]
    assert [row['symbol'] for row in members] == ['ONE', 'TWO']
    assert all(not Path(row['path']).is_absolute() for row in members)
    release = tmp_path/'release'
    assert main(['release', 'build', '--expected-members', str(inventory),
                 '--partitions', str(output/'partitions.jsonl'), '--output', str(release),
                 '--allow-partial']) == 0
    assert verify_release(release)['member_count'] == 2


def test_inventory_duplicate_rejects_root_completion(tmp_path, monkeypatch):
    from tape_data_product.features.api import build_inventory

    inventory = inventory_fixture(tmp_path, monkeypatch)
    with inventory.open() as stream:
        duplicate = stream.readline()
    with inventory.open('a') as stream:
        stream.write(duplicate)
    output = tmp_path/'features'
    with pytest.raises(ValueError, match='Duplicate inventory'):
        build_inventory(inventory, output, seconds=8, synthetic=True)
    assert not (output/'stage.json').exists()
    assert not (output/'partitions.jsonl').exists()


def test_inventory_cli_rejects_mixed_direct_arguments(tmp_path):
    from tape_data_product.cli import main

    assert main(['features', 'build', '--inventory', 'unused.jsonl', '--symbol', 'OTHER',
                 '--output', str(tmp_path/'features')]) == 2
    assert not (tmp_path/'features').exists()
