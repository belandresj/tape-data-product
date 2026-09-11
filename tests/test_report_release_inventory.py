from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src/04_research'))
import compact_preview_reader as READER
import compact_product as P
import compact_product_schema as S
import report_release_inventory as R


DAY = '2026-03-09'


def calculation(version='fixture-current'):
    return dict(version=version, midpoint='exact_binary64_integer_nanoseconds_v1', files={'fixture': version})


def publication(symbol='TEST', day=DAY, *, calc=None, mode='legacy', status='uploaded_verified', meta_extra=None):
    calc = calc or calculation()
    meta = dict(session_date=day, symbol=symbol, expected_rows=57600,
        layout_version=S.LAYOUT_VERSION, feature_contract=S.FEATURE_CONTRACT,
        builder_version=P.BUILDER_VERSION, calculation=calc, discovery_verified=True,
        discovery={'first_discovery_endpoint_ns': 1})
    meta.update(meta_extra or {})
    if mode != 'legacy':
        meta['validation_policy'] = dict(version=P.VALIDATION_POLICY_VERSION, mode=mode)
    pid = P.digest(meta)
    prefix = f'derived/tape_data_product/{S.LAYOUT_VERSION}/partitions/{pid}'
    objects = {}
    for name, schema in (('features', S.FEATURE_SCHEMA), ('support', S.SUPPORT_SCHEMA)):
        objects[name] = dict(file=name + '.parquet', sha256=('a' if name == 'features' else 'b') * 64,
            size_bytes=100 if name == 'features' else 200, rows=57600, schema_hash=S.schema_hash(schema))
    if mode == 'legacy':
        evidence = dict(rows_verified=57600, reconstructed_fields=list(S.FEATURES))
    elif mode == 'integrity':
        evidence = dict(rows_verified=57600, rows_integrity_checked=57600, mode='integrity',
            independent_reconstruction='not_run', reconstructed_fields=[])
    else:
        evidence = dict(rows_verified=57600, rows_integrity_checked=57600, mode='reconstruction',
            independent_reconstruction='passed', reconstructed_fields=list(S.FEATURES))
    manifest = dict(metadata=meta, partition_identity=pid, objects=objects, validation=evidence)
    raw = json.dumps(manifest, sort_keys=True).encode()
    frozen = {name: {key: obj[key] for key in ('sha256', 'size_bytes', 'rows')} |
        {'object_key': prefix + '/' + obj['file'], 'status': status}
        for name, obj in objects.items()}
    frozen['manifest'] = dict(object_key=prefix + '/manifest.json', sha256=hashlib.sha256(raw).hexdigest(),
        size_bytes=len(raw), rows=None, status=status)
    receipt = dict(state='complete', rows_verified=57600, partition_identity=pid,
        publication=dict(bucket='massive-equities', manifest_key=frozen['manifest']['object_key'], objects=frozen))
    return dict(receipt=receipt, member={'session_date': day, 'symbol': symbol},
                receipt_sha256='c' * 64, receipt_path='fixture', manifest=manifest,
                manifest_json=raw.decode())


def refresh_marker(record):
    raw = json.dumps(record['manifest'], sort_keys=True).encode()
    record['manifest_json'] = raw.decode()
    record['receipt']['publication']['objects']['manifest'].update(
        sha256=hashlib.sha256(raw).hexdigest(), size_bytes=len(raw))


def entry(record):
    import compact_preview_inventory as I
    return I.entry_from_record(record)


def write_jsonl(path, rows):
    path.write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in rows))
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize('mode,expected', [
    ('legacy', 'legacy_reconstruction'), ('integrity', 'integrity'), ('reconstruction', 'reconstruction')])
def test_reader_accepts_supported_validation_levels_without_upgrading(mode, expected):
    record = publication(mode=mode)
    manifest, frozen = record['manifest'], entry(record)
    READER.validate_manifest(manifest, frozen, [calculation()])
    level = manifest['report_compatibility']['validation']
    assert level['mode'] == expected
    assert level['independent_reconstruction'] == ('not_run' if mode == 'integrity' else 'passed')
    assert level['reconstructed_fields'] == ([] if mode == 'integrity' else list(S.FEATURES))


@pytest.mark.parametrize('mutation,match', [
    (lambda m: m['metadata'].update(validation_policy={'version': 'unknown', 'mode': 'integrity'}), 'policy'),
    (lambda m: m['validation'].update(mode='reconstruction'), 'contradiction'),
    (lambda m: m['validation'].update(independent_reconstruction='passed'), 'claims reconstruction'),
    (lambda m: m['validation'].update(reconstructed_fields=[S.FEATURES[0]]), 'claims reconstruction'),
    (lambda m: m['validation'].update(rows_integrity_checked=1), 'contradiction'),
])
def test_integrity_policy_rejects_malformed_or_contradictory_evidence(mutation, match):
    record = publication(mode='integrity')
    mutation(record['manifest'])
    with pytest.raises(ValueError, match=match):
        R.validation_level(record['manifest']['metadata'], record['manifest']['validation'])


def test_legacy_evidence_cannot_claim_integrity_or_partial_reconstruction():
    record = publication(mode='legacy')
    record['manifest']['validation']['independent_reconstruction'] = 'not_run'
    with pytest.raises(ValueError, match='contradictory legacy'):
        R.manifest_compatibility(record['manifest'], entry(record), [calculation()])
    record = publication(mode='legacy')
    record['manifest']['validation']['reconstructed_fields'].pop()
    with pytest.raises(ValueError, match='exhaustive'):
        R.manifest_compatibility(record['manifest'], entry(record), [calculation()])


def test_release_reconciles_membership_and_derives_actual_coverage_and_identities(tmp_path):
    calc_a, calc_b = calculation('a'), calculation('b')
    selection = tmp_path / 'selection.jsonl'
    selection_sha = write_jsonl(selection, [
        {'session_date': DAY, 'symbol': 'AAA'},
        {'session_date': '2026-03-10', 'symbol': 'BBB'},
        {'session_date': '2026-03-11', 'symbol': 'NOPE', 'selection_status': 'excluded', 'reason': 'accepted source exclusion'},
    ])
    captures = tmp_path / 'publications.jsonl'
    write_jsonl(captures, [publication('AAA', calc=calc_a, mode='legacy'),
                           publication('BBB', '2026-03-10', calc=calc_b, mode='integrity'),
                           publication('OUTSIDE', calc=calc_a, mode='reconstruction')])
    manifest = R.freeze(selection, captures, tmp_path / 'release', selection_sha256=selection_sha,
                        accepted_calculations=[calc_a, calc_b])
    assert manifest['release_status'] == 'complete'
    assert manifest['reconciliation']['counts'] == {'completed': 2, 'excluded': 2}
    assert manifest['dates'] == [DAY, '2026-03-10'] and manifest['date_min'] == DAY
    assert manifest['symbols'] == ['AAA', 'BBB'] and manifest['symbol_days'] == 2
    assert manifest['calculation_identities'] == sorted([P.digest(calc_a), P.digest(calc_b)])
    assert manifest['validation_counts'] == {'legacy_reconstruction': 1, 'integrity': 1}
    loaded, members = R.read(tmp_path / 'release', accepted_calculations=[calc_a, calc_b])
    assert loaded['release_identity'] == manifest['release_identity']
    assert [(m['session_date'], m['symbol']) for m in members] == [(DAY, 'AAA'), ('2026-03-10', 'BBB')]


def test_missing_selected_member_keeps_release_incomplete(tmp_path):
    selection = tmp_path / 'selection.jsonl'
    sha = write_jsonl(selection, [{'session_date': DAY, 'symbol': 'AAA'}, {'session_date': DAY, 'symbol': 'MISSING'}])
    captures = tmp_path / 'captures.jsonl'; write_jsonl(captures, [publication('AAA')])
    manifest = R.freeze(selection, captures, tmp_path / 'release', selection_sha256=sha,
                        accepted_calculations=[calculation()])
    assert manifest['release_status'] == 'incomplete'
    assert manifest['reconciliation']['counts'] == {'completed': 1, 'missing': 1}
    with pytest.raises(ValueError, match='incomplete'):
        R.read(tmp_path / 'release')
    assert len(R.read(tmp_path / 'release', require_complete=False)[1]) == 1


@pytest.mark.parametrize('mutation,reason', [
    (lambda r: r['manifest']['objects']['features'].update(schema_hash='0' * 64), 'schema'),
    (lambda r: r['manifest']['objects']['features'].update(sha256='d' * 64), 'identity'),
    (lambda r: r['manifest']['metadata'].update(calculation=calculation('unknown')), 'calculation'),
])
def test_incompatible_schema_hash_or_calculation_is_ledgered_not_admitted(tmp_path, mutation, reason):
    selection = tmp_path / 'selection.jsonl'; sha = write_jsonl(selection, [{'session_date': DAY, 'symbol': 'TEST'}])
    record = publication(); mutation(record)
    refresh_marker(record)
    captures = tmp_path / 'captures.jsonl'; write_jsonl(captures, [record])
    manifest = R.freeze(selection, captures, tmp_path / 'release', selection_sha256=sha,
                        accepted_calculations=[calculation()])
    assert manifest['reconciliation']['counts'] == {'incompatible': 1}
    ledger = list(R._records(tmp_path / 'release' / 'reconciliation.jsonl'))
    assert reason in ledger[0]['reason']
    assert manifest['accepted_members'] == 0


def test_duplicate_selection_and_competing_completed_partitions_are_rejected_or_ledgered(tmp_path):
    selection = tmp_path / 'duplicate.jsonl'
    sha = write_jsonl(selection, [{'session_date': DAY, 'symbol': 'TEST'}] * 2)
    captures = tmp_path / 'empty.jsonl'; captures.write_text('')
    with pytest.raises(ValueError, match='duplicate selected'):
        R.freeze(selection, captures, tmp_path / 'bad', selection_sha256=sha,
                 accepted_calculations=[calculation()])
    selection = tmp_path / 'selection.jsonl'; sha = write_jsonl(selection, [{'session_date': DAY, 'symbol': 'TEST'}])
    one = publication(calc=calculation('a')); two = publication(calc=calculation('b'))
    write_jsonl(captures, [one, two])
    manifest = R.freeze(selection, captures, tmp_path / 'release', selection_sha256=sha,
                        accepted_calculations=[calculation('a'), calculation('b')])
    assert manifest['release_status'] == 'incomplete'
    assert manifest['reconciliation']['counts'] == {'incompatible': 1}
    ledger = list(R._records(tmp_path / 'release' / 'reconciliation.jsonl'))
    assert ledger[0]['reason'] == 'ambiguous competing completed partitions'


def test_selection_hash_and_read_compatibility_policy_are_immutable(tmp_path):
    selection = tmp_path / 'selection.jsonl'; sha = write_jsonl(selection, [{'session_date': DAY, 'symbol': 'TEST'}])
    captures = tmp_path / 'captures.jsonl'; write_jsonl(captures, [publication()])
    with pytest.raises(ValueError, match='selection identity'):
        R.freeze(selection, captures, tmp_path / 'bad', selection_sha256='0' * 64,
                 accepted_calculations=[calculation()])
    R.freeze(selection, captures, tmp_path / 'release', selection_sha256=sha,
             accepted_calculations=[calculation()])
    with pytest.raises(ValueError, match='policy changed'):
        R.read(tmp_path / 'release', accepted_calculations=[calculation('different')])


def test_selection_source_identity_mismatch_is_incompatible(tmp_path):
    wanted_inputs = {'quotes': {'sha256': '1' * 64}, 'trades': {'sha256': '2' * 64}}
    selection = tmp_path / 'selection.jsonl'
    sha = write_jsonl(selection, [{'session_date': DAY, 'symbol': 'TEST', 'inputs': wanted_inputs}])
    record = publication(meta_extra={'inputs': {'quotes': {'sha256': '3' * 64},
                                                 'trades': {'sha256': '2' * 64}}})
    captures = tmp_path / 'captures.jsonl'; write_jsonl(captures, [record])
    manifest = R.freeze(selection, captures, tmp_path / 'release', selection_sha256=sha,
                        accepted_calculations=[calculation()])
    assert manifest['reconciliation']['counts'] == {'incompatible': 1}
    ledger = list(R._records(tmp_path / 'release' / 'reconciliation.jsonl'))
    assert ledger[0]['reason'] == 'selected inputs identity mismatch'


def test_malformed_captured_publication_for_selected_member_is_incompatible(tmp_path):
    selection = tmp_path / 'selection.jsonl'; sha = write_jsonl(selection, [{'session_date': DAY, 'symbol': 'TEST'}])
    record = publication(); record['receipt']['publication']['objects']['features']['status'] = 'uploaded'
    captures = tmp_path / 'captures.jsonl'; write_jsonl(captures, [record])
    manifest = R.freeze(selection, captures, tmp_path / 'release', selection_sha256=sha,
                        accepted_calculations=[calculation()])
    assert manifest['reconciliation']['counts'] == {'incompatible': 1}
    ledger = list(R._records(tmp_path / 'release' / 'reconciliation.jsonl'))
    assert 'unverified immutable publication' in ledger[0]['reason']


def test_public_aggregation_interfaces_iterate_release_members_and_projected_batches(tmp_path, monkeypatch):
    selection = tmp_path / 'selection.jsonl'; sha = write_jsonl(selection, [{'session_date': DAY, 'symbol': 'TEST'}])
    captures = tmp_path / 'captures.jsonl'; write_jsonl(captures, [publication(mode='integrity')])
    release = tmp_path / 'release'
    R.freeze(selection, captures, release, selection_sha256=sha, accepted_calculations=[calculation()])
    members = list(R.iter_accepted_members(release, accepted_calculations=[calculation()]))
    assert len(members) == 1 and members[0]['validation']['mode'] == 'integrity'
    calls = []
    def batches(client, member, accepted, features, scratch, stats, batch_size, **kwargs):
        calls.append((member['symbol'], accepted, features, batch_size))
        yield ({'trade_rate_60s': [1.]}, [True], [1])
    monkeypatch.setattr(READER, 'batches', batches)
    projected = list(R.projected_feature_batches(release, object(), ['trade_rate_60s'], tmp_path / 'scratch',
        accepted_calculations=[calculation()], batch_size=7))
    assert projected[0][0]['symbol'] == 'TEST' and projected[0][1][0]['trade_rate_60s'] == [1.]
    assert calls == [('TEST', [calculation()], ['trade_rate_60s'], 7)]


def test_valid_publication_does_not_hide_competing_malformed_record(tmp_path):
    valid = publication()
    broken = deepcopy(valid)
    broken['receipt']['publication']['objects']['features']['sha256'] = 'bad'
    selection = tmp_path/'selection.jsonl'
    digest = write_jsonl(selection, [{'session_date': DAY, 'symbol': 'TEST'}])
    capture = tmp_path/'capture.jsonl';write_jsonl(capture, [valid, broken])
    result = R.freeze(selection, capture, tmp_path/'release', selection_sha256=digest,
                      accepted_calculations=[calculation()])
    assert result['accepted_members'] == 0
    assert result['reconciliation']['counts'] == {'incompatible': 1}


def test_capture_run_freezes_exact_manifest_bytes_locally(tmp_path):
    record = publication(mode='integrity')
    run = tmp_path/'run';(run/'receipts').mkdir(parents=True);(run/'payloads').mkdir()
    product = tmp_path/'product';product.mkdir()
    (product/'manifest.json').write_text(record['manifest_json'])
    receipt = record['receipt'] | {'manifest_directory': str(product)}
    (run/'receipts/one.json').write_text(json.dumps(receipt))
    (run/'payloads/one.json').write_text(json.dumps({'member':record['member']}))
    result = R.capture_run(run,tmp_path/'capture.jsonl',accepted_calculations=[calculation()])
    assert result['captured']==1
    captured=json.loads((tmp_path/'capture.jsonl').read_text())
    assert captured['manifest_json']==record['manifest_json']
    assert 'manifest_path' not in captured
