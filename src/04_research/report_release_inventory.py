"""Frozen report-release membership and compact-manifest compatibility.

The selection and publication captures are local control data.  This module does
not scan object storage.  Reconciliation is O(S + P) time and O(S + P) bounded
control memory; feature reads remain streaming and bounded by the compact reader.
"""
from collections import Counter
from datetime import date
import hashlib
import json
from pathlib import Path
import re

import compact_product as P
import compact_product_schema as S
import compact_preview_inventory as PREVIEW


SCHEMA = 'report_release_inventory_v1'
MAX_RECORD = 256 * 1024
_HEX64 = re.compile(r'[0-9a-f]{64}')


def _digest_file(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        while block := stream.read(1024 * 1024):
            h.update(block)
    return h.hexdigest()


def _records(path):
    with Path(path).open() as stream:
        while line := stream.readline(MAX_RECORD + 1):
            if len(line) > MAX_RECORD:
                raise ValueError('inventory record exceeds 256 KiB')
            if line.strip():
                yield json.loads(line)


def _offset_records(path):
    with Path(path).open() as stream:
        while True:
            offset = stream.tell()
            line = stream.readline(MAX_RECORD+1)
            if not line: return
            if len(line) > MAX_RECORD: raise ValueError('inventory record exceeds 256 KiB')
            if line.strip(): yield offset, json.loads(line)


def _key(item):
    day, symbol = item.get('session_date'), item.get('symbol')
    if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
        raise ValueError('invalid session date')
    if not isinstance(symbol, str) or not symbol or len(symbol) > 32:
        raise ValueError('invalid symbol')
    return day, symbol


def _accepted(accepted_calculations):
    if isinstance(accepted_calculations, dict):
        accepted_calculations = [accepted_calculations]
    values = list(accepted_calculations or ())
    if not values or any(not isinstance(value, dict) for value in values):
        raise ValueError('explicit accepted calculation identities required')
    identities = [P.digest(value) for value in values]
    if len(set(identities)) != len(identities):
        raise ValueError('duplicate accepted calculation identity')
    return values, identities


def validation_level(metadata, evidence, expected_rows=57600):
    """Validate, classify, and preserve validation evidence without upgrading it."""
    if not isinstance(evidence, dict):
        raise ValueError('missing validation evidence')
    fields = evidence.get('reconstructed_fields')
    if not isinstance(fields, list) or any(not isinstance(field, str) for field in fields) \
            or len(fields) != len(set(fields)):
        raise ValueError('malformed reconstructed fields')
    if set(fields) - set(S.FEATURES):
        raise ValueError('unknown reconstructed field')
    if evidence.get('rows_verified') != expected_rows:
        raise ValueError('validation row count mismatch')
    policy = metadata.get('validation_policy')
    if policy is None:
        if evidence.get('mode') is not None or evidence.get('independent_reconstruction') is not None \
                or evidence.get('rows_integrity_checked') is not None:
            raise ValueError('contradictory legacy validation evidence')
        if set(fields) != set(S.FEATURES):
            raise ValueError('legacy completion lacks exhaustive reconstruction evidence')
        return dict(policy_version='legacy_exhaustive_v1', mode='legacy_reconstruction',
                    rows_verified=expected_rows, rows_integrity_checked=expected_rows,
                    independent_reconstruction='passed', reconstructed_fields=list(fields))
    if not isinstance(policy, dict) or set(policy) != {'version', 'mode'} \
            or policy.get('version') != P.VALIDATION_POLICY_VERSION \
            or policy.get('mode') not in P.VALIDATION_MODES:
        raise ValueError('unknown or malformed validation policy')
    mode = policy['mode']
    if evidence.get('mode') != mode or evidence.get('rows_integrity_checked') != expected_rows:
        raise ValueError('validation policy/evidence contradiction')
    if mode == 'integrity':
        if fields or evidence.get('independent_reconstruction') != 'not_run':
            raise ValueError('integrity evidence claims reconstruction')
    elif set(fields) != set(S.FEATURES) or evidence.get('independent_reconstruction') != 'passed':
        raise ValueError('reconstruction evidence incomplete')
    return dict(policy_version=policy['version'], mode=mode, rows_verified=expected_rows,
                rows_integrity_checked=expected_rows,
                independent_reconstruction=evidence['independent_reconstruction'],
                reconstructed_fields=list(fields))


def manifest_compatibility(manifest, entry, accepted_calculations):
    """Apply the compact reader's non-negotiable identity and schema contract."""
    accepted, accepted_ids = _accepted(accepted_calculations)
    meta = manifest.get('metadata')
    if not isinstance(meta, dict):
        raise ValueError('missing completion metadata')
    if not isinstance(meta.get('calculation'), dict):
        raise ValueError('missing or malformed calculation identity')
    P.require_compatible(meta, accepted)
    calculation_identity = P.digest(meta['calculation'])
    if calculation_identity not in accepted_ids:
        raise ValueError('unknown calculation identity')
    if meta.get('layout_version') != S.LAYOUT_VERSION \
            or meta.get('feature_contract') != S.FEATURE_CONTRACT \
            or meta.get('builder_version') != P.BUILDER_VERSION:
        raise ValueError('unsupported compact layout/contract')
    if P.digest(meta) != entry.get('partition_identity') \
            or manifest.get('partition_identity') != entry.get('partition_identity'):
        raise ValueError('completion partition identity mismatch')
    for name in ('session_date', 'symbol', 'expected_rows'):
        if meta.get(name) != entry.get(name):
            raise ValueError('completion key/coverage mismatch')
    if meta['expected_rows'] != 57600:
        raise ValueError('report release requires full completed symbol-days')
    level = validation_level(meta, manifest.get('validation'), meta['expected_rows'])
    for name, schema in (('features', S.FEATURE_SCHEMA), ('support', S.SUPPORT_SCHEMA)):
        obj = manifest.get('objects', {}).get(name, {})
        frozen = entry.get('objects', {}).get(name, {})
        if obj.get('file') != name + '.parquet' or obj.get('schema_hash') != S.schema_hash(schema):
            raise ValueError('unsupported object schema/filename')
        if any(obj.get(key) != frozen.get(key) for key in ('sha256', 'size_bytes', 'rows')):
            raise ValueError('completion feature/support identity mismatch')
    return dict(calculation_identity=calculation_identity, validation=level)


def _selection_compatibility(selected, metadata):
    """Bind the publication to source identities frozen by intended selection."""
    if selected.get('partition_identity') is not None \
            and selected['partition_identity'] != P.digest(metadata):
        raise ValueError('selected partition identity mismatch')
    for name in ('inputs', 'overlay'):
        if name in selected and selected[name] != metadata.get(name):
            raise ValueError('selected ' + name + ' identity mismatch')
    if selected.get('expected_rows', 57600) != metadata.get('expected_rows'):
        raise ValueError('selected row-count identity mismatch')
    if 'discovery' in selected:
        discovery = selected['discovery']
        expected = dict(first_discovery_endpoint_ns=discovery.get('endpoint_ns'),
            discovery_received_at_ns=discovery.get('received_at_ns'),
            discovery_timing_basis=discovery.get('timing_basis', 'unavailable'),
            discovery_provenance_hash=discovery.get('provenance_hash'))
        if metadata.get('discovery') != expected \
                or metadata.get('discovery_verified') != bool(discovery.get('verified')):
            raise ValueError('selected discovery identity mismatch')


def _selection(path, expected_sha256):
    actual = _digest_file(path)
    if not _HEX64.fullmatch(expected_sha256 or '') or actual != expected_sha256:
        raise ValueError('frozen selection identity mismatch')
    selected = {}
    excluded = []
    seen = set()
    for item in _records(path):
        key = _key(item)
        if key in seen:
            raise ValueError('duplicate selected symbol-day')
        seen.add(key)
        disposition = item.get('selection_status', 'selected')
        if disposition == 'selected':
            selected[key] = {name: item[name] for name in ('session_date', 'symbol', 'partition_identity', 'inputs', 'overlay', 'expected_rows', 'discovery') if name in item}
            if 'discovery' in selected[key]:
                selected[key]['discovery'] = {k: v for k, v in item['discovery'].items() if k != 'provenance'}
        elif disposition == 'excluded' and isinstance(item.get('reason'), str) and item['reason']:
            excluded.append(item)
        else:
            raise ValueError('malformed selection disposition')
    if not selected:
        raise ValueError('frozen selection has no selected members')
    return actual, selected, excluded


def _publication(record):
    """Convert a locally captured receipt plus marker into a preview reader entry."""
    entry = PREVIEW.entry_from_record(record)
    manifest = None
    if isinstance(record.get('manifest_json'), str):
        raw = record['manifest_json'].encode()
        marker = entry['objects']['manifest']
        if len(raw) != marker['size_bytes'] or hashlib.sha256(raw).hexdigest() != marker['sha256']:
            raise ValueError('captured completion marker identity mismatch')
        manifest = json.loads(raw)
    elif record.get('manifest_path'):
        path = Path(record['manifest_path'])
        if path.stat().st_size > MAX_RECORD:
            raise ValueError('completion manifest exceeds 256 KiB')
        raw = path.read_bytes()
        marker = entry['objects']['manifest']
        if len(raw) != marker['size_bytes'] or hashlib.sha256(raw).hexdigest() != marker['sha256']:
            raise ValueError('local completion marker identity mismatch')
        manifest = json.loads(raw)
    if not isinstance(manifest, dict):
        raise ValueError('identity-verified completion manifest required for compatibility decision')
    return entry, manifest


def freeze(selection, publications, output, *, selection_sha256, accepted_calculations,
           label='Frozen report release inventory'):
    """Reconcile intended membership against locally captured publications.

    A selected member is accepted only after publication identity, manifest,
    schema, calculation, row-count, and validation-policy compatibility pass.
    """
    accepted, accepted_ids = _accepted(accepted_calculations)
    selection_id, intended, explicit_exclusions = _selection(selection, selection_sha256)
    candidates = {}
    malformed_by_key = {}
    malformed = []
    for number, (offset, record) in enumerate(_offset_records(publications), 1):
        try:
            entry, manifest = _publication(record)
            key = _key(entry)
            candidates.setdefault(key, []).append(offset)
        except (ValueError, KeyError, TypeError, OSError, json.JSONDecodeError) as exc:
            problem = dict(record=number, reason=str(exc)[:2048])
            malformed.append(problem)
            try:
                key = _key(record.get('member', {}))
                malformed_by_key.setdefault(key, []).append(problem)
            except (ValueError, TypeError):
                pass

    accepted_entries = []
    ledger = [dict(session_date=row['session_date'], symbol=row['symbol'], status='excluded',
                   reason=row['reason']) for row in explicit_exclusions]
    for key, selected in sorted(intended.items()):
        found = candidates.pop(key, [])
        if not found:
            problems = malformed_by_key.get(key, [])
            ledger.append(dict(session_date=key[0], symbol=key[1],
                status='incompatible' if problems else 'missing',
                reason=('malformed completed publication record(s): ' + '; '.join(p['reason'] for p in problems)
                        if problems else 'no completed publication captured')))
            continue
        if len(found) != 1 or malformed_by_key.get(key):
            ledger.append(dict(session_date=key[0], symbol=key[1], status='incompatible',
                               reason='ambiguous competing completed partitions', candidates=len(found)))
            continue
        with Path(publications).open() as stream:
            stream.seek(found[0])
            entry, manifest = _publication(json.loads(stream.readline(MAX_RECORD+1)))
        try:
            compatibility = manifest_compatibility(manifest, entry, accepted)
            _selection_compatibility(selected, manifest['metadata'])
        except (ValueError, KeyError, TypeError) as exc:
            ledger.append(dict(session_date=key[0], symbol=key[1], status='incompatible', reason=str(exc),
                               partition_identity=entry.get('partition_identity')))
            continue
        enriched = dict(entry, calculation_identity=compatibility['calculation_identity'],
                        validation=compatibility['validation'])
        accepted_entries.append(enriched)
        ledger.append(dict(session_date=key[0], symbol=key[1], status='completed', reason=None,
                           partition_identity=entry['partition_identity'],
                           calculation_identity=compatibility['calculation_identity'],
                           validation=compatibility['validation']))
    for key, found in sorted(candidates.items()):
        ledger.append(dict(session_date=key[0], symbol=key[1], status='excluded',
                           reason='completed publication is outside intended selection', candidates=len(found)))

    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    accepted_path = output / 'accepted.jsonl'
    ledger_path = output / 'reconciliation.jsonl'
    exclusions_path = output / 'capture_exclusions.jsonl'
    for path, rows in ((accepted_path, accepted_entries), (ledger_path, ledger),
                       (exclusions_path, malformed)):
        with path.open('x') as stream:
            for row in rows:
                stream.write(json.dumps(row, sort_keys=True) + '\n')
    counts = Counter(row['status'] for row in ledger)
    unresolved = counts['missing'] + counts['incompatible']
    dates = sorted({entry['session_date'] for entry in accepted_entries})
    symbols = sorted({entry['symbol'] for entry in accepted_entries})
    calculations = sorted({entry['calculation_identity'] for entry in accepted_entries})
    validation_counts = Counter(entry['validation']['mode'] for entry in accepted_entries)
    manifest = dict(schema=SCHEMA, state='frozen',
        release_status='complete' if unresolved == 0 else 'incomplete', label=label,
        selection=dict(path=str(Path(selection).resolve()), sha256=selection_id,
                       selected=len(intended), explicitly_excluded=len(explicit_exclusions)),
        publication_capture=dict(path=str(Path(publications).resolve()), sha256=_digest_file(publications),
            malformed_records=len(malformed), capture_exclusions_sha256=_digest_file(exclusions_path)),
        compatibility_policy=dict(layout_version=S.LAYOUT_VERSION, feature_contract=S.FEATURE_CONTRACT,
            builder_version=P.BUILDER_VERSION, feature_schema_hash=S.schema_hash(S.FEATURE_SCHEMA),
            support_schema_hash=S.schema_hash(S.SUPPORT_SCHEMA),
            accepted_calculation_identities=accepted_ids,
            validation_policies=['legacy_exhaustive_v1', P.VALIDATION_POLICY_VERSION]),
        reconciliation=dict(counts=dict(counts), unresolved=unresolved,
                            intended_accounted=len(intended) + len(explicit_exclusions)),
        accepted_members=len(accepted_entries), rows=sum(e['expected_rows'] for e in accepted_entries),
        symbol_days=len(accepted_entries), dates=dates, date_min=dates[0] if dates else None,
        date_max=dates[-1] if dates else None, symbols=symbols,
        calculation_identities=calculations, validation_counts=dict(validation_counts),
        accepted_sha256=_digest_file(accepted_path), reconciliation_sha256=_digest_file(ledger_path))
    manifest['release_identity'] = P.digest({key: manifest[key] for key in manifest if key != 'release_identity'})
    P.atomic_json(output / 'manifest.json', manifest)
    return manifest


def read(directory, *, accepted_calculations=None, require_complete=True):
    """Read a frozen release and optionally re-assert an explicit calculation policy."""
    directory = Path(directory)
    raw = (directory / 'manifest.json').read_bytes()
    if len(raw) > MAX_RECORD:
        raise ValueError('release manifest exceeds 256 KiB')
    manifest = json.loads(raw)
    if manifest.get('schema') != SCHEMA or manifest.get('state') != 'frozen':
        raise ValueError('unsupported report release inventory')
    expected_identity = manifest.get('release_identity')
    body = {key: manifest[key] for key in manifest if key != 'release_identity'}
    if P.digest(body) != expected_identity:
        raise ValueError('release identity mismatch')
    if _digest_file(directory / 'accepted.jsonl') != manifest.get('accepted_sha256') \
            or _digest_file(directory / 'reconciliation.jsonl') != manifest.get('reconciliation_sha256'):
        raise ValueError('report release member identity mismatch')
    capture = manifest.get('publication_capture', {})
    if _digest_file(directory / 'capture_exclusions.jsonl') != capture.get('capture_exclusions_sha256'):
        raise ValueError('report release exclusion identity mismatch')
    if require_complete and manifest.get('release_status') != 'complete':
        raise ValueError('report release reconciliation is incomplete')
    if accepted_calculations is not None:
        _, ids = _accepted(accepted_calculations)
        if ids != manifest['compatibility_policy']['accepted_calculation_identities']:
            raise ValueError('report calculation compatibility policy changed')
    entries = list(_records(directory / 'accepted.jsonl'))
    keys = [_key(entry) for entry in entries]
    if len(keys) != len(set(keys)) or len(entries) != manifest['accepted_members'] \
            or sum(entry['expected_rows'] for entry in entries) != manifest['rows']:
        raise ValueError('report release coverage mismatch')
    for entry in entries:
        if entry.get('calculation_identity') not in manifest['compatibility_policy']['accepted_calculation_identities']:
            raise ValueError('accepted member calculation identity mismatch')
    return manifest, entries


def iter_accepted_members(directory, *, accepted_calculations=None, require_complete=True):
    """Documented aggregation interface: sorted, verified report members."""
    _, entries = read(directory, accepted_calculations=accepted_calculations,
                      require_complete=require_complete)
    yield from entries


def projected_feature_batches(directory, client, features, scratch, *, accepted_calculations,
                              batch_size=4096, require_complete=True, stats=None):
    """Yield ``(member, batch_result)`` using the verified compact reader."""
    import compact_preview_reader as READER
    accepted = ([accepted_calculations] if isinstance(accepted_calculations, dict)
                else list(accepted_calculations))
    _, entries = read(directory, accepted_calculations=accepted,
                      require_complete=require_complete)
    stats = stats if stats is not None else READER.stats_new()
    for entry in entries:
        for batch in READER.batches(client, entry, accepted, features, scratch, stats,
                                    batch_size=batch_size):
            yield entry, batch


def capture_run(run, output, *, accepted_calculations):
    """Snapshot local completed receipts and exact immutable manifest bytes.

    Never scans R2 or attempts to repair/restart production. Only completed
    receipts are read; failed/pending membership remains unresolved at freeze.
    O(receipts) time, one receipt/manifest in RAM plus sorted path names.
    """
    run, output = Path(run), Path(output)
    accepted, _ = _accepted(accepted_calculations)
    count = 0
    with output.open('x') as handle:
        for path in sorted((run/'receipts').glob('*.json')):
            raw = path.read_bytes()
            if len(raw) > MAX_RECORD: raise ValueError('receipt exceeds control bound')
            receipt = json.loads(raw)
            if receipt.get('state') != 'complete': continue
            payload = PREVIEW.read_json(run/'payloads'/path.name)
            manifest_path = Path(receipt['manifest_directory'])/'manifest.json'
            record = dict(receipt=receipt, member=payload['member'], receipt_path=str(path.resolve()),
                receipt_sha256=hashlib.sha256(raw).hexdigest(), manifest_path=str(manifest_path))
            entry, manifest = _publication(record)
            manifest_compatibility(manifest, entry, accepted)
            _selection_compatibility(payload['member'], manifest['metadata'])
            record.pop('manifest_path')
            record['manifest_json'] = manifest_path.read_text()
            # Revalidate the exact bytes copied after the initial read.
            _publication(record)
            line = json.dumps(record, sort_keys=True)+'\n'
            if len(line) > MAX_RECORD: raise ValueError('publication capture record exceeds control bound')
            handle.write(line); count += 1
    return dict(captured=count, sha256=_digest_file(output))


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('capture','freeze'))
    parser.add_argument('--run', type=Path)
    parser.add_argument('--selection', type=Path)
    parser.add_argument('--selection-sha256')
    parser.add_argument('--publications', type=Path)
    parser.add_argument('--calculations', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    accepted = json.loads(args.calculations.read_text())
    if args.action == 'capture':
        if args.run is None: parser.error('capture requires --run')
        result = capture_run(args.run, args.output, accepted_calculations=accepted)
    else:
        if not all((args.selection,args.selection_sha256,args.publications)):
            parser.error('freeze requires --selection, --selection-sha256, --publications')
        result = freeze(args.selection,args.publications,args.output,
                        selection_sha256=args.selection_sha256,accepted_calculations=accepted)
    print(json.dumps(result, indent=2))


if __name__ == '__main__': main()
