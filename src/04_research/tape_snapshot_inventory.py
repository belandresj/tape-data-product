"""Freeze canonical R2 pairs independently of an ongoing acquisition job.

Metadata only: O(N) small object records, with at most eight HEADs in flight.
The initial listing is persisted before HEAD verification and never expanded on
resume. Source content hashes, coverage and lengths are then frozen together.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/01_data'))
import r2_tq_storage as STORAGE


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.partial')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n')
    temporary.replace(path)


def validate_metadata(day, stream, metadata):
    expected = '0355-2000' if stream == 'quotes' else '0400-2000'
    if metadata.get('session-window-et') != expected or metadata.get('pagination-complete', 'true') != 'true':
        raise ValueError('incomplete canonical coverage metadata')
    if not metadata.get('source-provider') or not metadata.get('acquisition-method'):
        raise ValueError('missing acquisition provenance')
    from datetime import time, date
    from zoneinfo import ZoneInfo
    a = time(3, 55) if stream == 'quotes' else time(4)
    start = int(datetime.combine(date.fromisoformat(day), a, ZoneInfo('America/New_York')).timestamp()) * 10**9
    end = int(datetime.combine(date.fromisoformat(day), time(20), ZoneInfo('America/New_York')).timestamp()) * 10**9
    if ('requested-start-ns' in metadata and int(metadata['requested-start-ns']) != start) or ('requested-end-ns' in metadata and int(metadata['requested-end-ns']) != end):
        raise ValueError('requested timestamps differ from canonical session')


def freeze(output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    final = output / 'snapshot_v2.json'
    if final.exists():
        return load(final)
    settings = STORAGE.load_r2_settings()
    client = STORAGE.build_client(settings)
    listing = output / 'listing.json'
    if listing.exists():
        record = json.loads(listing.read_text())
    else:
        pairs = defaultdict(dict)
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=settings.bucket, Prefix='tq/'):
            for obj in page.get('Contents', []):
                parts = obj['Key'].split('/')
                if len(parts) != 4 or parts[-1] not in ('trades.parquet', 'quotes.parquet'):
                    continue
                day, symbol = STORAGE._validate_date_symbol(parts[1].split('=', 1)[1], parts[2].split('=', 1)[1])
                pairs[(day, symbol)][parts[-1].split('.')[0]] = dict(key=obj['Key'], bytes=obj['Size'], etag=obj['ETag'])
        record = dict(listed_at_utc=datetime.now(timezone.utc).isoformat(), bucket=settings.bucket,
                      complete_pairs=[dict(session_date=d, symbol=s, objects=v) for (d, s), v in sorted(pairs.items()) if len(v) == 2],
                      incomplete_pairs=[dict(session_date=d, symbol=s, objects=v) for (d, s), v in sorted(pairs.items()) if len(v) != 2])
        save(listing, record)

    def verify(source):
        result = dict(session_date=source['session_date'], symbol=source['symbol'])
        try:
            for stream, item in source['objects'].items():
                head = client.head_object(Bucket=settings.bucket, Key=item['key'])
                if head['ETag'] != item['etag'] or head['ContentLength'] != item['bytes']:
                    raise ValueError('object changed since frozen listing')
                identity = STORAGE.object_identity_from_head(item['key'], head)
                if identity.rows is None:
                    raise ValueError('missing row count')
                validate_metadata(source['session_date'], stream, head.get('Metadata', {}))
                result[stream] = dict(**asdict(identity), metadata=head['Metadata'], etag=head['ETag'],
                                     coverage_evidence='requested_bounds_and_complete_pagination' if 'pagination-complete' in head['Metadata'] else 'legacy_canonical_session_window_declaration')
            result['status'] = 'accepted_canonical_metadata'
        except Exception as exc:
            result.update(status='excluded', reason=f'{type(exc).__name__}: {exc}')
        return result

    # Submit only eight items at a time; do not retain a corpus-sized future list.
    sources = []
    prior_path = output / 'snapshot.json'
    prior = load(prior_path) if prior_path.exists() else {'sources': []}
    reusable = {(s['session_date'], s['symbol']): s for s in prior['sources'] if s['status'] == 'accepted_canonical_metadata'}
    def verify_or_reuse(source):
        return reusable.get((source['session_date'], source['symbol'])) or verify(source)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for offset in range(0, len(record['complete_pairs']), 8):
            sources.extend(pool.map(verify_or_reuse, record['complete_pairs'][offset:offset+8]))
            if offset % 200 == 0:
                print(json.dumps({'verified_source_pairs': len(sources)}), flush=True)
    payload = dict(version='tape_source_snapshot_v2', bucket=record['bucket'], listed_at_utc=record['listed_at_utc'],
                   listing_sha256=STORAGE.sha256_file(listing), sources=sources,
                   incomplete_pairs=record['incomplete_pairs'], selection='initial complete-pair listing; accepted canonical acquisition metadata; no feature selection',
                   limitation='Object metadata is acquisition coverage evidence; event-level QA is still enforced during feature replay.')
    payload['snapshot_hash'] = digest(payload)
    save(final, payload)
    return payload


def load(path):
    value = json.loads(Path(path).read_text())
    expected = value.pop('snapshot_hash')
    if digest(value) != expected:
        raise ValueError('snapshot hash mismatch')
    value['snapshot_hash'] = expected
    return value


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = freeze(args.output)
    print(json.dumps({'snapshot_hash': result['snapshot_hash'], 'sources': len(result['sources']),
                      'accepted': sum(x['status'] == 'accepted_canonical_metadata' for x in result['sources'])}))
