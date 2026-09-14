"""Bounded Phase 1 transport proof; not a production source-admission manifest."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import time

import pyarrow.parquet as pq
from tape_data_product.storage import r2_tq_storage as storage

MIB = 1024**2
MAX_DOWNLOAD = 256 * MIB
MAX_DISK = 512 * MIB
RESERVE = 20 * 1024**3


def save(path, value):
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w') as f:
        json.dump(value, f, indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def identity(record):
    return storage.ObjectIdentity(record['key'], record['bytes'], record['sha256'], record['rows'])


def verify(path, record):
    storage.validate_identities(storage.local_parquet_identity(path, record['key']), identity(record))
    rows = 0
    previous = None
    source = pq.ParquetFile(path)
    if 'sip_timestamp' not in source.schema_arrow.names:
        raise ValueError('Missing SIP timestamp')
    # Decode all columns in bounded batches to detect body/encoding failures.
    for batch in source.iter_batches(batch_size=4096):
        clocks = batch.column(batch.schema.get_field_index('sip_timestamp')).to_pylist()
        for stamp in clocks:
            if stamp is None or (previous is not None and stamp < previous):
                raise ValueError('Invalid SIP ordering')
            previous = stamp
        rows += len(batch)
    if rows != record['rows']:
        raise ValueError('Decoded row count mismatch')
    return rows


def run(client, bucket, manifest, root, interrupt=False):
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'run.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        objects = manifest['objects']
        if len(objects) != 4 or len({r['key'] for r in objects}) != 4:
            raise ValueError('Expected four unique approved objects')
        for r in objects:
            if r['symbol'] not in ('KDP', 'NVDA') or r['stream'] not in ('trades', 'quotes'):
                raise ValueError('Unapproved member')
            if r['key'] != storage.tq_object_key('2026-09-02', r['symbol'], r['stream']):
                raise ValueError('Unapproved key')
        ledger_path = root / 'transfer-ledger.json'
        if ledger_path.exists():
            ledger = json.loads(ledger_path.read_text())
            if ledger['objects'] != objects:
                raise ValueError('Frozen source identities changed')
        else:
            ledger = dict(objects=objects, reserved_download_bytes=0, delivered_payload_bytes=0,
                          partial_high_water_bytes=0, local_high_water_bytes=0, attempts=[])
            save(ledger_path, ledger)
        def disk_check(extra=0):
            used = sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
            if used + extra > MAX_DISK or shutil.disk_usage(root).free - extra < RESERVE:
                raise RuntimeError('Disk budget or reserve exceeded')
            ledger['local_high_water_bytes'] = max(used, ledger['local_high_water_bytes'])
        disk_check()
        results = []
        started = time.monotonic()
        for r in objects:
            remote = storage.remote_identity(client, bucket, r['key'])
            if remote != identity(r):
                raise ValueError('Remote identity changed')
            directory = root / r['symbol']
            directory.mkdir(exist_ok=True)
            final = directory / (r['stream'] + '.parquet')
            partial = directory / (r['stream'] + '.partial')
            if final.exists():
                rows = verify(final, r)
                results.append(dict(symbol=r['symbol'], stream=r['stream'], reused=True, rows=rows))
                continue
            # Remove only this runner's uncommitted partial; never source files.
            if partial.exists():
                partial.unlink()
            disk_check(r['bytes'] + MIB)
            if ledger['reserved_download_bytes'] + r['bytes'] > MAX_DOWNLOAD:
                raise RuntimeError('Cumulative download reservation exhausted')
            # Reserve the complete body BEFORE GET. A killed attempt still consumes
            # its reservation, bounding retries across process restarts.
            ledger['reserved_download_bytes'] += r['bytes']
            attempt = dict(key=r['key'], expected_bytes=r['bytes'], delivered_bytes=0, status='started')
            ledger['attempts'].append(attempt)
            save(ledger_path, ledger)
            begin = time.monotonic()
            response = client.get_object(Bucket=bucket, Key=r['key'])
            body = response['Body']
            try:
                if response.get('ContentLength') != r['bytes']:
                    raise ValueError('Response byte length mismatch')
                with partial.open('xb') as sink:
                    while attempt['delivered_bytes'] < r['bytes']:
                        amount = min(MIB, r['bytes'] - attempt['delivered_bytes'])
                        chunk = body.read(amount)
                        if not chunk:
                            raise ValueError('Truncated response')
                        sink.write(chunk)
                        sink.flush()
                        attempt['delivered_bytes'] += len(chunk)
                        ledger['delivered_payload_bytes'] += len(chunk)
                        ledger['partial_high_water_bytes'] = max(ledger['partial_high_water_bytes'], attempt['delivered_bytes'])
                        disk_check()
                        save(ledger_path, ledger)
                        if interrupt and r['symbol'] == 'NVDA' and r['stream'] == 'quotes' and attempt['delivered_bytes'] >= 8*MIB:
                            sink.flush()
                            os.fsync(sink.fileno())
                            attempt['status'] = 'intentional_process_exit'
                            save(ledger_path, ledger)
                            # Actual worker termination leaves partial bytes on disk.
                            os._exit(75)
                    os.fsync(sink.fileno())
            finally:
                body.close()
            rows = verify(partial, r)
            if storage.remote_identity(client, bucket, r['key']) != identity(r):
                raise ValueError('Remote identity changed during download')
            os.replace(partial, final)
            attempt.update(status='verified', seconds=round(time.monotonic()-begin, 6))
            save(ledger_path, ledger)
            results.append(dict(symbol=r['symbol'], stream=r['stream'], reused=False, rows=rows))
        # A completion record exists only after all four verified objects.
        record = dict(status='complete', objects=objects, results=results,
                      elapsed_seconds=round(time.monotonic()-started,6),
                      retained_raw_bytes=sum(r['bytes'] for r in objects),
                      provenance='transport verified; historical coverage declarations are not new acquisition receipts')
        save(root/'complete.json',record)
        disk_check()
        save(ledger_path,ledger)
        print(json.dumps(record))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--interrupt', action='store_true')
    args = parser.parse_args()
    settings = storage.load_r2_settings(Path('/etc/tape-data-product/r2.env'))
    # Never retry GET requests internally; the durable reservation owns retries.
    from botocore.config import Config
    client = storage.boto3.client('s3', endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key_id, aws_secret_access_key=settings.secret_access_key,
        region_name=settings.region, config=Config(signature_version='s3v4', connect_timeout=10,
        read_timeout=30, retries={'total_max_attempts':1}))
    run(client, settings.bucket, json.loads(args.manifest.read_text()), args.root, args.interrupt)
