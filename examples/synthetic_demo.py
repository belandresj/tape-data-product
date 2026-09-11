"""Synthetic SIP events -> compact features -> the selected six-condition cohort.

720 seconds, 721 quotes and 8,640 trades; generated in <=512-row batches.
This fixture is invented, has no vendor data, and is not empirical evidence.
"""
import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src/04_research'))
sys.path.insert(0, str(ROOT/'src/03_features'))
import pyarrow as pa
import pyarrow.parquet as pq
import compact_product as P
import direct_frozen_product as D
from tape_cohort_config import normalize_config, query_hash
from tape_cohort_reader import iter_query_batches
from tape_cohort_state import CohortMachine
from verify_tape_cohort_query import worked_transition_oracle


def quote(ts, mid):
    return dict(sip_timestamp=ts, sequence_number=0, participant_timestamp=ts,
        bid_price=mid*.9999, ask_price=mid*1.0001, bid_size=100., ask_size=101.,
        bid_exchange=1, ask_exchange=1, conditions=[], indicators=[], tape=1, trf_timestamp=None)


def trade(ts, mid):
    return dict(sip_timestamp=ts, sequence_number=0, participant_timestamp=ts,
        price=mid, size=100., decimal_size=None, conditions=[], correction=0)


def write_events(path, rows, prototype):
    schema = pa.Table.from_pylist([prototype]).schema
    for name in ('conditions','indicators'):
        if name in schema.names:
            schema = schema.set(schema.get_field_index(name), pa.field(name, pa.list_(pa.int64())))
    if 'decimal_size' in schema.names:
        schema = schema.set(schema.get_field_index('decimal_size'), pa.field('decimal_size', pa.float64()))
    count = 0; buffer = []
    with pq.ParquetWriter(path, schema) as writer:
        for row in rows:
            buffer.append(row); count += 1
            if len(buffer) == 512:
                writer.write_table(pa.Table.from_pylist(buffer, schema=schema)); buffer.clear()
        if buffer: writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT/'output/synthetic-demo')
    args = parser.parse_args(); out = args.output
    out.mkdir(parents=True, exist_ok=False)
    pa.set_cpu_count(1); pa.set_io_thread_count(1)
    day = '2026-07-01'; start = P.C.session_start(day); seconds = 720; ns = 10**9
    mid = lambda i: 100*math.exp(.008*math.sin(i/8))
    quotes = out/'quotes.parquet'; trades = out/'trades.parquet'
    def quote_rows():
        yield quote(start-1,mid(0))
        for i in range(seconds): yield quote(start+i*ns,mid(i))
    nq = write_events(quotes, quote_rows(), quote(start,100.))
    nt = write_events(trades, (trade(start+i*ns+j*80_000_000,mid(i)) for i in range(seconds) for j in range(12)), trade(start,100.))
    discovery = dict(verified=True, endpoint_ns=start+ns, provenance_hash='synthetic-known-at-start')
    metadata = dict(session_date=day, symbol='SYNTH', expected_rows=seconds,
        inputs={name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in [('quotes',quotes),('trades',trades)]},
        calculation=P.calculation_identity(), overlay={}, discovery_verified=True)
    product = out/'compact'
    manifest = P.write_partition(product, D.product_pairs(quotes,trades,day,'SYNTH',discovery,
        seconds=seconds,batch_size=127), metadata, output_rows=127, validation_mode='reconstruction')
    P.verify_complete(product); audit = P.audit_complete(product)
    config = normalize_config(json.loads((ROOT/'config/tape_cohort_300s_ms3_p050_selected_v1.json').read_text()))
    sinks = {'windows':[], 'window_features':[], 'strict_runs':[]}
    machine = CohortMachine(config, manifest['partition_identity'],session_date=day,symbol='SYNTH',checkpoint=True)
    member = dict(session_date=day,symbol='SYNTH',expected_rows=seconds)
    for batch in iter_query_batches(product/'features.parquet',member,manifest['metadata'],config,batch_size=127):
        for _ in machine.consume(batch,sinks): pass
    counters = machine.finish('selection_boundary',sinks)
    assert counters['observed_available']+counters['observed_unavailable']==seconds
    assert counters['active_seconds']>0 and len(sinks['windows'])==1
    oracle = worked_transition_oracle()
    result = dict(kind='synthetic_only',seconds=seconds,quotes=nq,trades=nt,query_hash=query_hash(config),
        source_numerical_reconstruction=audit['state'],counters=counters,windows=sinks['windows'],
        worked_transition_oracle=oracle,notes='Invented events. Prefix closes at selection boundary. No network or market data.')
    (out/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))

if __name__ == '__main__': main()
