"""Immutable feature partitions independent of experiment membership and settings.

Stable semantic identity is separate from implementation/build provenance. A math
change requires a new semantic contract; same-version engines require exact
reference equivalence. RAM O(25k raw rows + 1024 features + 300 rolling states).
"""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import tempfile
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import snapshot_feature_pipeline as F
import tape_snapshot_inventory as I

S = I.STORAGE
SEMANTIC_PATH = F.ROOT/'config/tape_snapshot_feature_semantics_v1.json'


def semantic_identity():
    contract=json.loads(SEMANTIC_PATH.read_text())
    return dict(version=contract['semantic_version'],contract_hash=I.digest(contract),
                schema=str(F.schema().remove_metadata()))


def source_identity(source):
    return dict(session_date=source['session_date'],symbol=source['symbol'],
        **{k:{name:source[k][name] for name in ('object_key','sha256','size_bytes','rows')}
           for k in ('quotes','trades')})


def partition_identity(entry, halt_policy, seconds):
    if not 1<=seconds<=57600:raise ValueError('invalid coverage')
    return dict(feature=semantic_identity(),source=source_identity(entry['source']),
                halts=sorted([list(h) for h in entry['halts']]),halt_policy=halt_policy,
                seconds=seconds,quote_initialization_seconds=300)


def feature_prefix(identity):
    source=identity['source']
    return (f"derived/tape_features/{I.digest(identity)}/session_date={source['session_date']}"
            f"/symbol={source['symbol']}")


def read_manifest(client,bucket,key):
    head=S._head_or_none(client,bucket,key)
    if head is None:return None
    ident=S.object_identity_from_head(key,head)
    if ident.size_bytes>2*1024**2:raise ValueError('manifest exceeds metadata bound')
    body=client.get_object(Bucket=bucket,Key=key)['Body']
    try:payload=body.read(2*1024**2+1)
    finally:body.close()
    if len(payload)!=ident.size_bytes or hashlib.sha256(payload).hexdigest()!=ident.sha256:
        raise ValueError('manifest identity mismatch')
    return json.loads(payload)


def verify_object(client,bucket,identity):
    expected=S.ObjectIdentity(**{k:identity[k] for k in ('object_key','sha256','size_bytes','rows')})
    S.validate_identities(expected,S.remote_identity(client,bucket,expected.object_key),require_rows=True)


def recover_features(client,bucket,identity):
    key=feature_prefix(identity)
    meta=read_manifest(client,bucket,key+'/manifest.json')
    if meta is None:return None
    if meta['partition_identity']!=identity:raise ValueError('feature semantic identity mismatch')
    if meta['counts']['rows']!=identity['seconds'] or meta['verification']['rows_verified']!=identity['seconds']:
        raise ValueError('incomplete feature partition')
    expected=dict(object_key=key+'/features.parquet',sha256=meta['sha256'],size_bytes=meta['bytes'],rows=identity['seconds'])
    verify_object(client,bucket,expected)
    return meta


def crop(source,target,end):
    pf=pq.ParquetFile(source)
    with pq.ParquetWriter(target,pf.schema_arrow,compression='zstd') as out:
        for batch in pf.iter_batches(batch_size=25000,use_threads=False):
            table=pa.Table.from_batches([batch]);keep=table.filter(pc.less(table['sip_timestamp'],end))
            if keep.num_rows:out.write_table(keep)
            if keep.num_rows<batch.num_rows:break


def materialize_features(client,bucket,entry,halt_policy,seconds,scratch,*,summary_contract=None,summarizer=None,include_distributions=False):
    identity=partition_identity(entry,halt_policy,seconds)
    existing=recover_features(client,bucket,identity)
    if existing is not None:
        result=dict(recovered=True,measurement=existing)
        if summary_contract is not None:
            result['summary']=materialize_summary(client,bucket,identity,summary_contract,summarizer,scratch)
        return result
    source=entry['source'];key=feature_prefix(identity)
    for stream in ('quotes','trades'):verify_object(client,bucket,source[stream])
    # One staged pair and (probe only) a bounded prefix. Keep 3 GiB disk headroom.
    import shutil
    if shutil.disk_usage(scratch).free<2*sum(source[k]['size_bytes'] for k in ('quotes','trades'))+3*1024**3:
        raise ValueError('insufficient scratch headroom')
    with S.staged_symbol_day(client,bucket,source['session_date'],source['symbol'],scratch,
                            download_workers=1,transfer_workers=1) as staged:
        staged_identities={o.object_key:o for o in staged.source_objects}
        for stream in ('quotes','trades'):
            expected=S.ObjectIdentity(**{k:source[stream][k] for k in ('object_key','sha256','size_bytes','rows')})
            S.validate_identities(expected,staged_identities[expected.object_key],require_rows=True)
        with tempfile.TemporaryDirectory(dir=scratch) as work:
            work=Path(work);quotes,trades=staged.quotes,staged.trades
            if seconds!=57600:
                end=F.V.session_bounds(source['session_date'])[0]+seconds*F.V.NS
                for stream in ('quotes','trades'):crop(getattr(staged,stream),work/(stream+'.parquet'),end)
                quotes,trades=work/'quotes.parquet',work/'trades.parquet'
            path=work/'features.parquet';tick=time.monotonic()
            meta=F.write(quotes,trades,source['session_date'],source['symbol'],path,
                dict(source=identity['source'],config_hash=I.digest(identity),halt_policy=halt_policy),
                halts=entry['halts'],seconds=seconds)
            meta['verification']=F.verify(path,source['session_date'],source['symbol'],expected_rows=seconds)
            if include_distributions:meta['distributions']=F.distributions(path)
            meta['feature_and_verify_seconds']=time.monotonic()-tick
            meta['partition_identity']=identity
            # Keep coverage/acquisition evidence as provenance, not experimental identity.
            meta['source_provenance']=source
            S.publish_parquet_immutable(client,bucket,path,key+'/features.parquet',upload_workers=1)
            I.save(work/'manifest.json',meta)
            S.publish_file_immutable(client,bucket,work/'manifest.json',key+'/manifest.json',upload_workers=1,content_type='application/json')
            if recover_features(client,bucket,identity) is None:raise ValueError('feature recovery failed')
            result=dict(recovered=False,measurement=meta)
            if summary_contract is not None:
                result['summary']=materialize_summary(client,bucket,identity,summary_contract,summarizer,scratch,local_feature=path)
    return result


def retrieval_prefix(feature_identity, summary_contract):
    return feature_prefix(feature_identity)+'/summaries/'+I.digest(summary_contract)


def recover_summary(client,bucket,identity,contract):
    key=retrieval_prefix(identity,contract)
    meta=read_manifest(client,bucket,key+'/manifest.json')
    if meta is None:return None
    if meta['feature_identity']!=identity or meta['summary_contract']!=contract:raise ValueError('summary identity mismatch')
    verify_object(client,bucket,meta['retrieval_identity'])
    return meta


def materialize_summary(client,bucket,identity,contract,summarizer,scratch,*,local_feature=None):
    # Caller cannot summarize an uncommitted feature object.
    feature=recover_features(client,bucket,identity)
    if feature is None:raise ValueError('features not committed')
    existing=recover_summary(client,bucket,identity,contract)
    if existing is not None:return existing
    key=retrieval_prefix(identity,contract)
    with tempfile.TemporaryDirectory(dir=scratch) as work:
        work=Path(work);source=work/'features.parquet';target=work/'retrieval.parquet'
        if local_feature is None:
            S.download_verified_parquet(client,bucket,feature_prefix(identity)+'/features.parquet',source,transfer_workers=1)
        else:
            source=Path(local_feature)
            if S.sha256_file(source)!=feature['sha256']:raise ValueError('local feature identity mismatch')
        tick=time.monotonic();rows=summarizer(source,target)
        elapsed=time.monotonic()-tick
        artifact=S.publish_parquet_immutable(client,bucket,target,key+'/retrieval.parquet',upload_workers=1)
        meta=dict(feature_identity=identity,summary_contract=contract,retrieval_rows=rows,
                  summary_seconds=elapsed,implementation_sha256=S.sha256_file(Path(summarizer.__code__.co_filename)),
                  retrieval_identity={k:artifact[k] for k in ('object_key','size_bytes','sha256','rows')})
        I.save(work/'manifest.json',meta)
        S.publish_file_immutable(client,bucket,work/'manifest.json',key+'/manifest.json',upload_workers=1,content_type='application/json')
        if recover_summary(client,bucket,identity,contract) is None:raise ValueError('summary recovery failed')
    return meta
