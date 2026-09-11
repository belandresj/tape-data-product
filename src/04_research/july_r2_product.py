"""Immutable narrow tape product and bounded base/extension read interface.

O(Q + NF + NH) compute, O(N log H) reference verification; O(BF + HF)
resident state, B<=25000 (4096 default), H<=300, output buffer<=1024.
Only this versioned entry point opts future builders into the shared reducers.
The incumbent raw-T/Q builder and immutable base schema remain unchanged.
"""
from collections import Counter
from itertools import zip_longest
from pathlib import Path
import json
import shutil
import time
import pyarrow as pa
import pyarrow.parquet as pq
import all_feature_month_core as C
import all_feature_month_verify as V
import run_all_feature_month as OLD
import tape_feature_store as STORE
from all_feature_month_inventory import digest, sha, read
from all_feature_month_runtime import save, check_yield, ResourceWait, GiB

VERSION = 'tape_product_r2_extension_v1'
S = STORE.S


def code_identity():
    from july_r2_identity import code_identity as current
    return current()


def identity(payload, seconds, inventory_hash, probe_run=None):
    if not 1 <= seconds <= 57600: raise ValueError('invalid coverage')
    source = payload['entry']['source']
    if probe_run and seconds==57600:raise ValueError('probe namespace cannot contain a full partition')
    result=dict(version=VERSION, contract='tape_data_product_v1', code=code_identity(),
        inventory_hash=inventory_hash, input_hash=digest(payload), day=source['session_date'],
        symbol=source['symbol'], seconds=seconds, full_session=seconds==57600)
    if probe_run:result['probe_run']=probe_run
    return result


def prefix(ident):
    scope = 'partitions' if ident['full_session'] else 'partial_prefixes'
    return f"derived/tape_data_product/{VERSION}/{scope}/{digest(ident)}/session_date={ident['day']}/symbol={ident['symbol']}"


def object_id(value):
    return {k:value[k] for k in ('object_key','sha256','size_bytes','rows')}


def verify_remote(client, bucket, expected):
    S.validate_identities(S.ObjectIdentity(**object_id(expected)),
        S.remote_identity(client,bucket,expected['object_key']),require_rows=expected['rows'] is not None)


def stage(client, bucket, expected, path):
    actual = S.download_verified_parquet(client,bucket,expected['object_key'],path,transfer_workers=1)
    S.validate_identities(S.ObjectIdentity(**object_id(expected)),actual,require_rows=True)


def validate_catalog(catalog):
    body = {k:v for k,v in catalog.items() if k!='catalog_hash'}
    if digest(body)!=catalog.get('catalog_hash'): raise ValueError('catalog hash mismatch')
    ident=catalog['identity']
    if ident['version']!=VERSION or ident['contract']!='tape_data_product_v1': raise ValueError('unsupported contract')
    if not 1<=ident['seconds']<=57600 or ident['full_session']!=(ident['seconds']==57600): raise ValueError('invalid partial coverage')
    if catalog['conversions']!=C.field_metadata() or catalog['columns']!=C.final_columns(): raise ValueError('schema/conversions mismatch')
    if catalog['extension']['rows']!=ident['seconds'] or catalog['base']['rows']<ident['seconds']: raise ValueError('catalog row coverage mismatch')
    if catalog['extension']['object_key']!=prefix(ident)+'/extension.parquet': raise ValueError('extension namespace mismatch')


def local_rows(catalog, base, extension, *, columns=None, batch_size=4096, check=check_yield):
    """Strict canonical grid, even if caller requests only a single numerical field.

    Disk reads project the required base capabilities and narrow extension. Output
    projection is applied after common conversions/masks; no raw quotes required.
    Consume through exhaustion to establish the complete grid.
    """
    from july_r2_batches import local_batches
    for batch in local_batches(catalog,base,extension,columns=columns,batch_size=batch_size,check=check):
        yield from batch.to_pylist()


def read_batches(client, bucket, catalog, scratch, *, columns=None, batch_size=4096, allow_partial=False):
    """Public R2-backed reader. Stages only one bound pair, cleans its own cache."""
    import tempfile
    validate_catalog(catalog)
    if not catalog['identity']['full_session'] and not allow_partial: raise ValueError('explicit partial-prefix opt-in required')
    if not 1<=batch_size<=25000: raise ValueError('batch size must be 1..25000')
    scratch=Path(scratch);scratch.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(scratch).free<3*GiB+sum(catalog[k]['size_bytes'] for k in ('base','extension')): raise ResourceWait('reader disk reserve')
    with tempfile.TemporaryDirectory(prefix='reader-',dir=scratch) as directory:
        work=Path(directory)
        for name in ('base','extension'): stage(client,bucket,catalog[name],work/(name+'.parquet'))
        from july_r2_batches import local_batches
        yield from local_batches(catalog,work/'base.parquet',work/'extension.parquet',columns=columns,batch_size=min(batch_size,1024),check=check_yield)


def verify_rows(iterator, seconds):
    reference=V.Reference();summary=V.Summary();n=0
    for row in iterator:
        reference.push(row);summary.push(row);n+=1
    if n!=seconds: raise ValueError('verification coverage mismatch')
    return dict(rows_verified=n,reconstructed_fields=list(C.FEATURES),coverage=summary.result())


def builder_extension_v1(base_rows, decoded_quote_events, start, *, check=check_yield):
    """Future builder adapter: feed the existing ordered quote decode and base pass.

    Accepts the exact V.events quote tuples; no decoding or alternate mathematics.
    Consumers must supply an independently advancing quote iterator (not tee(),
    which would retain unbounded raw events). This opt-in API is not installed in
    or invoked by the live incumbent builder.
    """
    movement=C.Movement();age=C.MidpointAge(decoded_quote_events,start,check)
    for base in base_rows:
        yield {k:base[k] for k in C.KEYS}|movement.push(base)|age.push(base)


def recover(client,bucket,ident):
    root=prefix(ident);manifest=STORE.read_manifest(client,bucket,root+'/manifest.json')
    if manifest is None: return None
    if manifest['identity']!=ident or manifest['state']!='complete': raise ValueError('completion identity mismatch')
    if manifest['verification']['rows_verified']!=ident['seconds'] or manifest['verification']['reconstructed_fields']!=list(C.FEATURES): raise ValueError('incomplete verification')
    for obj in manifest['artifacts'].values(): verify_remote(client,bucket,obj)
    catalog=STORE.read_manifest(client,bucket,root+'/catalog.json')
    validate_catalog(catalog)
    if catalog['identity']!=ident or catalog['catalog_hash']!=manifest['catalog_hash']: raise ValueError('completion catalog mismatch')
    if catalog['extension']!=manifest['artifacts']['extension']: raise ValueError('completion extension mismatch')
    verify_remote(client,bucket,catalog['base'])
    return manifest


def build_publish(payload, work, receipt_path, *, client, bucket, seconds, inventory_hash, progress=lambda **kw:None, requests=None, probe_run=None):
    """One attempt; manifest last, durable receipt before private staging release.

    An interrupted attempt requires diagnosis and explicit reviewed recovery.
    Completed remote identities are checked before skip, even after local cleanup.
    """
    started=time.monotonic();ident=identity(payload,seconds,inventory_hash,probe_run);root=prefix(ident)
    prior=recover(client,bucket,ident)
    if prior is not None:
        for name in ('catalog','coverage'):
            path=Path(receipt_path).parent/(name+'.json')
            save(path,STORE.read_manifest(client,bucket,root+'/'+name+'.json'))
            obj=prior['artifacts'][name]
            if sha(path)!=obj['sha256'] or path.stat().st_size!=obj['size_bytes']:raise ValueError('recovered control artifact identity mismatch')
        save(receipt_path,dict(state='recovered_verified',manifest=prior,manifest_key=root+'/manifest.json'))
        return read(receipt_path)
    work=Path(work)
    if work.exists() and any(work.iterdir()): raise ValueError('incomplete private attempt requires reviewed recovery')
    # Uncommitted remote objects cannot silently trigger a rebuild.
    for name in ('extension.parquet','catalog.json','coverage.json','validation.json'):
        if S._head_or_none(client,bucket,root+'/'+name) is not None: raise ValueError('uncommitted remote publication requires reviewed recovery: '+root)
    work.mkdir(parents=True,exist_ok=True)
    save(work/'attempt.json',dict(identity=ident,state='staging'))
    source=payload['entry']['source'];base_id=dict(object_key=payload['base_key'],sha256=payload['base']['sha256'],size_bytes=payload['base']['bytes'],rows=payload['base']['counts']['rows'])
    needed=3*(source['quotes']['size_bytes']+base_id['size_bytes'])
    if shutil.disk_usage(work).free<3*GiB+needed: raise ResourceWait('partition disk reserve')
    phase_times={};tick=time.monotonic()
    progress(phase='staging',rows=0,events=0,bytes=0)
    verify_remote(client,bucket,source['trades'])
    for name,obj in (('base',base_id),('quotes',source['quotes'])):
        stage(client,bucket,obj,work/(name+'.parquet'))
        progress(phase='staging',bytes=obj['size_bytes'])
    phase_times['staging']=time.monotonic()-tick
    pf=pq.ParquetFile(work/'base.parquet');C_schema=json.loads(pf.schema_arrow.metadata[b'tape_snapshot'])
    from all_feature_month_schema import capabilities
    capabilities(pf.schema_arrow)
    if C_schema['expected_rows']!=base_id['rows']: raise ValueError('base embedded coverage mismatch')
    if 'partition_identity' in payload['base']:
        binding=payload['base']['partition_identity'];prov=C_schema['provenance']
        if prov['source']!=binding['source'] or prov['config_hash']!=digest(binding) or prov['halt_policy']!=binding['halt_policy'] or C_schema['contract']!=payload['base']['contract'] or C_schema['contract']!=STORE.F.identity(): raise ValueError('base embedded provenance mismatch')
    stats=Counter();tick=time.monotonic();last=[0.]
    def check():
        check_yield()
        if time.monotonic()-last[0]>2:
            progress(phase='compute',events=stats.get('quote_rows_consumed',0));last[0]=time.monotonic()
    ext=C.write_rows(work/'extension.parquet',C.extension_rows(work/'base.parquet',work/'quotes.parquet',ident['day'],ident['symbol'],check=check,full=seconds==57600,stats=stats,limit=seconds,halts=payload['entry']['halts']),ident,check=check,columns=C.extension_columns())
    phase_times['compute']=time.monotonic()-tick
    catalog=dict(identity=ident,base=base_id,extension=dict(object_key=root+'/extension.parquet',sha256=ext['sha256'],size_bytes=ext['bytes'],rows=seconds),discovery=payload['discovery'],conversions=C.field_metadata(),columns=C.final_columns(),coverage_claim='acquired universe; historical halts and nominal discovery do not establish live availability')
    catalog['catalog_hash']=digest(catalog);save(work/'catalog.json',catalog)
    progress(phase='stored_verification',rows=seconds,events=stats['quote_events_reduced'])
    from july_r2_batches import verify,local_batches
    tick=time.monotonic();verified=verify(local_batches(catalog,work/'base.parquet',work/'extension.parquet',check=check_yield),seconds)
    phase_times['stored_verification']=time.monotonic()-tick
    save(work/'coverage.json',verified.pop('coverage'));save(work/'validation.json',verified)
    tick=time.monotonic();artifacts={}
    for name,file in (('extension','extension.parquet'),('coverage','coverage.json'),('validation','validation.json'),('catalog','catalog.json')):
        progress(phase='upload_'+name,bytes=(work/file).stat().st_size)
        publisher=S.publish_parquet_immutable if name=='extension' else S.publish_file_immutable
        artifacts[name]=object_id(publisher(client,bucket,work/file,root+'/'+file,upload_workers=1))
    phase_times['upload_remote_verification']=time.monotonic()-tick
    manifest=dict(identity=ident,state='complete',catalog_hash=catalog['catalog_hash'],artifacts=artifacts,verification=verified)
    save(work/'manifest.json',manifest)
    # Publication marker is the final remote mutation for this partition.
    marker=S.publish_file_immutable(client,bucket,work/'manifest.json',root+'/manifest.json',upload_workers=1)
    if STORE.read_manifest(client,bucket,root+'/manifest.json')!=manifest: raise ValueError('completion recovery failed')
    receipt=dict(state='published_verified',manifest=manifest,manifest_key=root+'/manifest.json',manifest_object=object_id(marker),phase_seconds=phase_times,quote_stats=dict(stats),rows=seconds,staged_bytes=source['quotes']['size_bytes']+base_id['size_bytes'],uploaded_bytes=sum(x['size_bytes'] for x in artifacts.values())+marker['size_bytes'],scratch_bytes=sum(p.stat().st_size for p in work.iterdir()),requests=dict(requests or {}),elapsed_seconds=time.monotonic()-started)
    for name in ('catalog.json','coverage.json'):
        shutil.copyfile(work/name,Path(receipt_path).parent/name)
    save(receipt_path,receipt)
    # Exact allowlist; never recursive source/cache deletion or failed cleanup.
    for name in ('base.parquet','quotes.parquet','extension.parquet','catalog.json','coverage.json','validation.json','manifest.json','attempt.json'):
        (work/name).unlink()
    work.rmdir()
    progress(phase='released',rows=seconds)
    return receipt


def raw_product_rows_v1(quotes,trades,day,symbol,discovery,*,seconds=57600,halts=(),batch_size=4096,continuity_breaks_ns=(),stats=None):
    """Versioned main-builder integration: decode each raw stream exactly once.

    The quote iterator taps consumed events after yield, so the primitive cursor's
    one-event lookahead never reaches this reducer early. No event queue/tee or
    per-second raw-event collection is allocated. Same reducers as July backfill.
    """
    if not 1<=batch_size<=25000:raise ValueError('invalid batch size')
    start=C.session_start(day);age=C.MidpointAge((),start);movement=C.Movement();model=C.V.FeatureStream()
    intervals=sorted(halts);index=epoch=0;previous_halt=False;breaks=set(continuity_breaks_ns)
    def reader(path,stream,date,batch,statistics):
        for item in C.V._primitive_events(path,stream,date,batch,statistics):
            yield item
            if stream!='quote':continue
            ts,event=item
            if ts<start:
                if ts>=start-300*C.NS and not age.interval_row['halt_interval_active']:
                    age.current=event;age.latest_quote=ts
                    age.mid=event['midpoint'] if event['price_state_valid'] else None
                    age.origin=start if age.mid is not None else None
            elif ts<start+seconds*C.NS:age.consume_event(ts,event)
    primitives=C.V.primitives(quotes,trades,day,symbol,halts=halts,seconds=seconds,batch_size=batch_size,
        continuity_breaks_ns=continuity_breaks_ns,stats=stats,event_reader=reader)
    for position in range(seconds):
        left=start+position*C.NS;right=left+C.NS
        while index<len(intervals) and intervals[index][1]<=left:index+=1
        active=bool(index<len(intervals) and intervals[index][0]<right and intervals[index][1]>left)
        if left in breaks and not active and not previous_halt:epoch+=1
        age.begin_interval(dict(interval_end_ns=right,continuity_segment_id=epoch,halt_interval_active=active,primitive_quote_source_file_accepted=True))
        primitive=next(primitives);base=STORE.F.enrich(model.push(primitive),primitive)
        extension={k:base[k] for k in C.KEYS}|movement.push(base)|age.end_interval(base)
        yield C.assemble(base,extension,discovery)
        previous_halt=active
    # Exhaust the upstream full-file validators; the tap ignores session tails.
    for _ in primitives:raise ValueError('unexpected extra primitive')


def dataset_batches(client,bucket,manifest_key,scratch,*,columns=None,batch_size=4096):
    """Complete dataset interface: verified catalog, one R2 pair at a time.

    Catalog bytes are streamed to private disk and verified before any data is
    exposed. JSONL lines are bounded; membership and row totals reconcile at EOF.
    """
    import tempfile
    manifest=STORE.read_manifest(client,bucket,manifest_key)
    if manifest is None or manifest['state']!='complete':raise ValueError('dataset not committed')
    if (manifest['partitions'],manifest['rows'])!=(1022,58867200):raise ValueError('incomplete July dataset')
    expected=manifest['objects']['catalog.jsonl'];verify_remote(client,bucket,expected)
    scratch=Path(scratch);scratch.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(scratch).free<3*GiB+expected['size_bytes']:raise ResourceWait('catalog disk reserve')
    with tempfile.TemporaryDirectory(prefix='catalog-',dir=scratch) as directory:
        path=Path(directory)/'catalog.jsonl'
        client.download_file(bucket,expected['object_key'],str(path),Config=S.transfer_config(1))
        if path.stat().st_size!=expected['size_bytes'] or sha(path)!=expected['sha256']:raise ValueError('dataset catalog identity mismatch')
        count=rows=0;previous=None
        with path.open('rb') as f:
            while line:=f.readline(2*1024**2+1):
                if len(line)>2*1024**2:raise ValueError('catalog record exceeds bound')
                catalog=json.loads(line);validate_catalog(catalog);ident=catalog['identity']
                key=(ident['day'],ident['symbol'])
                if previous is not None and key<=previous:raise ValueError('duplicate or unordered catalog member')
                accepted_codes=manifest.get('accepted_partition_code_identities',[manifest['code_identity']])
                if ident['inventory_hash']!=manifest['inventory_hash'] or ident['code'] not in accepted_codes:raise ValueError('catalog dataset binding mismatch')
                completion=recover(client,bucket,ident)
                if completion is None or completion['catalog_hash']!=catalog['catalog_hash']:raise ValueError('catalog member not committed')
                yield from read_batches(client,bucket,catalog,scratch,columns=columns,batch_size=batch_size)
                count+=1;rows+=ident['seconds'];previous=key
        if (count,rows)!=(manifest['partitions'],manifest['rows']):raise ValueError('dataset catalog reconciliation mismatch')


def recover_attempt(payload,work,receipt_path,*,client,bucket,seconds,inventory_hash,review_reason):
    """Explicit reviewed recovery of an already materialized narrow extension.

    Does not rebuild or replay events. Missing/corrupt materialization stops here.
    Immutable helpers may finish the previously interrupted object publication.
    """
    if not review_reason or not review_reason.strip():raise ValueError('recorded diagnosis required')
    work=Path(work);ident=identity(payload,seconds,inventory_hash);root=prefix(ident)
    attempt=read(work/'attempt.json');catalog=read(work/'catalog.json')
    if attempt['identity']!=ident or catalog['identity']!=ident:raise ValueError('recovery code/input identity changed')
    validate_catalog(catalog)
    # Verify surviving source/extension bytes and all values/masks again.
    verified=verify_rows(local_rows(catalog,work/'base.parquet',work/'extension.parquet'),seconds)
    save(work/'coverage.json',verified.pop('coverage'));save(work/'validation.json',verified)
    save(work/'recovery_review.json',dict(identity=ident,reason=review_reason))
    artifacts={}
    for name,file in (('extension','extension.parquet'),('coverage','coverage.json'),('validation','validation.json'),('catalog','catalog.json')):
        publisher=S.publish_parquet_immutable if name=='extension' else S.publish_file_immutable
        artifacts[name]=object_id(publisher(client,bucket,work/file,root+'/'+file,upload_workers=1))
        verify_remote(client,bucket,artifacts[name])
    manifest=dict(identity=ident,state='complete',catalog_hash=catalog['catalog_hash'],artifacts=artifacts,verification=verified)
    save(work/'manifest.json',manifest)
    marker=S.publish_file_immutable(client,bucket,work/'manifest.json',root+'/manifest.json',upload_workers=1)
    if recover(client,bucket,ident)!=manifest:raise ValueError('reviewed recovery did not commit')
    receipt=dict(state='reviewed_recovery_verified',review_reason=review_reason,manifest=manifest,manifest_key=root+'/manifest.json',manifest_object=object_id(marker))
    for name in ('catalog.json','coverage.json'):
        shutil.copyfile(work/name,Path(receipt_path).parent/name)
    save(receipt_path,receipt)
    for name in ('base.parquet','quotes.parquet','extension.parquet','catalog.json','coverage.json','validation.json','manifest.json','attempt.json','recovery_review.json'):
        (work/name).unlink()
    work.rmdir()
    return receipt
