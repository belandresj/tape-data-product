"""Build the local all-feature EDA product with one independently supervised worker.

Commands: freeze, inventory, probe, run, _partition, _synthetic. Full acceptance
requires a successful same-code representative checkpoint plus explicit approval.
Neither code path uploads R2 objects or changes the existing feature queue.
"""
from __future__ import annotations
import argparse
from collections import Counter
import fcntl
import json
import math
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import time
import uuid
import all_feature_month_selection as SELECTION

from all_feature_month_inventory import connect,digest,freeze,read,sha,verify_selection,remote_entry
from all_feature_month_runtime import ResourceWait,save,supervise,check_yield,worker_setup,GiB,MiB,AdmissionHistory

ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve()


def code_identity():
    # Bind all reused semantic implementation inputs, including optimized base
    # equivalence identity, without editing any incumbent module.
    paths=[HERE,*HERE.parent.glob('all_feature_month_*.py'),
        ROOT/'src/03_features/economic_tape_state_v3.py',ROOT/'src/03_features/build_rolling_tape_state_v2.py',
        ROOT/'src/02_preprocessing/build_market_state.py',ROOT/'src/04_research/tape_feature_store.py',
        ROOT/'src/04_research/snapshot_feature_pipeline.py',ROOT/'src/04_research/tape_snapshot_inventory.py',
        ROOT/'src/01_data/r2_tq_storage.py',ROOT/'src/01_data/tq_cost_guard.py',
        ROOT/'config/tape_snapshot_feature_semantics_v1.json']
    return {str(p.relative_to(ROOT)):sha(p) for p in sorted(set(paths))}


INVENTORY_DEPENDENCIES = (
    'src/04_research/all_feature_month_inventory.py', 'src/03_features/economic_tape_state_v3.py',
    'src/03_features/build_rolling_tape_state_v2.py','src/02_preprocessing/build_market_state.py',
    'src/04_research/tape_feature_store.py','src/04_research/snapshot_feature_pipeline.py',
    'src/04_research/tape_snapshot_inventory.py','src/01_data/r2_tq_storage.py',
    'src/01_data/tq_cost_guard.py','config/tape_snapshot_feature_semantics_v1.json')


def inventory_identity():
    return {p:sha(ROOT/p) for p in INVENTORY_DEPENDENCIES}


def client_for(queue):
    sys.path.insert(0,str(ROOT/'src/01_data'))
    import r2_tq_storage as S
    from tq_cost_guard import CostGuard,load_policy
    plan=read(Path(queue)/'plan.json')
    policy=load_policy(ROOT/plan['cost_policy_path']) if 'cost_policy_path' in plan else plan['cost_policy']
    if plan.get('cost_policy')!=policy:raise ValueError('shared request policy identity changed')
    guard=CostGuard(Path(plan['cost_ledger']),policy)
    cfg=S.load_r2_settings();client=S.build_client(cfg)
    client.meta.events.register('before-send.s3.*',guard.before_send)
    requests=Counter()
    def count(request,**kwargs):
        check_yield();requests[request.method]+=1
    client.meta.events.register('before-send.s3.*',count)
    return client,cfg.bucket,requests


def inventory(selection,output):
    """Freeze ALL raw/base admissions before reading any feature values."""
    selected=verify_selection(selection);output=Path(output)
    if output.exists():raise FileExistsError(output)
    output.mkdir(parents=True)
    client,bucket,requests=client_for(selected['queue'])
    source=connect(Path(selection)/'selection.sqlite',True)
    db=connect(output/'inventory.sqlite')
    db.execute('CREATE TABLE members(day TEXT,symbol TEXT,status TEXT,reason TEXT,payload TEXT,PRIMARY KEY(day,symbol))')
    counts=Counter()
    try:
        for member in source.execute('SELECT * FROM members ORDER BY day,symbol'):
            check_yield()
            try:
                payload=remote_entry(member,client,bucket);status='admitted';reason=None
            except (ValueError,KeyError) as exc:
                payload=dict(discovery=json.loads(member['discovery']),frozen_entry=member['entry'],frozen_receipt=member['receipt'])
                status='excluded';reason=str(exc)
            db.execute('INSERT INTO members VALUES (?,?,?,?,?)',(member['day'],member['symbol'],status,reason,json.dumps(payload)))
            db.commit();counts[status]+=1
    finally:source.close();db.close()
    meta=dict(version='tape_eda_verified_inventory_v1',month=selected['month'],selection_hash=selected['selection_hash'],
        selection_root=str(Path(selection).resolve()),inventory_sha256=sha(output/'inventory.sqlite'),
        counts=dict(counts),requests=dict(requests),code_identity=code_identity(),inventory_code_identity=inventory_identity(),queue=selected['queue'],
        coverage_claim='verified available inputs only; exclusions and aggregate date coverage remain explicit')
    meta['inventory_hash']=digest(meta);save(output/'inventory_manifest.json',meta)
    return meta


def verified_inventory(root):
    root=Path(root);meta=read(root/'inventory_manifest.json');expected=meta.pop('inventory_hash')
    if digest(meta)!=expected or sha(root/'inventory.sqlite')!=meta['inventory_sha256']:
        raise ValueError('frozen inventory identity changed')
    selected=verify_selection(meta['selection_root'])
    if selected['selection_hash']!=meta['selection_hash']:raise ValueError('selection changed')
    # Immutable input evidence survives unrelated extension/runner changes.
    # Changes to the actual inventory validator or any reused base/storage
    # semantic implementation require renewed verification. The execution
    # checkpoint separately binds every extension/runner/resource-control file.
    current=inventory_identity()
    recorded=meta.get('inventory_code_identity',meta['code_identity'])
    if any(recorded.get(k)!=v for k,v in current.items()):
        raise ValueError('inventory verification implementation changed; renew inventory')
    meta['inventory_hash']=expected;return meta


def local_identity(path,expected):
    import pyarrow.parquet as pq
    if sha(path)!=expected['sha256'] or Path(path).stat().st_size!=expected['size_bytes'] or pq.ParquetFile(path).metadata.num_rows!=expected['rows']:
        raise ValueError('local staged identity mismatch')


def partition(payload,work,*,seconds=57600,batch_size=4096,client=None,bucket=None,local=None,execution_selection_hash=None,requests=None):
    """Same verified staging -> extension -> exact join -> verification in probes.

    Only the requested prefix is decoded for a probe. Full verified objects are
    staged so measured download/scratch costs are conservative for prefix runs.
    At most one source quote, base, extension and assembly working set exists.
    The raw trade identity is verified, but trade events are not replayed.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    import all_feature_month_core as C
    import tape_feature_store as STORE
    if not 1<=seconds<=57600 or not 1<=batch_size<=25000:raise ValueError('invalid bounded configuration')
    work=Path(work);work.mkdir(parents=True,exist_ok=True)
    if (work/'manifest.json').exists():
        prior=read(work/'manifest.json')
        if prior['input_hash']!=digest(payload) or prior['code_identity']!=code_identity() or prior['seconds']!=seconds or prior.get('execution_selection_hash')!=execution_selection_hash:
            raise ValueError('resume identity changed')
        C.verify(work/'features.parquet',payload['entry']['source']['session_date'],payload['entry']['source']['symbol'],seconds,check_yield)
        if sha(work/'features.parquet')!=prior['output']['sha256']:raise ValueError('completed output identity changed')
        return prior
    if any(work.iterdir()):raise ValueError('private incomplete partition requires reviewed restart in a new attempt directory')
    check_yield();started=time.monotonic()
    attempt_identity=dict(input_hash=digest(payload),code_identity=code_identity(),seconds=seconds,execution_selection_hash=execution_selection_hash,state='staging')
    save(work/'attempt.json',attempt_identity)
    source=payload['entry']['source'];day,symbol=source['session_date'],source['symbol']
    source_paths={}
    source_objects={
        'quotes':source['quotes'],
        'base':dict(object_key=payload['base_key'],sha256=payload['base']['sha256'],size_bytes=payload['base']['bytes'],rows=57600 if local is None else payload['base']['counts']['rows'])}
    needed=sum(x['size_bytes'] for x in source_objects.values())*4
    if shutil.disk_usage(work).free<3*GiB+needed:raise ResourceWait('insufficient staged partition disk headroom')
    for name,expected in source_objects.items():
        check_yield();path=work/(name+'.parquet')
        if local:
            local_identity(local[name],expected)
            shutil.copyfile(local[name],path)
        else:
            actual=STORE.S.download_verified_parquet(client,bucket,expected['object_key'],path,transfer_workers=1)
            STORE.S.validate_identities(STORE.S.ObjectIdentity(**{k:expected[k] for k in ('object_key','sha256','size_bytes','rows')}),actual,require_rows=True)
        source_paths[name]=path
    staging_seconds=time.monotonic()-started
    save(work/'attempt.json',attempt_identity|{'state':'staged'})
    base=source_paths['base'];pf=pq.ParquetFile(base)
    published=json.loads(pf.schema_arrow.metadata[b'tape_snapshot'])
    if published['expected_rows']!=source_objects['base']['rows']:
        raise ValueError('base declared coverage mismatch')
    if not local:
        identity=payload['base']['partition_identity']
        provenance=published['provenance']
        if provenance['source']!=identity['source'] or provenance['config_hash']!=digest(identity) or provenance['halt_policy']!=identity['halt_policy']:
            raise ValueError('embedded base provenance mismatch')
        # Quote validator code is identical to the frozen semantic reference;
        # optimized publication must retain the reference identity.
        expected=STORE.F.identity()
        if published['contract']!=payload['base']['contract'] or published['contract']!=expected:
            raise ValueError('embedded base implementation identity differs from verified current engine')
    from all_feature_month_schema import capabilities
    capability=capabilities(pf.schema_arrow)
    identity=dict(version=C.VERSION,age_version=C.AGE_VERSION,execution_selection_hash=execution_selection_hash,input_hash=digest(payload),code_identity=code_identity(),
        source=source,base_sha256=payload['base']['sha256'],halts=payload['entry']['halts'],
        effective_halt_identity=payload['base'].get('partition_identity'),seconds=seconds,
        batch_size=batch_size,output_batch_size=1024,quantile='linear',movement_mean_tolerance=dict(relative=1e-10,absolute=1e-12),
        mappings=C.MAPPINGS,fields=C.field_metadata(),reason_bits=C.REASONS,full_session=seconds==57600,capability=capability,numerical_policy=C.NUMERICAL_POLICY)
    ext=work/'extension.parquet'
    raw_stats=Counter();extension_started=time.monotonic()
    ext_result=C.write_rows(ext,C.extension_rows(base,source_paths['quotes'],day,symbol,batch_size=batch_size,check=check_yield,full=seconds==57600,stats=raw_stats,limit=seconds,halts=payload['entry']['halts']),identity,check=check_yield,columns=C.extension_columns())
    extension_seconds=time.monotonic()-extension_started
    save(work/'attempt.json',attempt_identity|{'state':'extension_written','extension':ext_result})
    footer=pq.ParquetFile(ext)
    if footer.metadata.num_rows!=seconds or footer.schema_arrow.names!=C.extension_columns():raise ValueError('extension footer/schema mismatch')
    start=C.V.session_bounds(day)[0]
    assembly_started=time.monotonic()
    final=work/'features.parquet'
    result=C.write_rows(final,C.joined_rows(base,ext,payload['discovery'],batch_size,check_yield,limit=seconds),identity|{'extension':ext_result},check=check_yield,columns=C.final_columns())
    assembly_seconds=time.monotonic()-assembly_started
    verification_started=time.monotonic()
    trace_ends={start+C.NS,start+60*C.NS,start+300*C.NS,start+seconds*C.NS}
    if payload['entry']['halts']:
        for edge in payload['entry']['halts'][0][:2]:
            second=(edge-start)//C.NS
            trace_ends.update(start+(second+d)*C.NS for d in (0,1,2,301))
    verification=C.verify(final,day,symbol,seconds,check_yield,trace_ends)
    coverage=verification.pop('coverage');traces=verification.pop('traces')
    if result['rows']!=seconds or result['sha256']!=verification['sha256']:raise ValueError('final verification mismatch')
    measurement=dict(**identity,output=result,extension=ext_result,verification=verification,
        elapsed_seconds=time.monotonic()-started,staging_seconds=staging_seconds,assembly_seconds=assembly_seconds,verification_seconds=time.monotonic()-verification_started,requests=dict(requests or {}),assembly_verify_seconds=time.monotonic()-assembly_started,staged_bytes=sum(x['size_bytes'] for x in source_objects.values()),
        raw_quote_rows=source['quotes']['rows'],raw_stats=dict(raw_stats),extension_seconds=extension_seconds,scratch_bytes=sum(p.stat().st_size for p in work.iterdir() if p.is_file()))
    save(work/'coverage.json',coverage)
    measurement['coverage_seconds']=0.0  # Included in the single stored verification pass.
    measurement['coverage_sha256']=sha(work/'coverage.json')
    measurement['io_passes']=dict(base_decodes=2,quote_decodes=1,extension_decodes=1,final_decodes=1,
        base_projected_columns=C.BASE_COLUMNS,extension_base_projected_columns=C.EXTENSION_BASE_COLUMNS,extension_columns=C.extension_columns(),final_columns=C.final_columns(),
        base_rows_per_pass=seconds,quote_projected_columns=list(C.V.MARKET.QUOTE_COLUMNS),base_compressed_bytes=payload['base']['bytes'],quote_compressed_bytes=source['quotes']['size_bytes'],extension_compressed_bytes=ext_result['bytes'],final_compressed_bytes=result['bytes'],hashing='bounded byte streams, additional to decodes',footer_reads='staging and each completed write')
    save(work/'validation_traces.json',traces)
    save(work/'attempt.json',attempt_identity|{'state':'verified','output':result})
    check_yield();save(work/'manifest.json',measurement)
    return measurement


def _partition(args):
    worker_setup();meta=verified_inventory(args.inventory)
    if args.seconds==57600 and os.environ.get('TAPE_EDA_FULL_APPROVED')!=meta['inventory_hash']:
        raise ValueError('full symbol-day worker requires approved parent run')
    db=connect(Path(args.inventory)/'inventory.sqlite',True)
    row=db.execute("SELECT payload FROM members WHERE day=? AND symbol=? AND status='admitted'",(args.day,args.symbol)).fetchone();db.close()
    if row is None:raise ValueError('partition is not in frozen admitted inventory')
    selection_hash=None
    if args.execution_selection:
        chosen=SELECTION.verify(args.execution_selection,meta['inventory_hash']);selection_hash=chosen['selection_hash']
        selected=connect(args.execution_selection/'selection.sqlite',True)
        member=selected.execute('SELECT seconds FROM members WHERE day=? AND symbol=?',(args.day,args.symbol)).fetchone();selected.close()
        if member is None or member['seconds']!=args.seconds:raise ValueError('worker selection membership/grid mismatch')
    client,bucket,requests=client_for(meta['queue'])
    result=partition(json.loads(row['payload']),args.output,seconds=args.seconds,client=client,bucket=bucket,execution_selection_hash=selection_hash,requests=requests)
    save(Path(args.output)/'requests.json',dict(requests))


def synthetic(args):
    """Tiny deterministic production-path bootstrap, never a corpus proxy."""
    worker_setup()
    import all_feature_month_core as C
    import snapshot_feature_pipeline as F
    sys.path.insert(0,str(ROOT/'tests'))
    from test_economic_tape_v3 import quote,trade,raw_pair
    root=Path(args.output);root.mkdir(parents=True,exist_ok=False)
    day='2026-07-01';start=C.V.session_bounds(day)[0];seconds=660
    quotes=[]
    for i in range(seconds):
        for seq,phase,offset in ((0,0,0),(0,100,1),(1,100,0)):
            quotes.append(quote(start+i*C.NS+phase,seq=seq,bid=99.95+i*.001+offset,ask=100.05+i*.001+offset))
    q,t=raw_pair(root/'fixture',day,quotes,[trade(start)])
    halts=[(start+310*C.NS+100,start+314*C.NS+100,'synthetic_boundary')]
    base=root/'fixture'/'base.parquet'
    manifest=F.write(q,t,day,'TEST',base,{},halts=halts,seconds=seconds,batch_size=4096)
    def obj(path):return dict(object_key='synthetic/'+path.name,sha256=sha(path),size_bytes=path.stat().st_size,rows=C.pq.ParquetFile(path).metadata.num_rows)
    source=dict(session_date=day,symbol='TEST',quotes=obj(q),trades=obj(t))
    payload=dict(entry=dict(source=source,halts=halts),base=manifest,base_key='synthetic/base.parquet',
        discovery=dict(verified=True,endpoint_ns=start+100*C.NS,received_at_ns=None,timing_basis='synthetic nominal completion',provenance_hash='synthetic'))
    result=partition(payload,root/'partition',seconds=seconds,local=dict(quotes=q,base=base))
    save(root/'result.json',dict(code_identity=code_identity(),rows=seconds,quote_events=len(quotes),trade_events=1,
                               result=result,kind='synthetic_production_bootstrap'))


def choose_probes(db):
    # Selection uses only frozen source byte counts and known halt metadata.
    # SQLite temporary table bounds Python state independently of month size.
    db.execute('CREATE TEMP TABLE sizes(day TEXT,symbol TEXT,bytes INTEGER,halted INTEGER)')
    for row in db.execute("SELECT day,symbol,payload FROM members WHERE status='admitted'"):
        p=json.loads(row['payload']);db.execute('INSERT INTO sizes VALUES (?,?,?,?)',
            (row['day'],row['symbol'],p['entry']['source']['quotes']['size_bytes'],bool(p['entry']['halts'])))
    result=[]
    for label,order,where in [('sparse','bytes ASC',''),('dense','bytes DESC',''),('halt_boundary','bytes ASC','WHERE halted=1')]:
        row=db.execute(f'SELECT day,symbol FROM sizes {where} ORDER BY {order},day,symbol LIMIT 1').fetchone()
        if row:result.append(dict(label=label,day=row['day'],symbol=row['symbol']))
    return result



def release_staged_copies(work):
    """Only completed, private working copies; final evidence remains intact."""
    work=Path(work)
    if not (work/'manifest.json').exists():raise ValueError('cannot clean an incomplete attempt')
    for name in ('quotes.parquet','base.parquet','base_prefix.parquet','extension.parquet'):
        (work/name).unlink(missing_ok=True)


def probe(args):
    meta=verified_inventory(args.inventory)
    selection=SELECTION.verify(args.execution_selection,meta['inventory_hash']) if args.execution_selection else None
    bootstrap=read(args.bootstrap)
    if bootstrap.get('returncode')!=0 or not bootstrap.get('target_met') or bootstrap.get('stop_reason'):
        raise ValueError('successful resource-compliant synthetic bootstrap required')
    result_path=Path(args.bootstrap).with_suffix('')/'result.json'
    if not result_path.exists() or read(result_path)['code_identity']!=code_identity():raise ValueError('bootstrap code identity changed')
    output=Path(args.output);output.mkdir(parents=True,exist_ok=False)
    db=connect(Path(args.inventory)/'inventory.sqlite',True)
    selected=choose_probes(db);db.close()
    if {x['label'] for x in selected}!={'sparse','dense','halt_boundary'}:raise ValueError('representative halt case unavailable; report inventory issue')
    measurements=[]
    for choice in selected:
        work=output/choice['label'];report=output/(choice['label']+'_resources.json')
        # Boundary prefix includes resumption plus a fresh 300-second horizon;
        # do not turn a prefix into an unapproved full symbol-day.
        db=connect(Path(args.inventory)/'inventory.sqlite',True)
        payload=json.loads(db.execute('SELECT payload FROM members WHERE day=? AND symbol=?',(choice['day'],choice['symbol'])).fetchone()[0]);db.close()
        from datetime import datetime,time as dt_time
        from zoneinfo import ZoneInfo
        start=int(datetime.combine(datetime.fromisoformat(choice['day']).date(),dt_time(4),ZoneInfo('America/New_York')).timestamp())*10**9
        seconds=max(660,(payload['discovery']['endpoint_ns']-start)//10**9+301)
        if choice['label']=='dense':seconds=max(seconds,24000)  # Include dense RTH events through 10:40 ET.
        if choice['label']=='halt_boundary':
            seconds=max(seconds,math.ceil((payload['entry']['halts'][0][1]-start)/10**9)+301)
        if seconds>54000:raise ValueError('representative prefix would approach a full session; select an earlier independently known case')
        needed=4*(payload['entry']['source']['quotes']['size_bytes']+payload['base']['bytes'])
        command=[sys.executable,str(HERE),'_partition','--inventory',str(args.inventory),'--day',choice['day'],'--symbol',choice['symbol'],
                 '--output',str(work),'--seconds',str(seconds)]
        measurement=supervise(command,research=ROOT/'research',scratch=output,report=report,
            projected_peak=max(bootstrap['peak_new_rss_bytes'],192*MiB),needed=needed)
        if measurement['returncode'] or measurement['stop_reason'] or not measurement['target_met']:
            raise ResourceWait('representative probe stopped; inspect '+str(report))
        part=read(work/'manifest.json')
        measurements.append(dict(**choice,seconds=seconds,resources=measurement,partition=part,requests=read(work/'requests.json')))
        release_staged_copies(work)
    db=connect(Path(args.inventory)/'inventory.sqlite',True)
    total_rows=total_quotes=total_raw_bytes=total_base_bytes=admitted=0
    for r in SELECTION.members(db,args.execution_selection):
        p=json.loads(r['payload']);admitted+=1;total_rows+=r['seconds'];total_quotes+=p['entry']['source']['quotes']['rows']
        total_raw_bytes+=p['entry']['source']['quotes']['size_bytes'];total_base_bytes+=p['base']['bytes']
    db.close()
    bytes_per_row=max(x['partition']['output']['bytes']/x['seconds'] for x in measurements)
    # Keep staging, row work, dense quote work and per-partition admission
    # explicit. Charging both row and event extension terms is conservative.
    row_seconds=max((x['partition']['extension_seconds']+x['partition']['assembly_verify_seconds']+x['partition']['coverage_seconds'])/x['seconds'] for x in measurements)
    dense=next(x for x in measurements if x['label']=='dense')
    event_seconds=dense['partition']['extension_seconds']/max(1,dense['partition']['raw_stats'].get('quote_rows_consumed',0))
    bytes_per_second=min(x['partition']['staged_bytes']/max(.001,x['partition']['staging_seconds']) for x in measurements)
    download_seconds=(total_raw_bytes+total_base_bytes)/bytes_per_second
    runtime_upper=total_rows*row_seconds+total_quotes*event_seconds+download_seconds+admitted*11
    projected_final=max(total_rows*bytes_per_row*1.5,total_base_bytes*1.8)+admitted*128*1024
    staging_fit=staging_model(measurements,total_raw_bytes+total_base_bytes,admitted)
    warm_runtime=total_rows*row_seconds+total_quotes*event_seconds+staging_fit['projected_seconds']+admitted+10

    checkpoint=dict(kind='representative_prefix_checkpoint',inventory_hash=meta['inventory_hash'],execution_selection_hash=selection['selection_hash'] if selection else None,code_identity=code_identity(),
        cases=measurements,projected_rows=total_rows,projected_final_bytes=math.ceil(projected_final),
        projected_download_bytes=total_raw_bytes+total_base_bytes,projected_runtime_seconds_conservative=runtime_upper,
        projected_runtime_seconds_continuous_admission=warm_runtime,staging_model=staging_fit,
        projection_components=dict(row_seconds=row_seconds,event_seconds=event_seconds,staged_bytes_per_second=bytes_per_second,
            admission_and_startup_seconds_per_partition=11,continuous_admission_initial_seconds=10,
            continuous_admission_startup_allowance_per_partition=1,continuous_admission_limitation='fresh 10-second admission if observation gap exceeds one second, pressure, or incumbent changes; startup allowance is an assumption',measured_max_output_bytes_per_row=bytes_per_row,
            storage_basis='max(1.5 * measured max bytes/row * rows, 1.8 * complete base bytes) + 128 KiB/partition metadata'),
        runtime_limitation='prefix-based conservative model; later quote density, network and resource waits can differ; month summaries merge the already measured partition summaries',
        projected_requests=admitted*max(sum(x['requests'].values()) for x in measurements),
        peak_new_rss_bytes=max(x['resources']['peak_new_rss_bytes'] for x in measurements),
        full_run_approved=False)
    checkpoint['checkpoint_hash']=digest(checkpoint);save(output/'checkpoint.json',checkpoint)
    return checkpoint


def staging_model(measurements, total_bytes, partitions):
    """Nonnegative fixed-cost + byte-cost fit, shifted above observed samples.

    Three source-size strata do not establish network tail latency. Retain the
    legacy slowest-throughput projection separately for that uncertainty.
    """
    points=[(float(x['partition']['staged_bytes']),float(x['partition']['staging_seconds'])) for x in measurements]
    mx=sum(x for x,y in points)/len(points);my=sum(y for x,y in points)/len(points)
    variance=sum((x-mx)**2 for x,y in points)
    slope=max(0.,sum((x-mx)*(y-my) for x,y in points)/variance) if variance else 0.
    intercept=max(0.,my-slope*mx)
    # Refitting with the intercept constrained to zero avoids a negative fixed cost.
    if intercept==0:slope=sum(x*y for x,y in points)/sum(x*x for x,y in points)
    intercept=max(intercept,max(y-slope*x for x,y in points))
    return dict(fixed_seconds_per_partition=intercept,seconds_per_byte=slope,
        projected_seconds=partitions*intercept+total_bytes*slope,
        observations=[dict(bytes=x,seconds=y) for x,y in points],
        limitation='observed envelope, not a network upper bound; source-size and transient request latency are confounded')


def publish_partition(attempt,target):
    target=Path(target)
    if target.exists():raise FileExistsError(target)
    target.mkdir(parents=True)
    # Final marker is moved last. Interrupted publication remains incomplete.
    for name in ('features.parquet','coverage.json','validation_traces.json','manifest.json'):
        os.replace(Path(attempt)/name,target/name)


def run(args):
    meta=verified_inventory(args.inventory);checkpoint=read(args.checkpoint)
    selection=SELECTION.verify(args.execution_selection,meta['inventory_hash']) if args.execution_selection else None
    selection_hash=selection['selection_hash'] if selection else None
    if checkpoint.get('execution_selection_hash')!=selection_hash:raise ValueError('checkpoint selection changed')
    expected=checkpoint.pop('checkpoint_hash')
    if digest(checkpoint)!=expected:raise ValueError('checkpoint identity mismatch')
    if checkpoint['inventory_hash']!=meta['inventory_hash'] or checkpoint['code_identity']!=code_identity():raise ValueError('checkpoint inputs/code changed')
    if not args.full_run_approved:raise ValueError('full external-data acceptance requires user confirmation after the measured checkpoint')
    output=Path(args.output);output.mkdir(parents=True,exist_ok=True)
    with (output/'run.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        manifest_path=output/'dataset_manifest.json'
        identity=dict(version='tape_data_product_eda_v2',month=meta['month'],inventory_hash=meta['inventory_hash'],
            checkpoint_hash=expected,code_identity=code_identity(),full_run_approved=True,execution_selection_hash=selection_hash,
            execution_selection_root=str(args.execution_selection.resolve()) if args.execution_selection else None,
            schema_version='tape_data_product_eda_v2',horizons_seconds=[60,300],cadence_seconds=1,
            age_version='exact_midpoint_change_age_reset_halt_continuity_v1',quantile='linear',
            field_metadata='per-partition Parquet eda metadata: fields, source/output units, reason bits, halt identities; manifest output.schema_sha256',
            selection_root=meta['selection_root'],inventory_root=str(Path(args.inventory).resolve()))
        if manifest_path.exists():
            old=read(manifest_path)
            if old['identity']!=identity:raise ValueError('run identity changed')
            if old['state'] in ('failed','resource_wait') and not args.resume_reviewed:
                raise ResourceWait('prior interruption requires reviewed resume')
        if shutil.disk_usage(output).free<3*GiB+checkpoint['projected_final_bytes']:
            raise ResourceWait('volume cannot hold projected dataset plus 3 GiB reserve')
        save(manifest_path,dict(identity=identity,state='running',coverage_claim=meta['coverage_claim']))
        os.environ['TAPE_EDA_FULL_APPROVED']=meta['inventory_hash']
        db=connect(Path(args.inventory)/'inventory.sqlite',True)
        admission_history=AdmissionHistory()
        try:
            for member in SELECTION.members(db,args.execution_selection):
                target=output/f"session_date={member['day']}"/f"symbol={member['symbol']}"
                payload=json.loads(member['payload'])
                if (target/'manifest.json').exists():
                    prior=read(target/'manifest.json')
                    if prior['input_hash']!=digest(payload) or prior['code_identity']!=code_identity() or prior['seconds']!=member['seconds'] or sha(target/'features.parquet')!=prior['output']['sha256']:
                        raise ValueError('completed partition identity changed')
                    continue
                if target.exists():raise ValueError('partial final publication requires inspection')
                attempt=output/'scratch'/uuid.uuid4().hex
                attempt.mkdir(parents=True)
                work=attempt/'partition'
                needed=4*(payload['entry']['source']['quotes']['size_bytes']+payload['base']['bytes'])
                command=[sys.executable,str(HERE),'_partition','--inventory',str(args.inventory),'--day',member['day'],'--symbol',member['symbol'],'--output',str(work),'--seconds',str(member['seconds'])]
                if args.execution_selection:command+=['--execution-selection',str(args.execution_selection)]
                result=supervise(command,research=ROOT/'research',scratch=attempt,
                    report=output/'resources'/f"{member['day']}_{member['symbol']}_{attempt.name}.json",
                    projected_peak=checkpoint['peak_new_rss_bytes'],needed=needed,admission_history=admission_history)
                if result['returncode'] or result['stop_reason']:raise ResourceWait('partition stopped; inspect private attempt '+str(attempt))
                publish_partition(work,target)
                # Only successful, run-owned staged copies are removed.
                shutil.rmtree(attempt)
            result=supervise([sys.executable,str(HERE),'_artifacts','--inventory',str(args.inventory),'--output',str(output)],
                research=ROOT/'research',scratch=output/'summary_scratch',report=output/'resources'/'summaries.json',
                projected_peak=checkpoint['peak_new_rss_bytes'],needed=64*MiB,timeout=7200)
            if result['returncode'] or result['stop_reason']:raise ResourceWait('summary worker interrupted')
            save(manifest_path,dict(identity=identity,state='complete_selected_preview' if selection else 'complete_over_verified_available_inputs',
                coverage_claim=meta['coverage_claim'],excluded_members=meta['counts'].get('excluded',0)))
        except BaseException as exc:
            save(manifest_path,dict(identity=identity,state='resource_wait' if isinstance(exc,ResourceWait) else 'failed',reason=str(exc)))
            raise
        finally:db.close()



def artifacts(inventory_root,output):
    """Disk-backed cross-partition coverage; fixed per-feature/session counters."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import all_feature_month_core as C
    output=Path(output);db=connect(output/'coverage.sqlite')
    db.execute('PRAGMA temp_store=FILE')
    db.execute('DROP TABLE IF EXISTS counts')
    db.execute('DROP TABLE IF EXISTS bins')
    db.execute('CREATE TABLE bins(session TEXT,feature TEXT,bin INTEGER,mass INTEGER)')
    catalog_counts=Counter();verified_rows=0
    db.execute('CREATE TABLE counts(day TEXT,symbol TEXT,session TEXT,feature TEXT,rows INTEGER,valid INTEGER,nulls INTEGER,zeros INTEGER,post INTEGER,eda INTEGER,reason TEXT,histogram TEXT,minimum REAL,maximum REAL,axis TEXT)')
    source=connect(Path(inventory_root)/'inventory.sqlite',True)
    dataset=read(output/'dataset_manifest.json') if (output/'dataset_manifest.json').exists() else {}
    selection_root=dataset.get('identity',{}).get('execution_selection_root')
    selected=connect(Path(selection_root)/'selection.sqlite',True) if selection_root else None
    catalog_schema=pa.schema([pa.field(x,pa.string()) for x in ('session_date','symbol','status','reason','discovery','source_base_identity','output_reference','output_sha256')]+[pa.field('rows',pa.int64())])
    with pq.ParquetWriter(output/'catalog.parquet.partial',catalog_schema,compression='zstd') as catalog:
        for member in source.execute('SELECT * FROM members ORDER BY day,symbol'):
            payload=json.loads(member['payload']);path=output/f"session_date={member['day']}"/f"symbol={member['symbol']}"/'features.parquet'
            item=dict(session_date=member['day'],symbol=member['symbol'],status=member['status'],reason=member['reason'],
                discovery=json.dumps(payload.get('discovery')),source_base_identity=json.dumps(payload),output_reference=None,output_sha256=None,rows=0)
            chosen=selected is None or selected.execute('SELECT 1 FROM members WHERE day=? AND symbol=?',(member['day'],member['symbol'])).fetchone() is not None
            if not chosen:item.update(status='unselected',reason='not in immutable execution selection')
            elif member['status']=='admitted' and not (path.parent/'manifest.json').exists():item.update(status='missing_selected',reason='no verified selected partition')
            elif member['status']=='admitted':
                part=read(path.parent/'manifest.json');item.update(status='complete',output_reference=str(path.relative_to(output)),output_sha256=part['output']['sha256'],rows=part['output']['rows'])
                if part['verification'].get('reconstructed_fields')!=list(C.FEATURES) or part['verification']['rows_verified']!=part['output']['rows']:raise ValueError('partition reconstruction evidence missing')
                verified_rows+=part['verification']['rows_verified']
                coverage_path=path.parent/'coverage.json'
                if sha(coverage_path)!=part['coverage_sha256']:raise ValueError('partition coverage identity changed')
                for g in read(coverage_path):
                    db.execute('INSERT INTO counts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(member['day'],member['symbol'],g['session_segment'],g['feature'],
                        *(g[x] for x in ('rows','valid','nulls','zeros','post','eda')),json.dumps(g['reason']),json.dumps(g['histogram']),g['minimum'],g['maximum'],json.dumps(g['axis'])))
                    if sum(g['histogram'])!=g['eda']:raise ValueError('summary mass inconsistent')
                    db.executemany('INSERT INTO bins VALUES (?,?,?,?)',((g['session_segment'],g['feature'],i,mass) for i,mass in enumerate(g['histogram'])))
                db.commit()
            catalog_counts[item['status']]+=1
            catalog.write_table(pa.Table.from_pylist([item],schema=catalog_schema))
    source.close()
    if selected:selected.close()
    os.replace(output/'catalog.parquet.partial',output/'catalog.parquet')
    # Day/symbol-level coverage is already bounded and explicitly identifies
    # contributing dates/symbols; global aggregation is in SQLite, not Python.
    schema=pa.schema([pa.field(x,pa.string()) for x in ('session_date','symbol','session_segment','feature')]+
        [pa.field(x,pa.int64()) for x in ('rows','valid','nulls','zeros','post_discovery','eda_eligible')]+
        [pa.field('reason_counts_json',pa.string()),pa.field('histogram_json',pa.string()),pa.field('minimum',pa.float64()),pa.field('maximum',pa.float64()),pa.field('axis_json',pa.string())])
    with pq.ParquetWriter(output/'coverage.parquet.partial',schema,compression='zstd') as writer:
        cursor=db.execute('SELECT * FROM counts ORDER BY day,symbol,session,feature')
        while batch:=cursor.fetchmany(1024):
            writer.write_table(pa.Table.from_pylist([dict(zip(schema.names,tuple(r))) for r in batch],schema=schema))
    os.replace(output/'coverage.parquet.partial',output/'coverage.parquet')
    totals=[]
    for r in db.execute('''SELECT session,feature,SUM(rows),SUM(valid),SUM(nulls),SUM(zeros),SUM(post),SUM(eda),
        COUNT(DISTINCT CASE WHEN valid>0 THEN symbol END),COUNT(DISTINCT CASE WHEN valid>0 THEN day END),
        COUNT(DISTINCT CASE WHEN eda>0 THEN symbol END),COUNT(DISTINCT CASE WHEN eda>0 THEN day END)
        FROM counts GROUP BY session,feature ORDER BY session,feature'''):
        totals.append(dict(zip(('session_segment','feature','rows','valid','nulls','zeros','post_discovery','eda_eligible',
            'contributing_symbols','contributing_dates','eda_contributing_symbols','eda_contributing_dates'),tuple(r))))
    if totals:
        pq.write_table(pa.Table.from_pylist(totals),output/'coverage_totals.parquet',compression='zstd')
    hist_schema=pa.schema([pa.field('session_segment',pa.string()),pa.field('feature',pa.string()),pa.field('bin',pa.int64()),pa.field('eligible_seconds',pa.int64())])
    with pq.ParquetWriter(output/'histogram_totals.parquet',hist_schema,compression='zstd') as writer:
        cursor=db.execute('SELECT session,feature,bin,SUM(mass) FROM bins GROUP BY session,feature,bin ORDER BY session,feature,bin')
        while batch:=cursor.fetchmany(1024):writer.write_table(pa.Table.from_pylist([dict(zip(hist_schema.names,tuple(r))) for r in batch],schema=hist_schema))
    db.close()
    (output/'README.md').write_text(READ_GUIDE+'\ncoverage_totals.parquet aggregates feature/session time counts and distinct contributing symbols/dates.\n')
    (output/'validation_report.md').write_text(
        f"V2 final-file reconstruction verified {verified_rows:,} rows across {catalog_counts['complete']} completed partitions.\n"
        f"Catalog status counts: {json.dumps(dict(catalog_counts),sort_keys=True)}.\n"
        "Each verified partition independently reconstructs all eighteen eligible values and support/maturity/reason gates.\n"
        "EDA histogram mass reconciles to eligible seconds; axes are in coverage.axis_json and partition coverage.json.\n"
        "Counts describe this selection only. Missing selected or unselected parent members are not completed data.\n"
        "Source copies/conversions and quote replay are checked during construction; identities are retained in manifests.\n"
        "Measured worker/supervisor/combined RSS and observation gaps are in resources/*.json.\n")



READ_GUIDE='''# All-feature EDA dataset

The eighteen numerical fields represent nine definitions at 60s and 300s. Movement
uses duration-weighted one-second midpoints and overlapping five-second changes.
Movement is bps per five-second observation; ages are seconds, activity is trades/s
and USD/s, spreads are full quoted bps, ratios are dimensionless. Quantiles are
exact linear interpolation. Midpoint age uses exact eligible quote events.

Keep feature-specific `*_analysis_valid` and `*_reason_mask`; use
`*_eda_eligible` for verified post-discovery, valid, fully rebuilt history.
A finite legacy carried value is retained but excluded by the primary EDA mask.
Discovery is nominal minute-bar completion unless received-at evidence is present;
historical halts are ex-post and these rows are not asserted live reproducible.

Read only required columns and stream batches:

```python
from pathlib import Path
import pyarrow.parquet as pq
root = Path("PATH_TO_RUN")
f = "movement_participation_60s"  # dimensionless
g = "movement_mean_5s_bps_60s"  # bps
catalog = pq.ParquetFile(root / "catalog.parquet")
for records in catalog.iter_batches(batch_size=128, columns=["status", "output_reference"], use_threads=False):
    for member in records.to_pylist():
        if member["status"] != "complete":
            continue
        partition = pq.ParquetFile(root / member["output_reference"])
        columns = ["session_date", "symbol", "interval_end_ns", f, g, f+"_eda_eligible", g+"_eda_eligible"]
        for batch in partition.iter_batches(batch_size=4096, columns=columns, use_threads=False):
            import pyarrow.compute as pc
            eligible = batch.filter(pc.and_(batch[f+"_eda_eligible"], batch[g+"_eda_eligible"]))
            # Consume and release this batch. Never concatenate the month.
```

For a synchronized trace, project interval_end_ns, movement_5s_bps,
midpoint_age_observation_status, midpoint_change_age_end_seconds and the desired
rolling fields and their individual masks from one final partition. Filter each
batch to a fixed requested [start_ns,end_ns] interval and release the batch; do
not collapse gaps or forward-fill nulls. Integer quote support durations convert
to seconds by dividing by 1e9. Spread aggregation is sum(integral)/sum(duration/1e9).

For reliable file discovery, select completed feature paths from catalog.parquet
in batches and open one ParquetFile at a time. Catalog/coverage are not feature
partitions. Coverage includes per-symbol/date/session counts; sum counts for time
coverage and count distinct symbol/date keys for breadth. Repeated seconds and
overlapping windows are not independent observations. Histograms use each feature's EDA mask. Participation uses 200 linear bins on [0,1],
including 1 in the final bin. Other fields retain zero, positive underflow <1e-6,
72 quarter-log10 bins [1e-6,1e12), and overflow >=1e12. Nulls and reasons are
counted separately. Bin edges/population/weighting are in coverage.axis_json;
cumulative displays are approximate CDFs, not exact ECDFs. No fitted quantiles, cohorts, or executable edge claims.
'''


def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='command',required=True)
    p=sub.add_parser('freeze');p.add_argument('--universe',type=Path,required=True);p.add_argument('--queue',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--month',default='2026-07')
    for name in ('inventory','_inventory'):
        p=sub.add_parser(name);p.add_argument('--selection',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p=sub.add_parser('bootstrap');p.add_argument('--output',type=Path,required=True)
    p=sub.add_parser('_synthetic');p.add_argument('--output',type=Path,required=True)
    p=sub.add_parser('_partition');p.add_argument('--inventory',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--day',required=True);p.add_argument('--symbol',required=True);p.add_argument('--seconds',type=int,default=660)
    p=sub.add_parser('_artifacts');p.add_argument('--inventory',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p=sub.add_parser('probe');p.add_argument('--inventory',type=Path,required=True);p.add_argument('--bootstrap',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p=sub.add_parser('run');p.add_argument('--inventory',type=Path,required=True);p.add_argument('--checkpoint',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--full-run-approved',action='store_true');p.add_argument('--resume-reviewed',action='store_true')
    for name in ('probe','run','_partition'):
        sub.choices[name].add_argument('--execution-selection',type=Path,help='immutable preview/full membership, independently hashed')
    p=sub.add_parser('select');p.add_argument('--inventory',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--members',type=Path,required=True,help='JSONL: day, symbol, seconds (default 57600)');p.add_argument('--rule',required=True);p.add_argument('--seed',type=int)
    args=parser.parse_args()
    try:
        if args.command=='freeze':result=freeze(args.universe,args.queue,args.output,args.month)
        elif args.command=='select':result=SELECTION.create(args.inventory,args.output,args.members,args.rule,args.seed)
        elif args.command=='inventory':
            args.output.parent.mkdir(parents=True,exist_ok=True)
            result=supervise([sys.executable,str(HERE),'_inventory','--selection',str(args.selection),'--output',str(args.output)],
                research=ROOT/'research',scratch=args.output.parent,report=args.output.parent/(args.output.name+'_resources.json'),projected_peak=320*MiB,needed=64*MiB,timeout=7200)
        elif args.command=='_inventory':
            worker_setup();result=inventory(args.selection,args.output)
        elif args.command=='_partition':result=_partition(args)
        elif args.command=='_synthetic':result=synthetic(args)
        elif args.command=='_artifacts':
            worker_setup();result=artifacts(args.inventory,args.output)
        elif args.command=='probe':result=probe(args)
        elif args.command=='run':result=run(args)
        else:
            output=args.output.resolve();output.parent.mkdir(parents=True,exist_ok=True)
            result=supervise([sys.executable,str(HERE),'_synthetic','--output',str(output.with_suffix(''))],
                research=ROOT/'research',scratch=output.parent,report=output,projected_peak=320*MiB,needed=32*MiB)
        if isinstance(result,dict) and result.get('kind')=='representative_prefix_checkpoint':
            print(json.dumps({k:v for k,v in result.items() if k not in ('cases','code_identity')},default=str))
        else:print(json.dumps(result,default=str))
    except ResourceWait as exc:
        print(json.dumps(dict(state='resource_wait',reason=str(exc))));sys.exit(75)


if __name__=='__main__':main()
