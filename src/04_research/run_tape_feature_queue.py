"""Detached one-partition feature follower. No model is involved in its run loop.

Raw RAM O(25k event columns + 1024 outputs + fixed 300-state windows).
Queue/inventory live in SQLite; discovery streams <=1000-object pages. Metadata
references are capped at 25k rows/64 MiB. Per-partition CPU inherits the optimized
O(R*C + S*(F+4W)) path. One staged pair, <=512 MiB scratch, >=3 GiB free.
Each child attempt has a 120s wall deadline and aggregate live 2.8 GiB RSS stop.
Source row and byte counts are measurements, not admission limits; batching bounds
memory independently. The multi-worker supervisor owns retry and failure isolation.
"""
from __future__ import annotations
import argparse
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime,timezone
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

ROOT=Path(__file__).resolve().parents[2]
DEFAULT=ROOT/'research/tape_feature_queue_20260908'
PRIORITY_DATES=['2026-08-27','2026-08-28','2026-08-31']
POLICY='official-only historical research; inferred registry unvalidated'


def now():return datetime.now(timezone.utc).isoformat()

def save(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix('.partial');tmp.write_text(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n');tmp.replace(path)

def read(path):return json.loads(Path(path).read_text())

@contextmanager
def db(root):
    c=sqlite3.connect(Path(root)/'queue.sqlite',timeout=10);c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA cache_size=-2048')
    c.execute('CREATE TABLE IF NOT EXISTS objects (key TEXT PRIMARY KEY, day TEXT, symbol TEXT, stream TEXT, bytes INTEGER, etag TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS jobs (day TEXT, symbol TEXT, priority INTEGER, state TEXT DEFAULT "pending", entry TEXT, result TEXT, PRIMARY KEY(day,symbol))')
    try:
        yield c
        c.commit()
    finally:c.close()

def code_identity():
    import run_bounded_tape_retrieval as B
    names=['src/04_research/run_tape_feature_queue.py','src/04_research/run_bounded_tape_retrieval.py',
        'src/04_research/tape_feature_store.py','src/04_research/acquire_nasdaq_luld_halts.py',
        'src/04_research/historical_halt_registry_v1.py','src/04_research/tape_snapshot_inventory.py',
        'src/01_data/r2_tq_storage.py','src/01_data/tq_cost_guard.py',
        'config/tape_snapshot_feature_semantics_v1.json','config/tape_official_halt_reviews_v1.json',
        'config/tape_feature_cost_policy_v2.json']
    return dict(files={n:B.S.sha256_file(ROOT/n) for n in names},feature=B.F.identity(),semantic=B.STORE.semantic_identity())

def check_plan(root):
    p=read(Path(root)/'plan.json')
    if p['implementation']!=code_identity():raise ValueError('code changed since queue plan; review and remeasure before resume')
    return p

def plan_cost_policy(plan):
    from tq_cost_guard import POLICY as LEGACY,load_policy,validate_policy
    if 'cost_policy_path' in plan:
        path=ROOT/plan['cost_policy_path']
        policy=load_policy(path)
        if plan.get('cost_policy')!=policy:raise ValueError('R2 cost policy changed since queue plan')
        return policy
    return validate_policy(plan.get('cost_policy',LEGACY))

def discover(root):
    import run_bounded_tape_retrieval as B
    from tq_cost_guard import CostGuard
    p=check_plan(root);cfg=B.S.load_r2_settings();client=B.S.build_client(cfg)
    guard=CostGuard(Path(p['cost_ledger']),plan_cost_policy(p));client.meta.events.register('before-send.s3.*',guard.before_send)
    count=0
    with db(root) as c:
        # Existing objects are immutable; changed size/ETag stops admission.
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=cfg.bucket,Prefix='tq/'):
            for obj in page.get('Contents',[]):
                parts=obj['Key'].split('/')
                if len(parts)!=4 or parts[-1] not in ('trades.parquet','quotes.parquet'):continue
                day,symbol=B.S._validate_date_symbol(parts[1].split('=',1)[1],parts[2].split('=',1)[1])
                stream=parts[-1].split('.')[0]
                old=c.execute('SELECT bytes,etag FROM objects WHERE key=?',(obj['Key'],)).fetchone()
                if old and (old['bytes']!=obj['Size'] or old['etag']!=obj['ETag']):raise ValueError('immutable source changed: '+obj['Key'])
                c.execute('INSERT OR IGNORE INTO objects VALUES (?,?,?,?,?,?)',(obj['Key'],day,symbol,stream,obj['Size'],obj['ETag']));count+=1
            c.commit()
        c.execute('INSERT OR IGNORE INTO jobs(day,symbol,priority) SELECT day,symbol,CASE WHEN day IN (?,?,?) THEN 0 ELSE 1 END FROM objects GROUP BY day,symbol HAVING COUNT(*)=2',PRIORITY_DATES)
    save(Path(root)/'discovery.json',dict(at=now(),objects_seen=count))

def reviewed_official_intervals(halts,source,reference_hash):
    """Resolve only exact, evidence-reviewed inputs; unknown overlaps still fail.

    Metadata is bounded by the existing reference limit. No raw data is read here.
    Non-overlapping inputs retain their original IDs and semantic cache identity.
    """
    import run_bounded_tape_retrieval as B
    if not any(halts[i][0]<halts[i-1][1] for i in range(1,len(halts))):return halts,None
    path=ROOT/'config/tape_official_halt_reviews_v1.json'
    for review in read(path)['reviews']:
        if (review['symbol'],review['session_date'])!=(source['symbol'],source['session_date']):continue
        hashes={source[s]['object_key']:source[s]['sha256'] for s in ('trades','quotes')}
        if (review['input_intervals']!=[list(x) for x in halts]
            or review['reference_manifest_sha256']!=reference_hash or review['source_sha256']!=hashes):
            raise ValueError('reviewed official overlap inputs changed; new review required')
        output=[tuple(x) for x in review['output_intervals']]
        if any(a>=b for a,b,_ in output) or any(output[i][0]<output[i-1][1] for i in range(1,len(output))):
            raise ValueError('invalid reviewed official intervals')
        # A review may coalesce records, but cannot silently add/remove halted time.
        def union(items):
            result=[]
            for a,b,_ in sorted(items):
                if result and a<=result[-1][1]:result[-1][1]=max(result[-1][1],b)
                else:result.append([a,b])
            return result
        if union(output)!=union(halts):raise ValueError('review changes official interval union')
        return output,dict(review=review,review_sha256=B.I.digest(review))
    raise ValueError('overlapping official intervals require review')


def official_reference(root,day,source):
    import run_bounded_tape_retrieval as B
    import acquire_nasdaq_luld_halts as A
    root=Path(root);base=ROOT/'research/tape_feature_snapshot_20260908/official_reference'
    august=ROOT/'research/bounded_august_retrieval_20260908/official_reference'
    chosen=None
    for candidate in (base,august,root/'references'/day):
        if (candidate/'manifest.json').exists() and day in read(candidate/'manifest.json')['requested_dates']:
            chosen=candidate;break
    if chosen is None:
        import certifi
        os.environ['SSL_CERT_FILE']=certifi.where()
        chosen=root/'references'/day
        A.acquire(dates=[day],output_dir=chosen,timeout_seconds=15,max_attempts=1,resume=True)
    manifest=read(chosen/'manifest.json')
    for name,h in manifest['content_sha256'].items():
        if B.S.sha256_file(chosen/name)!=h:raise ValueError('official reference changed')
    refhash=B.S.sha256_file(chosen/'manifest.json')
    norm=root/'normalized_references'/refhash
    if not (norm/'external_halts_normalized.parquet').exists():
        B.P.OLD.normalize_external_halts(input_path=chosen/'all_halts.csv',provider='nasdaq_trader_rss',
            run_dir=norm,reference_root=root/'reference_cache',request_parameters={'reference_manifest_sha256':refhash})
    frame=B.P.project_official_sessions(B.P.metadata_frame(norm/'external_halts_normalized.parquet'),[source])
    if frame.official_halt_start.isna().any() or frame.official_trade_resume_time.isna().any():raise ValueError('unresolved official halt bounds')
    start,end=B.F.V.session_bounds(day);halts=[]
    for row in frame.to_dict('records'):
        a=B.P.OLD._ns(row['official_halt_start']);b=B.P.OLD._ns(row['official_trade_resume_time'])
        if max(a,start)<min(b,end):halts.append((max(a,start),min(b,end),'official:'+row['provider_event_id']))
    halts.sort()
    halts,review=reviewed_official_intervals(halts,source,refhash)
    provenance=dict(manifest_sha256=refhash,reference=str(chosen))
    if review is not None:provenance['overlap_review']=review
    return halts,provenance

def source_entry(root,day,symbol,client,bucket):
    import run_bounded_tape_retrieval as B
    with db(root) as c:
        old=c.execute('SELECT entry FROM jobs WHERE day=? AND symbol=?',(day,symbol)).fetchone()
        if old and old['entry']:return json.loads(old['entry'])
        objects=c.execute('SELECT * FROM objects WHERE day=? AND symbol=?',(day,symbol)).fetchall()
    if len(objects)!=2:raise ValueError('incomplete pair')
    source=dict(session_date=day,symbol=symbol,status='accepted_canonical_metadata')
    for obj in objects:
        head=client.head_object(Bucket=bucket,Key=obj['key'])
        if head['ContentLength']!=obj['bytes'] or head['ETag']!=obj['etag']:raise ValueError('source listing changed')
        ident=B.S.object_identity_from_head(obj['key'],head)
        if ident.rows is None:raise ValueError('source rows unavailable')
        B.I.validate_metadata(day,obj['stream'],head.get('Metadata',{}))
        source[obj['stream']]=dict(**asdict(ident),metadata=head['Metadata'])
    halts,reference=official_reference(root,day,source)
    entry=dict(source=source,halts=halts,official_reference=reference)
    with db(root) as c:c.execute('UPDATE jobs SET entry=? WHERE day=? AND symbol=?',(json.dumps(entry),day,symbol))
    return entry

def work(root,day,symbol,scratch,probe=False,probe_seconds=21600):
    import run_bounded_tape_retrieval as B
    from tq_cost_guard import CostGuard
    root=Path(root);p=check_plan(root);cfg=B.S.load_r2_settings();client=B.S.build_client(cfg)
    guard=CostGuard(Path(p['cost_ledger']),plan_cost_policy(p));client.meta.events.register('before-send.s3.*',guard.before_send)
    entry=source_entry(root,day,symbol,client,cfg.bucket)
    original_publish=B.S.publish_file_immutable
    def budgeted_publish(client,bucket,path,key,**kwargs):
        guard.reserve(key,Path(path).stat().st_size)
        return original_publish(client,bucket,path,key,**kwargs)
    B.S.publish_file_immutable=budgeted_publish
    seconds=probe_seconds if probe else 57600
    if probe and not 1<=seconds<57600:raise ValueError('probe seconds must be below full-session coverage')
    probe_suffix=('_probe' if seconds==21600 else f'_probe_{seconds}s') if probe else ''
    name=f'{day}_{symbol}{probe_suffix}'
    key=f"derived/tape_feature_runs/{p['run_id']}/receipts/{name}.json"
    prior=B.STORE.read_manifest(client,cfg.bucket,key)
    if prior is not None:
        identity=B.STORE.partition_identity(entry,POLICY,seconds)
        if prior['measurement']['partition_identity']!=identity:raise ValueError('receipt belongs to different features')
        if B.STORE.recover_features(client,cfg.bucket,identity) is None or B.STORE.recover_summary(client,cfg.bucket,identity,B.summary_contract()) is None:raise ValueError('receipt references missing objects')
        save(root/'receipts'/f'{name}.json',prior);return
    result=B.STORE.materialize_features(client,cfg.bucket,entry,POLICY,seconds,scratch,
        summary_contract=B.summary_contract(),summarizer=B.summarize)
    identity=B.STORE.partition_identity(entry,POLICY,seconds)
    report=dict(day=day,symbol=symbol,probe=probe,recovered=result['recovered'],feature_prefix=B.STORE.feature_prefix(identity),
        summary_prefix=B.STORE.retrieval_prefix(identity,B.summary_contract()),measurement=result['measurement'],summary=result['summary'],
        official_reference=entry['official_reference'],queue_plan_sha256=B.S.sha256_file(root/'plan.json'))
    # Shared ledger reserves durable feature bytes too. Single bounded objects are
    # uploaded by the existing store; this predeclared envelope also caps total output.
    artifact=Path(scratch)/'receipt.json';save(artifact,report)
    name=f'{day}_{symbol}{probe_suffix}'
    key=f"derived/tape_feature_runs/{p['run_id']}/receipts/{name}.json"
    B.S.publish_file_immutable(client,cfg.bucket,artifact,key,upload_workers=1,content_type='application/json')
    save(root/'receipts'/f'{name}.json',report)


def counts(root):
    with db(root) as c:
        states={r['state']:r['n'] for r in c.execute('SELECT state,COUNT(*) n FROM jobs GROUP BY state')}
        august={r['state']:r['n'] for r in c.execute('SELECT state,COUNT(*) n FROM jobs WHERE priority=0 GROUP BY state')}
    return dict(counts=states,august=august)

def alive(root):
    import psutil
    try:
        s=read(Path(root)/'status.json');p=psutil.Process(s['pid'])
        return p.is_running() and abs(p.create_time()-s['process_created'])<.01 and str(Path(__file__).resolve()) in p.cmdline()
    except (FileNotFoundError,KeyError,psutil.Error):return False

def scratch_size(path):
    total=0
    for f in Path(path).rglob('*'):
        try:
            if f.is_file():total+=f.stat().st_size
        except FileNotFoundError:pass
    return total


def supervised(root,command,name,state,timeout=120):
    import psutil
    root=Path(root);scratch=root/'scratch'/name
    if scratch.exists():raise ValueError('previous scratch requires inspection: '+str(scratch))
    scratch.mkdir(parents=True);log=root/'logs'/f'{name}.log';log.parent.mkdir(exist_ok=True)
    started=time.monotonic();peak=0;peak_disk=0;reason=None
    with log.open('a') as output:
        p=subprocess.Popen([*command,'--scratch',str(scratch)],stdout=output,stderr=subprocess.STDOUT,start_new_session=True,
            env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1'))
        last=0
        try:
            while p.poll() is None:
                rss=psutil.Process().memory_info().rss
                try:
                    proc=psutil.Process(p.pid)
                    for x in [proc,*proc.children(recursive=True)]:
                        try:rss+=x.memory_info().rss
                        except psutil.NoSuchProcess:pass
                except psutil.NoSuchProcess:pass
                peak=max(peak,rss);elapsed=time.monotonic()-started
                if elapsed-last>=1:
                    disk=scratch_size(scratch);peak_disk=max(peak_disk,disk)
                    if disk>512*1024**2:reason='scratch_limit'
                    if shutil.disk_usage(root).free<3*1024**3:reason='free_disk_limit'
                    state.update(active_seconds=elapsed,active_peak_rss_bytes=peak,updated_at=now());save(root/'status.json',state);last=elapsed
                if rss>=int(2.8*1024**3):reason='memory_limit'
                if elapsed>=timeout:reason='timeout'
                if (root/'STOP').exists():reason='user_stop'
                if reason:break
                time.sleep(.05)
        finally:
            if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=2)
    report=dict(elapsed_seconds=time.monotonic()-started,peak_aggregate_rss_bytes=peak,peak_scratch_bytes=peak_disk,
                stop_reason=reason,returncode=p.returncode,timeout_seconds=timeout)
    save(root/'resources'/f'{name}.json',report)
    if p.returncode==0 and not reason:shutil.rmtree(scratch)
    else:raise RuntimeError(f'{name}: {reason or "worker_failed"}; inspect {log}')
    return report

def daemon(root):
    import psutil
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    with (root/'lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        p=check_plan(root);me=psutil.Process();os.nice(10)
        state=dict(state='starting',pid=me.pid,process_created=me.create_time(),started_at=now(),**counts(root))
        save(root/'status.json',state);awake=None;last_discovery=0;producer_complete_scan=False
        try:
            if sys.platform=='darwin':awake=subprocess.Popen(['caffeinate','-i','-w',str(me.pid)])
            while not (root/'STOP').exists():
                check_plan(root)
                if time.monotonic()-last_discovery>=600:
                    state.update(state='discovering',active=None)
                    supervised(root,[sys.executable,__file__,'_discover','--root',str(root)],'discovery_'+str(time.time_ns()),state)
                    last_discovery=time.monotonic()
                with db(root) as c:
                    row=c.execute('SELECT * FROM jobs WHERE state!="complete" ORDER BY priority,day,symbol LIMIT 1').fetchone()
                if row is None:
                    producer=ROOT/'research/tq_volatility_6m_incremental_20260907/status.json'
                    producer_state=read(producer)['state'] if producer.exists() else 'unknown'
                    if producer_state=='complete':
                        if producer_complete_scan:
                            state.update(state='complete',active=None,updated_at=now(),**counts(root));save(root/'status.json',state);return
                        producer_complete_scan=True;last_discovery=0;continue
                    if producer_state not in ('running','starting'):
                        raise ValueError('queue drained; acquisition requires attention: '+producer_state)
                    state.update(state='waiting_for_downloads',active=None,**counts(root),updated_at=now());save(root/'status.json',state)
                    time.sleep(5);continue
                if row['state']!='pending':raise ValueError('prior failed/interrupted partition requires diagnosis before resume')
                day,symbol=row['day'],row['symbol'];name=f'{day}_{symbol}'
                with db(root) as c:c.execute('UPDATE jobs SET state="running" WHERE day=? AND symbol=?',(day,symbol))
                state.update(state='running',active=dict(day=day,symbol=symbol),**counts(root))
                resources=supervised(root,[sys.executable,__file__,'_work','--root',str(root),'--day',day,'--symbol',symbol],name,state)
                result=read(root/'receipts'/f'{name}.json')
                with db(root) as c:c.execute('UPDATE jobs SET state="complete",result=? WHERE day=? AND symbol=?',(json.dumps(dict(feature_prefix=result['feature_prefix'],rows=result['measurement']['counts']['rows'],bytes=result['measurement']['bytes'],resources=resources)),day,symbol))
                state.update(last_complete=dict(day=day,symbol=symbol,seconds=resources['elapsed_seconds']),**counts(root));save(root/'status.json',state)
            state.update(state='stopped',updated_at=now());save(root/'status.json',state)
        except BaseException as e:
            state.update(state='failed',error=str(e),updated_at=now(),**counts(root));save(root/'status.json',state)
            raise
        finally:
            if awake and awake.poll() is None:awake.terminate()


def plan(root):
    import run_bounded_tape_retrieval as B
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    if (root/'plan.json').exists():raise FileExistsError('immutable plan already exists')
    # Bind optimized representative proof, not a cached-recovery measurement.
    bench=ROOT/'research/tape_feature_optimization_20260908'
    evidence={}
    for case in ('sparse','dense','halted'):
        result=read(bench/f'{case}_final.json');resources=read(bench/f'{case}_final.resources.json')
        if result['current_implementation']!=B.F.identity() or resources['returncode'] or resources['stop_reason']:raise ValueError('optimization checkpoint no longer matches')
        if resources['peak_aggregate_rss_bytes']>=2*1024**3:raise ValueError('checkpoint lacks memory headroom')
        evidence[case]=dict(result_sha256=B.S.sha256_file(bench/f'{case}_final.json'),resources=resources,raw_stats=result['raw_stats'],medians=result['medians'])
    p=dict(version='tape_feature_queue_v1',run_id='august_then_chronological_20260908',created_at=now(),implementation=code_identity(),
        priority_dates=PRIORITY_DATES,halt_policy=POLICY,
        feature_seconds=57600,timeout_seconds=120,rss_target_bytes=2*1024**3,rss_stop_bytes=int(2.8*1024**3),scratch_stop_bytes=512*1024**2,
        free_disk_reserve_bytes=3*1024**3,discovery_interval_seconds=600,checkpoint=evidence,
        cost_ledger=str(ROOT/'research/tq_volatility_6m_incremental_20260907/cost_ledger.sqlite'),
        authorization='User requested start now: August 27-31 first, then earliest downloaded T/Q chronologically; follow new downloads, immutable R2 features, ten-minute heartbeat with bug repair.')
    save(root/'plan.json',p)
    with db(root):pass
    print(json.dumps(dict(root=str(root),state='planned',priority_dates=PRIORITY_DATES)))

def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('action',choices=['plan','start','status','stop','discover','_daemon','_discover','_work','probe'])
    ap.add_argument('--probe-worker',action='store_true',help=argparse.SUPPRESS)
    ap.add_argument('--root',type=Path,default=DEFAULT);ap.add_argument('--day');ap.add_argument('--symbol');ap.add_argument('--scratch',type=Path)
    a=ap.parse_args();root=a.root.resolve()
    if a.action=='plan':plan(root)
    elif a.action=='status':
        s=read(root/'status.json') if (root/'status.json').exists() else {'state':'not_started'}
        print(json.dumps(dict(s,process_alive=alive(root),**{k:v for k,v in counts(root).items() if k not in s}),indent=2))
    elif a.action=='stop':(root/'STOP').touch()
    elif a.action=='start':
        check_plan(root)
        checkpoint=read(root/'checkpoint.json')
        if checkpoint['implementation']!=code_identity() or checkpoint['resources']['returncode'] or checkpoint['resources']['stop_reason'] or checkpoint['recovered']:raise ValueError('queue checkpoint invalid')
        if checkpoint['resources']['peak_aggregate_rss_bytes']>=2*1024**3 or checkpoint['projected_full_seconds']>=90:raise ValueError('queue checkpoint lacks headroom')
        if alive(root):raise ValueError('already running')
        if (root/'STOP').exists():raise ValueError('explicit stop present; do not auto-resume')
        if (root/'status.json').exists() and read(root/'status.json')['state']=='failed':raise ValueError('failed run requires diagnosis; no automatic restart')
        with (root/'supervisor.log').open('a') as log:
            p=subprocess.Popen([sys.executable,__file__,'_daemon','--root',str(root)],stdout=log,stderr=subprocess.STDOUT,start_new_session=True,stdin=subprocess.DEVNULL)
        print(json.dumps(dict(state='launched',pid=p.pid)))
    elif a.action=='_daemon':daemon(root)
    elif a.action=='_discover':discover(root)
    elif a.action=='discover':
        check_plan(root)
        supervised(root,[sys.executable,__file__,'_discover','--root',str(root)],'discovery_'+str(time.time_ns()),dict(state='discovering'))
        print(json.dumps(counts(root)))
    elif a.action=='_work':work(root,a.day,a.symbol,a.scratch,probe=a.probe_worker)
    elif a.action=='probe':
        check_plan(root);state=dict(state='probe',active=dict(day=a.day,symbol=a.symbol))
        resources=supervised(root,[sys.executable,__file__,'_work','--root',str(root),'--day',a.day,'--symbol',a.symbol,'--probe-worker'],f'{a.day}_{a.symbol}_probe',state)
        result=read(root/'receipts'/f'{a.day}_{a.symbol}_probe.json');sample=result['measurement'];plan_=check_plan(root)
        consumed=sum(sample['raw_stats'].values());compute=sample['feature_and_verify_seconds']+result['summary']['summary_seconds']
        source=result['measurement']['partition_identity']['source']
        full_rows=sum(source[s]['rows'] for s in ('quotes','trades'))
        scale=max(57600/21600,full_rows/max(1,consumed))
        projected=2*(max(0,resources['elapsed_seconds']-compute)+compute*scale)
        save(root/'checkpoint.json',dict(implementation=code_identity(),resources=resources,recovered=result['recovered'],raw_rows=consumed,projected_full_seconds=projected))
        print(json.dumps(read(root/'checkpoint.json')['resources']));print('Projected largest full partition seconds:',projected)

if __name__=='__main__':main()
