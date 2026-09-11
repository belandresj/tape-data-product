"""Bounded multi-worker feature queue; unchanged feature/store math and data batches.

Atomic SQLite claims, one supervisor, independently isolated symbol-day children.
RAM O(workers*(25k raw columns + 300 rolling state + 1024 output)), queue on disk.
Combined 2.8 GiB RSS/512 MiB scratch stops; each child deadline 120s; 3 GiB
free-disk reserve. Reference normalization is serialized by a file lock.
"""
from __future__ import annotations
import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import traceback
import psutil
import run_tape_feature_queue as Q

ROOT=Q.ROOT
DEFAULT=ROOT/'research/tape_feature_queue_two_workers_20260908'


def verify(root):
    plan=Q.check_plan(root)
    import run_bounded_tape_retrieval as B
    if plan['supervisor_sha256']!=B.S.sha256_file(Path(__file__)):raise ValueError('two-worker supervisor changed; new immutable plan/checkpoint required')
    if not 1<=plan['workers']<=8:raise ValueError('invalid worker count')
    if not 1<=plan.get('max_worker_attempts',2)<=3:raise ValueError('invalid worker retry count')
    if not 1<=plan.get('max_partition_failures',25)<=100:raise ValueError('invalid partition failure limit')
    if not 1<=plan.get('max_inflight_source_bytes',1)<=512*1024**2:raise ValueError('invalid in-flight source-byte budget')
    return plan


def init_db(root):
    with Q.db(root) as c:
        if 'owner' not in {r['name'] for r in c.execute('PRAGMA table_info(jobs)')}:c.execute('ALTER TABLE jobs ADD COLUMN owner TEXT')
        c.execute('CREATE TABLE IF NOT EXISTS failures(day TEXT,symbol TEXT,attempt INTEGER,at TEXT,error TEXT,resource TEXT,PRIMARY KEY(day,symbol,attempt))')


def claim(root,owner,max_source_bytes=None):
    with Q.db(root) as c:
        c.execute('BEGIN IMMEDIATE')
        bad=c.execute('SELECT day,symbol,state FROM jobs WHERE state NOT IN ("pending","running","complete","failed") LIMIT 1').fetchone()
        if bad:raise ValueError('failed/interrupted partition requires diagnosis: '+str(dict(bad)))
        # All priority-date work must finish before dispatching ordinary backlog.
        priority=c.execute('SELECT MIN(priority) FROM jobs WHERE state IN ("pending","running")').fetchone()[0]
        if priority is None:return None
        row=c.execute('SELECT j.*,COALESCE(SUM(o.bytes),0) source_bytes FROM jobs j LEFT JOIN objects o USING(day,symbol) '
            'WHERE j.state="pending" AND j.priority=? GROUP BY j.day,j.symbol ORDER BY j.day,j.symbol LIMIT 1',(priority,)).fetchone()
        if row is None:return None
        if max_source_bytes is not None and row['source_bytes']>max_source_bytes:return None
        changed=c.execute('UPDATE jobs SET state="running",owner=? WHERE day=? AND symbol=? AND state="pending"',
                          (owner,row['day'],row['symbol'])).rowcount
        if changed!=1:raise RuntimeError('atomic claim failed')
        return dict(row)


def complete(root,job,owner,result):
    with Q.db(root) as c:
        changed=c.execute('UPDATE jobs SET state="complete",result=?,owner=NULL WHERE day=? AND symbol=? AND state="running" AND owner=?',
            (json.dumps(result),job['day'],job['symbol'],owner)).rowcount
        if changed!=1:raise ValueError('completion does not own claim')

def fail(root,job,owner,resource):
    failure=resource.get('failure') or {'type':'unknown','message':'worker exited without structured failure'}
    attempt=int(resource.get('attempt',1))
    with Q.db(root) as c:
        changed=c.execute('UPDATE jobs SET state="failed",result=?,owner=NULL WHERE day=? AND symbol=? AND state="running" AND owner=?',
            (json.dumps(dict(failure=failure,resources=resource)),job['day'],job['symbol'],owner)).rowcount
        if changed!=1:raise ValueError('failure does not own claim')
        c.execute('INSERT OR REPLACE INTO failures VALUES (?,?,?,?,?,?)',
            (job['day'],job['symbol'],attempt,Q.now(),json.dumps(failure),json.dumps(resource)))


def active_identity(root):
    try:
        state=Q.read(Path(root)/'status.json');p=psutil.Process(state['pid'])
        return p.is_running() and abs(p.create_time()-state['process_created'])<.01 and str(Path(__file__).resolve()) in p.cmdline()
    except (OSError,KeyError,psutil.Error):return False


class Pool:
    def __init__(self,root,state,timeout=120,workers=2,max_attempts=2,retry_backoff_seconds=1):
        self.root=Path(root);self.state=state;self.timeout=timeout;self.workers=workers;self.max_attempts=max_attempts;self.retry_backoff_seconds=retry_backoff_seconds
        self.active={};self.peak=0;self.disk_peak=0;self.minimum_available=psutil.virtual_memory().available
        self.last_status=0;self.low_since=None
    def launch(self,job,command=None,probe=False,probe_seconds=21600,attempt=1):
        if len(self.active)>=self.workers:raise ValueError('worker cap')
        suffix=('_probe_v2' if probe_seconds==21600 else f'_probe_v2_{probe_seconds}s') if probe else ''
        name=f"{job['day']}_{job['symbol']}{suffix}"
        scratch=self.root/'scratch'/name
        if scratch.exists():raise ValueError('stale scratch requires inspection: '+str(scratch))
        scratch.mkdir(parents=True)
        logs=self.root/'logs';logs.mkdir(exist_ok=True);log=(logs/f'{name}.log').open('a')
        cmd=command or [sys.executable,__file__,'_work','--root',str(self.root),'--day',job['day'],'--symbol',job['symbol']]+(['--probe-worker','--probe-seconds',str(probe_seconds)] if probe else [])
        try:p=subprocess.Popen([*cmd,'--scratch',str(scratch)],stdout=log,stderr=subprocess.STDOUT,start_new_session=True,
                    env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1'))
        except BaseException:log.close();raise
        self.active[name]=dict(job=job,process=p,log=log,scratch=scratch,started=time.monotonic(),peak=0,disk_peak=0,probe=probe,
            probe_seconds=probe_seconds,command=command,attempt=attempt)
    def _archive_attempt(self,name,value):
        source=value['scratch'];target=self.root/'failed_attempts'/f"{name}_attempt{value['attempt']}"
        target.parent.mkdir(exist_ok=True)
        if target.exists():raise ValueError('existing failed-attempt evidence: '+str(target))
        source.replace(target)
        return target
    def tick(self):
        me=psutil.Process();rss=me.memory_info().rss
        for child in me.children(recursive=True):
            try:rss+=child.memory_info().rss
            except psutil.NoSuchProcess:pass
        self.peak=max(self.peak,rss);available=psutil.virtual_memory().available;self.minimum_available=min(self.minimum_available,available)
        now=time.monotonic();reason=None
        if rss>=int(2.8*1024**3):reason='aggregate_memory_limit'
        self.low_since=(self.low_since or now) if available<512*1024**2 else None
        if self.low_since and now-self.low_since>2:reason='system_available_memory_below_512MiB'
        if now-self.last_status>=1:
            disk=Q.scratch_size(self.root/'scratch');self.disk_peak=max(self.disk_peak,disk)
            if disk>512*1024**2:reason='combined_scratch_limit'
            if shutil.disk_usage(self.root).free<3*1024**3:reason='free_disk_limit'
            self.state.update(active=[dict(day=v['job']['day'],symbol=v['job']['symbol'],pid=v['process'].pid,seconds=round(now-v['started'],2),attempt=v['attempt']) for v in self.active.values()],
                active_workers=len(self.active),peak_aggregate_rss_bytes=self.peak,peak_combined_scratch_bytes=self.disk_peak,
                minimum_available_bytes=self.minimum_available,updated_at=Q.now())
            Q.save(self.root/'status.json',self.state);self.last_status=now
        if (self.root/'STOP').exists():reason='user_stop'
        finished=[]
        for name,v in list(self.active.items()):
            v['peak']=max(v['peak'],rss);v['disk_peak']=max(v['disk_peak'],self.disk_peak)
            elapsed=now-v['started'];p=v['process']
            if p.poll() is not None:
                resource=dict(elapsed_seconds=elapsed,peak_aggregate_rss_bytes=v['peak'],peak_combined_scratch_bytes=v['disk_peak'],attempt=v['attempt'],
                              returncode=p.returncode,stop_reason=None if p.returncode==0 else 'worker_failed',timeout_seconds=self.timeout)
                failure_path=v['scratch']/'failure.json'
                if failure_path.exists():resource['failure']=Q.read(failure_path)
                resource_name=name if v['attempt']==1 else f"{name}_attempt{v['attempt']}"
                Q.save(self.root/'resources'/f'{resource_name}.json',resource);v['log'].close();del self.active[name]
                if p.returncode:
                    failure=resource.get('failure',{})
                    if failure.get('global_fatal'):raise RuntimeError(name+': '+failure.get('message','global worker failure'))
                    if failure.get('retryable') and v['attempt']<self.max_attempts:
                        resource['evidence_path']=str(self._archive_attempt(name,v));Q.save(self.root/'resources'/f'{resource_name}.json',resource)
                        time.sleep(self.retry_backoff_seconds*(2**(v['attempt']-1)))
                        self.launch(v['job'],v['command'],v['probe'],v['probe_seconds'],v['attempt']+1)
                        continue
                    finished.append((v['job'],resource));continue
                shutil.rmtree(v['scratch']);finished.append((v['job'],resource))
            elif elapsed>=self.timeout:reason='worker_timeout:'+name
        if reason:raise RuntimeError(reason)
        return finished
    def stop(self,reason):
        for name,v in list(self.active.items()):
            p=v['process']
            if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=2)
            v['log'].close()
            Q.save(self.root/'resources'/f'{name}.json',dict(elapsed_seconds=time.monotonic()-v['started'],
                peak_aggregate_rss_bytes=v['peak'],peak_combined_scratch_bytes=self.disk_peak,returncode=p.returncode,stop_reason=reason,timeout_seconds=self.timeout))
        self.active.clear()


def failure_record(error):
    name=f'{type(error).__module__}.{type(error).__name__}'
    message=str(error)
    status=None
    response=getattr(error,'response',None)
    if isinstance(response,dict):status=response.get('ResponseMetadata',{}).get('HTTPStatusCode')
    retryable=(type(error).__name__ in {'EndpointConnectionError','ConnectionClosedError','ConnectTimeoutError','ReadTimeoutError','HTTPClientError'}
        or status in {408,429,500,502,503,504})
    global_fatal=(type(error).__name__=='CostLimit' or any(token in message for token in (
        'code changed since queue plan','cost policy changed since queue plan','supervisor changed','R2 class A request budget reached',
        'R2 class B request budget reached','publication budget would be exceeded')))
    return dict(type=name,message=message,http_status=status,retryable=bool(retryable),global_fatal=bool(global_fatal),traceback=traceback.format_exc(limit=20))

def work(args):
    verify(args.root)
    original=Q.official_reference
    def locked_reference(root,day,source):
        # Shared normalization/cache files must never have concurrent writers.
        with (Path(root)/'reference.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            return original(root,day,source)
    Q.official_reference=locked_reference
    try:Q.work(args.root,args.day,args.symbol,args.scratch,probe=args.probe_worker,probe_seconds=args.probe_seconds)
    except BaseException as error:
        Q.save(Path(args.scratch)/'failure.json',failure_record(error));raise


def daemon(root):
    root=Path(root);plan=verify(root);init_db(root)
    with (root/'lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        with Q.db(root) as c:
            if c.execute('SELECT 1 FROM jobs WHERE state NOT IN ("pending","complete","failed") LIMIT 1').fetchone():raise ValueError('unfinished claims require diagnosis')
        me=psutil.Process();os.nice(10);owner=f'{me.pid}:{me.create_time()}'
        state=dict(state='starting',pid=me.pid,process_created=me.create_time(),started_at=Q.now(),workers=plan['workers'],**Q.counts(root))
        Q.save(root/'status.json',state);pool=Pool(root,state,plan['timeout_seconds'],plan['workers'],plan.get('max_worker_attempts',2),plan.get('retry_backoff_seconds',1));last_discovery=0;producer_complete_scan=False;awake=None
        try:
            if sys.platform=='darwin':awake=subprocess.Popen(['caffeinate','-i','-w',str(me.pid)])
            while not (root/'STOP').exists():
                for job,resource in pool.tick():
                    if resource['returncode']==0:
                        name=f"{job['day']}_{job['symbol']}";result=Q.read(root/'receipts'/f'{name}.json')
                        complete(root,job,owner,dict(feature_prefix=result['feature_prefix'],rows=result['measurement']['counts']['rows'],bytes=result['measurement']['bytes'],resources=resource))
                        state.update(last_complete=dict(day=job['day'],symbol=job['symbol'],seconds=resource['elapsed_seconds']),**Q.counts(root))
                    else:
                        fail(root,job,owner,resource);state.update(last_failure=dict(day=job['day'],symbol=job['symbol'],failure=resource.get('failure')),**Q.counts(root))
                        if state['counts'].get('failed',0)>=plan.get('max_partition_failures',25):raise RuntimeError('partition failure limit reached')
                draining=(root/'DRAIN').exists()
                if draining and not pool.active:break
                discovery_due=time.monotonic()-last_discovery>=600
                if discovery_due and not pool.active:
                    verify(root);state.update(state='discovering',active=[],active_workers=0)
                    Q.supervised(root,[sys.executable,Q.__file__,'_discover','--root',str(root)],'discovery_'+str(time.time_ns()),state)
                    last_discovery=time.monotonic();continue
                if not draining and not discovery_due:
                    while len(pool.active)<plan['workers']:
                        verify(root)
                        used=sum(v['job'].get('source_bytes',0) for v in pool.active.values())
                        job=claim(root,owner,plan.get('max_inflight_source_bytes',2**63-1)-used)
                        if job is None:break
                        pool.launch(job);state.update(state='running',**Q.counts(root))
                if not pool.active and not draining:
                    with Q.db(root) as c:pending=c.execute('SELECT COUNT(*) FROM jobs WHERE state IN ("pending","running")').fetchone()[0]
                    if pending:raise ValueError('unclaimed unfinished work requires diagnosis')
                    producer=Q.read(ROOT/'research/tq_volatility_6m_incremental_20260907/status.json')['state']
                    if producer not in ('running','starting'):
                        if producer_complete_scan:
                            final='complete_with_failures' if Q.counts(root)['counts'].get('failed',0) else 'complete'
                            state.update(state=final,active=[],active_workers=0,**Q.counts(root),updated_at=Q.now());Q.save(root/'status.json',state);return
                        producer_complete_scan=True;last_discovery=0;continue
                    state.update(state='waiting_for_downloads',active=[],active_workers=0,**Q.counts(root));time.sleep(2)
                time.sleep(.05)
            pool.stop('user_stop' if (root/'STOP').exists() else 'drained')
            state.update(state='stopped',active=[],active_workers=0,updated_at=Q.now(),**Q.counts(root));Q.save(root/'status.json',state)
        except BaseException as e:
            pool.stop(str(e))
            with Q.db(root) as c:c.execute('UPDATE jobs SET state="interrupted" WHERE state="running" AND owner=?',(owner,))
            state.update(state='failed',error=str(e),active=[],active_workers=0,updated_at=Q.now(),**Q.counts(root));Q.save(root/'status.json',state);raise
        finally:
            if awake and awake.poll() is None:awake.terminate()


def checkpoint(root,jobs=None,probe_seconds=21600):
    import run_bounded_tape_retrieval as B
    root=Path(root);plan=verify(root);state=dict(state='checkpoint',workers=plan['workers']);pool=Pool(root,state,plan['timeout_seconds'],plan['workers'],1)
    jobs=jobs or [dict(day='2026-01-26',symbol='BATL'),dict(day='2026-08-31',symbol='HPE')]
    if not 1<=len(jobs)<=plan['workers']:raise ValueError('checkpoint pair count must fit configured workers')
    with Q.db(root) as con:
        checkpoint_source_bytes=sum(con.execute('SELECT COALESCE(SUM(bytes),0) FROM objects WHERE day=? AND symbol=?',
            (job['day'],job['symbol'])).fetchone()[0] for job in jobs)
    if checkpoint_source_bytes>plan.get('max_inflight_source_bytes',2**63-1):raise ValueError('checkpoint exceeds in-flight source-byte budget')
    started=time.monotonic();resources={}
    try:
        for job in jobs:pool.launch(job,probe=True,probe_seconds=probe_seconds)
        while pool.active:
            for job,resource in pool.tick():
                if resource['returncode']!=0:raise RuntimeError('checkpoint worker failed: '+str(resource.get('failure')))
                resources[(job['day'],job['symbol'])]=resource
            time.sleep(.05)
        suffix='_probe' if probe_seconds==21600 else f'_probe_{probe_seconds}s'
        results=[Q.read(root/'receipts'/f"{j['day']}_{j['symbol']}{suffix}.json") for j in jobs]
        if any(r['recovered'] for r in results):raise ValueError('checkpoint cache hit cannot prove replay resources')
        projections=[]
        for job,result in zip(jobs,results):
            resource=resources[(job['day'],job['symbol'])]
            measurement=result['measurement'];source=measurement['partition_identity']['source']
            full_rows=sum(source[s]['rows'] for s in ('quotes','trades'))
            probed_rows=sum(measurement['raw_stats'].values())
            compute=measurement['feature_and_verify_seconds']+result['summary']['summary_seconds']
            scale=max(57600/probe_seconds,full_rows/max(1,probed_rows))
            projected=max(0,resource['elapsed_seconds']-compute)+compute*scale
            projections.append(dict(day=job['day'],symbol=job['symbol'],source_rows=full_rows,
                source_bytes=sum(source[s]['size_bytes'] for s in ('quotes','trades')),
                probed_rows=probed_rows,probe_seconds=resource['elapsed_seconds'],scale=scale,
                projected_full_seconds=projected,projected_full_seconds_with_20pct_margin=projected*1.2))
        report=dict(supervisor_sha256=B.S.sha256_file(Path(__file__)),implementation=Q.code_identity(),workers=plan['workers'],
            elapsed_seconds=time.monotonic()-started,peak_aggregate_rss_bytes=pool.peak,peak_combined_scratch_bytes=pool.disk_peak,
            minimum_available_bytes=pool.minimum_available,resources=[resources[(j['day'],j['symbol'])] for j in jobs],recovered=False,
            raw_rows=sum(sum(r['measurement']['raw_stats'].values()) for r in results),
            feature_rows=sum(r['measurement']['counts']['rows'] for r in results),checkpoint_source_bytes=checkpoint_source_bytes,projections=projections)
        Q.save(root/'checkpoint_v2.json',report);print(json.dumps(report))
    finally:pool.stop('checkpoint_exit')


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['status','start','stop','drain','checkpoint','_daemon','_work'])
    p.add_argument('--root',type=Path,default=DEFAULT);p.add_argument('--day');p.add_argument('--symbol');p.add_argument('--scratch',type=Path);p.add_argument('--probe-worker',action='store_true');p.add_argument('--probe-seconds',type=int,default=21600)
    p.add_argument('--checkpoint-pair',action='append',default=[],metavar='DATE:SYMBOL')
    p.add_argument('--checkpoint-seconds',type=int,default=21600)
    a=p.parse_args();root=a.root.resolve()
    if a.action=='status':
        s=Q.read(root/'status.json') if (root/'status.json').exists() else {'state':'not_started'}
        print(json.dumps(dict(s,process_alive=active_identity(root)),indent=2))
    elif a.action in ('stop','drain'):(root/('STOP' if a.action=='stop' else 'DRAIN')).touch()
    elif a.action=='_work':work(a)
    elif a.action=='_daemon':daemon(root)
    elif a.action=='checkpoint':
        jobs=[]
        for value in a.checkpoint_pair:
            day,separator,symbol=value.partition(':')
            if not separator:raise ValueError('checkpoint pair must be DATE:SYMBOL')
            jobs.append(dict(day=day,symbol=symbol.upper()))
        checkpoint(root,jobs or None,a.checkpoint_seconds)
    elif a.action=='start':
        plan=verify(root);c=Q.read(root/'checkpoint_v2.json')
        if c['implementation']!=Q.code_identity() or c['supervisor_sha256']!=plan['supervisor_sha256'] or c['recovered'] or c['workers']!=plan['workers']:raise ValueError('checkpoint mismatch')
        if c['peak_aggregate_rss_bytes']>=2*1024**3 or c['peak_combined_scratch_bytes']>512*1024**2:raise ValueError('checkpoint lacks resource headroom')
        if max(x['projected_full_seconds_with_20pct_margin'] for x in c['projections'])>=plan['timeout_seconds']:raise ValueError('checkpoint projection exceeds child timeout')
        if active_identity(root):raise ValueError('already running')
        if any((root/x).exists() for x in ('STOP','DRAIN')):raise ValueError('stop/drain marker requires explicit resolution')
        if (root/'status.json').exists() and Q.read(root/'status.json')['state']=='failed':raise ValueError('failed run requires diagnosis')
        if Q.alive(Path(plan['predecessor_root'])):raise ValueError('predecessor feature queue still running')
        with (root/'supervisor.log').open('a') as log:
            child=subprocess.Popen([sys.executable,__file__,'_daemon','--root',str(root)],stdout=log,stderr=subprocess.STDOUT,start_new_session=True,stdin=subprocess.DEVNULL)
        print(json.dumps(dict(state='launched',pid=child.pid,workers=plan['workers'])))

if __name__=='__main__':main()
