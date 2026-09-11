"""Five-date, official-only research retrieval. Every replay is watchdog bounded.

No changes to upstream math. RAM O(25000 raw rows + 1024 output rows + 300
rolling observations + 300 summary rows); time inherited O(R + S*F*300).
Only one partition per invocation. R2 is durable; scratch is disposable.
"""
from __future__ import annotations
import argparse
from dataclasses import asdict
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import run_tape_snapshot_pipeline as P
import tape_feature_store as STORE

F, S, I = P.FEATURE, P.S, P.INV
VERSION = 'bounded_tape_retrieval_v1'
DATES = ['2026-03-17','2026-06-04','2026-06-03','2026-09-01','2026-09-02']
FIELDS = ['trade_rate_300s','dollar_rate_300s','midpoint_movement_bps_per_30s_300s',
          'quoted_spread_bps_300s','movement_to_spread_300s']
FLAGS = ['state_mature_300s','activity_support_valid_300s','movement_support_valid_300s',
         'movement_to_spread_valid_300s','state_fully_post_halt_300s']
MAX_PAIR_ROWS = 1_000_000
STOP = int(2.8*1024**3)


def row_count(s):
    return sum(s[k]['rows'] for k in ('trades','quotes'))


def select(sources, dates=None):
    dates = DATES if dates is None else dates
    if not dates or len(set(dates)) != len(dates): raise ValueError("dates must be nonempty and unique")
    out=[]
    for day in dates:
        pool=sorted((s for s in sources if s['session_date']==day and row_count(s)<=MAX_PAIR_ROWS),
                    key=lambda s:(row_count(s),s['symbol']))
        if not pool: raise ValueError(f'{day}: no bounded sources')
        chosen=pool if len(pool)<=4 else [pool[round(q*(len(pool)-1))] for q in (0,.33,.67,1)]
        if day=='2026-09-01':
            known=next(s for s in pool if s['symbol']=='BIAF')
            if known not in chosen: chosen[2]=known
        out.extend(chosen)
    if len({(s['session_date'],s['symbol']) for s in out})!=len(out): raise ValueError('duplicate selection')
    return out


def seal(d):
    return dict(d,plan_hash=I.digest(d))


def load(path):
    d=json.loads(Path(path).read_text()); h=d.pop('plan_hash')
    if I.digest(d)!=h: raise ValueError('plan identity mismatch')
    if d['feature_identity']!=F.identity() or d['runner_sha256']!=S.sha256_file(Path(__file__)):
        raise ValueError('implementation changed; create a new plan')
    if d.get('storage_version')==2 and (d['semantic_identity']!=STORE.semantic_identity() or d['store_sha256']!=S.sha256_file(Path(STORE.__file__))):
        raise ValueError('semantic contract changed; create a new plan')
    return dict(d,plan_hash=h)


def make_plan(base_path, target, dates=None):
    dates = DATES if dates is None else dates
    base,sources=P.load_plan(base_path,verify_implementation=False)
    selected=select(sources, dates)
    # Existing normalized checkpoint references already include carry-in sessions.
    reference=Path(base['reference_root']).parent/'checkpoint_current/external_halts_normalized.parquet'
    frame=P.metadata_frame(reference)
    entries=[]
    for source in selected:
        matching=frame[(frame.symbol==source['symbol']) & (frame.session_date==source['session_date'])]
        if matching.official_halt_start.isna().any() or matching.official_trade_resume_time.isna().any():
            raise ValueError('unresolved official halt bounds')
        entries.append(dict(source=source,halts=P.official_intervals(reference,source)))
    plan=seal(dict(version=VERSION,storage_version=2,summary_contract=summary_contract(),semantic_identity=STORE.semantic_identity(),store_sha256=S.sha256_file(Path(STORE.__file__)),runner_sha256=S.sha256_file(Path(__file__)),feature_identity=F.identity(),
        source_snapshot_hash=base['snapshot_hash'],reference_sha256=S.sha256_file(reference),
        halt_policy='official-only historical research; inferred registry unvalidated',
        selection='requested fixed dates, up to four raw-row strata among <=1M-row pairs per date; BIAF known halt control when selected; not volatility selection',
        dates=dates,entries=entries,total_raw_rows=sum(row_count(s) for s in selected),
        max_pair_rows=max(row_count(s) for s in selected),seconds=57600,
        timeout_seconds=120,target_rss_bytes=2*1024**3,stop_rss_bytes=STOP,
        sample_seconds=21600,summary_valid_fraction=.8))
    if Path(target).exists(): raise FileExistsError(target)
    I.save(target,plan)
    return plan


def eligible(r):
    return not r['halt_interval_active'] and all(r[k] for k in FLAGS) and all(
        r[k] is not None and math.isfinite(r[k]) for k in FIELDS)


def window_record(rows, width):
    good=[r for r in rows if eligible(r)]
    out=dict(session_date=rows[0]['session_date'],symbol=rows[0]['symbol'],
             window_seconds=width,interval_end_ns=rows[-1]['interval_end_ns'],
             valid_fraction=len(good)/width,eligible=len(good)>=math.ceil(.8*width))
    for field in FIELDS:
        out['median_'+field]=float(np.median([r[field] for r in good])) if good else None
    # Minute-bar-like controls derived from accepted per-second activity, not vendor bars.
    out['window_trades']=sum(r['primitive_trade_count'] or 0 for r in rows)
    out['window_dollars']=sum(r['primitive_dollars'] or 0 for r in rows)
    return out


def summarize(source, target):
    cols=['session_date','symbol','interval_end_ns','halt_interval_active',*FLAGS,*FIELDS,
          'primitive_trade_count','primitive_dollars']
    buffers={60:[],300:[]}; pending=[]; writer=None; count=0
    try:
        for batch in pq.ParquetFile(source).iter_batches(batch_size=1024,columns=cols,use_threads=False):
            for row in batch.to_pylist():
                for width,buf in buffers.items():
                    buf.append(row)
                    if len(buf)==width:
                        pending.append(window_record(buf,width));buf.clear()
                if len(pending)>=100:
                    table=pa.Table.from_pylist(pending, schema=summary_schema())
                    if writer is None: writer=pq.ParquetWriter(target,table.schema,compression='zstd')
                    writer.write_table(table);count+=len(pending);pending.clear()
        if pending:
            table=pa.Table.from_pylist(pending,schema=summary_schema())
            if writer is None: writer=pq.ParquetWriter(target,table.schema,compression='zstd')
            writer.write_table(table);count+=len(pending)
    finally:
        if writer: writer.close()
    return count


def summary_schema():
    return pa.schema([('session_date',pa.string()),('symbol',pa.string()),('window_seconds',pa.int64()),
        ('interval_end_ns',pa.int64()),('valid_fraction',pa.float64()),('eligible',pa.bool_()),
        *[('median_'+f,pa.float64()) for f in FIELDS],('window_trades',pa.float64()),('window_dollars',pa.float64())])


def summary_contract():
    return dict(version='tape_retrieval_summary_v1',fields=FIELDS,flags=FLAGS,
        windows_seconds=[60,300],anchor='04:00 ET',valid_fraction=.8,
        statistics='linear median on joint valid seconds; TQ-derived window trades/dollars')


def partition_identity(plan,index,probe):
    return STORE.partition_identity(plan['entries'][index],plan['halt_policy'],
        plan['sample_seconds'] if probe else plan['seconds'])


def prefix(plan,index,probe):
    if plan.get('storage_version')==2:
        return STORE.retrieval_prefix(partition_identity(plan,index,probe),summary_contract())
    s=plan['entries'][index]['source']
    return (f"derived/tape_retrieval/{plan['plan_hash']}/"+('probes/' if probe else 'features/')+
            f"session_date={s['session_date']}/symbol={s['symbol']}")


def recover(client,bucket,key,entry,config,seconds):
    result=P.recover_feature_partition(client,bucket,key,entry['source'],config,
                                     sample=seconds!=57600,expected_rows=seconds)
    if result:
        expected=S.ObjectIdentity(**result['measurement']['retrieval_identity'])
        S.validate_identities(expected,S.remote_identity(client,bucket,expected.object_key))
    return result


def worker(plan,index,probe,scratch,result_path):
    if plan.get('storage_version')!=2:raise ValueError('create a current plan before publishing; legacy objects remain immutable')
    entry=plan['entries'][index];seconds=plan['sample_seconds'] if probe else plan['seconds']
    settings=S.load_r2_settings();client=S.build_client(settings)
    result=STORE.materialize_features(client,settings.bucket,entry,plan['halt_policy'],seconds,scratch,
                                     summary_contract=summary_contract(),summarizer=summarize)
    # Keep the established checkpoint report interface without altering durable metadata.
    result['measurement']=dict(result['measurement'],
        compute_verify_summary_seconds=result['measurement']['feature_and_verify_seconds']+result['summary']['summary_seconds'],
        retrieval_rows=result['summary']['retrieval_rows'],retrieval_identity=result['summary']['retrieval_identity'])
    with tempfile.TemporaryDirectory(dir=scratch) as work:
        path=Path(work)/'plan.json';I.save(path,plan)
        S.publish_file_immutable(client,settings.bucket,path,f"derived/tape_retrieval/{plan['plan_hash']}/plan.json",
                                upload_workers=1,content_type='application/json')
    I.save(result_path,result)


def monitor(command,measurement,timeout=120):
    import psutil
    started=time.monotonic();peak=0;reason=None
    p=subprocess.Popen(command,start_new_session=True,env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1'))
    try:
        while p.poll() is None:
            rss=psutil.Process().memory_info().rss
            try:
                proc=psutil.Process(p.pid)
                for child in [proc,*proc.children(recursive=True)]:
                    try:rss+=child.memory_info().rss
                    except psutil.NoSuchProcess:pass
            except psutil.NoSuchProcess:pass
            peak=max(peak,rss)
            reason='timeout' if time.monotonic()-started>=timeout else 'memory' if rss>=STOP else None
            if reason:break
            time.sleep(.05)
    finally:
        if p.poll() is None:
            os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=2)
        I.save(measurement,dict(elapsed_seconds=time.monotonic()-started,peak_aggregate_rss_bytes=peak,
            stop_reason=reason,returncode=p.returncode,timeout_seconds=timeout))
    return p.returncode


def retrieve(plan, scratch, output, width=300, limit=10):
    import heapq
    settings=S.load_r2_settings();client=S.build_client(settings)
    heaps={f:[] for f in FIELDS};completed=[];missing=[];serial=0
    for index,entry in enumerate(plan['entries']):
        key=prefix(plan,index,False)
        if plan.get('storage_version')==2:
            identity=partition_identity(plan,index,False)
            found=STORE.recover_features(client,settings.bucket,identity)
            if found is not None:found=STORE.recover_summary(client,settings.bucket,identity,summary_contract())
        else:
            found=recover(client,settings.bucket,key,entry,plan['plan_hash'],57600)
        if found is None:missing.append(index);continue
        completed.append(index)
        path=Path(scratch)/'retrieval.parquet'
        S.download_verified_parquet(client,settings.bucket,key+'/retrieval.parquet',path,transfer_workers=1)
        for batch in pq.ParquetFile(path).iter_batches(batch_size=1024,use_threads=False):
            for row in batch.to_pylist():
                if row['window_seconds']!=width or not row['eligible']:continue
                # Default RTH view. Every returned window is wholly inside RTH.
                start,_=F.V.session_bounds(row['session_date'])
                end=(row['interval_end_ns']-start)//F.V.NS
                if end-width<19800 or end>43200:continue
                serial+=1
                for f,heap in heaps.items():
                    score=row['median_'+f]
                    if score is None:continue
                    item=(score,serial,row)
                    if len(heap)<limit:heapq.heappush(heap,item)
                    elif score>heap[0][0]:heapq.heapreplace(heap,item)
        path.unlink()
    I.save(output,dict(plan_hash=plan['plan_hash'],complete=len(completed)==len(plan['entries']),completed_indices=completed,
        missing_indices=missing,scope='RTH completed windows, selected universe; overlapping state histories are dependent',
        rankings={f:[v[2] for v in sorted(h,reverse=True)] for f,h in heaps.items()}))


def projection(plan, resources, sample):
    consumed=sum(sample['raw_stats'].values())
    scale=max(plan['seconds']/plan['sample_seconds'],plan['max_pair_rows']/max(1,consumed))
    compute=sample['compute_verify_summary_seconds']
    # Full raw-pair transfer already occurs during the probe. Prefix preparation,
    # uploads and setup are included in the fixed term; double total for headroom.
    fixed=max(0,resources['elapsed_seconds']-compute)
    return 2*(fixed+compute*scale)


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    sub=ap.add_subparsers(dest='action',required=True)
    p=sub.add_parser('plan');p.add_argument('--base-plan',required=True,type=Path);p.add_argument('--output',required=True,type=Path);p.add_argument('--dates',nargs='+')
    p=sub.add_parser('query');p.add_argument('--plan',required=True,type=Path);p.add_argument('--output',required=True,type=Path);p.add_argument('--worker',action='store_true',help=argparse.SUPPRESS);p.add_argument('--scratch',type=Path)
    p=sub.add_parser('partition');p.add_argument('--plan',required=True,type=Path);p.add_argument('--index',required=True,type=int)
    p.add_argument('--probe',action='store_true');p.add_argument('--output',required=True,type=Path)
    p.add_argument('--checkpoint',type=Path);p.add_argument('--full-run-approved',action='store_true')
    p.add_argument('--worker',action='store_true',help=argparse.SUPPRESS);p.add_argument('--scratch',type=Path)
    a=ap.parse_args()
    if a.action=='plan':
        p=make_plan(a.base_plan,a.output,a.dates);print(json.dumps({k:p[k] for k in ('plan_hash','total_raw_rows','max_pair_rows')}));return
    plan=load(a.plan)
    if a.action=='query':
        if a.worker:retrieve(plan,a.scratch,a.output.with_suffix('.result.json'));return
        with tempfile.TemporaryDirectory(prefix='tape-retrieval-query-') as scratch:
            code=monitor([sys.executable,__file__,*sys.argv[1:],'--worker','--scratch',scratch],a.output)
        raise SystemExit(code)
    if not 0<=a.index<len(plan['entries']):raise ValueError('index outside plan')
    if not a.probe:
        if not a.full_run_approved or not a.checkpoint:raise ValueError('measured checkpoint and post-measurement confirmation required')
        m=json.loads(a.checkpoint.read_text());r=json.loads(a.checkpoint.with_suffix('.result.json').read_text())
        if r.get('recovered'):raise ValueError('cached recovery is not a replay resource checkpoint')
        if m['returncode']!=0 or m['stop_reason'] or m['peak_aggregate_rss_bytes']>=plan['target_rss_bytes']:
            raise ValueError('checkpoint failed resource gate')
        if r['measurement']['contract']!=plan['feature_identity']:raise ValueError('checkpoint feature math mismatch')
        sample=r['measurement']
        if row_count(sample['provenance']['source'])<plan['max_pair_rows']:raise ValueError('checkpoint source smaller than largest selected source')
        if sample['expected_rows']!=plan['sample_seconds'] or not sample['sample']:raise ValueError('checkpoint coverage mismatch')
        if projection(plan,m,sample)>=90:raise ValueError('conservative full replay projection >=90s; optimize first')
    if a.worker:
        worker(plan,a.index,a.probe,a.scratch,a.output.with_suffix('.result.json'));return
    a.output.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='tape-retrieval-') as scratch:
        command=[sys.executable,__file__,*sys.argv[1:],'--worker','--scratch',scratch]
        code=monitor(command,a.output)
    print(a.output.read_text())
    raise SystemExit(code)


if __name__=='__main__':main()
