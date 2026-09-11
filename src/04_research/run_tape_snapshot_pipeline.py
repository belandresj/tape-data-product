"""Resumable frozen-universe halt detection and one-second feature production.

Subcommands: plan, checkpoint, detect, validate, features, publish-inputs.
Raw stages use one verified staged pair, a disk event index, <=25k raw batches,
<=1024 feature rows, and a process-tree watchdog. The full detector and feature
stages require a measured checkpoint and explicit --full-run-approved.
"""
from __future__ import annotations
import argparse
from collections import Counter
from dataclasses import asdict
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import sqlite3
import sys
import tempfile
import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import bounded_halt_detection as HALT
import historical_halt_registry_v1 as OLD
import snapshot_feature_pipeline as FEATURE
import tape_snapshot_inventory as INV
import run_economic_tape_v3 as WATCH

S = INV.STORAGE
ROOT = INV.ROOT
PIPELINE_VERSION = 'tape_snapshot_pipeline_v1'
MAX_METADATA_ROWS = 25000


def code_identity():
    names = ('run_tape_snapshot_pipeline.py','bounded_halt_detection.py','snapshot_feature_pipeline.py',
             'tape_snapshot_inventory.py','historical_halt_registry_v1.py','acquire_nasdaq_luld_halts.py','tape_feature_store.py')
    hashes={n:S.sha256_file(Path(__file__).parent/n) for n in names}
    hashes['semantic_contract']=S.sha256_file(ROOT/'config/tape_snapshot_feature_semantics_v1.json')
    return hashes


def make_plan(snapshot_path, reference_root, output):
    snapshot = INV.load(snapshot_path)
    sources = [s for s in snapshot['sources'] if s['status']=='accepted_canonical_metadata']
    sources.sort(key=lambda s:(s['session_date'],s['symbol']))
    ordered = sorted(sources,key=lambda s:sum(s[k]['rows'] for k in ('trades','quotes')))
    selected = [ordered[0],ordered[-1]]
    # An independently known official positive exercises halt/resumption; it
    # is a checkpoint stratum, not a detector-validation or feature-selected sample.
    positive = next((s for s in sources if (s['session_date'],s['symbol'])==('2026-09-01','BIAF')),None)
    if positive and positive not in selected: selected.append(positive)
    ref = json.loads((Path(reference_root)/'manifest.json').read_text())
    if set(ref['requested_dates']) != {s['session_date'] for s in sources}:
        raise ValueError('reference date coverage differs from frozen sources')
    plan = dict(version=PIPELINE_VERSION,snapshot_path=str(Path(snapshot_path).resolve()),snapshot_hash=snapshot['snapshot_hash'],
                reference_root=str(Path(reference_root).resolve()),reference_manifest_sha256=S.sha256_file(Path(reference_root)/'manifest.json'),
                source_count=len(sources),planned_feature_rows=len(sources)*57600,
                total_raw_rows=sum(s[k]['rows'] for s in sources for k in ('trades','quotes')),
                total_raw_bytes=sum(s[k]['size_bytes'] for s in sources for k in ('trades','quotes')),
                checkpoint_sources=[[s['session_date'],s['symbol']] for s in selected],checkpoint_seconds=21600,
                checkpoint_selection='min/max raw row counts, plus independently known BIAF official halt; no feature selection',
                checkpoint_input='canonical session prefix through 10:00 ET; partial-source diagnostic only, never a complete symbol-day partition',
                source_hashes=code_identity(),feature_identity=FEATURE.identity(),candidate_config=asdict(HALT.DetectionConfig()),
                memory={'target_bytes':2*1024**3,'stop_bytes':int(2.8*1024**3),'batch_rows':25000,'sqlite_cache_mib':8,
                        'feature_batch_rows':1024,'workers':1,'largest_state':'fixed 57600-second activity arrays, bounded Arrow batches, 8 MiB SQLite cache',
                        'metadata_limits':'25000 rows and 64 MiB decoded list values before legacy validation; refuse larger metadata'},
                complexity={'trade_detection':'O(N log N + C log N + selected review rows), O(N) temporary disk, fixed session arrays',
                            'quote_evidence':'O(Q*C) vectorized batch masks, C<=240 per symbol-day',
                            'features':'O(R + 57600*F*W), inherited sorted rolling containers W<=300; memory independent of raw-row count',
                            'witnesses':'disk SQLite presence queries; no corpus presence table in RAM'},
                halt_policy='existing V1 candidate/acceptance rules; inferred registry publication still requires empirical validation gate',
                r2_prefix='derived/tape_snapshots/'+snapshot['snapshot_hash'])
    plan['plan_hash']=INV.digest(plan)
    if Path(output).exists(): raise FileExistsError(output)
    INV.save(output,plan)
    return plan


def load_plan(path, *, verify_implementation=True):
    p=json.loads(Path(path).read_text()); h=p.pop('plan_hash')
    if INV.digest(p)!=h: raise ValueError('plan hash mismatch')
    p['plan_hash']=h
    if verify_implementation and (p['source_hashes']!=code_identity() or p['feature_identity']!=FEATURE.identity()):
        raise ValueError('pipeline changed since plan; create a new plan, do not reuse old outputs')
    snap=INV.load(p['snapshot_path'])
    if snap['snapshot_hash']!=p['snapshot_hash']: raise ValueError('source snapshot changed')
    ref=Path(p['reference_root'])
    if S.sha256_file(ref/'manifest.json')!=p['reference_manifest_sha256']: raise ValueError('official reference manifest changed')
    m=json.loads((ref/'manifest.json').read_text())
    for name,h in m['content_sha256'].items():
        if S.sha256_file(ref/name)!=h: raise ValueError('official reference content changed')
    return p,[s for s in snap['sources'] if s['status']=='accepted_canonical_metadata']


def crop_prefix(source,destination,end_ns):
    pf=pq.ParquetFile(source)
    n=0
    with pq.ParquetWriter(destination,pf.schema_arrow,compression='zstd') as writer:
        for batch in pf.iter_batches(batch_size=25000,use_threads=False):
            table=pa.Table.from_batches([batch]); mask=pc.less(table['sip_timestamp'],end_ns)
            keep=table.filter(mask)
            if keep.num_rows: writer.write_table(keep); n+=keep.num_rows
            if keep.num_rows<table.num_rows: break
    return n


def bind_source(client,bucket,source):
    for stream in ('trades','quotes'):
        expected=S.ObjectIdentity(**{k:source[stream][k] for k in ('object_key','size_bytes','sha256','rows')})
        S.validate_identities(expected,S.remote_identity(client,bucket,expected.object_key))


def recover_feature_partition(client,bucket,prefix,source,config_hash,*,sample=False,expected_rows=57600):
    """Recover a committed remote partition after loss of its local receipt."""
    key=prefix+'/manifest.json'
    head=S._head_or_none(client,bucket,key)
    if head is None:return None
    manifest_identity=S.object_identity_from_head(key,head)
    if manifest_identity.size_bytes>2*1024**2:raise ValueError('feature manifest exceeds metadata bound')
    body=client.get_object(Bucket=bucket,Key=key)['Body']
    try:payload=body.read(2*1024**2+1)
    finally:body.close()
    if len(payload)!=manifest_identity.size_bytes or hashlib.sha256(payload).hexdigest()!=manifest_identity.sha256:
        raise ValueError('remote completion manifest identity mismatch')
    meta=json.loads(payload)
    if (meta['provenance']['source']!=source or meta['provenance']['config_hash']!=config_hash
        or meta['sample']!=sample or meta['counts']['rows']!=expected_rows
        or meta['verification']['rows_verified']!=expected_rows):
        raise ValueError('remote completion marker does not match requested feature partition')
    feature_key=prefix+('/features_sample.parquet' if sample else '/features.parquet')
    feature_identity=S.ObjectIdentity(feature_key,meta['bytes'],meta['sha256'],expected_rows)
    S.validate_identities(feature_identity,S.remote_identity(client,bucket,feature_key))
    return dict(config_hash=config_hash,measurement=meta,published=[dict(status='recovered_verified',**asdict(i))
                                                                 for i in (feature_identity,manifest_identity)])


def partition_root(output,source):
    return Path(output)/'partitions'/source['session_date']/source['symbol']


def scan_source(source,quotes,trades,output,scratch,sample_seconds=None):
    output=Path(output); output.mkdir(parents=True,exist_ok=True)
    cfg=HALT.DetectionConfig()
    tick=time.monotonic()
    summary,candidates,presence=HALT.scan_trade_symbol_day(session_date=source['session_date'],symbol=source['symbol'],
        instrument_type='unverified',universe_version='frozen_r2_snapshot',trade_path=trades,quote_path=quotes,
        config=cfg,scratch_root=scratch,review_root=output/'review_traces')
    detection_elapsed=time.monotonic()-tick
    # Temporary replay paths are not durable provenance. Retain their content
    # hashes (including excerpt hashes for checkpoints), but bind source locations
    # to the frozen canonical R2 keys before publishing detector metadata.
    for row in [summary, *candidates]:
        for stream,singular in (('trades','trade'),('quotes','quote')):
            row[singular+'_source_path']='r2://massive-equities/'+source[stream]['object_key']
        row['source_provenance']=OLD.canonical_json(dict(source=source,sample_seconds=sample_seconds,
            replay_trade_sha256=row['trade_source_sha256'],replay_quote_sha256=row['quote_source_sha256']))
    # Freeze candidates before quote evidence or official matching.
    OLD.atomic_parquet(pd.DataFrame(candidates) if candidates else OLD._empty_candidate_table(),output/'candidates.parquet')
    INV.save(output/'candidate_freeze.json',dict(sha256=S.sha256_file(output/'candidates.parquet'),candidate_config_hash=cfg.digest()))
    tick=time.monotonic()
    evidence,quote_rows=HALT.quote_evidence(quotes,candidates,output/'review_traces',cfg)
    OLD.atomic_parquet(pd.DataFrame(evidence) if evidence else pd.DataFrame(columns=['candidate_id','session_date','symbol']),output/'quote_evidence.parquet')
    OLD.atomic_parquet(pd.DataFrame([summary]),output/'summary.parquet')
    pschema=pa.schema([('session_date',pa.string()),('symbol',pa.string()),('second_index',pa.int64()),('first_eligible_sip',pa.int64()),('last_eligible_sip',pa.int64())])
    with pq.ParquetWriter(output/'presence.parquet',pschema,compression='zstd') as writer:
        for i in range(0,len(presence),25000): writer.write_table(pa.Table.from_pylist(presence[i:i+25000],schema=pschema))
    files={str(p.relative_to(output)):S.sha256_file(p) for p in output.rglob('*.parquet')}
    result=dict(source=source,sample_seconds=sample_seconds,detection_elapsed_seconds=detection_elapsed,
                quote_evidence_elapsed_seconds=time.monotonic()-tick,quote_rows=quote_rows,
                trade_rows=summary['raw_trade_rows'],candidate_count=len(candidates),
                sqlite_peak_bytes=summary['bounded_store_peak_bytes'],content_sha256=files)
    INV.save(output/'complete.json',result)
    return result,candidates


def project_official_sessions(frame, sources):
    """Apply official bounds to every acquired session they overlap.

    Keep original timestamps/event IDs in provenance. A session-specific ID is
    required by the existing one-to-one matcher for multi-session interruptions.
    Missing resume bounds remain QA on the halt date, never invented intervals.
    """
    dates={}
    for source in sources:
        dates.setdefault(source['symbol'],set()).add(source['session_date'])
    rows=[]
    for record in frame.to_dict('records'):
        a=OLD._ns(record.get('official_halt_start')); b=OLD._ns(record.get('official_trade_resume_time'))
        for day in sorted(dates.get(record['symbol'],())):
            start,end=HALT.session_bounds_ns(day)
            if a is not None and b is not None:
                include=max(start,a)<min(end,b)
            else:
                include=day==str(record['session_date'])
            if include:
                rows.append(dict(record,session_date=day,original_provider_event_id=record['provider_event_id'],
                    provider_event_id=INV.digest(dict(original_event_id=record['provider_event_id'],session_date=day)),
                    official_session_projection_version='official_session_projection_v1'))
    return pd.DataFrame(rows,columns=list(frame.columns)+['original_provider_event_id','official_session_projection_version'])


def normalized_reference(plan,output):
    path=Path(output)/'external_halts_normalized.parquet'
    if not path.exists():
        original=Path(output)/'original_reference';original.mkdir(parents=True,exist_ok=True)
        OLD.normalize_external_halts(input_path=Path(plan['reference_root'])/'all_halts.csv',provider='nasdaq_trader_rss',
            run_dir=original,reference_root=Path(output)/'reference_cache',request_parameters={'reference_manifest_sha256':plan['reference_manifest_sha256']})
        sources=[s for s in INV.load(plan['snapshot_path'])['sources'] if s['status']=='accepted_canonical_metadata']
        projected=project_official_sessions(metadata_frame(original/'external_halts_normalized.parquet'),sources)
        OLD.atomic_parquet(projected,path)
    return path


def metadata_frame(path):
    pf=pq.ParquetFile(path)
    if pf.metadata.num_rows>MAX_METADATA_ROWS or sum(pf.metadata.row_group(i).total_byte_size for i in range(pf.metadata.num_row_groups))>64*1024**2:
        raise ValueError('metadata exceeds 25000-row/64-MiB bound; partition the validation run')
    return pd.read_parquet(path)


def require_checkpoint(plan, directory):
    directory=Path(directory)
    summary=json.loads((directory/'run_summary.json').read_text())
    measurement=json.loads((directory.parent/(directory.name+'_checkpoint_resources.json')).read_text())
    if summary['plan_hash']!=plan['plan_hash'] or summary['stage']!='checkpoint' or summary['status']!='complete':
        raise ValueError('checkpoint belongs to a different plan or is incomplete')
    if measurement['returncode']!=0 or measurement['killed_for_memory'] or measurement['peak_aggregate_rss_bytes']>plan['memory']['target_bytes']:
        raise ValueError('checkpoint failed or lacks required memory headroom')
    observed={(r['source']['session_date'],r['source']['symbol']) for r in summary['sources']}
    if observed!={tuple(k) for k in plan['checkpoint_sources']}:raise ValueError('checkpoint source coverage differs')
    for item in summary['sources']:
        root=partition_root(directory,item['source'])
        for name,h in item['content_sha256'].items():
            if S.sha256_file(root/name)!=h:raise ValueError('checkpoint content changed')
        f=json.loads((root/'feature_measurement.json').read_text())
        if f['inherited_v3_exact_equivalence_rows']!=plan['checkpoint_seconds'] or not item.get('published_sample'):
            raise ValueError('checkpoint omitted feature equivalence or R2 publication')


def official_intervals(path,source):
    out=[]
    start,end=HALT.session_bounds_ns(source['session_date'])
    for row in metadata_frame(path).to_dict('records'):
        a=OLD._ns(row.get('official_halt_start')); b=OLD._ns(row.get('official_trade_resume_time'))
        if row['session_date']==source['session_date'] and row['symbol']==source['symbol'] and a is not None and b is not None and max(a,start)<min(b,end):
            out.append((max(a,start),min(b,end),'official:'+row['provider_event_id']))
    out.sort()
    if any(out[i][0]<out[i-1][1] for i in range(1,len(out))): raise ValueError('overlapping official intervals require reconciliation')
    return out


def raw_stage(args):
    plan,sources=load_plan(args.plan)
    output=args.output; output.mkdir(parents=True,exist_ok=True)
    selected={tuple(x) for x in plan['checkpoint_sources']}
    checkpoint=args.stage=='checkpoint'
    if not checkpoint and not args.full_run_approved: raise ValueError('full detector needs post-checkpoint confirmation')
    if not checkpoint:require_checkpoint(plan,args.checkpoint)
    if checkpoint: sources=[s for s in sources if (s['session_date'],s['symbol']) in selected]
    existing=output/'plan_identity.json'
    if existing.exists() and json.loads(existing.read_text())['plan_hash']!=plan['plan_hash']: raise ValueError('output belongs to another plan')
    INV.save(existing,dict(plan_hash=plan['plan_hash'],sample=checkpoint))
    reference=normalized_reference(plan,output)
    settings=S.load_r2_settings();client=S.build_client(settings)
    results=[]
    for source in sources:
        target=partition_root(output,source)
        complete=target/'complete.json'
        if complete.exists():
            prior=json.loads(complete.read_text())
            if prior['source']!=source: raise ValueError('resumed source differs')
            for name,h in prior['content_sha256'].items():
                if S.sha256_file(target/name)!=h: raise ValueError('partition hash mismatch')
            if not checkpoint or (target/'feature_measurement.json').exists():
                results.append(prior);continue
            raise ValueError('incomplete checkpoint feature stage; use a fresh output after investigating')
        bind_source(client,settings.bucket,source)
        required=sum(source[k]['size_bytes'] for k in ('trades','quotes'))*2+source['trades']['rows']*128+3*1024**3
        WATCH.require_space(args.scratch,required)
        started=time.monotonic()
        with S.staged_symbol_day(client,settings.bucket,source['session_date'],source['symbol'],args.scratch,
                                download_workers=1,transfer_workers=1) as staged:
            with tempfile.TemporaryDirectory(prefix='snapshot-work-',dir=args.scratch) as work:
                work=Path(work); quotes,trades=staged.quotes,staged.trades
                seconds=plan['checkpoint_seconds'] if checkpoint else None
                if checkpoint:
                    end=HALT.session_bounds_ns(source['session_date'])[0]+seconds*HALT.NS
                    for stream in ('quotes','trades'): crop_prefix(getattr(staged,stream),work/f'{stream}.parquet',end)
                    quotes,trades=work/'quotes.parquet',work/'trades.parquet'
                result,candidates=scan_source(source,quotes,trades,target,work,seconds)
                if checkpoint:
                    # Only confirmed official intervals affect this plumbing
                    # checkpoint. Inferred candidates remain unvalidated here.
                    halts=official_intervals(reference,source)
                    provenance=dict(plan_hash=plan['plan_hash'],source=source,halt_scope='official intervals only in partial-source checkpoint',
                                    inferred_candidate_count=len(candidates),full_registry_validated=False,config_hash=plan['plan_hash'])
                    feature=target/'features_sample.parquet'
                    measurement=FEATURE.write(quotes,trades,source['session_date'],source['symbol'],feature,provenance,halts=halts,seconds=seconds)
                    measurement['verification']=FEATURE.verify(feature,source['session_date'],source['symbol'],expected_rows=seconds)
                    baseline=work/'baseline_features.parquet'
                    FEATURE.V.write_features(quotes,trades,source['session_date'],source['symbol'],baseline,halts=halts,seconds=seconds)
                    left=pq.ParquetFile(feature).iter_batches(batch_size=1024,columns=FEATURE.V.output_schema().names,use_threads=False)
                    right=pq.ParquetFile(baseline).iter_batches(batch_size=1024,use_threads=False)
                    matched=0
                    for a,b in zip(left,right,strict=True):
                        if not a.replace_schema_metadata(None).equals(b.replace_schema_metadata(None)):
                            raise ValueError('feature publication differs from inherited V3')
                        matched+=a.num_rows
                    measurement['inherited_v3_exact_equivalence_rows']=matched
                    measurement['distributions']=FEATURE.distributions(feature)
                    INV.save(target/'feature_measurement.json',measurement)
                    sample_prefix=plan['r2_prefix']+'/checkpoints/'+plan['plan_hash']+'/'+source['session_date']+'/'+source['symbol']
                    result['published_sample']=[
                        S.publish_parquet_immutable(client,settings.bucket,feature,sample_prefix+'/features_sample.parquet',upload_workers=1),
                        S.publish_file_immutable(client,settings.bucket,target/'feature_measurement.json',sample_prefix+'/manifest.json',
                                                 content_type='application/json',upload_workers=1)]
                    recovered=recover_feature_partition(client,settings.bucket,sample_prefix,source,plan['plan_hash'],
                                                        sample=True,expected_rows=seconds)
                    result['r2_completion_recovery_verified']=recovered is not None
                    result['content_sha256'].update({'features_sample.parquet':S.sha256_file(feature),
                        'feature_measurement.json':S.sha256_file(target/'feature_measurement.json')})
                result['end_to_end_seconds']=time.monotonic()-started
                INV.save(complete,result);results.append(result)
        print(json.dumps({'completed_sources':len(results),'total':len(sources),'source':[source['session_date'],source['symbol']],
                          'candidates':result['candidate_count']}),flush=True)
    diagnostic=None
    if checkpoint:
        diagnostic=assemble_validation(args,diagnostic_sources=sources)
    INV.save(output/'run_summary.json',dict(plan_hash=plan['plan_hash'],stage=args.stage,status='complete',sources=results,
                                          diagnostic_validation=diagnostic))


def assemble_validation(args,diagnostic_sources=None):
    """Bounded metadata assembly; presence and official activity stay on disk."""
    plan,sources=load_plan(args.plan); output=args.output
    is_sample=bool(json.loads((output/'plan_identity.json').read_text()).get('sample'))
    if is_sample and diagnostic_sources is None: raise ValueError('cannot validate a partial-source checkpoint as a corpus')
    if diagnostic_sources is not None:
        if not is_sample:raise ValueError('diagnostic subset requires a marked checkpoint')
        sources=diagnostic_sources
    presence_rows=sum(pq.ParquetFile(partition_root(output,s)/'presence.parquet').metadata.num_rows for s in sources)
    WATCH.require_space(output,presence_rows*160+3*1024**3)
    db=sqlite3.connect(output/'presence.sqlite');db.execute('PRAGMA cache_size=-8192');db.execute('PRAGMA temp_store=FILE')
    db.execute('CREATE TABLE IF NOT EXISTS presence(day TEXT,symbol TEXT,first_ns INTEGER,last_ns INTEGER)')
    db.execute('DELETE FROM presence')
    official=metadata_frame(normalized_reference(plan,output))
    candidates=[];evidence=[];summaries=[]
    for source in sources:
        part=partition_root(output,source);complete=json.loads((part/'complete.json').read_text())
        if (complete['sample_seconds'] is not None and not is_sample) or complete['source']!=source: raise ValueError('partial or mismatched detector source')
        for name,h in complete['content_sha256'].items():
            if S.sha256_file(part/name)!=h: raise ValueError('changed detector partition')
        c=metadata_frame(part/'candidates.parquet').to_dict('records'); e=metadata_frame(part/'quote_evidence.parquet').to_dict('records')
        summary=metadata_frame(part/'summary.parquet').iloc[0].to_dict()
        # Preserve only endpoints queried by V1 official relevance. The full
        # vector remains in the per-source partition, never duplicated corpus-wide.
        full=np.asarray(summary['activity_qualified_end_ns'],dtype=np.int64)
        keep=np.zeros(len(full),dtype=bool)
        for row in official[(official.session_date.astype(str)==source['session_date']) & (official.symbol==source['symbol'])].to_dict('records'):
            stamp=OLD._ns(row['official_halt_start'])
            if stamp is not None: keep |= (full<=stamp)&(full>=stamp-60*HALT.NS)
        summary['activity_qualified_end_ns']=full[keep].tolist()
        summaries.append(summary);candidates.extend(c);evidence.extend(e)
        if max(len(candidates),len(summaries))>MAX_METADATA_ROWS: raise ValueError('validation metadata row bound exceeded')
        for batch in pq.ParquetFile(part/'presence.parquet').iter_batches(batch_size=25000,use_threads=False):
            p=batch.to_pydict()
            db.executemany('INSERT INTO presence VALUES(?,?,?,?)',zip(p['session_date'],p['symbol'],p['first_eligible_sip'],p['last_eligible_sip']))
        db.commit()
    db.execute('CREATE INDEX IF NOT EXISTS witness_lookup ON presence(day,first_ns,last_ns,symbol)');db.commit()
    by_id={c['candidate_id']:c for c in candidates}
    processed_counts=Counter(s['session_date'] for s in summaries)
    for row in evidence:
        c=by_id[row['candidate_id']];a=OLD._ns(c['last_pre_gap_trade_sip']);b=OLD._ns(c['first_post_gap_trade_sip'])
        witnesses=db.execute('SELECT count(DISTINCT symbol) FROM presence WHERE day=? AND symbol!=? AND first_ns<? AND last_ns>?',
            (row['session_date'],row['symbol'],b,a)).fetchone()[0]
        overlapping=0
        for other in evidence:
            if other['session_date']!=row['session_date'] or other['symbol']==row['symbol'] or other['quote_message_count_in_gap']!=0:continue
            oc=by_id[other['candidate_id']]
            overlapping+=OLD._overlap_fraction(a,b,OLD._ns(oc['last_pre_gap_trade_sip']),OLD._ns(oc['first_post_gap_trade_sip']))>=.5
        row.update(other_symbol_event_witness_count=witnesses,overlapping_candidate_symbol_count=int(overlapping),
                   same_date_upstream_rejected_symbol_count=0,acquired_symbol_count=processed_counts[row['session_date']])
        row['source_health_class']=OLD.classify_source_health(same_symbol_quote_stream_observed_in_gap=row['same_symbol_quote_stream_observed_in_gap'],
            other_symbol_event_witness_count=witnesses,same_date_upstream_rejected_symbol_count=0,
            acquired_symbol_count=processed_counts[row['session_date']],overlapping_candidate_symbol_count=overlapping)
    db.close()
    # A failed source stops detection; it is never silently counted as healthy.
    for name,rows in [('exact_candidates.parquet',candidates),('candidate_quote_evidence.parquet',evidence),
                      ('processed_symbol_days.parquet',summaries),('symbol_day_trade_summaries.parquet',summaries)]:
        frame=pd.DataFrame(rows)
        if not rows: frame=pd.DataFrame(columns=['candidate_id','session_date','symbol'])
        if frame.memory_usage(deep=True).sum()>64*1024**2: raise ValueError('decoded metadata memory bound exceeded')
        OLD.atomic_parquet(frame,output/name)
    cfg=HALT.DetectionConfig()
    INV.save(output/'candidate_freeze.json',dict(exact_candidates_sha256=S.sha256_file(output/'exact_candidates.parquet'),candidate_config_hash=cfg.digest()))
    INV.save(output/'run_config.json',dict(candidate_config_hash=cfg.digest(),candidate_config=asdict(cfg),source_snapshot_hash=plan['snapshot_hash'],
                                         pipeline_plan_hash=plan['plan_hash'],diagnostic_sample=is_sample,
                                         presence_storage='disk SQLite; exact per-second first/last eligible-event witnesses'))
    result=OLD.validate_detection_run(run_dir=output,render_plots=False,presence_artifact_name='presence.sqlite')
    if is_sample:
        result=dict(result,diagnostic_sample=True,registry_publication_allowed=False)
        manifest=json.loads((output/'manifest.json').read_text());manifest['diagnostic_sample']=True
        INV.save(output/'manifest.json',manifest)
        INV.save(output/'diagnostic_validation.json',result)
    if result['validation_gate_passed'] and not is_sample:
        if (output/'registry').exists():
            if OLD.verify_frozen_registry(output/'registry')['registry_config_hash']!=result['registry_config_hash']:
                raise ValueError('existing frozen registry differs from validation')
        else:OLD.freeze_registry(run_dir=output,output_dir=output/'registry')
    print(json.dumps(result,default=str))
    return result


def feature_stage(args):
    plan,sources=load_plan(args.plan)
    if not args.full_run_approved: raise ValueError('full features need post-checkpoint confirmation')
    require_checkpoint(plan,args.checkpoint)
    registry=args.output/'registry'
    registry_manifest=OLD.verify_frozen_registry(registry)
    if S.sha256_file(args.output/'validation_summary.json')!=registry_manifest['source_validation_summary_sha256']:
        raise ValueError('registry validation summary changed since freeze')
    if not json.loads((args.output/'validation_summary.json').read_text())['validation_gate_passed']:
        raise ValueError('inferred halt validation gate has not passed')
    official=metadata_frame(args.output/'external_halts_normalized.parquet')
    if official.official_halt_start.isna().any() or official.official_trade_resume_time.isna().any():
        raise ValueError('unresolved official interval bounds require review before feature publication')
    settings=S.load_r2_settings();client=S.build_client(settings)
    import tape_feature_store as STORE
    for source in sources:
        halts,_=WATCH.load_halts(registry,source['session_date'],source['symbol'])
        result=STORE.materialize_features(client,settings.bucket,dict(source=source,halts=halts),
            'validated historical inferred-plus-official registry',57600,args.scratch,include_distributions=True)
        # Experiment association is separate from the reusable feature object.
        local=partition_root(args.output,source)
        INV.save(local/'feature_complete.json',dict(plan_hash=plan['plan_hash'],registry=registry_manifest,**result))
        # Durable experiment/registry association does not enter feature cache keys.
        identity=result['measurement']['partition_identity']
        link=local/'feature_link.json'
        INV.save(link,dict(plan_hash=plan['plan_hash'],registry=registry_manifest,
                           partition_identity=identity,feature_prefix=STORE.feature_prefix(identity)))
        S.publish_file_immutable(client,settings.bucket,link,
            plan['r2_prefix']+'/feature_links/'+plan['plan_hash']+'/session_date='+source['session_date']+
            '/symbol='+source['symbol']+'/manifest.json',upload_workers=1,content_type='application/json')
        print(json.dumps({'feature_complete':[source['session_date'],source['symbol']],
                          'recovered':result['recovered']}),flush=True)


def publish_inputs(args):
    plan,_=load_plan(args.plan);settings=S.load_r2_settings();client=S.build_client(settings)
    published=[]
    files=[(Path(plan['snapshot_path']),'snapshot.json'),(args.plan,'plans/'+plan['plan_hash']+'.json')]
    ref=Path(plan['reference_root'])
    files += [(p,'official_reference/'+str(p.relative_to(ref))) for p in sorted(ref.rglob('*')) if p.is_file() and p.name!='manifest.json']
    files.append((ref/'manifest.json','official_reference/manifest.json'))
    registry=args.output/'registry'
    if registry.exists():
        registry_manifest=OLD.verify_frozen_registry(registry)
        registry_prefix='registry/'+registry_manifest['registry_config_hash']+'/'
        files += [(p,registry_prefix+p.name) for p in sorted(registry.iterdir()) if p.is_file() and p.name!='manifest.json']
        files.append((registry/'manifest.json',registry_prefix+'manifest.json'))
        # Preserve the evidence needed to audit inferred intervals, including
        # the disk presence index and exact candidate review traces.
        audit_prefix='halt_build/'+registry_manifest['registry_config_hash']+'/'
        for path in sorted(args.output.rglob('*')):
            if not path.is_file() or registry in path.parents or path.name in ('r2_input_publication.json','feature_complete.json'):
                continue
            files.append((path,audit_prefix+str(path.relative_to(args.output))))
    for path,name in files:
        published.append(S.publish_file_immutable(client,settings.bucket,path,plan['r2_prefix']+'/'+name,upload_workers=1))
    INV.save(args.output/'r2_input_publication.json',dict(published=published))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('stage',choices=['plan','checkpoint','detect','validate','features','publish-inputs','run'])
    p.add_argument('--plan',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--snapshot',type=Path);p.add_argument('--reference',type=Path)
    p.add_argument('--checkpoint',type=Path)
    p.add_argument('--scratch',type=Path,default=ROOT/'data/.tape_snapshot_scratch')
    p.add_argument('--full-run-approved',action='store_true');p.add_argument('--worker',action='store_true')
    a=p.parse_args()
    if a.checkpoint is None:a.checkpoint=a.plan.parent/'checkpoint_final'
    if a.stage=='plan':
        result=make_plan(a.snapshot,a.reference,a.plan);print(json.dumps({'plan_hash':result['plan_hash'],'sources':result['source_count']}));return
    a.scratch.mkdir(parents=True,exist_ok=True)
    if not a.worker and a.stage in ('checkpoint','detect','validate','features','run'):
        raise SystemExit(WATCH.monitored([sys.executable,str(Path(__file__).resolve()),*sys.argv[1:],'--worker'],
                                        a.output.parent/(a.output.name+'_'+a.stage+'_resources.json')))
    signal.signal(signal.SIGTERM,lambda *_:sys.exit('terminated by resource watchdog'))
    if a.stage in ('checkpoint','detect'): raw_stage(a)
    elif a.stage=='validate': assemble_validation(a)
    elif a.stage=='features':feature_stage(a)
    elif a.stage=='run':
        raw_stage(a)
        if (a.output/'registry').exists():
            OLD.verify_frozen_registry(a.output/'registry')
            result=json.loads((a.output/'validation_summary.json').read_text())
        else:result=assemble_validation(a)
        if result['validation_gate_passed']:
            publish_inputs(a);feature_stage(a)
        else:print('Registry validation is pending or failed; feature production has not started.',flush=True)
    else:publish_inputs(a)


if __name__=='__main__':main()
