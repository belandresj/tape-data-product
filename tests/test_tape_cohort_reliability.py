"""Failure fixtures: no remote data or complete symbol-day scans."""
import json
import pathlib
import sys
import pytest
from botocore.exceptions import ClientError
from tape_data_product.query.tape_cohort_reliability import retry_download,fatal
from tape_data_product.query import tape_cohort_pipeline as P


def test_stream_failure_restarts_private_partial(tmp_path):
    partial=tmp_path/'download.partial';delays=[];calls=[]
    def operation():
        assert not partial.exists()
        partial.write_bytes(b'prefix');calls.append(1)
        if len(calls)<3:raise TimeoutError('fixture')
        partial.write_bytes(b'complete');return 'ok'
    assert retry_download(operation,partial,tmp_path/'retry.jsonl',sleep=delays.append)=='ok'
    assert delays==[2,5] and partial.read_bytes()==b'complete'
    assert len((tmp_path/'retry.jsonl').read_text().splitlines())==2


@pytest.mark.parametrize('exc',[ValueError('bad sha'),ClientError({'ResponseMetadata':{'HTTPStatusCode':403},'Error':{'Code':'AccessDenied'}},'GetObject')])
def test_integrity_and_auth_not_retried(tmp_path,exc):
    calls=[]
    def operation():calls.append(1);raise exc
    with pytest.raises(type(exc)):retry_download(operation,tmp_path/'partial',tmp_path/'log',sleep=lambda _:pytest.fail('must not sleep'))
    assert len(calls)==1


def test_transient_retry_is_bounded(tmp_path):
    calls=[]
    def operation():calls.append(1);raise ConnectionError('fixture')
    with pytest.raises(ConnectionError):retry_download(operation,tmp_path/'partial',tmp_path/'log',sleep=lambda _:None)
    assert len(calls)==3
    assert fatal(OSError('disk')) and fatal(MemoryError()) and fatal(RuntimeError('bug'))
    assert not fatal(ValueError('one corrupt source'))


@pytest.fixture
def fake_run(tmp_path,monkeypatch):
    source=tmp_path/'source';source.mkdir()
    for name in ['manifest.json','accepted.jsonl','reconciliation.jsonl','capture_exclusions.jsonl']:(source/name).write_text('{}')
    calc=tmp_path/'calculations.json';calc.write_text('{}')
    members=[{'session_date':f'2026-06-{day:02d}','symbol':'TEST','partition_identity':str(day)} for day in [16,17,18,19]]
    plan={'release_identity':'synthetic','query_run_hash':'fixture','query_hash':'query','query':{'conditions':[{}]*5},'approval':{'confirmed':True,'scope':'synthetic test'},'pilot_date':'2026-06-16','control_hashes':{p.name:P._sha(p) for p in source.iterdir()},'settings':{'cache_limit_bytes':1024},'cache_root':str(tmp_path/'cache')}
    monkeypatch.setattr(P,'_load_plan',lambda _:plan)
    monkeypatch.setattr(P.RELEASE,'read',lambda *a,**k:({},members))
    monkeypatch.setattr(P,'_member_map',lambda *a:members)
    monkeypatch.setattr(P,'verify_date',lambda *a:None)
    class Cache:
        def __init__(self,*a,**k):pass
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def register_release(self,*a):pass
    monkeypatch.setattr(P,'FeatureCache',Cache)
    attempts=[];bad={'2026-06-17':ValueError('corrupt fixture')}
    def process(ms,plan,directory,*args,**kwargs):
        day=ms[0]['session_date'];attempts.append(day);directory.mkdir()
        if day in bad:raise bad[day]
        record={'session_date':day,'symbol':'TEST','window_count':0,'active_seconds':0}
        value={'query_run_hash':'fixture','query_hash':'query','members':[record],'outputs':{'window_count':0,'active_seconds':0}}
        P.atomic_json(directory/'manifest.json',value)
        P.atomic_json(directory/'daily.json',{'session_date':day,'qualifying_stocks':0,'windows':0,'total_active_seconds':0})
        return value
    monkeypatch.setattr(P,'_process_members',process)
    def run(resume=False):return P.run_query(source,calc,plan['query'],'2026-06-16','2026-06-19',tmp_path/'cache',tmp_path/'results','range','unused',resume=resume)
    return run,bad,attempts,tmp_path/'results/fixture'


def test_failed_date_does_not_block_later_dates_and_resume_skips_complete(fake_run):
    run,bad,attempts,root=fake_run
    assert run()['state']=='incomplete'
    assert attempts==['2026-06-16','2026-06-17','2026-06-18','2026-06-19']
    assert not (root/'dates/2026-06-17').exists()
    assert not (root/'summary.json').exists()
    bad.clear();attempts.clear()
    result=run(resume=True)
    assert result['state']=='complete' and result['summary']['dates']==4
    assert attempts==['2026-06-17']


def test_resource_failure_stops_without_auto_rerun(fake_run):
    run,bad,attempts,root=fake_run;bad['2026-06-17']=OSError('disk full')
    with pytest.raises(OSError):run()
    assert attempts==['2026-06-16','2026-06-17']
    assert json.loads((root/'manifest.json').read_text())['state']=='incomplete'


def test_consecutive_failure_circuit_breaker(fake_run):
    run,bad,attempts,root=fake_run
    bad.update({f'2026-06-{d}':ValueError('broken') for d in [16,18,19]})
    with pytest.raises(ValueError):run()
    assert len(attempts)==3
    assert json.loads((root/'manifest.json').read_text())['stop_reason']=='consecutive_failures'


def test_resume_rejects_changed_date_identity(fake_run):
    run,bad,attempts,root=fake_run;run()
    path=root/'dates/2026-06-16/manifest.json';value=json.loads(path.read_text());value['query_hash']='different';path.write_text(json.dumps(value))
    with pytest.raises(ValueError,match='resume identity'):run(resume=True)


def test_supervisor_stops_worker_at_resource_floor(tmp_path,monkeypatch):
    import subprocess,psutil,types
    from tape_data_product.query.tape_cohort_reliability import supervise
    plan={'query_run_hash':'fixture','result_root':str(tmp_path),'settings':{'rss_stop_bytes':1024**3,'minimum_available_bytes':768*1024**2,'minimum_free_disk_bytes':0}}
    monkeypatch.setattr(P,'_load_plan',lambda _:plan)
    real_popen=subprocess.Popen;children=[]
    def spawn(*args,**kwargs):
        child=real_popen([sys.executable,'-c','import time;time.sleep(30)'],**kwargs);children.append(child);return child
    monkeypatch.setattr(subprocess,'Popen',spawn)
    monkeypatch.setattr(psutil,'virtual_memory',lambda:types.SimpleNamespace(available=100))
    result=supervise(tmp_path/'plan.json')
    assert result['exit_code']!=0 and len(children)==1 and children[0].poll() is not None
    assert json.loads(pathlib.Path(result['resources']).read_text())['stop_reason']=='available_memory_floor'


def test_interrupted_json_temp_does_not_block_resume(tmp_path):
    from tape_data_product.query.tape_cohort_outputs import atomic_json
    orphan=tmp_path/'.progress.json.partial';orphan.write_text('interrupted')
    target=tmp_path/'progress.json'
    atomic_json(target,{'state':'running'});atomic_json(target,{'state':'complete'})
    assert json.loads(target.read_text())=={'state':'complete'}
    assert orphan.read_text()=='interrupted'


def test_cli_defaults_to_supervised_run():
    from tape_data_product.query.run_tape_cohort_query import parser
    args=parser().parse_args(['run','--plan','fixture.json','--resume'])
    assert args.resume and not args.worker


@pytest.mark.parametrize('exit_code',[0,2])
def test_supervisor_propagates_worker_completion(tmp_path,monkeypatch,exit_code):
    import subprocess,psutil,types
    from tape_data_product.query.tape_cohort_reliability import supervise
    plan={'query_run_hash':'fixture','result_root':str(tmp_path),'settings':{'rss_stop_bytes':1024**3,'minimum_available_bytes':768*1024**2,'minimum_free_disk_bytes':0}}
    monkeypatch.setattr(P,'_load_plan',lambda _:plan)
    real_popen=subprocess.Popen
    def spawn(*args,**kwargs):return real_popen([sys.executable,'-c',f'import sys;sys.exit({exit_code})'],**kwargs)
    monkeypatch.setattr(subprocess,'Popen',spawn)
    monkeypatch.setattr(psutil,'virtual_memory',lambda:types.SimpleNamespace(available=2*1024**3))
    result=supervise(tmp_path/'plan.json')
    assert result['exit_code']==exit_code
    assert json.loads(pathlib.Path(result['resources']).read_text())['stop_reason'] is None
