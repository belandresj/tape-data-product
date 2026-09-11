from pathlib import Path
import json
import math
import sys
import pytest
import numpy as np
import pyarrow.parquet as pq

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src/04_research'),str(ROOT/'tests')]
import all_feature_month_core as C
import all_feature_month_runtime as R
import snapshot_feature_pipeline as F
from test_economic_tape_v3 import primitive,quote,trade,raw_pair


def test_reference_sum_exact_zero_after_nonzero_expiry():
    from all_feature_month_verify import ReferenceSum
    values=[3238327.6483316235,1508491.7392450192,6509344.730398538,
            724362.8666754276,5358820.043066892,3656889.1691258554,579989.2477470681]
    s=ReferenceSum(60)
    for x in values+[0.]*60:s.append(x)
    assert s.total==0. and s.mean()==0. and s.n==60
    s.append(1e-200)
    assert s.total==1e-200 and s.mean()==1e-200/60
    for _ in range(60):s.append(None)
    assert s.total==0. and s.mean() is None and s.n==0


def test_reference_sum_preserves_small_live_value_after_large_eviction():
    """A rounded rebuild must not erase a live value before an eviction."""
    from all_feature_month_verify import ReferenceSum
    s=ReferenceSum(3)
    values=[1e16,1.,0.,0.]
    for value in values:s.append(value)
    assert s.total==math.fsum(values[-3:])==1.


def test_exact_movement_participation_support_halts_gaps_and_invalid():
    model=C.V.FeatureStream();movement=C.Movement()
    reference={h:[] for h in C.HORIZONS}
    for i in range(1000):
        p=primitive(i,halt_interval_active=350<=i<354,
                    halt_resume_boundary=i==354,generation=int(i>=354),
                    continuity_segment_id=int(i>=720),
                    midpoint=math.nan if i in (8,57,87) else 100*math.exp((i%77)*.00001),
                    quote_source_file_accepted=i not in (80,370))
        row=F.enrich(model.push(p),p)
        out=movement.push(row)
        for h in C.HORIZONS:
            expected=list(model.windows[h].move5.queue)
            expected=[x for x in expected if math.isfinite(x)]
            value=out[f'movement_participation_{h}s']
            if expected and any(expected) and row[f'state_mature_{h}s'] and row[f'movement_support_valid_{h}s']:
                assert value==pytest.approx(sum(expected)**2/(len(expected)*sum(x*x for x in expected)),abs=1e-12)
            else:assert math.isnan(value)
    row['movement_valid_5s_count_60s']-=1
    with pytest.raises(ValueError):movement.push(row)


def age_row(i,**kw):
    row=dict(interval_end_ns=(i+1)*C.NS,continuity_segment_id=0,halt_interval_active=False,
             primitive_quote_source_file_accepted=True)
    row.update(kw);return row


def event(ts,mid=100,valid=True):return ts,dict(midpoint=mid,price_state_valid=valid)


def test_exact_age_refresh_reversal_ties_initialization_and_invalid():
    events=[event(-2,99),event(-1,100),event(0),event(100,101),event(200,100),
            event(C.NS,100),event(C.NS,102),event(C.NS,100),
            event(2*C.NS,100,False),event(2*C.NS+1,150),event(3*C.NS,151)]
    a=C.MidpointAge(iter(events),0)
    x=a.push(age_row(0));assert x['midpoint_change_age_end_seconds']==(C.NS-200)/C.NS
    x=a.push(age_row(1));assert x['midpoint_change_age_end_seconds']==1
    x=a.push(age_row(2));assert math.isnan(x['midpoint_change_age_end_seconds'])
    x=a.push(age_row(3));assert x['midpoint_change_age_end_seconds']==1
    for i in range(4,650):x=a.push(age_row(i))
    assert x['midpoint_change_age_p90_seconds_300s']>300
    assert x['midpoint_change_age_mature_300s']
    a=C.MidpointAge(iter([event(-1,100),event(0,100)]),0)
    assert math.isnan(a.push(age_row(0))['midpoint_change_age_end_seconds'])


def test_age_halt_boundary_no_leakage_and_maturity():
    events=[event(0),event(1,101),event(60*C.NS,105),event(61*C.NS,109),event(62*C.NS,110),event(63*C.NS,111)]
    a=C.MidpointAge(iter(events),0)
    for i in range(60):x=a.push(age_row(i))
    assert x['midpoint_change_age_mature_60s']
    for i in (60,61):
        x=a.push(age_row(i,halt_interval_active=True));assert x['midpoint_change_age_observation_count_60s']==0
    x=a.push(age_row(62));assert math.isnan(x['midpoint_change_age_end_seconds'])
    x=a.push(age_row(63));assert x['midpoint_change_age_end_seconds']==1
    assert not x['midpoint_change_age_mature_60s']
    x=a.push(age_row(65));assert math.isnan(x['midpoint_change_age_end_seconds'])
    assert x['midpoint_change_age_observation_count_60s']==0


def build_fixture(tmp_path,seconds=660):
    day='2026-07-01';start=C.V.session_bounds(day)[0]
    q=[quote(start-1)]
    for i in range(seconds):
        q += [quote(start+i*C.NS,seq=0,bid=99.95+i*.001,ask=100.05+i*.001),
              quote(start+i*C.NS+100,seq=0,bid=100.95+i*.001,ask=101.05+i*.001),
              quote(start+i*C.NS+100,seq=1,bid=99.95+i*.001,ask=100.05+i*.001)]
    quotes,trades=raw_pair(tmp_path/'raw',day,q,[trade(start)])
    halts=[(start+310*C.NS+100,start+314*C.NS+100,'H')]
    base=tmp_path/'base.parquet'
    F.write(quotes,trades,day,'TEST',base,{},halts=halts,seconds=seconds,batch_size=17)
    return quotes,trades,base,day,halts


def test_production_extensions_join_units_masks_and_nulls(tmp_path):
    quotes,trades,base,day,halts=build_fixture(tmp_path)
    extension=tmp_path/'extension.parquet';final=tmp_path/'features.parquet'
    C.write_rows(extension,C.extension_rows(base,quotes,day,'TEST',batch_size=17,full=False),{})
    discovery=dict(verified=True,endpoint_ns=C.V.session_bounds(day)[0]+100*C.NS,timing_basis='nominal_bar_completion',provenance_hash='test')
    C.write_rows(final,C.joined_rows(base,extension,discovery,batch_size=19),{})
    assert C.verify(final,day,'TEST',660)['rows_verified']==660
    for src,out in zip(C.rows(base),C.rows(final)):
        for h in C.HORIZONS:
            for name,(up,scale,unit,family) in C.MAPPINGS.items():
                actual=out[f'{name}_{h}s'];expected=src[f'{up}_{h}s']
                if expected is None:assert actual is None
                else:assert actual==expected/(6 if name=='movement_mean_5s_bps' else 1000 if 'age_p90' in name else 1)
        if out['halt_interval_active']:
            assert not out['movement_participation_60s_analysis_valid']
            assert out['midpoint_change_age_p90_seconds_60s'] is None
    with pytest.raises(FileExistsError):C.write_rows(final,iter([]),{})


def test_future_quote_changes_do_not_backdate(tmp_path):
    day='2026-07-01';start=C.V.session_bounds(day)[0]
    q,t=raw_pair(tmp_path/'a',day,[quote(start-1),quote(start+100,bid=100,ask=100.1),quote(start+10*C.NS,bid=102,ask=103)],[])
    def evaluate(path):
        a=C.MidpointAge(C.V.events(path,'quote',day,batch_size=1),start)
        return [a.push(age_row(i,interval_end_ns=start+(i+1)*C.NS))['midpoint_change_age_end_seconds'] for i in range(10)]
    before=evaluate(q)
    q,t=raw_pair(tmp_path/'b',day,[quote(start-1),quote(start+100,bid=100,ask=100.1),quote(start+10*C.NS,bid=202,ask=203)],[])
    assert evaluate(q)==before


def test_interruption_preserves_partial_without_completion(tmp_path):
    def fail():
        yield dict(session_date='2026-07-01',symbol='X',interval_end_ns=1,value=1.)
        raise R.ResourceWait('pressure')
    path=tmp_path/'features.parquet'
    with pytest.raises(R.ResourceWait):C.write_rows(path,fail(),{})
    assert not path.exists() and path.with_suffix('.partial').exists()
    with pytest.raises(FileExistsError):C.write_rows(path,fail(),{})


def test_resource_thresholds():
    safe=dict(new_rss=300*R.MiB,incumbent_rss=R.GiB,available=2*R.GiB,free=10*R.GiB)
    assert R.pressure(**safe) is None
    for name,value in [('new_rss',480*R.MiB),('available',R.GiB-1),('incumbent_rss',2*R.GiB),('free',2*R.GiB)]:
        assert R.pressure(**(safe|{name:value}))


def test_incumbent_pid_reuse_and_stale_status_are_rejected(tmp_path,monkeypatch):
    import time
    from datetime import datetime,timezone
    queue=tmp_path/'tape_feature_queue_live';queue.mkdir()
    status=dict(pid=123,process_created=42,updated_at=datetime.now(timezone.utc).isoformat(),state='running')
    (queue/'status.json').write_text(json.dumps(status))
    class P:
        pid=123
        info=dict(pid=123,cmdline=['run_tape_feature_queue_v2.py','_daemon','--root',str(queue)],create_time=43)
        def __init__(self,*args):pass
        def create_time(self):return 43
        def cmdline(self):return self.info['cmdline']
    monkeypatch.setattr(R.psutil,'Process',P);monkeypatch.setattr(R.psutil,'process_iter',lambda *args:[P()])
    with pytest.raises(R.ResourceWait,match='unverified'):R.discover_incumbent(tmp_path)
    status['process_created']=43;(queue/'status.json').write_text(json.dumps(status))
    assert R.discover_incumbent(tmp_path)['pid']==123
    status['state']='discovering';(queue/'status.json').write_text(json.dumps(status))
    assert R.discover_incumbent(tmp_path)['pid']==123
    with pytest.raises(R.ResourceWait):R.discover_incumbent(tmp_path,now=time.time()+200)


def test_pressure_monitor_only_signals_its_owned_worker(tmp_path,monkeypatch):
    import subprocess
    import sys
    real_process=R.psutil.Process
    incumbent_pid=999999
    monkeypatch.setattr(R,'admit',lambda *args,**kwargs:dict(incumbent=dict(pid=incumbent_pid,created=1),minimum_available_bytes=2*R.GiB))
    monkeypatch.setattr(R,'discover_incumbent',lambda *args:dict(pid=incumbent_pid,created=1))
    real_tree=R.tree
    monkeypatch.setattr(R,'tree',lambda identity: {incumbent_pid:1} if identity and identity['pid']==incumbent_pid else real_tree(identity))
    monkeypatch.setattr(R,'pressure',lambda **kwargs:'synthetic pressure')
    signalled=[]
    original_terminate=R.psutil.Process.terminate
    def terminate(self):signalled.append(self.pid);return original_terminate(self)
    monkeypatch.setattr(R.psutil.Process,'terminate',terminate)
    result=R.supervise([sys.executable,'-c','import time; time.sleep(10)'],research=tmp_path,scratch=tmp_path,
                       report=tmp_path/'resources.json',projected_peak=100*R.MiB)
    assert result['stop_reason']=='synthetic pressure'
    assert incumbent_pid not in signalled
    assert signalled and result['returncode']!=0
    assert (tmp_path/'yield.request').exists()


def test_frozen_selection_includes_absent_base_and_provenance_failures(tmp_path,monkeypatch):
    import csv,sqlite3
    import all_feature_month_inventory as I
    universe=tmp_path/'universe';folder=universe/'days'/'2026-07-01';folder.mkdir(parents=True)
    (universe/'plan.json').write_text(json.dumps(dict(sessions=['2026-07-01','2026-07-02'])))
    columns=['ticker','session_date','window_end_ns','last_bar_start_ns','first_trigger_time_et','configuration_hash']
    start=C.V.session_bounds('2026-07-01')[0];endpoint=start+120*C.NS
    with (folder/'candidates.csv').open('w') as f:
        w=csv.DictWriter(f,fieldnames=columns);w.writeheader()
        w.writerow(dict(zip(columns,['TEST','2026-07-01',endpoint,endpoint-60*C.NS,'2026-07-01T04:02:00-04:00','config'])))
    meta=dict(sample_only=False,session_date='2026-07-01',outputs={'candidates.csv':I.sha(folder/'candidates.csv')},configuration_hash='config',source_identity={'sha256':'raw'},reference_sha256='ref')
    (folder/'manifest.json').write_text(json.dumps(meta))
    (folder/'complete.json').write_text(json.dumps(dict(plan_sha256=I.sha(universe/'plan.json'),files={x:I.sha(folder/x) for x in ('manifest.json','candidates.csv')})))
    queue=tmp_path/'queue';queue.mkdir();db=sqlite3.connect(queue/'queue.sqlite')
    db.execute('CREATE TABLE jobs(day TEXT,symbol TEXT,state TEXT,entry TEXT,result TEXT)');db.commit();db.close()
    monkeypatch.setattr(I,'discover_incumbent',lambda *args:None)
    output=tmp_path/'selection';result=I.freeze(universe,queue,output)
    assert result['members']==1
    db=I.connect(output/'selection.sqlite',True)
    row=db.execute('SELECT * FROM members').fetchone();assert row['inventory_reason']=='missing raw/queue membership'
    assert json.loads(row['discovery'])['verified']
    assert db.execute('SELECT reason FROM dates WHERE day="2026-07-02"').fetchone()[0]
    db.close();I.verify_selection(output)
    with (output/'selection.sqlite').open('ab') as f:f.write(b'changed')
    with pytest.raises(ValueError,match='changed'):I.verify_selection(output)


def test_quote_only_eligibility_matches_primitive_reader(tmp_path):
    day='2026-07-01';start=C.V.session_bounds(day)[0]
    raw=[quote(start-1),quote(start,seq=0,size=20),
         quote(start+1,seq=0,bid=99.9,ask=100.1), # same midpoint
         quote(start+2,seq=0,bid=100,ask=100,conditions=[85]), # valid locked
         quote(start+3,seq=0,bid=101,ask=100), # invalid crossed
         quote(start+4,seq=0,bid=99,ask=101),
         quote(start+5,seq=0,bid=100,ask=102),
         quote(start+C.NS,seq=0,bid=100,ask=102,conditions=[20])]
    q,t=raw_pair(tmp_path,day,raw,[])
    first=list(C.V.events(q,'quote',day,batch_size=1))
    second=list(C.V._primitive_events(q,'quote',day,batch_size=3))
    for (ts,a),(other,b) in zip(first,second):
        assert ts==other
        for key in ('price_state_valid','midpoint'):
            if isinstance(a[key],float) and math.isnan(a[key]):assert math.isnan(b[key])
            else:assert a[key]==b[key]
    age=C.MidpointAge(iter(first),start)
    x=age.push(age_row(0,interval_end_ns=start+C.NS))
    assert x['midpoint_change_age_end_seconds']==(C.NS-5)/C.NS
    assert math.isnan(age.push(age_row(1,interval_end_ns=start+2*C.NS))['midpoint_change_age_end_seconds'])


def test_burst_versus_sustained_zero_eviction():
    burst=C.Moments(56);sustained=C.Moments(56)
    for i in range(56):burst.append(10 if i<5 else 0);sustained.append(1)
    assert burst.participation()==5/56 and sustained.participation()==1
    for i in range(56):burst.append(0)
    assert burst.mean()==0 and math.isnan(burst.participation())


def test_staging_partition_identity_completion_and_catalog(tmp_path,monkeypatch):
    import sqlite3
    import run_all_feature_month as RUN
    import all_feature_month_inventory as I
    quotes,trades,base,day,halts=build_fixture(tmp_path/'fixture',seconds=660)
    def obj(path):return dict(object_key='synthetic/'+path.name,sha256=I.sha(path),size_bytes=path.stat().st_size,rows=pq.ParquetFile(path).metadata.num_rows)
    source=dict(session_date=day,symbol='TEST',quotes=obj(quotes),trades=obj(trades))
    payload=dict(entry=dict(source=source,halts=halts),base=dict(sha256=I.sha(base),bytes=base.stat().st_size,counts={'rows':660}),
        base_key='synthetic/base.parquet',discovery=dict(verified=True,endpoint_ns=C.V.session_bounds(day)[0]+100*C.NS,provenance_hash='test'))
    output=tmp_path/'run';part=output/f'session_date={day}'/'symbol=TEST'
    result=RUN.partition(payload,part,seconds=660,local=dict(quotes=quotes,base=base))
    assert result['verification']['rows_verified']==660
    assert RUN.partition(payload,part,seconds=660,local=dict(quotes=quotes,base=base))==json.loads(json.dumps(result))
    changed=payload|{'discovery':payload['discovery']|{'endpoint_ns':1}}
    with pytest.raises(ValueError,match='identity changed'):RUN.partition(changed,part,seconds=660,local=dict(quotes=quotes,base=base))
    inv=tmp_path/'inventory';inv.mkdir();db=sqlite3.connect(inv/'inventory.sqlite')
    db.execute('CREATE TABLE members(day TEXT,symbol TEXT,status TEXT,reason TEXT,payload TEXT)')
    db.execute('INSERT INTO members VALUES (?,?,?,?,?)',(day,'TEST','admitted',None,json.dumps(payload)))
    db.execute('INSERT INTO members VALUES (?,?,?,?,?)',(day,'ABSENT','excluded','missing raw data',json.dumps({'discovery':{}})))
    db.commit();db.close()
    RUN.artifacts(inv,output)
    catalog=pq.ParquetFile(output/'catalog.parquet').read().to_pylist()
    assert len(catalog)==2 and sum(x['rows'] for x in catalog)==660
    coverage=pq.ParquetFile(output/'coverage.parquet').read().to_pylist()
    assert sum(x['rows'] for x in coverage)==660*18
    assert sum(x['post_discovery'] for x in coverage)==561*18
    assert all(sum(json.loads(x['histogram_json']))==x['eda_eligible'] for x in coverage)


def test_halt_overlay_mismatch_cannot_publish(tmp_path):
    import run_all_feature_month as RUN
    quotes,trades,base,day,halts=build_fixture(tmp_path/'fixture')
    def obj(path):return dict(object_key='synthetic/'+path.name,sha256=RUN.sha(path),size_bytes=path.stat().st_size,rows=pq.ParquetFile(path).metadata.num_rows)
    source=dict(session_date=day,symbol='TEST',quotes=obj(quotes),trades=obj(trades))
    payload=dict(entry=dict(source=source,halts=[]),base=dict(sha256=RUN.sha(base),bytes=base.stat().st_size,counts={'rows':660}),base_key='synthetic/base.parquet',discovery={})
    with pytest.raises(ValueError,match='halt overlay'):
        RUN.partition(payload,tmp_path/'partition',seconds=660,local=dict(quotes=quotes,base=base))
    assert not (tmp_path/'partition'/'manifest.json').exists()


def test_exact_join_rejects_missing_duplicate_and_changed_clock(tmp_path):
    quotes,trades,base,day,halts=build_fixture(tmp_path/'fixture',seconds=61)
    extension=tmp_path/'extension.parquet'
    C.write_rows(extension,C.extension_rows(base,quotes,day,'TEST',batch_size=7,full=False),{})
    a=next(C.rows(base));b=next(C.rows(extension));b['interval_end_ns']+=C.NS
    with pytest.raises(ValueError,match='join key'):C.assemble(a,b,{})
    shortened=tmp_path/'short.parquet';table=pq.ParquetFile(extension).read().slice(0,60);C.pq.write_table(table,shortened)
    with pytest.raises(ValueError,match='missing extension'):
        list(C.joined_rows(base,shortened,{}))
    wrong=tmp_path/'wrong.parquet';items=list(C.rows(base));items[10]['midpoint']*=1.01
    pq.write_table(C.pa.Table.from_pylist(items,schema=pq.ParquetFile(base).schema_arrow),wrong)
    with pytest.raises(ValueError,match='movement mean'):
        list(C.extension_rows(wrong,quotes,day,'TEST',full=False))


def test_age_eighty_percent_support_is_not_shortened_clock():
    events=[event(0),event(1,101),event(48*C.NS,101,False)]
    age=C.MidpointAge(iter(events),0)
    for i in range(60):x=age.push(age_row(i))
    assert x['midpoint_change_age_observation_count_60s']==48
    assert x['midpoint_change_age_mature_60s']
    assert math.isfinite(x['midpoint_change_age_p90_seconds_60s'])
    x=age.push(age_row(60))
    assert x['midpoint_change_age_observation_count_60s']==47
    assert math.isnan(x['midpoint_change_age_p90_seconds_60s'])


def test_interval_left_edge_session_labels():
    day='2026-07-01';start=C.V.session_bounds(day)[0]
    for second,expected in [(19800,'premarket'),(19801,'rth'),(43200,'rth'),(43201,'after_hours')]:
        p=primitive(second-1,day=day);base=F.enrich(C.V.FeatureStream().push(p),p)
        ext={k:base[k] for k in C.KEYS}|C.Movement().push(base)
        age=C.MidpointAge(iter([]),start)
        ext.update(age.push({k:v for k,v in base.items() if k not in ('midpoint','primitive_quote_age')}))
        assert C.assemble(base,ext,{})['session_segment']==expected


def test_remote_inventory_requires_actual_manifest_and_source_identity(monkeypatch):
    import copy
    import all_feature_month_inventory as I
    import tape_feature_store as STORE
    day='2026-07-01';symbol='TEST'
    source=dict(session_date=day,symbol=symbol)
    for stream,window in [('quotes','0355-2000'),('trades','0400-2000')]:
        source[stream]=dict(object_key=f'tq/session_date={day}/symbol={symbol}/{stream}.parquet',sha256='a'*64,size_bytes=100,rows=1,
            metadata={'session-window-et':window,'source-provider':'massive','acquisition-method':'rest'})
    entry=dict(source=source,halts=[])
    identity=STORE.partition_identity(entry,'test historical policy',57600)
    prefix=STORE.feature_prefix(identity)
    base=dict(partition_identity=identity,contract=STORE.F.identity(),sha256='b'*64,bytes=200,
              counts={'rows':57600},verification={'rows_verified':57600,'sha256':'b'*64})
    member=dict(day=day,symbol=symbol,inventory_reason=None,entry=json.dumps(entry),receipt=json.dumps({'feature_prefix':prefix}),discovery='{}')
    def head(client,bucket,key):
        obj=source['quotes' if key.endswith('quotes.parquet') else 'trades']
        return dict(ContentLength=obj['size_bytes'],Metadata=obj['metadata']|{'sha256':obj['sha256'],'rows':'1'})
    monkeypatch.setattr(STORE.S,'_head_or_none',head)
    monkeypatch.setattr(STORE,'read_manifest',lambda *args:base)
    monkeypatch.setattr(STORE,'verify_object',lambda *args:None)
    assert I.remote_entry(member,object(),'massive-equities')['base']['sha256']=='b'*64
    bad=copy.deepcopy(base);bad['partition_identity']['source']['quotes']['sha256']='c'*64
    monkeypatch.setattr(STORE,'read_manifest',lambda *args:bad)
    with pytest.raises(ValueError,match='source identity'):I.remote_entry(member,object(),'massive-equities')
    monkeypatch.setattr(STORE,'read_manifest',lambda *args:None)
    with pytest.raises(ValueError,match='missing completed'):I.remote_entry(member,object(),'massive-equities')


def test_cleanup_is_limited_to_verified_private_staging(tmp_path):
    import run_all_feature_month as RUN
    names=('quotes.parquet','base.parquet','base_prefix.parquet','extension.parquet','features.parquet','coverage.json','validation_traces.json')
    for name in names:(tmp_path/name).write_text('fixture')
    with pytest.raises(ValueError,match='incomplete'):RUN.release_staged_copies(tmp_path)
    (tmp_path/'manifest.json').write_text('{}')
    RUN.release_staged_copies(tmp_path)
    assert not (tmp_path/'quotes.parquet').exists()
    assert all((tmp_path/name).exists() for name in ('features.parquet','coverage.json','validation_traces.json','manifest.json'))


@pytest.mark.parametrize('values',[[1]*56,[2]*5+[0]*51,[1]+[0]*55,[0]*56,[],[1e-300,2e-300,3e-300],[1e300,2e300,3e300],[1,1e-200,3,0]])
def test_exact_moments_independent_scaled_reference(values):
    from all_feature_month_verify import participation
    m=C.Moments(56)
    for x in values:m.append(x)
    expected=participation(values)
    if expected is None:assert math.isnan(m.participation())
    else:assert m.participation()==pytest.approx(expected,rel=1e-10,abs=1e-12)
    assert m.count==len(values)
    rev=C.Moments(56)
    for x in reversed(values):rev.append(x)
    if values and any(values):assert rev.participation()==m.participation()


def test_moments_long_outlier_eviction_and_subnormal_scale():
    from all_feature_month_verify import participation
    from collections import deque
    m=C.Moments(56);reference=deque(maxlen=56)
    for i in range(10000):
        value=1e300 if i%113==0 else (i%7)*1e-300
        m.append(value);reference.append(value)
        assert m.participation()==pytest.approx(participation(reference),rel=1e-10,abs=1e-12)
    for scale in (5e-324,1e-300,1.,1e300):
        m=C.Moments(4)
        for x in [scale,scale,0,0]:m.append(x)
        assert m.participation()==.5


def test_v2_schema_exact_membership_and_null_first_row():
    from all_feature_month_schema import REGISTRY
    expected={'movement_mean_5s_bps','movement_participation','quoted_spread_mean_bps','trade_rate','dollar_rate',
        'trade_age_p90_seconds','quote_age_p90_seconds','midpoint_change_age_p90_seconds','movement_mean_to_spread'}
    assert set(REGISTRY)==expected
    assert set(C.FEATURES)=={f'{f}_{h}s' for f in expected for h in (60,300)} and len(C.FEATURES)==18
    schema=C.explicit_schema(C.final_columns(),{})
    assert schema.field('midpoint_observation_start_ns').type==C.pa.int64()
    assert schema.field('movement_5s_valid').type==C.pa.bool_()
    assert schema.field('trade_count_1s').type==C.pa.int64()
    assert not any('movement_p20_' in name for name in schema.names)  # Migration rejection assertion.


def test_quote_reducer_integrals_match_primitive_reference(tmp_path):
    day='2026-07-01';start=C.V.session_bounds(day)[0]
    raw=[quote(start-1),quote(start+C.NS//4,bid=100,ask=100),
        quote(start+C.NS//2,bid=101,ask=100),quote(start+3*C.NS//4,bid=99,ask=101),
        quote(start+C.NS,seq=0,bid=100,ask=102),quote(start+C.NS,seq=1,bid=99,ask=101),
        quote(start+2*C.NS,conditions=[20]),quote(start+3*C.NS+17,bid=99,ask=101)]
    q,t=raw_pair(tmp_path,day,raw,[])
    reducer=C.MidpointAge(C.V.events(q,'quote',day,batch_size=2),start)
    for p in C.V.primitives(q,t,day,'TEST',seconds=5,batch_size=2):
        row=F.enrich(C.V.FeatureStream().push(p),p)
        out=reducer.push(row)
        assert out['midpoint_valid_duration_ns']==p['midpoint_duration_ns']
        assert out['quoted_spread_valid_duration_ns']==p['unlocked_duration_ns']
        assert out['quoted_spread_integral_bps_seconds']==p['spread_mass']
    assert out['midpoint_age_observation_status']=='no_change_observed'
    assert out['midpoint_observation_start_ns']==start+3*C.NS+17


def test_age_status_seed_invalid_recovery_and_halt():
    a=C.MidpointAge(iter([event(-100),event(C.NS+100,valid=False),event(C.NS+200,100),event(2*C.NS+100,101)]),0)
    x=a.push(age_row(0));assert x['midpoint_age_observation_status']=='no_change_observed'
    assert x['midpoint_observation_start_ns']==0 and x['midpoint_no_change_observed_seconds']==1
    x=a.push(age_row(1));assert x['midpoint_observation_start_ns']==C.NS+200
    assert x['midpoint_age_observation_status']=='no_change_observed'
    x=a.push(age_row(2));assert x['midpoint_age_observation_status']=='known'
    assert x['midpoint_observation_start_ns']==C.NS+200 and x['midpoint_no_change_observed_seconds'] is None
    x=a.push(age_row(3,halt_interval_active=True));assert x['midpoint_age_observation_status']=='unobservable'
    assert x['midpoint_observation_start_ns'] is None


def test_final_file_only_reconstruction_and_corruption(tmp_path):
    import shutil
    q,t,base,day,halts=build_fixture(tmp_path/'inputs',seconds=65)
    ext=tmp_path/'extension.parquet';final=tmp_path/'features.parquet'
    C.write_rows(ext,C.extension_rows(base,q,day,'TEST',full=False),{},columns=C.extension_columns())
    C.write_rows(final,C.joined_rows(base,ext,dict(verified=True,endpoint_ns=C.V.session_bounds(day)[0])),{},columns=C.final_columns())
    shutil.rmtree(tmp_path/'inputs');ext.unlink()
    result=C.verify(final,day,'TEST',65)
    assert set(result['reconstructed_fields'])==set(C.FEATURES)
    assert all(sum(g['histogram'])==g['eda'] for g in result['coverage'])
    values=list(C.rows(final));values[62]['quoted_spread_integral_bps_seconds']*=2
    bad=tmp_path/'corrupted.parquet';pq.write_table(C.pa.Table.from_pylist(values,schema=pq.ParquetFile(final).schema_arrow),bad)
    with pytest.raises(ValueError,match='final reconstruction mismatch'):C.verify(bad,day,'TEST',65)


def test_prefix_no_copy_one_quote_decoder_and_one_final_pass(tmp_path,monkeypatch):
    import run_all_feature_month as RUN
    q,t,base,day,halts=build_fixture(tmp_path/'inputs',seconds=80)
    def obj(path):return dict(object_key='fixture/'+path.name,sha256=RUN.sha(path),size_bytes=path.stat().st_size,rows=pq.ParquetFile(path).metadata.num_rows)
    payload=dict(entry=dict(source=dict(session_date=day,symbol='TEST',quotes=obj(q),trades=obj(t)),halts=[]),
        base=dict(sha256=RUN.sha(base),bytes=base.stat().st_size,counts={'rows':80}),base_key='fixture/base',discovery={})
    calls=[];reader=C.V.events
    def count(*args,**kw):calls.append(args[1]);yield from reader(*args,**kw)
    monkeypatch.setattr(C.V,'events',count)
    result=RUN.partition(payload,tmp_path/'part',seconds=65,batch_size=7,local=dict(quotes=q,base=base))
    assert calls==['quote']
    assert not (tmp_path/'part'/'base_prefix.parquet').exists()
    assert result['raw_stats']['quote_events_reduced']<obj(q)['rows']
    assert result['io_passes']['final_decodes']==1
    assert result['output']['rows']==65


def test_immutable_execution_selection_and_unselected_catalog(tmp_path,monkeypatch):
    import sqlite3
    import all_feature_month_selection as S
    import run_all_feature_month as RUN
    inv=tmp_path/'inv';inv.mkdir();db=sqlite3.connect(inv/'inventory.sqlite')
    db.execute('CREATE TABLE members(day TEXT,symbol TEXT,status TEXT,reason TEXT,payload TEXT)')
    db.executemany('INSERT INTO members VALUES (?,?,?,?,?)',[('2026-07-01',s,'admitted',None,'{}') for s in ('A','B')]);db.commit();db.close()
    monkeypatch.setattr(RUN,'verified_inventory',lambda _:dict(inventory_hash='parent'))
    requested=tmp_path/'members.jsonl';requested.write_text(json.dumps(dict(day='2026-07-01',symbol='A',seconds=65))+'\n')
    selected=tmp_path/'selected';meta=S.create(inv,selected,requested,'fixture metadata selection',seed=3)
    assert S.verify(selected,'parent')['selection_hash']==meta['selection_hash']
    db=RUN.connect(inv/'inventory.sqlite',True);items=list(S.members(db,selected));db.close()
    assert [(x['symbol'],x['seconds']) for x in items]==[('A',65)]
    out=tmp_path/'out';out.mkdir()
    RUN.save(out/'dataset_manifest.json',dict(identity=dict(execution_selection_root=str(selected))))
    RUN.artifacts(inv,out)
    catalog=pq.ParquetFile(out/'catalog.parquet').read().to_pylist()
    assert {r['symbol']:r['status'] for r in catalog}=={'A':'missing_selected','B':'unselected'}
    with pytest.raises(ValueError,match='parent'):S.verify(selected,'other')
    with (selected/'selection.sqlite').open('ab') as f:f.write(b'x')
    with pytest.raises(ValueError,match='changed'):S.verify(selected,'parent')


@pytest.mark.parametrize('phase',['staging','extension','assembly','verification'])
def test_failure_each_partition_phase_has_no_completion(tmp_path,monkeypatch,phase):
    import run_all_feature_month as RUN
    import all_feature_month_verify as VERIFY
    q,t,base,day,halts=build_fixture(tmp_path/'fixture',seconds=61)
    def obj(path):return dict(object_key='fixture/'+path.name,sha256=RUN.sha(path),size_bytes=path.stat().st_size,rows=pq.ParquetFile(path).metadata.num_rows)
    payload=dict(entry=dict(source=dict(session_date=day,symbol='TEST',quotes=obj(q),trades=obj(t)),halts=[]),
        base=dict(sha256=RUN.sha(base),bytes=base.stat().st_size,counts={'rows':61}),base_key='fixture/base',discovery={})
    original_hash=RUN.sha(base)
    def fail(*a,**kw):raise R.ResourceWait('injected '+phase)
    target,name={'staging':(RUN,'local_identity'),'extension':(C.Movement,'push'),
                 'assembly':(C,'assemble'),'verification':(VERIFY.Reference,'push')}[phase]
    monkeypatch.setattr(target,name,fail)
    out=tmp_path/'part'
    with pytest.raises(R.ResourceWait,match='injected'):RUN.partition(payload,out,seconds=61,local=dict(quotes=q,base=base))
    assert not (out/'manifest.json').exists() and RUN.sha(base)==original_hash
    assert (out/'attempt.json').exists()


def test_zero_total_reason_preserves_good_source(tmp_path):
    day='2026-07-01';start=C.V.session_bounds(day)[0]
    q,t=raw_pair(tmp_path/'raw',day,[quote(start)],[])
    base=tmp_path/'base.parquet';F.write(q,t,day,'TEST',base,{},seconds=61)
    ext=tmp_path/'ext.parquet';C.write_rows(ext,C.extension_rows(base,q,day,'TEST',full=False),{})
    row=list(C.joined_rows(base,ext,dict(verified=True,endpoint_ns=start)))[-1]
    f='movement_participation_60s'
    assert row[f] is None and row['movement_mean_5s_bps_60s']==0
    assert row[f+'_reason_mask']==C.REASONS['undefined']|C.REASONS['zero_total_movement']
    assert not row[f+'_eda_eligible'] and row['movement_mean_5s_bps_60s_eda_eligible']
    assert row['trade_count_1s']==0 and row['dollar_volume_1s']==0


def test_capability_rejects_missing_transactions_and_wrong_type():
    from all_feature_month_schema import capabilities
    base=F.schema()
    assert capabilities(base)['raw_trades_replayed'] is False
    with pytest.raises(ValueError,match='capability absent'):capabilities(C.pa.schema([f for f in base if f.name!='primitive_trade_count']))
    wrong=C.pa.schema([C.pa.field(f.name,C.pa.string()) if f.name=='primitive_trade_count' else f for f in base])
    with pytest.raises(ValueError,match='numeric capability'):capabilities(wrong)


def test_optimized_reference_matches_brute_oracle(tmp_path):
    import all_feature_month_verify as fast
    import all_feature_month_verify_oracle as oracle
    q,t,base,day,halts=build_fixture(tmp_path/'input')
    ext=tmp_path/'ext.parquet';out=tmp_path/'features.parquet'
    C.write_rows(ext,C.extension_rows(base,q,day,'TEST',full=False),{},columns=C.extension_columns())
    C.write_rows(out,C.joined_rows(base,ext,dict(verified=True,endpoint_ns=C.V.session_bounds(day)[0])),{},columns=C.final_columns())
    args=(out,day,'TEST',660,C.rows,C.V.session_bounds(day)[0])
    assert fast.verify(*args)==oracle.verify(*args)


def test_reference_quantiles_null_duplicates_and_eviction():
    import random
    from all_feature_month_verify import ReferenceQuantile,quantile
    rng=random.Random(812)
    for capacity in (1,7,60,300):
        window=ReferenceQuantile(capacity);history=[]
        for i in range(capacity*3+5):
            value=rng.choice((None,0.,1.,2.,1e10,rng.random()))
            window.append(value);history.append(value);history=history[-capacity:]
            assert window.quantile()==quantile([x for x in history if x is not None])
        window.clear();assert len(window)==0 and window.quantile() is None


def test_reference_tree_extreme_scale_and_outlier_eviction():
    import random
    from all_feature_month_verify import ReferenceParticipation,participation
    rng=random.Random(721)
    for capacity in (1,7,56,296):
        tree=ReferenceParticipation(capacity);history=[]
        values=[1e308]+[5e-324]*capacity+[None,0.]*capacity
        values += [rng.choice((None,0.,10.**rng.uniform(-300,300))) for _ in range(capacity*2)]
        for x in values:
            tree.append(x);history.append(x);history=history[-capacity:]
            valid=[v for v in history if v is not None]
            assert tree.count==len(valid)
            assert tree.positive==sum(v>0 for v in valid)
            expected=participation(valid)
            if expected is None:assert tree.participation() is None
            else:assert tree.participation()==pytest.approx(expected,rel=1e-10,abs=1e-12)


def test_admission_history_expires_and_rejects_changed_incumbent(monkeypatch):
    clock=[20.];incumbent=[None]
    monkeypatch.setattr(R.time,'monotonic',lambda:clock[0])
    monkeypatch.setattr(R,'discover_incumbent',lambda _:incumbent[0])
    h=R.AdmissionHistory();assert h.seconds(None)==10
    h.record(19.5,None);assert h.seconds(None)==0
    clock[0]=21.;assert h.seconds(None)==10
    h.record(21.,None);incumbent[0]={'pid':123,'created':1}
    assert h.seconds(None)==10 and h.observed is None
    h.record(21.,incumbent[0]);h.clear();assert h.seconds(None)==10


def test_batch_finite_validation_preserves_nulls_and_rejects_infinity(tmp_path):
    path=tmp_path/'values.parquet'
    table=C.pa.table({'value':C.pa.array([None,0.,float('inf')],type=C.pa.float64())})
    pq.write_table(table,path)
    assert list(C.rows(path,limit=2,validate_finite=True))==[{'value':None},{'value':0.}]
    with pytest.raises(ValueError,match='nonfinite persisted value'):
        list(C.rows(path,validate_finite=True))


def test_staging_fixed_overhead_model():
    import run_all_feature_month as RUN
    samples=[dict(partition=dict(staged_bytes=b,staging_seconds=2+b*.001)) for b in (100,1000,10000)]
    model=RUN.staging_model(samples,100000,10)
    assert model['fixed_seconds_per_partition']==pytest.approx(2)
    assert model['seconds_per_byte']==pytest.approx(.001)
    assert model['projected_seconds']==pytest.approx(120)


def test_admission_identity_race_forces_cold_admission(tmp_path,monkeypatch):
    h=R.AdmissionHistory();h.record(0,{'pid':1})
    monkeypatch.setattr(h,'seconds',lambda _:0)
    calls=[]
    def admission(*args,seconds=10,**kwargs):
        calls.append(seconds)
        if seconds==10:raise R.ResourceWait('test stops before child launch')
        return {'incumbent':{'pid':2}}
    monkeypatch.setattr(R,'admit',admission)
    with pytest.raises(R.ResourceWait,match='before child launch'):
        R.supervise([],research=tmp_path,scratch=tmp_path,report=tmp_path/'resources.json',projected_peak=192*R.MiB,admission_history=h)
    assert calls==[0,10] and h.observed is None
