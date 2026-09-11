"""Small direct-path/source parity and transactional compact regressions."""
from pathlib import Path
import sys
import math
import json
from collections import Counter
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src/04_research'))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src/03_features'))
import compact_product as P
import compact_product_schema as S
import direct_frozen_product as D
import all_feature_month_core as C
import july_r2_product as J
from test_all_feature_month import build_fixture
from test_economic_tape_v3 import quote,raw_pair


def metadata(day,n):
    return dict(session_date=day,symbol='TEST',expected_rows=n,inputs={},overlay={},
                calculation=P.calculation_identity(),discovery_verified=True)


def test_explicit_split_contract():
    removed=set(S.FEATURE_COLUMNS)|set(S.DISCOVERY_FIELDS)|{'session_segment'}
    expected=list(S.KEYS)+[k for k in C.final_columns() if k not in removed and not k.endswith(('_analysis_valid','_eda_eligible'))]
    assert list(S.SUPPORT_COLUMNS)==expected
    assert len(S.FEATURE_COLUMNS)==45 and len(S.SUPPORT_COLUMNS)==63
    assert P.S.schema_hash(S.FEATURE_SCHEMA)==S.FEATURE_SCHEMA_HASH
    assert P.S.schema_hash(S.SUPPORT_SCHEMA)==S.SUPPORT_SCHEMA_HASH
    assert set(C.PRIMITIVES if hasattr(C,'PRIMITIVES') else D.REG.PRIMITIVES)<=set(S.FEATURE_COLUMNS+S.SUPPORT_COLUMNS)
    assert S.eligibility(32,True)==(True,False)
    for mask in (None,256,-1,1.0):
        with pytest.raises(ValueError):S.eligibility(mask,True)


@pytest.mark.parametrize('batch_size',[1,127])
def test_direct_all_field_parity_and_persisted_roundtrip(tmp_path,batch_size,monkeypatch):
    q,t,b,day,halts=build_fixture(tmp_path,660)
    discovery=dict(verified=True,endpoint_ns=C.session_start(day)+100*C.NS,provenance_hash='fixture')
    reference=list(J.raw_product_rows_v1(q,t,day,'TEST',discovery,seconds=660,halts=halts,batch_size=batch_size))
    def forbidden(*a,**k):raise AssertionError('wide legacy generation invoked')
    monkeypatch.setattr(C.V,'FeatureStream',forbidden)
    monkeypatch.setattr(C,'assemble',forbidden)
    calls=[];original=D.events
    def events(*a,**k):calls.append(a[1]);return original(*a,**k)
    monkeypatch.setattr(D,'events',events)
    stats=Counter()
    pairs=list(D.product_pairs(q,t,day,'TEST',discovery,seconds=660,halts=halts,batch_size=batch_size,stats=stats))
    assert calls==['quote','trade']
    for i,((f,s,d),r) in enumerate(zip(pairs,reference,strict=True)):
        for k,v in (s|f|d).items():
            if isinstance(v,float):
                assert r[k] is not None and math.isclose(v,r[k],rel_tol=1e-10,abs_tol=1e-12),(i,k,v,r[k])
            else:assert v==r[k],(i,k,v,r[k])
    out=tmp_path/'compact';meta=metadata(day,660)
    result=P.write_partition(out,iter(pairs),meta,output_rows=127)
    assert result['validation']['rows_verified']==660
    P.verify_complete(out,meta)
    # Repacking is exactly lossless, including carried finite diagnostics.
    repack=tmp_path/'repack';P.write_partition(repack,P.split_rows(iter(reference)),meta)
    assert list(P.joined_rows(repack,result['metadata']))==reference


@pytest.mark.parametrize('batch_size',[1,127])
def test_constant_refresh_and_nanosecond_excursion(tmp_path,batch_size):
    from fractions import Fraction
    day='2026-07-01';start=C.session_start(day)
    rows=[quote(start-1,bid=1.23,ask=1.24)]
    for i in range(306):
        for off in (3,123456789,789012345):rows.append(quote(start+i*C.NS+off,bid=1.23,ask=1.24))
    q,t=raw_pair(tmp_path,day,rows,[])
    for f,s,d in D.product_pairs(q,t,day,'TEST',{},seconds=306,batch_size=batch_size):
        assert f['midpoint']==(1.23+1.24)/2
        for h in (60,300):
            if s[f'state_mature_{h}s']:
                assert f[f'movement_mean_5s_bps_{h}s']==0
                assert f[f'movement_participation_{h}s'] is None
                assert f[f'movement_participation_{h}s_reason_mask']&128
    other=tmp_path/'excursion';other.mkdir()
    q,t=raw_pair(other,day,[quote(start-1,bid=1.23,ask=1.24),quote(start+5*C.NS+123,bid=1.24,ask=1.25),quote(start+5*C.NS+124,bid=1.23,ask=1.24)],[])
    result=list(D.product_pairs(q,t,day,'TEST',{},seconds=6,batch_size=batch_size))[-1][0]
    a,b=(1.23+1.24)/2,(1.24+1.25)/2
    assert result['midpoint']==float((Fraction(a)*(C.NS-1)+Fraction(b))/C.NS)>a
    assert result['movement_mean_5s_bps_60s']>0


@pytest.mark.parametrize('mutation',['metadata','mask','value','duplicate','missing_support'])
def test_corruption_cannot_commit(tmp_path,mutation):
    q,t,b,day,halts=build_fixture(tmp_path,12)
    rows=list(J.raw_product_rows_v1(q,t,day,'TEST',{},seconds=12,halts=halts))
    if mutation=='metadata':rows[2]['discovery_provenance_hash']='changed'
    if mutation=='mask':rows[2][S.FEATURES[0]+'_reason_mask']=256
    if mutation=='value':rows[2]['trade_rate_60s']=float('inf')
    if mutation=='duplicate':rows[2]['interval_end_ns']=rows[1]['interval_end_ns']
    if mutation=='missing_support':del rows[2]['trade_count_1s']
    out=tmp_path/'bad'
    with pytest.raises((ValueError,KeyError)):
        P.write_partition(out,P.split_rows(rows),metadata(day,12))
    assert not (out/'manifest.json').exists()


@pytest.mark.parametrize('initial_halt',[False,True])
def test_boundaries_locked_crossed_recovery_and_old_ages(tmp_path,initial_halt):
    day='2026-07-01';start=C.session_start(day)
    events=[quote(start-1,bid=99,ask=101),quote(start,seq=0,bid=100,ask=102),
        quote(start,seq=1,bid=99,ask=101),quote(start+2*C.NS,bid=100,ask=100),
        quote(start+3*C.NS,bid=101,ask=100),quote(start+3*C.NS+500,bid=99,ask=101),
        quote(start+4*C.NS,bid=100,ask=102),
        quote(start+10*C.NS,bid=99,ask=101),quote(start+405*C.NS,bid=98,ask=100),
        quote(start+406*C.NS,bid=99,ask=101)]
    q,t=raw_pair(tmp_path,day,events,[])
    halts=[(start-100,start+2*C.NS+100,'initial')] if initial_halt else []
    kw=dict(seconds=710,halts=halts,batch_size=1,continuity_breaks_ns=[start+400*C.NS])
    reference=J.raw_product_rows_v1(q,t,day,'TEST',{},**kw)
    pairs=D.product_pairs(q,t,day,'TEST',{},**kw)
    for (f,s,d),expected in zip(pairs,reference,strict=True):
        for k,value in (f|s|d).items():
            if isinstance(value,float):assert value==pytest.approx(expected[k],rel=1e-10,abs=1e-12),(k,value,expected[k])
            else:assert value==expected[k],(k,value,expected[k])
    # Missing discovery is an eligibility condition, never a global row filter.
    assert f['post_discovery_eligible'] is False
    assert s['midpoint_change_age_end_seconds']>300


@pytest.mark.parametrize('field',['trade_rate_60s','movement_mean_5s_bps_60s','quote_age_p90_seconds_60s'])
def test_persisted_finite_immature_diagnostic_corruption_rejected(tmp_path,field):
    q,t,b,day,halts=build_fixture(tmp_path,12)
    rows=list(J.raw_product_rows_v1(q,t,day,'TEST',{},seconds=12,halts=halts))
    rows[-1][field]+=1
    with pytest.raises(ValueError,match='mismatch'):
        P.write_partition(tmp_path/'bad',P.split_rows(rows),metadata(day,12),validation_mode='reconstruction')


def test_full_session_exhausts_order_validation_in_source_tail(tmp_path):
    day='2026-07-01';start=C.session_start(day);end=start+57600*C.NS
    q,t=raw_pair(tmp_path,day,[quote(start-1),quote(end+1),quote(end)],[])
    with pytest.raises(ValueError,match='unordered quote'):
        for _ in D.product_pairs(q,t,day,'TEST',{},batch_size=1):pass
