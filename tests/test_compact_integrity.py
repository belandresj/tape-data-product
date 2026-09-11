"""Integrity is structural validation; reconstruction is an explicit diagnostic."""
from pathlib import Path
import sys,json
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src/04_research'))
import compact_product as P
import compact_product_runtime as R
import compact_product_storage as STORE
from test_direct_frozen_product import metadata
from test_all_feature_month import build_fixture
from test_july_r2_product import Client
import july_r2_product as J


def fixture(tmp_path,n=12):
    q,t,b,day,halts=build_fixture(tmp_path,n)
    rows=list(J.raw_product_rows_v1(q,t,day,'TEST',{},seconds=n,halts=halts))
    return rows,metadata(day,n)


@pytest.mark.parametrize('mutation',['nan','inf','null','fractional','symbol','date','duplicate','missing_row','extra_column','missing_support','mask','duration'])
def test_integrity_rejects_structural_corruption(tmp_path,mutation):
    rows,meta=fixture(tmp_path);pairs=list(P.split_rows(rows));f,s,d=pairs[2]
    if mutation=='nan':f['trade_rate_60s']=float('nan')
    if mutation=='inf':f['trade_rate_60s']=float('inf')
    if mutation=='null':f['symbol']=None
    if mutation=='fractional':s['trade_count_1s']=.5
    if mutation=='symbol':s['symbol']='WRONG'
    if mutation=='date':f['session_date']='2026-07-02'
    if mutation=='duplicate':f['interval_end_ns']=pairs[1][0]['interval_end_ns']
    if mutation=='missing_row':pairs.pop()
    if mutation=='extra_column':s['unexpected']=1
    if mutation=='missing_support':del s['trade_count_1s']
    if mutation=='mask':f['trade_rate_60s_reason_mask']=256
    if mutation=='duration':s['midpoint_valid_duration_ns']=1000000001
    out=tmp_path/'output'
    with pytest.raises((ValueError,TypeError)):
        P.write_partition(out,iter(pairs),meta,output_rows=5)
    assert not (out/'manifest.json').exists()


def test_integrity_does_not_claim_numerical_audit(tmp_path):
    rows,meta=fixture(tmp_path);rows[-1]['trade_rate_60s']+=1
    out=tmp_path/'output';manifest=P.write_partition(out,P.split_rows(rows),meta)
    assert manifest['validation']['independent_reconstruction']=='not_run'
    assert manifest['validation']['reconstructed_fields']==[]
    original=(out/'manifest.json').read_bytes()
    assert P.verify_complete(out)==manifest
    with pytest.raises(ValueError,match='reconstruction mismatch'):P.audit_complete(out)
    assert (out/'manifest.json').read_bytes()==original


def test_integrity_never_constructs_reference_and_unknown_policy_fails(tmp_path,monkeypatch):
    rows,meta=fixture(tmp_path)
    monkeypatch.setattr(P.R,'Reference',lambda **kw:pytest.fail('reconstruction on normal path'))
    out=tmp_path/'output';P.write_partition(out,P.split_rows(rows),meta)
    P.verify_complete(out)
    with pytest.raises(ValueError,match='unknown validation mode'):
        P.write_partition(tmp_path/'bad',P.split_rows(rows),meta,validation_mode='skip_everything')


def test_legacy_manifest_and_explicit_audit(tmp_path):
    rows,meta=fixture(tmp_path)
    out=tmp_path/'old';manifest=P.write_partition(out,P.split_rows(rows),meta,validation_mode='reconstruction')
    # Recreate a legacy completion marker: no policy and original evidence shape.
    del manifest['metadata']['validation_policy']
    manifest['partition_identity']=P.digest(manifest['metadata'])
    manifest['validation']=P.verify(out,manifest['metadata'])
    P.atomic_json(out/'manifest.json',manifest)
    assert P.verify_complete(out)==manifest
    assert P.audit_complete(out)['state']=='passed'


def test_persisted_integrity_detects_damaged_file_and_missing_companion(tmp_path):
    rows,meta=fixture(tmp_path);out=tmp_path/'out';P.write_partition(out,P.split_rows(rows),meta)
    with (out/'features.parquet').open('ab') as f:f.write(b'corrupt')
    with pytest.raises(ValueError,match='object identity'):P.verify_complete(out)
    (out/'support.parquet').unlink()
    with pytest.raises(Exception):P.verify_integrity(out,meta)


def test_resume_publication_evidence_is_bound_to_member(tmp_path):
    rows,meta=fixture(tmp_path);out=tmp_path/'product';manifest=P.write_partition(out,P.split_rows(rows),meta)
    client=Client();publication=STORE.publish(out,manifest,client,'test',record=lambda *a:None)
    member=dict(inputs=meta['inputs'],overlay=meta['overlay'],session_date=meta['session_date'],symbol=meta['symbol'],discovery={'verified':True})
    receipt=dict(state='complete',publication=publication,manifest_directory=str(out),partition_identity=manifest['partition_identity'],rows_verified=12)
    assert R.verify_published_receipt(member,receipt,dict(seconds=12,bucket='test'))
    with pytest.raises(ValueError,match='dependencies'):
        R.verify_published_receipt(dict(member,symbol='OTHER'),receipt,dict(seconds=12))
    legacy_receipt=dict(receipt,attempt_path=str(tmp_path));legacy_receipt.pop('manifest_directory')
    assert R.verify_published_receipt(member,legacy_receipt,dict(seconds=12))
    publication['objects']['features']['sha256']='f'*64
    with pytest.raises(ValueError,match='object identity'):
        R.verify_published_receipt(member,receipt,dict(seconds=12))


def test_mixed_validation_report(tmp_path):
    inventory=tmp_path/'inventory.jsonl';inventory.write_text(''.join(json.dumps(dict(session_date='2026-07-01',symbol=s))+'\n' for s in ('A','B')))
    def worker(m,r,p):return dict(state='complete',rows_verified=12,manifest='fixture',partition_identity=m['symbol'],validation_mode='integrity' if m['symbol']=='A' else 'legacy_reconstruction',independent_reconstruction='not_run' if m['symbol']=='A' else 'passed')
    run=tmp_path/'run';assert R.run(run,inventory,dict(seconds=12),worker)==0
    status=R.read(run/'run_status.json')
    assert status['validation_counts']=={'integrity':1,'legacy_reconstruction':1}
    catalog=[json.loads(line) for line in (run/'catalog.jsonl').read_text().splitlines()]
    assert [r['independent_reconstruction'] for r in catalog]==['not_run','passed']


def test_drain_finishes_active_work_without_new_admission(tmp_path):
    import threading,time
    inventory=tmp_path/'inventory.jsonl';inventory.write_text(''.join(json.dumps(dict(session_date='2026-07-01',symbol=s))+'\n' for s in ('A','B','C')))
    root=tmp_path/'run';barrier=threading.Barrier(2);calls=[]
    def worker(m,r,p):
        calls.append(m['symbol']);barrier.wait(timeout=3)
        if m['symbol']=='A':(root/'DRAIN').write_text('user request')
        time.sleep(.05)
        return dict(state='complete',rows_verified=12,manifest='fixture',partition_identity=m['symbol'])
    assert R.run(root,inventory,dict(seconds=12,workers=2,worker_memory_reservation_bytes=64*R.MiB),worker)==1
    assert sorted(calls)==['A','B']
    status=R.read(root/'run_status.json');assert status['state']=='drained'
    assert status['counts']=={'complete':2,'pending':1}
