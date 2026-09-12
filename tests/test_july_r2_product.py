"""Tiny fixtures cover transactional publication and all-field reconstruction."""
import io
import json
from pathlib import Path
import sys
import pytest
from tape_data_product.features import july_r2_product as J
from tape_data_product.features import all_feature_month_core as C
from test_all_feature_month import build_fixture
from test_r2_tq_storage import FakeClient

class Client(FakeClient):
    fail=None
    def get_object(self,*,Bucket,Key): return {'Body':io.BytesIO(self.objects[Key])}
    def upload_file(self,Filename,Bucket,Key,ExtraArgs,Config):
        if self.fail and Key.endswith(self.fail): raise RuntimeError('injected publication failure')
        super().upload_file(Filename,Bucket,Key,ExtraArgs,Config)


def fixture(tmp_path,seconds=660):
    q,t,b,day,halts=build_fixture(tmp_path,seconds)
    client=Client()
    for path in (q,t,b):
        J.S.publish_parquet_immutable(client,'test',path,'fixture/'+path.name,upload_workers=1)
    def obj(p):return dict(object_key='fixture/'+p.name,sha256=J.sha(p),size_bytes=p.stat().st_size,rows=C.pq.ParquetFile(p).metadata.num_rows)
    source=dict(session_date=day,symbol='TEST',quotes=obj(q),trades=obj(t))
    payload=dict(entry=dict(source=source,halts=halts),base=dict(sha256=J.sha(b),bytes=b.stat().st_size,counts={'rows':seconds}),base_key='fixture/'+b.name,discovery=dict(verified=True,endpoint_ns=C.session_start(day)+100*C.NS,timing_basis='nominal',provenance_hash='test'))
    return client,payload,q,b,day


def test_roundtrip_all_fields_cleanup_and_restart(tmp_path):
    client,payload,q,base,day=fixture(tmp_path)
    receipt=J.build_publish(payload,tmp_path/'attempt',tmp_path/'receipt.json',client=client,bucket='test',seconds=660,inventory_hash='test')
    assert not (tmp_path/'attempt').exists()
    assert base.exists() and q.exists()
    catalog=J.STORE.read_manifest(client,'test',J.prefix(receipt['manifest']['identity'])+'/catalog.json')
    ext=tmp_path/'expected.parquet';wide=tmp_path/'wide.parquet'
    C.write_rows(ext,C.extension_rows(base,q,day,'TEST',full=False),{})
    C.write_rows(wide,C.joined_rows(base,ext,payload['discovery']),{})
    remote=(r for batch in J.read_batches(client,'test',catalog,tmp_path/'read',allow_partial=True,batch_size=19) for r in batch.to_pylist())
    for actual,expected in zip(remote,C.rows(wide),strict=True): assert actual==expected
    assert not list((tmp_path/'read').iterdir())
    uploads=client.upload_calls
    J.build_publish(payload,tmp_path/'second',tmp_path/'receipt.json',client=client,bucket='test',seconds=660,inventory_hash='test')
    assert client.upload_calls==uploads and not (tmp_path/'second').exists()
    assert set(C.FEATURES)&set(C.extension_columns())=={'movement_participation_60s','movement_participation_300s','midpoint_change_age_p90_seconds_60s','midpoint_change_age_p90_seconds_300s'}
    assert not any('p20' in name for name in catalog['columns'])
    with pytest.raises(ValueError,match='partial'): list(J.read_batches(client,'test',catalog,tmp_path/'read'))
    client.metadata[catalog['base']['object_key']]['sha256']='0'*64
    with pytest.raises(RuntimeError): J.recover(client,'test',receipt['manifest']['identity'])


@pytest.mark.parametrize('phase',['extension.parquet','coverage.json','validation.json','catalog.json','manifest.json'])
def test_interrupted_publication_no_completion_or_rebuild(tmp_path,phase):
    client,payload,*_=fixture(tmp_path,12);client.fail=phase
    with pytest.raises(RuntimeError): J.build_publish(payload,tmp_path/'attempt',tmp_path/'receipt.json',client=client,bucket='test',seconds=12,inventory_hash='test')
    ident=J.identity(payload,12,'test')
    assert J.recover(client,'test',ident) is None
    assert not (tmp_path/'receipt.json').exists()
    assert (tmp_path/'attempt'/'base.parquet').exists()
    with pytest.raises(ValueError,match='incomplete private'): J.build_publish(payload,tmp_path/'attempt',tmp_path/'receipt.json',client=client,bucket='test',seconds=12,inventory_hash='test')
    if phase!='extension.parquet':
        with pytest.raises(ValueError,match='uncommitted remote'): J.build_publish(payload,tmp_path/'new',tmp_path/'receipt.json',client=client,bucket='test',seconds=12,inventory_hash='test')


def test_versioned_builder_adapter_same_reducers(tmp_path):
    client,payload,q,base,day=fixture(tmp_path,12)
    a=J.builder_extension_v1(C.rows(base,C.EXTENSION_BASE_COLUMNS),C.V.events(q,'quote',day),C.session_start(day))
    b=C.extension_rows(base,q,day,'TEST',full=False)
    for x,y in zip(a,b,strict=True):
        for k in x:
            if isinstance(x[k],float):C.equal(x[k],y[k],k)
            else: assert x[k]==y[k]


@pytest.mark.parametrize('mutation',['gap','duplicate','short','extra'])
def test_reader_rejects_noncanonical_grid_with_valid_file_hash(tmp_path,mutation):
    client,payload,q,base,day=fixture(tmp_path,12)
    result=J.build_publish(payload,tmp_path/'attempt',tmp_path/'receipt.json',client=client,bucket='test',seconds=12,inventory_hash='test')
    catalog=J.STORE.read_manifest(client,'test',J.prefix(result['manifest']['identity'])+'/catalog.json')
    extension=tmp_path/'ext.parquet';J.stage(client,'test',catalog['extension'],extension)
    rows=list(C.rows(extension));extension.unlink()
    if mutation=='gap': rows[3]['interval_end_ns']+=C.NS
    if mutation=='duplicate': rows[3]=rows[2].copy()
    if mutation=='short':rows.pop()
    if mutation=='extra':rows.append(rows[-1].copy())
    meta=C.write_rows(extension,iter(rows),{})
    catalog['extension'].update(sha256=meta['sha256'],size_bytes=meta['bytes'],rows=meta['rows'])
    catalog['catalog_hash']=J.digest({k:v for k,v in catalog.items() if k!='catalog_hash'})
    with pytest.raises(ValueError): list(J.local_rows(catalog,base,extension,columns=['movement_participation_60s']))


def test_raw_main_builder_single_decode_matches_backfill(tmp_path,monkeypatch):
    client,payload,q,base,day=fixture(tmp_path)
    trades=tmp_path/'raw'/'trades.parquet'
    # Locate the source filename from the canonical fixture helper.
    trades=next((tmp_path/'raw').rglob('trades.parquet'))
    calls=[];original=C.V._primitive_events
    def record(*args,**kwargs):calls.append(args[1]);return original(*args,**kwargs)
    monkeypatch.setattr(C.V,'_primitive_events',record)
    ext=tmp_path/'extension.parquet'
    C.write_rows(ext,C.extension_rows(base,q,day,'TEST',full=False),{})
    raw=J.raw_product_rows_v1(q,trades,day,'TEST',payload['discovery'],seconds=660,halts=payload['entry']['halts'])
    for actual,expected in zip(raw,C.joined_rows(base,ext,payload['discovery']),strict=True): assert actual==expected
    assert calls==['quote','trade']


def test_reviewed_recovery_finishes_publication_without_compute(tmp_path,monkeypatch):
    client,payload,*_=fixture(tmp_path,12);client.fail='manifest.json'
    with pytest.raises(RuntimeError):J.build_publish(payload,tmp_path/'attempt',tmp_path/'receipt.json',client=client,bucket='test',seconds=12,inventory_hash='test')
    client.fail=None
    def forbidden(*args,**kwargs):raise AssertionError('recovery must not compute')
    monkeypatch.setattr(C,'extension_rows',forbidden)
    result=J.recover_attempt(payload,tmp_path/'attempt',tmp_path/'receipt.json',client=client,bucket='test',seconds=12,inventory_hash='test',review_reason='Test confirms injected manifest upload failure; retained extension validated.')
    assert result['state']=='reviewed_recovery_verified'
    assert not (tmp_path/'attempt').exists()
    assert J.recover(client,'test',result['manifest']['identity'])==result['manifest']


def test_catalog_binding_cannot_be_changed(tmp_path):
    client,payload,*_=fixture(tmp_path,12)
    result=J.build_publish(payload,tmp_path/'attempt',tmp_path/'receipt.json',client=client,bucket='test',seconds=12,inventory_hash='test')
    root=J.prefix(result['manifest']['identity'])
    catalog=J.STORE.read_manifest(client,'test',root+'/catalog.json')
    catalog['conversions']['trade_age_p90_seconds_60s']['scale']=1
    with pytest.raises(ValueError,match='catalog hash'):J.validate_catalog(catalog)
    catalog['catalog_hash']=J.digest({k:v for k,v in catalog.items() if k!='catalog_hash'})
    with pytest.raises(ValueError,match='conversions'):J.validate_catalog(catalog)


@pytest.fixture(autouse=True)
def ample_disk_for_small_transaction_fixtures(monkeypatch):
    """Exercise transactions independently of unrelated host free-space pressure.

    Fixtures write kilobytes; production reserve tests explicitly supply low free
    capacity to the resource predicates and retain the real reserve constants.
    """
    import shutil
    from collections import namedtuple
    Usage = namedtuple('Usage', 'total used free')
    monkeypatch.setattr(shutil, 'disk_usage', lambda path: Usage(100*1024**3, 10*1024**3, 90*1024**3))
