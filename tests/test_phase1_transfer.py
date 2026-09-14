import hashlib
import importlib.util
import io
import json
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location('transfer_sample', Path(__file__).resolve().parents[1] / 'scripts/phase1/transfer_sample.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Client:
    def __init__(self, records, bodies):
        self.records = {r['key']: r for r in records}
        self.bodies = bodies
        self.gets = []

    def head_object(self, Bucket, Key):
        r = self.records[Key]
        return dict(ContentLength=r['bytes'], Metadata=dict(sha256=r['sha256'], rows=str(r['rows'])))

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        return dict(Body=io.BytesIO(self.bodies[Key]), ContentLength=len(self.bodies[Key]))


@pytest.fixture
def source(tmp_path, monkeypatch):
    monkeypatch.setattr(m.shutil, 'disk_usage', lambda path: SimpleNamespace(free=100*1024**3))
    records, bodies = [], {}
    for symbol in ('KDP','NVDA'):
        for stream in ('trades','quotes'):
            sink = pa.BufferOutputStream()
            pq.write_table(pa.table({'sip_timestamp': list(range(4097)), 'price': [2.0]*4097}), sink)
            data = sink.getvalue().to_pybytes()
            key=m.storage.tq_object_key('2026-09-02',symbol,stream)
            records.append(dict(symbol=symbol,stream=stream,key=key,bytes=len(data),rows=4097,sha256=hashlib.sha256(data).hexdigest()))
            bodies[key]=data
    return Client(records,bodies), {'objects':records}, tmp_path/'sample'


def test_restart_and_reuse(source):
    client, manifest, root=source
    with patch.object(m,'MIB',64), patch.object(m.os,'_exit',side_effect=InterruptedError):
        with pytest.raises(InterruptedError):
            m.run(client,'fake',manifest,root,True)
    assert not (root/'complete.json').exists()
    assert (root/'NVDA/quotes.partial').exists()
    m.run(client,'fake',manifest,root)
    assert len(client.gets)==5
    assert sum(x['reused'] for x in json.loads((root/'complete.json').read_text())['results'])==3
    m.run(client,'fake',manifest,root)
    assert len(client.gets)==5


def test_corruption_rejected(source):
    client, manifest, root=source
    key=manifest['objects'][0]['key']
    raw=client.bodies[key]
    client.bodies[key]=raw[:20]+bytes([raw[20]^1])+raw[21:]
    with pytest.raises(RuntimeError,match='identity mismatch'):
        m.run(client,'fake',manifest,root)
    assert not (root/'complete.json').exists()


def test_download_cap_precedes_get(source):
    client, manifest, root=source
    with patch.object(m,'MAX_DOWNLOAD',1):
        with pytest.raises(RuntimeError,match='reservation'):
            m.run(client,'fake',manifest,root)
    assert client.gets==[]


def test_changed_remote_identity(source):
    client, manifest, root=source
    client.records={k:dict(v,sha256='0'*64) for k,v in client.records.items()}
    with pytest.raises(ValueError,match='identity changed'):
        m.run(client,'fake',manifest,root)
    assert client.gets==[]


def test_free_disk_reserve_precedes_get(source):
    client, manifest, root=source
    with patch.object(m.shutil,'disk_usage',return_value=SimpleNamespace(free=1)):
        with pytest.raises(RuntimeError,match='Disk budget'):
            m.run(client,'fake',manifest,root)
    assert client.gets==[]
