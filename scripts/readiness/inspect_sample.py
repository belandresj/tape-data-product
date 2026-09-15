"""Bounded source precision/order inspection; does not admit or build features."""
import io,json,time,hashlib
from pathlib import Path
import pyarrow.parquet as pq
from tape_data_product.contracts.policy import session_bounds,NS
from tape_data_product.contracts.source import share_units
ROOT=Path('/srv/tape-data-product/raw/phase1-sample')
OUT=Path('/srv/tape-data-product/control/feature-readiness-20260915/sample-inspection.json')
LIMIT=1024**3
read_bytes=0
class Counted(io.RawIOBase):
    def __init__(self,path):self.f=path.open('rb')
    def readable(self):return True
    def seekable(self):return True
    def tell(self):return self.f.tell()
    def seek(self,*args):return self.f.seek(*args)
    def read(self,n=-1):
        global read_bytes
        if n<0:raise ValueError('unbounded read')
        if read_bytes+n>LIMIT:raise ValueError('read guard')
        b=self.f.read(n);read_bytes+=len(b);return b
    def readinto(self,b):
        value=self.read(len(b));b[:len(value)]=value;return len(value)
    def close(self):self.f.close();super().close()
def file_hash(path):
    h=hashlib.sha256()
    with Counted(path) as f:
        while b:=f.read(1024**2):h.update(b)
    return h.hexdigest()
start,_=session_bounds('2026-09-02');end=start+720*NS
results=[];decoded=0;began=time.monotonic()
manifest={}
for line in Path('/srv/tape-data-product/control/raw-migration/8e77ed762721e336/transfer-manifest.jsonl').open():
    d=json.loads(line)
    if d.get('session_date')=='2026-09-02' and d.get('symbol') in ('KDP','NVDA'):manifest[(d['symbol'],d['stream'])]=d
for symbol in ('KDP','NVDA'):
    for stream in ('quotes','trades'):
        path=ROOT/symbol/(stream+'.parquet');expected=manifest[(symbol,stream)]
        if file_hash(path)!=expected['sha256']:raise ValueError('source hash mismatch')
        d={'symbol':symbol,'stream':stream,'prefix_rows':0,'seed_rows':0,'precision_failures':0,'decimal_text_rows':0,'size_fallback_rows':0,'order_failures':0}
        previous=None;done=False
        cols=['sip_timestamp','sequence_number']+(['decimal_size','size'] if stream=='trades' else [])
        with Counted(path) as f:
            pf=pq.ParquetFile(f)
            for batch in pf.iter_batches(batch_size=4096,columns=cols,use_threads=False):
                decoded+=batch.num_rows
                if decoded>2_000_000 or time.monotonic()-began>600:raise ValueError('scope guard')
                for r in batch.to_pylist():
                    key=(r['sip_timestamp'],r['sequence_number'])
                    if None in key:raise ValueError('untrusted key')
                    if previous is not None and key<=previous:d['order_failures']+=1
                    previous=key
                    if key[0]>=end:done=True;break
                    if key[0]<start:
                        if key[0]>=start-300*NS:d['seed_rows']+=1
                        continue
                    d['prefix_rows']+=1
                    if stream=='trades':
                        decimal=r['decimal_size'];d['decimal_text_rows' if decimal is not None else 'size_fallback_rows']+=1
                        value=decimal if decimal is not None else r['size']
                        try:share_units(value)
                        except (ValueError,TypeError):d['precision_failures']+=1
                if done:break
        if file_hash(path)!=expected['sha256']:raise ValueError('source changed')
        results.append(d)
out={'status':'diagnostic_only','coverage_admitted':False,'elapsed_seconds':time.monotonic()-began,'read_bytes':read_bytes,'decoded_rows':decoded,'prefix_seconds':720,'members':results}
with OUT.open('x') as f:json.dump(out,f,indent=2)
print(json.dumps(out))
