"""Bounded reducers for endpoint/EW marginals and joint distributions."""
from dataclasses import dataclass
import json, math, sqlite3
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

MAX_BATCH=4096
class DistributionError(ValueError): pass
def need(x,msg):
    if not x: raise DistributionError(msg)
def key(day,symbol): return json.dumps([str(day),symbol],separators=(",",":"))
def rows(batch,names):
    need(batch.num_rows<=MAX_BATCH and set(names)<=set(batch.schema.names),"invalid projected batch")
    d=batch.select(names).to_pydict(); return zip(*(d[n] for n in names))

@dataclass(frozen=True)
class FixedAxis:
    field:str; unit:str; edges:tuple[float,...]
    def __post_init__(self):
        need(len(self.edges)>=2 and all(math.isfinite(x) and x>0 for x in self.edges)
             and all(a<b for a,b in zip(self.edges,self.edges[1:])),"invalid axis")
    @property
    def bins(self): return len(self.edges)+2
    def ids(self,v):
        out=np.searchsorted(self.edges,v,side="right")+1; out[v==0]=0; out[v==self.edges[-1]]=len(self.edges); return out
    def labels(self):
        return ["0",f"0–<{self.edges[0]:g}"]+[f"{a:g}–<{b:g}" for a,b in zip(self.edges,self.edges[1:-1])]+[f"{self.edges[-2]:g}–{self.edges[-1]:g}",f">{self.edges[-1]:g}"]

def exact_marginal(batches,output,*,field,unit,mask_column=None,expected_members=(),batch_size=4096):
    """External-sort one field; coalesce ties across output batch boundaries."""
    need(1<=batch_size<=4096,"batch size must be 1..4096"); output=Path(output); output.mkdir(parents=True)
    mask_column=mask_column or field+"_reason_mask"; members={key(*m) for m in expected_members}; vm=set()
    extract=output/"valid_values.parquet"; schema=pa.schema([("day",pa.string()),("symbol",pa.string()),("value",pa.float64())])
    selected=valid=zeros=0; reason_bits={}
    with pq.ParquetWriter(extract,schema,compression="zstd") as w:
        for b in batches:
            out={n:[] for n in schema.names}
            for day,symbol,value,mask in rows(b,["session_date","symbol",field,mask_column]):
                m=key(day,symbol); members.add(m); selected+=1
                need(type(mask) is int and (value is not None)==(mask==0),"value/mask contradiction")
                if mask:
                    for bit in range(16):
                        if mask & (1<<bit): reason_bits[str(1<<bit)]=reason_bits.get(str(1<<bit),0)+1
                    continue
                value=float(value); need(math.isfinite(value) and value>=0,"invalid valid value")
                valid+=1; zeros+=value==0; vm.add(m); out["day"].append(str(day)); out["symbol"].append(symbol); out["value"].append(value)
            need(len(members)<=10000,"member cap exceeded")
            if out["value"]: w.write_batch(pa.RecordBatch.from_pydict(out,schema=schema))
    ecdf=output/"ecdf.parquet"; oschema=pa.schema([("value",pa.float64()),("pooled_count",pa.int64()),("pooled_cumulative",pa.int64()),("pooled_ecdf",pa.float64()),("equal_member_mass",pa.float64()),("equal_member_ecdf",pa.float64())])
    pc=0; ec=0.; distinct=0; quantiles={str(q):None for q in (0,.01,.05,.25,.5,.75,.95,.99,1)}; equant=dict(quantiles); buf=[]
    spill=output/"spill"
    with pq.ParquetWriter(ecdf,oschema,compression="zstd") as w:
        if valid:
            import duckdb
            spill.mkdir(); con=duckdb.connect(); con.execute("SET threads=1"); con.execute("SET memory_limit='256MiB'"); con.execute("SET max_temp_directory_size='1GiB'"); con.execute("SET temp_directory=?",[str(spill)])
            r=con.execute("""WITH a AS(SELECT day,symbol,value,count(*)::BIGINT c FROM read_parquet(?) GROUP BY day,symbol,value),s AS(SELECT *,sum(c)OVER(PARTITION BY day,symbol)::BIGINT n FROM a) SELECT value,c,n FROM s ORDER BY value,day,symbol""",[str(extract)])
            reader=r.to_arrow_reader(batch_size) if hasattr(r,"to_arrow_reader") else r.fetch_record_batch(batch_size)
            pending=None; count=0; mass=0.
            def emit():
                nonlocal pc,ec,distinct,count,mass
                pc+=count; ec+=mass/len(vm); distinct+=1; buf.append(dict(value=pending,pooled_count=count,pooled_cumulative=pc,pooled_ecdf=pc/valid,equal_member_mass=mass/len(vm),equal_member_ecdf=ec))
                for q in quantiles:
                    qf=float(q)
                    if quantiles[q] is None and (qf==0 or pc>=math.ceil(qf*valid)): quantiles[q]=pending
                    if equant[q] is None and (qf==0 or ec+1e-15>=qf): equant[q]=pending
                if len(buf)>=4096: w.write_table(pa.Table.from_pylist(buf,schema=oschema)); buf.clear()
            for b in reader:
                for value,c,n in rows(b,["value","c","n"]):
                    value=float(value)
                    need(pending is None or value>=pending,"sort order failure")
                    if pending is not None and value!=pending: emit(); count=0; mass=0.
                    pending=value; count+=int(c); mass+=int(c)/int(n)
            emit(); con.close()
        if buf: w.write_table(pa.Table.from_pylist(buf,schema=oschema))
    need(pc==valid and (not valid or abs(ec-1)<1e-12),"ECDF mass mismatch")
    summary=dict(schema="endpoint_marginal_ecdf_v1",field=field,unit=unit,selected=selected,valid=valid,unavailable=selected-valid,zeros=zeros,selected_members=len(members),contributing_members=len(vm),zero_valid_members=len(members-vm),distinct_values=distinct,pooled_quantiles=quantiles if valid else {},equal_member_quantiles=equant if valid else {},weightings=["pooled_observation","equal_symbol_day"],reason_bit_counts=reason_bits,reason_tallies_are_overlapping=True)
    (output/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n"); return summary

class JointHistogram:
    """Stream pair-valid counts; store per-member cells on disk for 1/(M*n_m)."""
    def __init__(self,path,x_axis,y_axis,expected_members=()):
        self.x,self.y=x_axis,y_axis; self.x_reasons={}; self.y_reasons={}; self.expected={key(*m) for m in expected_members}; self.counts=np.zeros((x_axis.bins,y_axis.bins),dtype=np.int64); self.db=sqlite3.connect(path)
        self.db.executescript("CREATE TABLE m(k TEXT PRIMARY KEY,s INT,xv INT,yv INT,n INT,xz INT,yz INT,zz INT);CREATE TABLE c(k TEXT,x INT,y INT,n INT,PRIMARY KEY(k,x,y));")
        self.db.executemany("INSERT INTO m VALUES(?,0,0,0,0,0,0,0)",[(k,) for k in self.expected])
    def add_arrow(self,b,*,x_column,x_mask_column,y_column,y_mask_column):
        grouped={}
        for day,symbol,x,xm,y,ym in rows(b,["session_date","symbol",x_column,x_mask_column,y_column,y_mask_column]):
            need(type(xm) is int and type(ym) is int and (x is not None)==(xm==0) and (y is not None)==(ym==0),"value/mask contradiction")
            for bit in range(16):
                if xm & (1<<bit): self.x_reasons[str(1<<bit)]=self.x_reasons.get(str(1<<bit),0)+1
                if ym & (1<<bit): self.y_reasons[str(1<<bit)]=self.y_reasons.get(str(1<<bit),0)+1
            if xm==0: x=float(x); need(math.isfinite(x) and x>=0,"invalid x")
            if ym==0: y=float(y); need(math.isfinite(y) and y>=0,"invalid y")
            k=key(day,symbol); need(not self.expected or k in self.expected,"unexpected member"); grouped.setdefault(k,[]).append((x,xm==0,y,ym==0))
        for k,rs in grouped.items():
            pairs=[(x,y) for x,xv,y,yv in rs if xv and yv]; args=(k,len(rs),sum(r[1] for r in rs),sum(r[3] for r in rs),len(pairs),sum(x==0 for x,y in pairs),sum(y==0 for x,y in pairs),sum(x==y==0 for x,y in pairs))
            self.db.execute("INSERT INTO m VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(k) DO UPDATE SET s=s+excluded.s,xv=xv+excluded.xv,yv=yv+excluded.yv,n=n+excluded.n,xz=xz+excluded.xz,yz=yz+excluded.yz,zz=zz+excluded.zz",args)
            if pairs:
                a=np.asarray(pairs); xb=self.x.ids(a[:,0]); yb=self.y.ids(a[:,1]); codes,ns=np.unique(xb*self.y.bins+yb,return_counts=True)
                for code,n in zip(codes,ns):
                    i,j=divmod(int(code),self.y.bins); self.counts[i,j]+=n; self.db.execute("INSERT INTO c VALUES(?,?,?,?) ON CONFLICT(k,x,y) DO UPDATE SET n=n+excluded.n",(k,i,j,int(n)))
        self.db.commit(); need(self.db.execute("SELECT count(*) FROM m").fetchone()[0]<=10000,"member cap exceeded")
    def finish(self):
        M,s,xv,yv,n,xz,yz,zz,maxn,sq=map(int,self.db.execute("SELECT count(*),coalesce(sum(s),0),coalesce(sum(xv),0),coalesce(sum(yv),0),coalesce(sum(n),0),coalesce(sum(xz),0),coalesce(sum(yz),0),coalesce(sum(zz),0),coalesce(max(n),0),coalesce(sum(n*n),0) FROM m").fetchone()); V=self.db.execute("SELECT count(*) FROM m WHERE n>0").fetchone()[0]; equal=np.zeros_like(self.counts,dtype=float)
        for i,j,mass in self.db.execute("SELECT c.x,c.y,sum(CAST(c.n AS REAL)/m.n) FROM c JOIN m USING(k) WHERE m.n>0 GROUP BY c.x,c.y"): equal[i,j]=mass/V
        need(self.counts.sum()==n and (not V or abs(equal.sum()-1)<1e-12),"joint mass mismatch")
        return dict(schema="endpoint_joint_histogram_v1",x=dict(field=self.x.field,unit=self.x.unit,edges=list(self.x.edges),labels=self.x.labels()),y=dict(field=self.y.field,unit=self.y.unit,edges=list(self.y.edges),labels=self.y.labels()),bin_convention="zero; positive underflow; left-closed/right-open finite bins; final edge included; overflow",selected=s,x_valid=xv,y_valid=yv,pair_valid=n,pair_unavailable=s-n,selected_members=M,contributing_members=V,zero_pair_valid_members=M-V,x_zero_pair_valid=xz,y_zero_pair_valid=yz,zero_zero=zz,pooled_counts=self.counts.tolist(),pooled_probability=(self.counts/n if n else equal).tolist(),equal_member_probability=equal.tolist(),x_reason_bit_counts=self.x_reasons,y_reason_bit_counts=self.y_reasons,reason_tallies_are_overlapping=True,concentration=dict(maximum_member_observation_share=maxn/n if n else None,observation_share_herfindahl=sq/n**2 if n else None))
    def close(self): self.db.close()
