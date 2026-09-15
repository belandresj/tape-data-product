"""Base-only bounded endpoint-return EW and exact-age feature calculator."""
from __future__ import annotations

from bisect import bisect_left, insort
from collections import deque
from itertools import zip_longest
from dataclasses import dataclass
import fcntl
import hashlib
import math
import os
from pathlib import Path
import shutil
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from ..contracts import DEFAULT_CONFIG, contract_identity
from ..contracts.config import ContractError, FeatureConfig, digest, integer
from ..contracts.interfaces import BuildResult
from ..contracts.policy import NS, endpoint_return
from ..contracts.reasons import Reason, SourceStatus
from ..contracts.registry import AGES, feature_registry
from ..contracts.schemas import BASE_SCHEMA, feature_schema, support_schema, schema_hash
from ..contracts.validation import validate_batch
from ..integrity import read_json, sha256_file, write_atomic_json, output_record, verify_output
from ..replay.builder import verify_base_partition


@dataclass
class ScaledSum:
    """Nonnegative m*2**e sum whose positive state survives long decay."""
    m: float = 0.0
    e: int = 0

    @property
    def zero(self): return self.m == 0.0

    def _set(self, m, e):
        if m == 0: self.m=0.; self.e=0
        else:
            m2,e2=math.frexp(m); self.m=m2; self.e=e+e2

    def decay(self, factor):
        if not self.zero: self._set(self.m*factor,self.e)

    def add_float(self, value):
        if type(value) not in (int,float) or isinstance(value,bool) or not math.isfinite(value) or value<0: raise ContractError("scaled sum requires finite nonnegative input")
        if value==0: return
        m,e=math.frexp(value); self.add_parts(m,e)

    def add_square(self, value):
        m,e=math.frexp(abs(value)); self.add_parts(m*m,2*e)

    def add_parts(self,m,e):
        if m==0:return
        m,adjust=math.frexp(m); e+=adjust
        if self.zero: self.m=m; self.e=e; return
        if e>self.e: self._set(m+math.ldexp(self.m,self.e-e),e)
        else: self._set(self.m+math.ldexp(m,e-self.e),self.e)

    def value(self):
        try:return math.ldexp(self.m,self.e)
        except OverflowError: raise ContractError("scaled diagnostic overflow")

    def ratio(self, other):
        if other.zero: raise ContractError("zero scaled denominator")
        try:return math.ldexp(self.m/other.m,self.e-other.e)
        except OverflowError: raise ContractError("scaled ratio overflow")

    def sqrt_ratio(self, other):
        if self.zero:return 0.0
        d=self.e-other.e; m=self.m/other.m
        if d&1: m*=2; d-=1
        try:return math.ldexp(math.sqrt(m),d//2)
        except OverflowError: raise ContractError("scaled root overflow")


def scaled_less(a, fraction, b):
    if a.zero:return not b.zero
    if b.zero:return False
    fm,fe=math.frexp(fraction); rhs_m,rhs_e=math.frexp(b.m*fm); rhs_e+=b.e+fe
    return a.e<rhs_e or a.e==rhs_e and a.m<rhs_m


def scaled_square_divide(a, b):
    if a.zero:return ScaledSum()
    if b.zero:raise ContractError("zero scaled denominator")
    result=ScaledSum();result.add_parts((a.m*a.m)/b.m,2*a.e-b.e);return result


def scaled_ratio_parts(a,b):
    if a.zero:return 0.0,0
    if b.zero:raise ContractError("zero scaled denominator")
    m,e=math.frexp(a.m/b.m);return m,e+a.e-b.e


def history_mask(source, halted, elapsed, startup, usable, possible, minimum):
    reason=Reason(0)
    if source==SourceStatus.UNVERIFIED:reason|=Reason.SOURCE_UNVERIFIED
    elif source==SourceStatus.UNAVAILABLE:reason|=Reason.SOURCE_UNAVAILABLE
    if halted:reason|=Reason.HALT
    if elapsed<startup:reason|=Reason.STARTUP
    if usable.zero:reason|=Reason.NO_SUPPORTED_DATA
    elif elapsed>=startup and scaled_less(usable,minimum,possible):reason|=Reason.LOW_COVERAGE
    return int(reason)


@dataclass
class Family:
    numerator: ScaledSum
    usable: ScaledSum
    possible: ScaledSum

    @classmethod
    def new(cls):return cls(ScaledSum(),ScaledSum(),ScaledSum())
    def decay(self,l):
        self.numerator.decay(l);self.usable.decay(l);self.possible.decay(l)
    def admit(self,numerator,exposure):
        self.possible.add_float(1)
        if exposure>0:self.numerator.add_float(numerator);self.usable.add_float(exposure)


class AgeWindow:
    def __init__(self,size):self.size=size;self.slots=deque();self.values=[]
    def clear(self):self.slots.clear();self.values.clear()
    def append(self,value):
        self.slots.append(value)
        if value is not None:insort(self.values,value)
        if len(self.slots)>self.size:
            old=self.slots.popleft()
            if old is not None:self.values.pop(bisect_left(self.values,old))
    def p90(self):
        p=.9*(len(self.values)-1);lo=int(math.floor(p));hi=int(math.ceil(p))
        return self.values[lo] if lo==hi else self.values[lo]+(p-lo)*(self.values[hi]-self.values[lo])


def _implementation_identity():
    root=Path(__file__).parent
    files={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((Path(__file__),root.parent/"contracts"/"registry.py",root.parent/"contracts"/"schemas.py",root.parent/"contracts"/"validation.py",root.parent/"integrity.py"))}
    return {"files":files,"sha256":digest(files)}


def build_from_base(base_partition, output, *, config=DEFAULT_CONFIG, batch_size=4096):
    integer(batch_size,"batch size",1,25000)
    if not isinstance(config,FeatureConfig):raise ContractError("invalid feature config")
    base_partition=Path(base_partition);base_manifest=verify_base_partition(base_partition)
    frozen_base={name:sha256_file(base_partition/name)[0] for name in ("manifest.json","base.parquet","context.json")}
    expected_cap=base_manifest["base_compatibility"]["descriptor"]["max_trade_reporting_age_ns"]
    if expected_cap!=config.max_trade_reporting_age_ns:raise ContractError("base raw measurement policy is incompatible")
    output=Path(output)
    if (output/"manifest.json").exists():
        manifest=verify_feature_partition(output,base_partition,config=config)
        expected_base_sha=sha256_file(base_partition/"manifest.json")[0]
        expected_implementation=_implementation_identity()
        if (manifest["contract_identity"]!=contract_identity(config)
                or manifest["implementation_identity"]!=expected_implementation
                or manifest["inputs"]!={"base_manifest_sha256":expected_base_sha,
                    "base_compatibility_sha256":base_manifest["base_compatibility"]["sha256"]}):
            raise ContractError("completed features do not match requested config/base/implementation")
        return BuildResult(digest(manifest["member"]),manifest["contract_identity"],output/"manifest.json",manifest["coverage"]["expected_rows"])
    if output.exists() and any(output.iterdir()):raise FileExistsError(output)
    output.parent.mkdir(parents=True,exist_ok=True);lock_path=output.parent/f".{output.name}.lock"
    with lock_path.open("a+b") as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as error:raise ContractError("concurrent member writer") from error
        attempt=Path(tempfile.mkdtemp(prefix=f".{output.name}.attempt-",dir=output.parent))
        try:
            rows,underflows=_calculate(base_partition/"base.parquet",base_partition/"context.json",attempt/"features.parquet",attempt/"support.parquet",config,batch_size)
            if any(sha256_file(base_partition/name)[0]!=value for name,value in frozen_base.items()):
                raise ContractError("base/context changed during feature calculation")
            verify_base_partition(base_partition)
            base_sha=sha256_file(base_partition/"manifest.json")[0];implementation=_implementation_identity()
            records=[output_record(attempt/"features.parquet",rows=rows,schema_sha256=schema_hash(feature_schema(config))),output_record(attempt/"support.parquet",rows=rows,schema_sha256=schema_hash(support_schema(config)))]
            manifest={"manifest_version":"tape_member_manifest_v1","member":base_manifest["member"],"coverage":base_manifest["coverage"],
              "inputs":{"base_manifest_sha256":base_sha,"base_compatibility_sha256":base_manifest["base_compatibility"]["sha256"]},"source_units":base_manifest["source_units"],
              "contract_identity":contract_identity(config),"contract_config":config.to_dict(),"implementation_identity":implementation,"outputs":records,
              "validation":{"integrity":"passed","independent_reconstruction":"pending","diagnostic_underflow_count":underflows},"complete":True}
            write_atomic_json(attempt/"manifest.json",manifest);verify_feature_partition(attempt,base_partition,config=config);os.replace(attempt,output)
        except Exception:shutil.rmtree(attempt,ignore_errors=True);raise
    return BuildResult(digest(manifest["member"]),manifest["contract_identity"],output/"manifest.json",rows)


def _calculate(base_path,context_path,feature_path,support_path,config,batch_size):
    context=read_json(context_path)
    exact_breaks={source:set(context["instantaneous_breaks"].get(source,[]))|{x[0] for x in context["gaps"].get(source,[])} for source in ("quotes","trades")}
    views=[]
    for view in config.views:
        views.append({"view":view,"lambda":2**(-1/view.half_life_seconds),"u1":ScaledSum(),"u2":ScaledSum(),"c":ScaledSum(),"w":ScaledSum(),"wp":ScaledSum(),
                      **{f:Family.new() for f in ("spread","activity_count","activity_share","activity_dollar","bid_size","ask_size")},"ever_positive":False})
    windows={(a,h):AgeWindow(h) for a in AGES for h in config.age_windows_seconds};ring=deque(maxlen=6);quote_elapsed=trade_elapsed=0;underflows=0
    fs,ss=feature_schema(config),support_schema(config);fw=pq.ParquetWriter(feature_path,fs,compression="zstd",compression_level=3);sw=pq.ParquetWriter(support_path,ss,compression="zstd",compression_level=3)
    fbuf=[];sbuf=[]
    try:
      for batch in pq.ParquetFile(base_path).iter_batches(batch_size=batch_size,use_threads=False):
       for row in batch.to_pylist():
        halt=row["halt_active"];qstatus=SourceStatus(row["quote_source_status"]);tstatus=SourceStatus(row["trade_source_status"])
        if halt:
            ring.clear();quote_elapsed=trade_elapsed=0
            for state in views:
                for key in ("u1","u2","c","w","wp"):state[key]=ScaledSum()
                for family in ("spread","activity_count","activity_share","activity_dollar","bid_size","ask_size"):state[family]=Family.new()
                state["ever_positive"]=False
            for window in windows.values():window.clear()
        else:
            quote_elapsed+=1;trade_elapsed+=1
            midpoint=row["bid_end_usd"]/2+row["ask_end_usd"]/2 if row["price_end_reason_mask"]==0 else None
            ring.append((midpoint,row["quote_continuity_id"],row["interval_end_ns"]))
            r=None
            if len(ring)==6:r=endpoint_return(ring[0][0],ring[-1][0],continuity_same=ring[0][1]==ring[-1][1],crosses_halt=False)
            for state in views:
                l=state["lambda"]
                for key in ("u1","u2","c","w","wp"):state[key].decay(l)
                if len(ring)==6:state["wp"].add_float(1)
                if r is not None:
                    magnitude=abs(r)
                    if not state["w"].zero:
                        mean=state["u1"].ratio(state["w"])
                        denominator=ScaledSum(state["w"].m,state["w"].e);denominator.add_float(1)
                        factor_m,factor_e=scaled_ratio_parts(state["w"],denominator)
                        delta_m,delta_e=math.frexp(abs(magnitude-mean))
                        state["c"].add_parts(factor_m*delta_m*delta_m,factor_e+2*delta_e)
                    state["u1"].add_float(abs(r));state["u2"].add_square(r);state["w"].add_float(1);state["ever_positive"]|=r!=0
                for family in ("spread","activity_count","activity_share","activity_dollar","bid_size","ask_size"):state[family].decay(l)
                state["spread"].admit(row["spread_integral_bps_seconds"] or 0,row["spread_valid_duration_ns"]/NS)
                exposure=row["activity_valid_duration_ns"]/NS
                state["activity_count"].admit(row["trade_count_1s"] or 0,exposure);state["activity_share"].admit(float(row["share_volume_1s"] or 0),exposure);state["activity_dollar"].admit(row["dollar_volume_1s_usd"] or 0,exposure)
                state["bid_size"].admit(row["bid_size_integral_shares_seconds"] or 0,row["bid_size_valid_duration_ns"]/NS);state["ask_size"].admit(row["ask_size_integral_shares_seconds"] or 0,row["ask_size_valid_duration_ns"]/NS)
            if row["quote_continuity_break_in_second"]:
                for a in ("quote","midpoint_change"):
                    for h in config.age_windows_seconds:windows[a,h].clear()
            if row["trade_continuity_break_in_second"]:
                for h in config.age_windows_seconds:windows["trade",h].clear()
            left=row["interval_end_ns"]-NS
            for a in AGES:
                source="trades" if a=="trade" else "quotes"
                full_observed=row[("trade" if source=="trades" else "quote")+"_observed_duration_ns"]==NS
                interior_break=any(left<x<row["interval_end_ns"] for x in exact_breaks[source])
                if full_observed and not interior_break:
                    value=row[f"{a}_age_seconds"] if row[f"{a}_age_reason_mask"]==0 else None
                    for h in config.age_windows_seconds:windows[a,h].append(value)
        fr={k:row[k] for k in ("session_date","symbol","interval_end_ns")};sr=dict(fr)
        for state in views:
            view=state["view"];suffix=f"_hl{view.half_life_seconds}s"
            return_reason=history_mask(qstatus,halt,quote_elapsed,view.startup_seconds,state["w"],state["wp"],config.other_min_coverage)
            rms=state["u2"].sqrt_ratio(state["w"]) if not return_reason else None
            part_reason=return_reason
            if not return_reason and not state["ever_positive"]:part_reason|=int(Reason.ZERO_RETURN_VARIATION)
            part=None
            if not part_reason:
                k=scaled_square_divide(state["u1"],state["w"])
                denominator=ScaledSum(k.m,k.e)
                if not state["c"].zero:denominator.add_parts(state["c"].m,state["c"].e)
                part=k.ratio(denominator)
                if not 0<=part<=1:raise ContractError("participation invariant failed")
            names={"midpoint_rms_5s_bps":(rms,return_reason),"movement_participation":(part,part_reason)}
            family_map={"quoted_spread_bps":("spread",config.spread_min_coverage,qstatus,quote_elapsed),"trade_rate_per_second":("activity_count",config.other_min_coverage,tstatus,trade_elapsed),"share_rate_per_second":("activity_share",config.other_min_coverage,tstatus,trade_elapsed),"dollar_rate_usd_per_second":("activity_dollar",config.other_min_coverage,tstatus,trade_elapsed),"bid_size_mean_shares":("bid_size",config.other_min_coverage,qstatus,quote_elapsed),"ask_size_mean_shares":("ask_size",config.other_min_coverage,qstatus,quote_elapsed)}
            for name,(family,minimum,status,elapsed) in family_map.items():
                f=state[family];reason=history_mask(status,halt,elapsed,view.startup_seconds,f.usable,f.possible,minimum);names[name]=(f.numerator.ratio(f.usable) if not reason else None,reason)
            spread,spread_reason=names["quoted_spread_bps"];ratio_reason=return_reason|spread_reason
            if not ratio_reason and spread==0:ratio_reason|=int(Reason.ZERO_SPREAD)
            names["midpoint_rms_5s_to_spread"]=(rms/spread if not ratio_reason else None,ratio_reason)
            for name,(value,reason) in names.items():fr[name+suffix]=value;fr[name+suffix+"_reason_mask"]=reason
            sr["return_usable_weight"+suffix]=state["w"].value();sr["return_possible_weight"+suffix]=state["wp"].value()
            if not state["w"].zero and sr["return_usable_weight"+suffix]==0:underflows+=1
            for family,key in (("spread","spread"),("activity","activity_count"),("bid_size","bid_size"),("ask_size","ask_size")):
                sr[f"{family}_usable_exposure_seconds{suffix}"]=state[key].usable.value();sr[f"{family}_possible_exposure_seconds{suffix}"]=state[key].possible.value()
                if not state[key].usable.zero and sr[f"{family}_usable_exposure_seconds{suffix}"]==0:underflows+=1
        sr["quote_ew_startup_elapsed_seconds"]=quote_elapsed;sr["trade_ew_startup_elapsed_seconds"]=trade_elapsed
        for h in config.age_windows_seconds:
            for a in AGES:
                window=windows[a,h];reason=Reason(0)
                status=tstatus if a=="trade" else qstatus
                if status==SourceStatus.UNVERIFIED:reason|=Reason.SOURCE_UNVERIFIED
                elif status==SourceStatus.UNAVAILABLE:reason|=Reason.SOURCE_UNAVAILABLE
                if halt:reason|=Reason.HALT
                if len(window.slots)<h:reason|=Reason.STARTUP
                if not window.values:reason|=Reason.NO_SUPPORTED_DATA
                elif len(window.slots)>=h and len(window.values)<math.ceil(config.age_min_coverage*h):reason|=Reason.LOW_COVERAGE
                name=f"{a}_age_p90_seconds_window{h}s";fr[name]=window.p90() if not reason else None;fr[name+"_reason_mask"]=int(reason)
                sr[f"{a}_age_sample_count_window{h}s"]=len(window.values);sr[f"{a}_age_elapsed_slots_window{h}s"]=len(window.slots)
        fbuf.append(fr);sbuf.append(sr)
        if len(fbuf)>=4096:
            fb=pa.RecordBatch.from_pylist(fbuf,schema=fs);sb=pa.RecordBatch.from_pylist(sbuf,schema=ss);validate_batch(fb,"features",config=config);validate_batch(sb,"support",config=config);fw.write_batch(fb);sw.write_batch(sb);fbuf=[];sbuf=[]
      if fbuf:
        fb=pa.RecordBatch.from_pylist(fbuf,schema=fs);sb=pa.RecordBatch.from_pylist(sbuf,schema=ss);validate_batch(fb,"features",config=config);validate_batch(sb,"support",config=config);fw.write_batch(fb);sw.write_batch(sb)
    finally:fw.close();sw.close()
    return pq.ParquetFile(base_path).metadata.num_rows,underflows


def verify_feature_partition(root,base_partition,*,config=DEFAULT_CONFIG):
    root=Path(root);base_partition=Path(base_partition);base_manifest=verify_base_partition(base_partition);manifest=read_json(root/"manifest.json")
    required={"manifest_version","member","coverage","inputs","source_units","contract_identity","contract_config","implementation_identity","outputs","validation","complete"}
    if set(manifest)!=required or not manifest["complete"]:raise ContractError("invalid feature manifest")
    if FeatureConfig.from_dict(manifest["contract_config"])!=config:raise ContractError("feature stored config mismatch")
    if manifest["contract_identity"]!=contract_identity(config):raise ContractError("feature config identity mismatch")
    if manifest["implementation_identity"]!=_implementation_identity():raise ContractError("feature implementation identity mismatch")
    if manifest["inputs"]["base_manifest_sha256"]!=sha256_file(base_partition/"manifest.json")[0]:raise ContractError("feature/base identity mismatch")
    if (manifest["member"]!=base_manifest["member"] or manifest["coverage"]!=base_manifest["coverage"]
            or manifest["source_units"]!=base_manifest["source_units"]
            or manifest["inputs"].get("base_compatibility_sha256")!=base_manifest["base_compatibility"]["sha256"]):
        raise ContractError("feature/base member or coverage mismatch")
    records={r["path"]:r for r in manifest["outputs"]}
    if set(records)!={"features.parquet","support.parquet"}:raise ContractError("feature companions missing")
    schemas={"features.parquet":feature_schema(config),"support.parquet":support_schema(config)}
    key_iterators=[]
    for name,kind in (("features.parquet","features"),("support.parquet","support")):
        path=verify_output(root,records[name]);pf=pq.ParquetFile(path)
        if (records[name]["schema_sha256"]!=schema_hash(schemas[name])
                or records[name]["rows"]!=manifest["coverage"]["expected_rows"]):raise ContractError("feature companion declaration mismatch")
        if not pf.schema_arrow.equals(schemas[name],check_metadata=True) or pf.metadata.num_rows!=manifest["coverage"]["expected_rows"]:raise ContractError("feature companion schema/count mismatch")
        def keys(parquet_file, table_kind):
            previous=None
            for batch in parquet_file.iter_batches(batch_size=4096,use_threads=False):
                previous=validate_batch(batch,table_kind,config=config,previous_key=previous)
                yield from zip(*(batch.column(i).to_pylist() for i in range(3)))
        key_iterators.append(keys(pf,kind))
    base_pf=pq.ParquetFile(base_partition/"base.parquet")
    def base_keys():
        previous=None
        for batch in base_pf.iter_batches(batch_size=4096,use_threads=False):
            previous=validate_batch(batch,"base",previous_key=previous)
            yield from zip(*(batch.column(i).to_pylist() for i in range(3)))
    key_iterators.append(base_keys())
    sentinel=object()
    for values in zip_longest(*key_iterators,fillvalue=sentinel):
        if sentinel in values or len(set(values))!=1:raise ContractError("base/feature/support key mismatch")
    return manifest
