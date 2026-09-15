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
import pyarrow.compute as pc
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
from ..replay.builder import _open_verified_base_partition, _verify_base_partition


@dataclass(slots=True)
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
        self.add_valid(value)

    def add_valid(self, value):
        """Add a value already admitted by the columnar input validator."""
        if value==0: return
        m,e=math.frexp(value); self.add_parts(m,e)

    def clear(self):self.m=0.;self.e=0

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


@dataclass(slots=True)
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


@dataclass(slots=True)
class _ExposureFamily:
    numerator: ScaledSum
    usable: ScaledSum

    @classmethod
    def new(cls):return cls(ScaledSum(),ScaledSum())
    def clear(self):self.numerator.clear();self.usable.clear()
    def decay(self,factor):self.numerator.decay(factor);self.usable.decay(factor)
    def admit(self,numerator,exposure):
        if exposure>0:self.numerator.add_valid(numerator);self.usable.add_valid(exposure)


class _ViewState:
    """One view's fixed estimator state with shared equivalent exposure histories."""
    __slots__=("view","decay_factor","u1","u2","central","return_usable",
               "return_possible","exposure_possible","spread","activity_count",
               "activity_share","activity_dollar","activity_usable","bid_size",
               "ask_size","ever_positive","families","decayed_sums")

    def __init__(self,view):
        self.view=view
        self.decay_factor=2**(-1/view.half_life_seconds)
        self.u1=ScaledSum();self.u2=ScaledSum();self.central=ScaledSum()
        self.return_usable=ScaledSum();self.return_possible=ScaledSum()
        self.exposure_possible=ScaledSum()
        self.spread=_ExposureFamily.new()
        self.activity_count=ScaledSum();self.activity_share=ScaledSum();self.activity_dollar=ScaledSum()
        self.activity_usable=ScaledSum()
        self.bid_size=_ExposureFamily.new();self.ask_size=_ExposureFamily.new()
        self.families=(self.spread,self.bid_size,self.ask_size)
        self.decayed_sums=(self.u1,self.u2,self.central,self.return_usable,
                           self.return_possible,self.exposure_possible,self.activity_count,
                           self.activity_share,self.activity_dollar,self.activity_usable)
        self.ever_positive=False

    def clear(self):
        for value in self.decayed_sums:value.clear()
        for family in self.families:family.clear()
        self.ever_positive=False

    def decay(self):
        factor=self.decay_factor
        for value in self.decayed_sums:value.decay(factor)
        for family in self.families:family.decay(factor)

    def admit_activity(self,count,shares,dollars,exposure):
        if exposure>0:
            self.activity_count.add_valid(count)
            self.activity_share.add_valid(shares)
            self.activity_dollar.add_valid(dollars)
            self.activity_usable.add_valid(exposure)


class _ColumnBuffers:
    __slots__=("schema","positions","columns","rows")

    def __init__(self,schema):
        self.schema=schema
        self.positions={name:i for i,name in enumerate(schema.names)}
        self.columns=[[] for _ in schema]
        self.rows=0

    def append_keys(self,date,symbol,end_ns):
        self.columns[0].append(date);self.columns[1].append(symbol);self.columns[2].append(end_ns)
        self.rows+=1

    def finish(self):
        batch=pa.RecordBatch.from_arrays(
            [pa.array(values,type=field.type) for values,field in zip(self.columns,self.schema)],
            schema=self.schema,
        )
        self.columns=[[] for _ in self.schema];self.rows=0
        return batch


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


def _file_snapshot(path):
    stat=Path(path).stat()
    return (stat.st_dev,stat.st_ino,stat.st_size,stat.st_mtime_ns,stat.st_ctime_ns)


def _verify_generated_outputs(root,records,rows,config):
    """Check freshly validated/written outputs without decoding them a second time."""
    schemas={"features.parquet":feature_schema(config),"support.parquet":support_schema(config)}
    by_name={record["path"]:record for record in records}
    if set(by_name)!=set(schemas):raise ContractError("feature companions missing")
    snapshots={}
    for name,schema in schemas.items():
        path=Path(root)/name;record=by_name[name]
        if not path.is_file() or path.stat().st_size!=record["bytes"]:raise ContractError("feature output changed after hashing")
        parquet=pq.ParquetFile(path)
        if (record["rows"]!=rows or record["schema_sha256"]!=schema_hash(schema)
                or parquet.metadata.num_rows!=rows or not parquet.schema_arrow.equals(schema,check_metadata=True)):
            raise ContractError("feature companion schema/count mismatch")
        snapshots[name]=_file_snapshot(path)
    return snapshots


def build_from_base(base_partition, output, *, config=DEFAULT_CONFIG, batch_size=4096):
    integer(batch_size,"batch size",1,25000)
    if not isinstance(config,FeatureConfig):raise ContractError("invalid feature config")
    base_partition=Path(base_partition);base_manifest=_verify_base_partition(base_partition,validate_rows=False)
    base_names=("manifest.json","base.parquet","context.json")
    frozen_base={name:_file_snapshot(base_partition/name) for name in base_names}
    base_sha=sha256_file(base_partition/"manifest.json")[0]
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
            if any(_file_snapshot(base_partition/name)!=value for name,value in frozen_base.items()):
                raise ContractError("base/context changed during feature calculation")
            implementation=_implementation_identity()
            records=[output_record(attempt/"features.parquet",rows=rows,schema_sha256=schema_hash(feature_schema(config))),output_record(attempt/"support.parquet",rows=rows,schema_sha256=schema_hash(support_schema(config)))]
            output_snapshots=_verify_generated_outputs(attempt,records,rows,config)
            manifest={"manifest_version":"tape_member_manifest_v1","member":base_manifest["member"],"coverage":base_manifest["coverage"],
              "inputs":{"base_manifest_sha256":base_sha,"base_compatibility_sha256":base_manifest["base_compatibility"]["sha256"]},"source_units":base_manifest["source_units"],
              "contract_identity":contract_identity(config),"contract_config":config.to_dict(),"implementation_identity":implementation,"outputs":records,
              "validation":{"integrity":"passed","independent_reconstruction":"pending","diagnostic_underflow_count":underflows},"complete":True}
            write_atomic_json(attempt/"manifest.json",manifest)
            if (any(_file_snapshot(base_partition/name)!=value for name,value in frozen_base.items())
                    or any(_file_snapshot(attempt/name)!=value for name,value in output_snapshots.items())):
                raise ContractError("build input/output changed before feature commit")
            os.replace(attempt,output)
        except Exception:shutil.rmtree(attempt,ignore_errors=True);raise
    return BuildResult(digest(manifest["member"]),manifest["contract_identity"],output/"manifest.json",rows)


def _calculate(base_path,context_path,feature_path,support_path,config,batch_size):
    context=read_json(context_path)
    exact_breaks={source:set(context["instantaneous_breaks"].get(source,[]))|{x[0] for x in context["gaps"].get(source,[])} for source in ("quotes","trades")}
    interior_break_ends={source:{(x//NS+1)*NS for x in values if x%NS} for source,values in exact_breaks.items()}
    fs,ss=feature_schema(config),support_schema(config)
    fbuf,sbuf=_ColumnBuffers(fs),_ColumnBuffers(ss)
    fp,sp=fbuf.positions,sbuf.positions
    views=[]
    for view in config.views:
        state=_ViewState(view);suffix=f"_hl{view.half_life_seconds}s"
        feature_positions={name:(fp[name+suffix],fp[name+suffix+"_reason_mask"]) for name in (
            "midpoint_rms_5s_bps","movement_participation","quoted_spread_bps",
            "trade_rate_per_second","share_rate_per_second","dollar_rate_usd_per_second",
            "bid_size_mean_shares","ask_size_mean_shares","midpoint_rms_5s_to_spread")}
        support_positions={name:sp[name+suffix] for name in (
            "return_usable_weight","return_possible_weight",
            "spread_usable_exposure_seconds","spread_possible_exposure_seconds",
            "activity_usable_exposure_seconds","activity_possible_exposure_seconds",
            "bid_size_usable_exposure_seconds","bid_size_possible_exposure_seconds",
            "ask_size_usable_exposure_seconds","ask_size_possible_exposure_seconds")}
        family_plans=(
            (state.spread.numerator,state.spread.usable,config.spread_min_coverage,False,feature_positions["quoted_spread_bps"]),
            (state.activity_count,state.activity_usable,config.other_min_coverage,True,feature_positions["trade_rate_per_second"]),
            (state.activity_share,state.activity_usable,config.other_min_coverage,True,feature_positions["share_rate_per_second"]),
            (state.activity_dollar,state.activity_usable,config.other_min_coverage,True,feature_positions["dollar_rate_usd_per_second"]),
            (state.bid_size.numerator,state.bid_size.usable,config.other_min_coverage,False,feature_positions["bid_size_mean_shares"]),
            (state.ask_size.numerator,state.ask_size.usable,config.other_min_coverage,False,feature_positions["ask_size_mean_shares"]),
        )
        support_plans=(("spread",state.spread.usable),("activity",state.activity_usable),
                       ("bid_size",state.bid_size.usable),("ask_size",state.ask_size.usable))
        views.append((state,feature_positions,support_positions,family_plans,support_plans))
    windows={(a,h):AgeWindow(h) for a in AGES for h in config.age_windows_seconds}
    age_plans=[]
    for h in config.age_windows_seconds:
        for age in AGES:
            name=f"{age}_age_p90_seconds_window{h}s"
            age_plans.append((age,h,math.ceil(config.age_min_coverage*h),windows[age,h],fp[name],fp[name+"_reason_mask"],
                              sp[f"{age}_age_sample_count_window{h}s"],
                              sp[f"{age}_age_elapsed_slots_window{h}s"]))
    quote_windows=tuple(windows[a,h] for a in ("quote","midpoint_change") for h in config.age_windows_seconds)
    trade_windows=tuple(windows["trade",h] for h in config.age_windows_seconds)
    base_positions={name:i for i,name in enumerate(BASE_SCHEMA.names)}
    ring=deque(maxlen=6);quote_elapsed=trade_elapsed=0;underflows=0
    fw=pq.ParquetWriter(feature_path,fs,compression="zstd",compression_level=3)
    sw=pq.ParquetWriter(support_path,ss,compression="zstd",compression_level=3)
    previous=None;first=None;last=None;processed_rows=0

    def numeric(batch,name,cast=None):
        array=batch.column(base_positions[name])
        valid=pc.is_valid(array).to_numpy(zero_copy_only=False)
        if cast is not None:array=pc.cast(array,cast)
        if array.null_count:array=pc.fill_null(array,pa.scalar(0,type=array.type))
        return array.to_numpy(zero_copy_only=False),valid

    def flush():
        fb=fbuf.finish();sb=sbuf.finish()
        validate_batch(fb,"features",config=config);validate_batch(sb,"support",config=config)
        fw.write_batch(fb);sw.write_batch(sb)

    try:
      for batch in pq.ParquetFile(base_path).iter_batches(batch_size=batch_size,use_threads=False):
       previous=validate_batch(batch,"base",previous_key=previous)
       if not batch.num_rows:continue
       date=batch.column(0)[0].as_py();symbol=batch.column(1)[0].as_py()
       end_ns,_=numeric(batch,"interval_end_ns")
       batch_first=(date,symbol,int(end_ns[0]));batch_last=(date,symbol,int(end_ns[-1]))
       if first is None:first=batch_first
       last=batch_last;processed_rows+=batch.num_rows
       halt,_=numeric(batch,"halt_active")
       qstatus,_=numeric(batch,"quote_source_status");tstatus,_=numeric(batch,"trade_source_status")
       bid,bid_valid=numeric(batch,"bid_end_usd");ask,ask_valid=numeric(batch,"ask_end_usd")
       price_reason,_=numeric(batch,"price_end_reason_mask")
       continuity,_=numeric(batch,"quote_continuity_id")
       spread,spread_valid=numeric(batch,"spread_integral_bps_seconds")
       spread_duration,_=numeric(batch,"spread_valid_duration_ns")
       activity_count,activity_count_valid=numeric(batch,"trade_count_1s")
       activity_share,activity_share_valid=numeric(batch,"share_volume_1s",pa.float64())
       activity_dollar,activity_dollar_valid=numeric(batch,"dollar_volume_1s_usd")
       activity_duration,_=numeric(batch,"activity_valid_duration_ns")
       bid_size,bid_size_valid=numeric(batch,"bid_size_integral_shares_seconds")
       bid_size_duration,_=numeric(batch,"bid_size_valid_duration_ns")
       ask_size,ask_size_valid=numeric(batch,"ask_size_integral_shares_seconds")
       ask_size_duration,_=numeric(batch,"ask_size_valid_duration_ns")
       quote_break,_=numeric(batch,"quote_continuity_break_in_second")
       trade_break,_=numeric(batch,"trade_continuity_break_in_second")
       quote_observed,_=numeric(batch,"quote_observed_duration_ns")
       trade_observed,_=numeric(batch,"trade_observed_duration_ns")
       age_values={};age_valid={};age_reasons={}
       for age in AGES:
           age_values[age],age_valid[age]=numeric(batch,f"{age}_age_seconds")
           age_reasons[age],_=numeric(batch,f"{age}_age_reason_mask")
       for i in range(batch.num_rows):
        halted=bool(halt[i]);qs=int(qstatus[i]);ts=int(tstatus[i]);row_end=int(end_ns[i])
        if halted:
            ring.clear();quote_elapsed=trade_elapsed=0
            for state,_,_,_,_ in views:state.clear()
            for window in windows.values():window.clear()
        else:
            quote_elapsed+=1;trade_elapsed+=1
            midpoint=float(bid[i]/2+ask[i]/2) if price_reason[i]==0 and bid_valid[i] and ask_valid[i] else None
            ring.append((midpoint,int(continuity[i]),row_end))
            r=None
            if len(ring)==6:r=endpoint_return(ring[0][0],ring[-1][0],continuity_same=ring[0][1]==ring[-1][1],crosses_halt=False)
            for state,_,_,_,_ in views:
                state.decay();state.exposure_possible.add_valid(1)
                if len(ring)==6:state.return_possible.add_valid(1)
                if r is not None:
                    magnitude=abs(r)
                    if not state.return_usable.zero:
                        mean=state.u1.ratio(state.return_usable)
                        denominator=ScaledSum(state.return_usable.m,state.return_usable.e);denominator.add_valid(1)
                        factor_m,factor_e=scaled_ratio_parts(state.return_usable,denominator)
                        delta_m,delta_e=math.frexp(abs(magnitude-mean))
                        state.central.add_parts(factor_m*delta_m*delta_m,factor_e+2*delta_e)
                    state.u1.add_valid(magnitude);state.u2.add_square(r);state.return_usable.add_valid(1);state.ever_positive|=r!=0
                spread_exposure=spread_duration[i]/NS
                state.spread.admit(spread[i] if spread_valid[i] else 0,spread_exposure)
                activity_exposure=activity_duration[i]/NS
                state.admit_activity(activity_count[i] if activity_count_valid[i] else 0,
                                     activity_share[i] if activity_share_valid[i] else 0,
                                     activity_dollar[i] if activity_dollar_valid[i] else 0,
                                     activity_exposure)
                state.bid_size.admit(bid_size[i] if bid_size_valid[i] else 0,bid_size_duration[i]/NS)
                state.ask_size.admit(ask_size[i] if ask_size_valid[i] else 0,ask_size_duration[i]/NS)
            if quote_break[i]:
                for window in quote_windows:window.clear()
            if trade_break[i]:
                for window in trade_windows:window.clear()
            for a in AGES:
                source="trades" if a=="trade" else "quotes"
                full_observed=(trade_observed[i] if source=="trades" else quote_observed[i])==NS
                interior_break=row_end in interior_break_ends[source]
                if full_observed and not interior_break:
                    value=float(age_values[a][i]) if age_reasons[a][i]==0 and age_valid[a][i] else None
                    for h in config.age_windows_seconds:windows[a,h].append(value)
        fbuf.append_keys(date,symbol,row_end);sbuf.append_keys(date,symbol,row_end)
        fc,sc=fbuf.columns,sbuf.columns
        for state,fpos,spos,family_plans,support_plans in views:
            view=state.view
            return_reason=history_mask(qs,halted,quote_elapsed,view.startup_seconds,state.return_usable,state.return_possible,config.other_min_coverage)
            rms=state.u2.sqrt_ratio(state.return_usable) if not return_reason else None
            part_reason=return_reason
            if not return_reason and not state.ever_positive:part_reason|=int(Reason.ZERO_RETURN_VARIATION)
            part=None
            if not part_reason:
                k=scaled_square_divide(state.u1,state.return_usable)
                denominator=ScaledSum(k.m,k.e)
                if not state.central.zero:denominator.add_parts(state.central.m,state.central.e)
                part=k.ratio(denominator)
                if not 0<=part<=1:raise ContractError("participation invariant failed")
            rms_slots=fpos["midpoint_rms_5s_bps"];fc[rms_slots[0]].append(rms);fc[rms_slots[1]].append(return_reason)
            part_slots=fpos["movement_participation"];fc[part_slots[0]].append(part);fc[part_slots[1]].append(part_reason)
            spread_value=None;spread_reason=0
            for family_index,(numerator,usable,minimum,is_trade,slots) in enumerate(family_plans):
                status,elapsed=(ts,trade_elapsed) if is_trade else (qs,quote_elapsed)
                reason=history_mask(status,halted,elapsed,view.startup_seconds,usable,state.exposure_possible,minimum)
                value=numerator.ratio(usable) if not reason else None
                fc[slots[0]].append(value);fc[slots[1]].append(reason)
                if family_index==0:spread_value=value;spread_reason=reason
            ratio_reason=return_reason|spread_reason
            if not ratio_reason and spread_value==0:ratio_reason|=int(Reason.ZERO_SPREAD)
            ratio_slots=fpos["midpoint_rms_5s_to_spread"]
            fc[ratio_slots[0]].append(rms/spread_value if not ratio_reason else None);fc[ratio_slots[1]].append(ratio_reason)
            return_usable=state.return_usable.value();return_possible=state.return_possible.value()
            sc[spos["return_usable_weight"]].append(return_usable);sc[spos["return_possible_weight"]].append(return_possible)
            if not state.return_usable.zero and return_usable==0:underflows+=1
            possible=state.exposure_possible.value()
            for family,usable_state in support_plans:
                usable=usable_state.value()
                sc[spos[f"{family}_usable_exposure_seconds"]].append(usable)
                sc[spos[f"{family}_possible_exposure_seconds"]].append(possible)
                if not usable_state.zero and usable==0:underflows+=1
        sc[sp["quote_ew_startup_elapsed_seconds"]].append(quote_elapsed)
        sc[sp["trade_ew_startup_elapsed_seconds"]].append(trade_elapsed)
        for a,h,minimum_count,window,value_position,reason_position,count_position,elapsed_position in age_plans:
                reason=Reason(0);status=ts if a=="trade" else qs
                if status==SourceStatus.UNVERIFIED:reason|=Reason.SOURCE_UNVERIFIED
                elif status==SourceStatus.UNAVAILABLE:reason|=Reason.SOURCE_UNAVAILABLE
                if halted:reason|=Reason.HALT
                if len(window.slots)<h:reason|=Reason.STARTUP
                if not window.values:reason|=Reason.NO_SUPPORTED_DATA
                elif len(window.slots)>=h and len(window.values)<minimum_count:reason|=Reason.LOW_COVERAGE
                fc[value_position].append(window.p90() if not reason else None);fc[reason_position].append(int(reason))
                sc[count_position].append(len(window.values));sc[elapsed_position].append(len(window.slots))
        if fbuf.rows>=4096:flush()
      if fbuf.rows:flush()
    finally:fw.close();sw.close()
    expected_rows=context["coverage"]["expected_rows"]
    member=context["member"].split("/",1)
    expected_first=(member[0],member[1],context["coverage"]["session_start_ns"]+NS)
    expected_last=(member[0],member[1],context["coverage"]["end_ns"])
    if processed_rows!=expected_rows or first!=expected_first or last!=expected_last:
        raise ContractError("base coverage boundary mismatch during feature calculation")
    return processed_rows,underflows


def _open_verified_feature_partition(root,base_partition,base_manifest,config):
    root=Path(root);base_partition=Path(base_partition);manifest=read_json(root/"manifest.json")
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
    parquet_files={}
    for name in ("features.parquet","support.parquet"):
        path=verify_output(root,records[name]);pf=pq.ParquetFile(path)
        if (records[name]["schema_sha256"]!=schema_hash(schemas[name])
                or records[name]["rows"]!=manifest["coverage"]["expected_rows"]):raise ContractError("feature companion declaration mismatch")
        if not pf.schema_arrow.equals(schemas[name],check_metadata=True) or pf.metadata.num_rows!=manifest["coverage"]["expected_rows"]:raise ContractError("feature companion schema/count mismatch")
        parquet_files[name]=pf
    return manifest,parquet_files


def _validate_member_rows(base_pf,feature_pf,support_pf,base_manifest,config):
    iterators=(
        base_pf.iter_batches(batch_size=4096,use_threads=False),
        feature_pf.iter_batches(batch_size=4096,use_threads=False),
        support_pf.iter_batches(batch_size=4096,use_threads=False),
    )
    sentinel=object();previous=[None,None,None]
    first_date=first_symbol=first_end=last_date=last_symbol=last_end=None
    for batches in zip_longest(*iterators,fillvalue=sentinel):
        if any(batch is sentinel for batch in batches) or len({batch.num_rows for batch in batches})!=1:
            raise ContractError("base/feature/support key mismatch")
        base_batch,feature_batch,support_batch=batches
        for index,(batch,kind) in enumerate(zip(batches,("base","features","support"))):
            previous[index]=validate_batch(batch,kind,config=config,previous_key=previous[index])
        for column in range(3):
            if (not base_batch.column(column).equals(feature_batch.column(column))
                    or not base_batch.column(column).equals(support_batch.column(column))):
                raise ContractError("base/feature/support key mismatch")
        if base_batch.num_rows:
            if first_date is None:
                first_date=base_batch.column(0)[0].as_py()
                first_symbol=base_batch.column(1)[0].as_py()
                first_end=base_batch.column(2)[0].as_py()
            last_date=base_batch.column(0)[-1].as_py()
            last_symbol=base_batch.column(1)[-1].as_py()
            last_end=base_batch.column(2)[-1].as_py()
    member=base_manifest["member"];coverage=base_manifest["coverage"]
    if (first_date!=member["session_date"] or first_symbol!=member["symbol"]
            or first_end!=coverage["session_start_ns"]+NS
            or last_date!=member["session_date"] or last_symbol!=member["symbol"]
            or last_end!=coverage["end_ns"]):
        raise ContractError("base coverage boundary mismatch")


def verify_member_partitions(base_partition,feature_partition,*,config=DEFAULT_CONFIG):
    """Hash/check all member artifacts and validate aligned rows in one bounded scan."""
    if not isinstance(config,FeatureConfig):raise ContractError("invalid feature config")
    base_partition=Path(base_partition);feature_partition=Path(feature_partition)
    base_manifest,base_pf=_open_verified_base_partition(base_partition)
    if base_manifest["base_compatibility"]["descriptor"]["max_trade_reporting_age_ns"]!=config.max_trade_reporting_age_ns:
        raise ContractError("base raw measurement policy is incompatible")
    feature_manifest,parquet_files=_open_verified_feature_partition(
        feature_partition,base_partition,base_manifest,config)
    _validate_member_rows(base_pf,parquet_files["features.parquet"],
                          parquet_files["support.parquet"],base_manifest,config)
    return {"base":base_manifest,"features":feature_manifest}


def verify_feature_partition(root,base_partition,*,config=DEFAULT_CONFIG):
    return verify_member_partitions(base_partition,root,config=config)["features"]
