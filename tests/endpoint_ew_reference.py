"""Test-only row-oriented baseline retained for bounded differential checks."""
from collections import deque
import math

import pyarrow as pa
import pyarrow.parquet as pq

from tape_data_product.contracts.policy import NS, endpoint_return
from tape_data_product.contracts.reasons import Reason, SourceStatus
from tape_data_product.contracts.registry import AGES
from tape_data_product.contracts.schemas import feature_schema, support_schema
from tape_data_product.contracts.validation import validate_batch
from tape_data_product.features.endpoint_ew import (
    AgeWindow,
    Family,
    ScaledSum,
    history_mask,
    scaled_ratio_parts,
    scaled_square_divide,
)
from tape_data_product.integrity import read_json


def calculate_reference(base_path,context_path,feature_path,support_path,config,batch_size):
    context=read_json(context_path)
    exact_breaks={source:set(context["instantaneous_breaks"].get(source,[]))|{x[0] for x in context["gaps"].get(source,[])} for source in ("quotes","trades")}
    views=[]
    for view in config.views:
        views.append({"view":view,"lambda":2**(-1/view.half_life_seconds),"u1":ScaledSum(),"u2":ScaledSum(),"c":ScaledSum(),"w":ScaledSum(),"wp":ScaledSum(),
                      **{f:Family.new() for f in ("spread","activity_count","activity_share","activity_dollar","bid_size","ask_size")},"ever_positive":False})
    windows={(a,h):AgeWindow(h) for a in AGES for h in config.age_windows_seconds};ring=deque(maxlen=6);quote_elapsed=trade_elapsed=0;underflows=0
    fs,ss=feature_schema(config),support_schema(config);fw=pq.ParquetWriter(feature_path,fs,compression="zstd",compression_level=3);sw=pq.ParquetWriter(support_path,ss,compression="zstd",compression_level=3)
    fbuf=[];sbuf=[]
    previous=None;first=None;last=None;processed_rows=0
    try:
      for batch in pq.ParquetFile(base_path).iter_batches(batch_size=batch_size,use_threads=False):
       previous=validate_batch(batch,"base",previous_key=previous)
       if batch.num_rows:
        batch_first=(batch.column(0)[0].as_py(),batch.column(1)[0].as_py(),batch.column(2)[0].as_py())
        batch_last=(batch.column(0)[-1].as_py(),batch.column(1)[-1].as_py(),batch.column(2)[-1].as_py())
        if first is None:first=batch_first
        last=batch_last
       processed_rows+=batch.num_rows
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
                if not 0<=part<=1:raise ValueError("participation invariant failed")
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
    expected_rows=context["coverage"]["expected_rows"]
    member=context["member"].split("/",1)
    expected_first=(member[0],member[1],context["coverage"]["session_start_ns"]+NS)
    expected_last=(member[0],member[1],context["coverage"]["end_ns"])
    if processed_rows!=expected_rows or first!=expected_first or last!=expected_last:
        raise ValueError("base coverage boundary mismatch during feature calculation")
    return processed_rows,underflows
