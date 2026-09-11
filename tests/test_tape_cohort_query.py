from datetime import datetime
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import pytest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src/04_research"))

from tape_cohort_config import INITIAL_CONFIG,normalize_config,query_hash
from tape_cohort_outputs import DateSinks,DurationHistogram,verify_date
from tape_cohort_state import CohortMachine,decision_stratum,source_stratum,NS
from verify_tape_cohort_query import worked_transition_oracle


def one_feature(entry=2.,continuation=1.,nentry=3,nexit=3):
    return normalize_config({"schema":"tape_cohort_config_v1","semantics_version":"tape_cohort_hysteresis_v1",
      "eligibility":"post_discovery_and_requested_zero_masks_v1","decision_session":"extended_0400_2000_ET",
      "entry_confirm_seconds":nentry,"exit_confirm_seconds":nexit,"conditions":[{
        "feature":"trade_rate_60s","unit":"trades/second",
        "entry":{"lower":entry,"lower_inclusive":True,"upper":None,"upper_inclusive":False},
        "continuation":{"lower":continuation,"lower_inclusive":True,"upper":None,"upper_inclusive":False}}]})


def row(t,value=2.,mask=0,continuity=1,halt=False,post=True):
    return {"interval_end_ns":t*NS,"continuity_segment_id":continuity,"halt_interval_active":halt,
            "post_discovery_eligible":post,"trade_rate_60s":value,"trade_rate_60s_reason_mask":mask}


def test_initial_five_feature_config_and_hash_normalization():
    assert [x["feature"] for x in INITIAL_CONFIG["conditions"]]==[
      "movement_mean_5s_bps_60s","quoted_spread_mean_bps_60s","trade_rate_60s","dollar_rate_60s","movement_mean_to_spread_60s"]
    reversed_config=dict(INITIAL_CONFIG,conditions=list(reversed(INITIAL_CONFIG["conditions"])))
    assert query_hash(reversed_config)==query_hash(INITIAL_CONFIG)
    assert normalize_config(dict(one_feature(nentry=1),entry_confirm_seconds=1))["entry_confirm_seconds"]==0
    assert query_hash(dict(one_feature(nentry=1),entry_confirm_seconds=1))==query_hash(dict(one_feature(nentry=0),entry_confirm_seconds=0))
    for bad in (float("nan"),float("inf"),-0.0,True):
        raw=json.loads(json.dumps(one_feature()));raw["conditions"][0]["entry"]["lower"]=bad
        with pytest.raises(ValueError): normalize_config(raw)


def test_bounds_and_unknowns_rejected():
    raw=json.loads(json.dumps(one_feature()));raw["conditions"][0]["unit"]="bps"
    with pytest.raises(ValueError):normalize_config(raw)
    raw=json.loads(json.dumps(one_feature()));raw["conditions"][0]["continuation"]["lower"]=3
    with pytest.raises(ValueError):normalize_config(raw)
    raw=json.loads(json.dumps(one_feature()));raw["surprise"]=1
    with pytest.raises(ValueError):normalize_config(raw)


def test_worked_transition_oracle_and_unavailable_variant():
    assert worked_transition_oracle()["active_seconds"]==6
    machine=CohortMachine(one_feature(),"a"*64);sinks={"windows":[],"window_features":[],"strict_runs":[]}
    seq=[2,2,0,2,2,2,1,0,1,0]
    for t,value in enumerate(seq,1):machine.consume_row(row(t,value),sinks)
    machine.consume_row(row(11,None,mask=1),sinks)
    assert sinks["windows"][0]["exit_effective_at_ns"]==11*NS
    assert sinks["windows"][0]["active_seconds"]==5
    assert sinks["windows"][0]["exit_reason"]=="unavailable:undefined"


def test_zero_null_semantics_and_unrequested_null():
    machine=CohortMachine(one_feature(entry=0,continuation=0,nentry=0,nexit=0),"b"*64)
    trace=machine.consume_row(row(1,0.0),{"windows":[],"window_features":[],"strict_runs":[]})
    assert trace["available"] and trace["strict"] and trace["is_member"]
    with pytest.raises(ValueError):machine.consume_row(row(2,None,mask=0),None)
    extra=row(2,0.0);extra["movement_participation_60s"]=None;extra["movement_participation_60s_reason_mask"]=128
    assert CohortMachine(one_feature(entry=0,continuation=0,nentry=0,nexit=0),"c"*64).consume_row(extra,None)["available"]


def test_mandatory_halt_unavailable_continuity_and_gap_breaks():
    for kind in ("halt","unavailable","continuity"):
        machine=CohortMachine(one_feature(nentry=0,nexit=5),kind*16);sinks={"windows":[],"window_features":[],"strict_runs":[]}
        machine.consume_row(row(1,2),sinks);machine.consume_row(row(2,0),sinks)
        broken=row(3,2,halt=kind=="halt",mask=1 if kind=="unavailable" else 0,continuity=2 if kind=="continuity" else 1)
        if kind=="unavailable":broken["trade_rate_60s"]=None
        machine.consume_row(broken,sinks)
        assert sinks["windows"][0]["exit_effective_at_ns"]==3*NS
        assert sinks["windows"][0]["active_seconds"]==2
        if kind=="continuity": assert machine.state=="ACTIVE" # immediate0
    machine=CohortMachine(one_feature(nentry=0),"gap");sinks={"windows":[],"window_features":[],"strict_runs":[]}
    machine.consume_row(row(1),sinks);machine.consume_row(row(4),sinks)
    assert sinks["windows"][0]["exit_effective_at_ns"]==2*NS and sinks["windows"][0]["right_censored"]


def test_clock_strata_and_2000_close():
    ny=ZoneInfo("America/New_York")
    def stamp(hour,minute):return int(datetime(2026,3,9,hour,minute,tzinfo=ny).timestamp()*NS)
    assert source_stratum(stamp(9,30))=="premarket" and decision_stratum(stamp(9,30))=="rth"
    assert source_stratum(stamp(16,0))=="rth" and decision_stratum(stamp(16,0))=="after_hours"
    machine=CohortMachine(one_feature(nentry=0),"close");sinks={"windows":[],"window_features":[],"strict_runs":[]}
    terminal={**row(1),"interval_end_ns":stamp(20,0)};trace=machine.consume_row(terminal,sinks);machine.finish("session_close",sinks)
    assert not trace["is_member"] and not sinks["windows"]


def test_bounded_writer_rolls_and_exact_histogram(tmp_path):
    sinks=DateSinks(tmp_path,buffer_rows=2,part_rows=2)
    machine=CohortMachine(one_feature(nentry=0,nexit=0),"d"*64,checkpoint=True);members=[]
    for t in range(1,9):machine.consume_row(row(t,2 if t%2 else 0),sinks)
    counters=machine.finish("selection_boundary",sinks);outputs=sinks.close()
    assert len(outputs["parts"]["windows"])>=2 and max(x["rows"] for x in outputs["parts"]["windows"])<=2
    assert outputs["median_duration"]==1
    manifest={"outputs":outputs,"members":[{"observed_strict":counters["observed_strict"],"strict_run_count":counters["strict_run_count"]}]}
    assert verify_date(tmp_path,manifest,1)["windows"]==4
    empty=DurationHistogram();assert empty.quantile(.5) is None
    h=DurationHistogram();h.add(3);h.add(7);assert h.quantile(.5)==5


def test_bulk_trace_suppression_preserves_windows_counters_and_boundaries():
    # Five-pass entry, failed continuation recovery/exit, unavailable/continuity
    # breaks, clock gaps and the terminal endpoint must produce identical artifacts.
    ny=ZoneInfo('America/New_York')
    start=int(datetime(2026,6,18,19,59,20,tzinfo=ny).timestamp())
    values=[2]*6+[0]*2+[1]+[0]*5+[2]*6+[None]+[2]*7+[0]*2+[2]*10
    rows=[row(start+i,v,mask=1 if v is None else 0,
              continuity=2 if i>=25 else 1,halt=i==30) for i,v in enumerate(values)]
    rows=[r for i,r in enumerate(rows) if i!=32]
    rows.append(row(start+40,2,continuity=2))
    outputs=[]
    for enabled in (True,False):
        machine=CohortMachine(one_feature(nentry=5,nexit=5),'trace-comparison',emit_trace=enabled)
        sinks={'windows':[],'window_features':[],'strict_runs':[]}
        traces=list(machine.consume(rows,sinks))
        counters=machine.finish('session_close',sinks)
        assert all(isinstance(t,dict) for t in traces) if enabled else all(t is None for t in traces)
        outputs.append((sinks,counters))
    assert outputs[0]==outputs[1]
    assert outputs[0][0]['windows']
