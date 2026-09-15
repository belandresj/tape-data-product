from __future__ import annotations
import hashlib,json
import fcntl
from dataclasses import replace
import math
import sqlite3
import sys
import os
import signal
import subprocess
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import tape_data_product.features.endpoint_ew as endpoint_module
import tape_data_product.replay.builder as builder_module

from tape_data_product.contracts import DEFAULT_CONFIG,contract_identity
from tape_data_product.contracts.config import EWView,FeatureConfig
from tape_data_product.contracts.config import digest
from tape_data_product.contracts.policy import NS,session_bounds
from tape_data_product.features.endpoint_ew import AgeWindow,ScaledSum,build_from_base
from tape_data_product.features.endpoint_ew import verify_feature_partition
from tape_data_product.integrity import sha256_file
from tape_data_product.replay.builder import build_base_partition,verify_base_partition
from tape_data_product.replay.admission import admit_inventory
from tape_data_product.calculate import create_plan,run_plan
from tape_data_product.replay.builder import _implementation_identity as base_implementation_identity
from tape_data_product.features.endpoint_ew import _implementation_identity as feature_implementation_identity
from tape_data_product.integrity import write_atomic_json

QSCHEMA=pa.schema([pa.field("sip_timestamp",pa.int64()),pa.field("sequence_number",pa.int64()),pa.field("bid_price",pa.float64()),pa.field("ask_price",pa.float64()),pa.field("bid_size",pa.float64()),pa.field("ask_size",pa.float64()),pa.field("conditions",pa.list_(pa.int64())),pa.field("indicators",pa.list_(pa.int64()))])
TSCHEMA=pa.schema([pa.field("sip_timestamp",pa.int64()),pa.field("sequence_number",pa.int64()),pa.field("participant_timestamp",pa.int64()),pa.field("price",pa.float64()),pa.field("decimal_size",pa.string()),pa.field("size",pa.float64()),pa.field("conditions",pa.list_(pa.int64())),pa.field("correction",pa.int64())])

def _write(path,schema,rows):pq.write_table(pa.Table.from_pylist(rows,schema=schema),path,row_group_size=2)

def _records(path):
    return {"path":path.name,"sha256":sha256_file(path)[0],"bytes":path.stat().st_size,"rows":pq.ParquetFile(path).metadata.num_rows,"schema_sha256":digest({"schema":str(pq.ParquetFile(path).schema_arrow)}),"clock":"sip_timestamp_utc_ns","terminal_complete":True}

def fixture(tmp_path,seconds=310,symbol="SYN"):
    day="2026-09-02";start,_=session_bounds(day);raw=tmp_path/"raw";raw.mkdir();evidence=tmp_path/"evidence";evidence.mkdir()
    quotes=[{"sip_timestamp":start,"sequence_number":1,"bid_price":99.,"ask_price":101.,"bid_size":10.,"ask_size":20.,"conditions":[],"indicators":[]},{"sip_timestamp":start+NS//4,"sequence_number":2,"bid_price":100.,"ask_price":102.,"bid_size":30.,"ask_size":40.,"conditions":[],"indicators":[]}]
    trades=[{"sip_timestamp":start+100_000_000,"sequence_number":1,"participant_timestamp":start+100_000_000,"price":100.,"decimal_size":"0.1","size":.1,"conditions":[],"correction":0},{"sip_timestamp":start+200_000_000,"sequence_number":2,"participant_timestamp":start+200_000_000,"price":102.,"decimal_size":"0.2","size":.2,"conditions":[],"correction":0}]
    _write(raw/"quotes.parquet",QSCHEMA,quotes);_write(raw/"trades.parquet",TSCHEMA,trades)
    interval=[[start,start+seconds*NS]]
    streams={}
    for name,path in (("quotes",raw/"quotes.parquet"),("trades",raw/"trades.parquet")):
        provenance=evidence/f"{name}-provenance.json";provenance.write_text(json.dumps({"member":f"{day}/{symbol}","source":"synthetic"}))
        coverage=evidence/f"{name}-coverage.json";coverage.write_text(json.dumps({"version":"source_coverage_v1","member":f"{day}/{symbol}","stream":name,"intervals":interval,"terminal_complete":True}))
        streams[name]={**_records(path),"provenance_path":provenance.name,"provenance_sha256":sha256_file(provenance)[0],"coverage_evidence_path":coverage.name,"coverage_evidence_sha256":sha256_file(coverage)[0]}
    quote_units=evidence/"quote-units.json";quote_units.write_text(json.dumps({"version":"source_units_v1","member":f"{day}/{symbol}","stream":"quotes","unit":"shares","multiplier":1,"object_sha256":streams["quotes"]["sha256"]}))
    trade_units=evidence/"trade-units.json";trade_units.write_text(json.dumps({"version":"source_units_v1","member":f"{day}/{symbol}","stream":"trades","quantity_precedence":"decimal_size_then_size","scale":9,"object_sha256":streams["trades"]["sha256"]}))
    pair={"version":"tape_source_pair_v1","symbol":symbol,"session_date":day,"currency":"USD","adapter":"massive_canonical_tq_v1","root":str(raw),"evidence_root":str(evidence),"streams":streams,"source_units":{"quote_size_unit":"shares","quote_size_evidence_sha256":sha256_file(quote_units)[0],"quote_size_evidence_path":quote_units.name,"trade_quantity_evidence_sha256":sha256_file(trade_units)[0],"trade_quantity_evidence_path":trade_units.name,"round_lot_shares":None}}
    halt=evidence/"halts.json";halt.write_text(json.dumps({"member":f"{day}/{symbol}","status":"verified_empty","halts":[]}))
    continuity=evidence/"continuity.json";continuity.write_text(json.dumps({"version":"source_continuity_v1","member":f"{day}/{symbol}","gaps":{"quotes":[],"trades":[]},"instantaneous_breaks":{"quotes":[],"trades":[]}}))
    context={"version":"tape_member_context_v1","member":f"{day}/{symbol}","coverage":{"kind":"prefix","session_start_ns":start,"end_ns":start+seconds*NS,"expected_rows":seconds},"observation_intervals":{"quotes":interval,"trades":interval},"gaps":{"quotes":[],"trades":[]},"instantaneous_breaks":{"quotes":[],"trades":[]},"halts":[],"halt_evidence":{"status":"verified_empty","path":halt.name,"sha256":sha256_file(halt)[0]},"continuity_evidence":{"path":continuity.name,"sha256":sha256_file(continuity)[0]},"seed":{"basis":"verified_empty"},"selection":{"basis":"synthetic"},"discovery":{"eligibility_basis":"nominal"}}
    for name,value in (("pair.json",pair),("context.json",context)):(tmp_path/name).write_text(json.dumps(value))
    return tmp_path/"pair.json",tmp_path/"context.json"

def test_q1_t1_literal_and_features(tmp_path):
    pair,context=fixture(tmp_path);base=tmp_path/"base";result=build_base_partition(pair,context,base,batch_size=1);assert result.rows==310
    row=pq.read_table(base/"base.parquet").to_pylist()[0]
    assert row["bid_twap_usd"]==99.75 and row["ask_twap_usd"]==101.75 and row["midpoint_twap_usd"]==100.75
    assert row["spread_integral_bps_seconds"]==pytest.approx(.25*200+.75*(20000/101),rel=1e-12)
    assert row["bid_size_integral_shares_seconds"]==25 and row["ask_size_integral_shares_seconds"]==35
    assert row["trade_count_1s"]==2 and str(row["share_volume_1s"])=="0.300000000" and row["dollar_volume_1s_usd"]==pytest.approx(30.4)
    features=tmp_path/"features";build_from_base(base,features,batch_size=7)
    values=pq.read_table(features/"features.parquet").to_pylist();assert values[58]["quoted_spread_bps_hl30s"] is None;assert values[59]["quoted_spread_bps_hl30s"] is not None
    assert values[298]["quoted_spread_bps_hl120s"] is None;assert values[299]["quoted_spread_bps_hl120s"] is not None
    verify_base_partition(base)

def test_scaled_hand_sum_and_age_interpolation():
    u=ScaledSum();w=ScaledSum()
    for value in (1.,3.):u.decay(.5);w.decay(.5);u.add_square(value);w.add_float(1)
    assert u.ratio(w)==pytest.approx(19/3)
    tiny=ScaledSum();tiny.add_square(1.)
    for _ in range(1200):tiny.decay(.5)
    assert not tiny.zero and tiny.e < -1100
    age=AgeWindow(4)
    for x in (0.,1.,2.,3.):age.append(x)
    assert age.p90()==pytest.approx(2.7)

def test_descriptor_traversal_and_identity_change(tmp_path):
    pair,context=fixture(tmp_path,6);value=json.loads(pair.read_text());value["streams"]["quotes"]["path"]="../quotes.parquet";pair.write_text(json.dumps(value))
    with pytest.raises(ValueError,match="traversal"):build_base_partition(pair,context,tmp_path/"base")

def test_batch_invariance(tmp_path):
    pair,context=fixture(tmp_path,10);b1=tmp_path/"b1";b2=tmp_path/"b2";build_base_partition(pair,context,b1,batch_size=1);build_base_partition(pair,context,b2,batch_size=7)
    assert pq.read_table(b1/"base.parquet").to_pylist()==pq.read_table(b2/"base.parquet").to_pylist()

def test_completed_reuse_requires_exact_source_and_feature_config(tmp_path):
    pair,context=fixture(tmp_path);base=tmp_path/"base";features=tmp_path/"features"
    build_base_partition(pair,context,base);build_from_base(base,features)
    changed=json.loads(pair.read_text());evidence=tmp_path/"evidence"/"quote-units.json";body=json.loads(evidence.read_text());evidence.write_text(json.dumps(body,indent=2));changed["source_units"]["quote_size_evidence_sha256"]=sha256_file(evidence)[0];pair.write_text(json.dumps(changed))
    with pytest.raises(ValueError,match="does not match"):build_base_partition(pair,context,base)
    stricter=replace(DEFAULT_CONFIG,spread_min_coverage=.95)
    with pytest.raises(ValueError,match="mismatch|do not match"):build_from_base(base,features,config=stricter)

def test_gap_onset_event_cannot_seed_recovery(tmp_path):
    pair,context=fixture(tmp_path,2);start,_=session_bounds("2026-09-02")
    raw=tmp_path/"raw";quotes=[{"sip_timestamp":start,"sequence_number":1,"bid_price":99.,"ask_price":101.,"bid_size":10.,"ask_size":20.,"conditions":[],"indicators":[]},{"sip_timestamp":start+NS//4,"sequence_number":2,"bid_price":100.,"ask_price":102.,"bid_size":30.,"ask_size":40.,"conditions":[],"indicators":[]}]
    _write(raw/"quotes.parquet",QSCHEMA,quotes)
    p=json.loads(pair.read_text());p["streams"]["quotes"].update(_records(raw/"quotes.parquet"));unit=tmp_path/"evidence"/"quote-units.json";body=json.loads(unit.read_text());body["object_sha256"]=p["streams"]["quotes"]["sha256"];unit.write_text(json.dumps(body));p["source_units"]["quote_size_evidence_sha256"]=sha256_file(unit)[0];pair.write_text(json.dumps(p))
    c=json.loads(context.read_text());c["gaps"]["quotes"]=[[start+NS//4,start+3*NS//4]];continuity=tmp_path/"evidence"/"continuity.json";continuity.write_text(json.dumps({"version":"source_continuity_v1","member":"2026-09-02/SYN","gaps":c["gaps"],"instantaneous_breaks":c["instantaneous_breaks"]}));c["continuity_evidence"]["sha256"]=sha256_file(continuity)[0];context.write_text(json.dumps(c))
    base=tmp_path/"base";build_base_partition(pair,context,base);row=pq.read_table(base/"base.parquet").to_pylist()[0]
    assert row["quote_observed_duration_ns"]==NS//2
    assert row["price_valid_duration_ns"]==NS//4
    assert row["bid_end_usd"] is None

def test_subsecond_halt_nulls_zero_support_activity(tmp_path):
    pair,context=fixture(tmp_path,3);start,_=session_bounds("2026-09-02")
    c=json.loads(context.read_text());c["halts"]=[{"start_ns":start+NS//5,"end_ns":start+NS+NS//4,"id":"halt-1"}];c["halt_evidence"]["status"]="accepted_intervals"
    halt=tmp_path/"evidence"/"halts.json";halt.write_text(json.dumps({"member":"2026-09-02/SYN","status":"accepted_intervals","halts":c["halts"]}));c["halt_evidence"]["sha256"]=sha256_file(halt)[0];context.write_text(json.dumps(c))
    base=tmp_path/"base";build_base_partition(pair,context,base);rows=pq.read_table(base/"base.parquet").to_pylist();row=rows[0]
    assert row["halt_active"] and row["activity_valid_duration_ns"]==0
    assert row["trade_count_1s"] is None and row["share_volume_1s"] is None and row["dollar_volume_1s_usd"] is None
    assert rows[1]["halt_active"] and rows[0]["quote_continuity_id"]==rows[1]["quote_continuity_id"]==1
    assert not rows[1]["quote_continuity_break_in_second"] and not rows[1]["trade_continuity_break_in_second"]

def test_alternate_features_require_no_raw_access(tmp_path):
    pair,context=fixture(tmp_path,100);base=tmp_path/"base";build_base_partition(pair,context,base)
    for path in (tmp_path/"raw").iterdir():path.chmod(0)
    config=FeatureConfig(views=(EWView(45,90),),age_windows_seconds=(30,),spread_min_coverage=.9,other_min_coverage=.8,age_min_coverage=.9,max_trade_reporting_age_ns=1_000_000_000)
    output=tmp_path/"alternate";build_from_base(base,output,config=config);verify_feature_partition(output,base,config=config)
    row=pq.read_table(output/"features.parquet").to_pylist()[89]
    assert "quoted_spread_bps_hl45s" in row and "trade_age_p90_seconds_window30s" in row

def test_explicit_history_feature_oracle(tmp_path):
    pair,context=fixture(tmp_path,70);start,_=session_bounds("2026-09-02");raw=tmp_path/"raw"
    quotes=[{"sip_timestamp":start+i*NS,"sequence_number":i+1,"bid_price":99.+i/100,"ask_price":101.+i/100,"bid_size":10.,"ask_size":20.,"conditions":[],"indicators":[]} for i in range(70)]
    _write(raw/"quotes.parquet",QSCHEMA,quotes);p=json.loads(pair.read_text());p["streams"]["quotes"].update(_records(raw/"quotes.parquet"));unit=tmp_path/"evidence"/"quote-units.json";body=json.loads(unit.read_text());body["object_sha256"]=p["streams"]["quotes"]["sha256"];unit.write_text(json.dumps(body));p["source_units"]["quote_size_evidence_sha256"]=sha256_file(unit)[0];pair.write_text(json.dumps(p))
    base=tmp_path/"base";features=tmp_path/"features";build_base_partition(pair,context,base);build_from_base(base,features)
    base_rows=pq.read_table(base/"base.parquet").to_pylist();feature_rows=pq.read_table(features/"features.parquet").to_pylist();i=59;lam=2**(-1/30)
    returns=[]
    for j in range(5,i+1):
        m0=base_rows[j-5]["bid_end_usd"]/2+base_rows[j-5]["ask_end_usd"]/2;m1=base_rows[j]["bid_end_usd"]/2+base_rows[j]["ask_end_usd"]/2
        returns.append(10000*math.log(m1/m0))
    weights=[lam**(len(returns)-1-k) for k in range(len(returns))];w=sum(weights);q=sum(x*x*y for x,y in zip(returns,weights))/w;a=sum(abs(x)*y for x,y in zip(returns,weights))/w
    assert feature_rows[i]["midpoint_rms_5s_bps_hl30s"]==pytest.approx(math.sqrt(q),rel=1e-10,abs=1e-12)
    assert feature_rows[i]["movement_participation_hl30s"]==pytest.approx(a*a/q,rel=1e-10,abs=1e-12)

def test_source_and_companion_faults_fail_closed(tmp_path):
    pair,context=fixture(tmp_path,10);base=tmp_path/"base";features=tmp_path/"features";build_base_partition(pair,context,base);build_from_base(base,features)
    with (tmp_path/"raw"/"quotes.parquet").open("ab") as handle:handle.write(b"changed")
    with pytest.raises(ValueError,match="identity changed"):build_base_partition(pair,context,base)
    with (features/"support.parquet").open("ab") as handle:handle.write(b"changed")
    with pytest.raises(ValueError,match="identity mismatch"):verify_feature_partition(features,base)


def test_control_evidence_is_rechecked_after_raw_replay(tmp_path,monkeypatch):
    pair,context=fixture(tmp_path,2);original=builder_module._replay
    def mutate(*args,**kwargs):
        result=original(*args,**kwargs)
        with (tmp_path/"evidence/continuity.json").open("ab") as handle:handle.write(b" ")
        return result
    monkeypatch.setattr(builder_module,"_replay",mutate)
    with pytest.raises(ValueError,match="evidence changed during replay"):build_base_partition(pair,context,tmp_path/"base")
    assert not (tmp_path/"base/manifest.json").exists()

def test_raw_source_is_rechecked_without_a_second_full_hash(tmp_path,monkeypatch):
    pair,context=fixture(tmp_path,2);original=builder_module._replay
    def mutate(*args,**kwargs):
        result=original(*args,**kwargs)
        with (tmp_path/"raw/quotes.parquet").open("ab") as handle:handle.write(b" ")
        return result
    monkeypatch.setattr(builder_module,"_replay",mutate)
    with pytest.raises(ValueError,match="source identity changed during replay"):build_base_partition(pair,context,tmp_path/"base")
    assert not (tmp_path/"base/manifest.json").exists()

def test_fresh_builds_do_not_invoke_standalone_output_verifiers(tmp_path,monkeypatch):
    pair,context=fixture(tmp_path,70)
    monkeypatch.setattr(builder_module,"verify_base_partition",lambda *args,**kwargs: (_ for _ in ()).throw(AssertionError("standalone base verification")))
    base=tmp_path/"base";build_base_partition(pair,context,base)
    monkeypatch.setattr(endpoint_module,"verify_feature_partition",lambda *args,**kwargs: (_ for _ in ()).throw(AssertionError("standalone feature verification")))
    features=tmp_path/"features";build_from_base(base,features)
    assert (base/"manifest.json").exists() and (features/"manifest.json").exists()

def test_reporting_cutoff_and_unknown_correction(tmp_path):
    pair,context=fixture(tmp_path,2);start,_=session_bounds("2026-09-02");raw=tmp_path/"raw"
    trades=[]
    for i,age in enumerate((0,NS,-1,NS+1)):
        sip=start+100+i;trades.append({"sip_timestamp":sip,"sequence_number":i+1,"participant_timestamp":sip-age,"price":100.,"decimal_size":"1","size":1.,"conditions":[],"correction":0})
    _write(raw/"trades.parquet",TSCHEMA,trades);p=json.loads(pair.read_text());p["streams"]["trades"].update(_records(raw/"trades.parquet"));unit=tmp_path/"evidence"/"trade-units.json";body=json.loads(unit.read_text());body["object_sha256"]=p["streams"]["trades"]["sha256"];unit.write_text(json.dumps(body));p["source_units"]["trade_quantity_evidence_sha256"]=sha256_file(unit)[0];pair.write_text(json.dumps(p))
    base=tmp_path/"base";build_base_partition(pair,context,base);assert pq.read_table(base/"base.parquet").to_pylist()[0]["trade_count_1s"]==2
    bad=trades[:1];bad[0]["correction"]=999;_write(raw/"trades.parquet",TSCHEMA,bad);p=json.loads(pair.read_text());p["streams"]["trades"].update(_records(raw/"trades.parquet"));body["object_sha256"]=p["streams"]["trades"]["sha256"];unit.write_text(json.dumps(body));p["source_units"]["trade_quantity_evidence_sha256"]=sha256_file(unit)[0];pair.write_text(json.dumps(p))
    with pytest.raises(ValueError,match="unknown correction"):build_base_partition(pair,context,tmp_path/"bad-base")

def test_calculation_plan_preflight_is_hash_bound(tmp_path):
    inventory={"members":[{"session_date":"2026-09-02","symbol":"SYN"}],"transfer_complete":False,"measurement_references":[],"base_root":str(tmp_path/"base"),"feature_root":str(tmp_path/"features"),"ledger_path":str(tmp_path/"ledger.sqlite")}
    admissions={"findings":[{"member":"2026-09-02/SYN","state":"blocked"}]};limits={"workers":1}
    for name,value in (("inventory.json",inventory),("admissions.json",admissions),("config.json",DEFAULT_CONFIG.to_dict()),("limits.json",limits)):(tmp_path/name).write_text(json.dumps(value))
    result=create_plan(tmp_path/"inventory.json",tmp_path/"admissions.json",tmp_path/"config.json",tmp_path/"limits.json",tmp_path/"plan")
    assert not result["ready"] and result["unresolved"]==1
    with pytest.raises(ValueError,match="plan identity mismatch"):run_plan(result["plan"],"0"*64)
    with pytest.raises(ValueError,match="transfer_incomplete"):run_plan(result["plan"],result["sha256"])
    lock_path=Path(result["plan"]).parent/f'.{Path(result["plan"]).name}.run.lock'
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        with pytest.raises(ValueError,match="already running"):run_plan(result["plan"],result["sha256"])

def test_admission_emits_validated_member_descriptors(tmp_path):
    pair,context=fixture(tmp_path,6)
    inventory={"members":[{"session_date":"2026-09-02","symbol":"SYN"}]}
    evidence={"members":{"2026-09-02/SYN":{"quote_units":True,"trade_representation":True,"terminal_coverage":True,"halt_context":True,"continuity":True,"source_pair_path":str(pair),"member_context_path":str(context)}}}
    for name,value in (("inventory.json",inventory),("evidence.json",evidence)):(tmp_path/name).write_text(json.dumps(value))
    result=admit_inventory(tmp_path/"inventory.json",tmp_path/"evidence.json",tmp_path/"admitted")
    assert result["metadata_admitted"]==1 and result["blocked"]==0
    assert (tmp_path/"admitted/members/2026-09-02/SYN/source-pair.json").exists()


def test_owner_accepted_historical_retrieval_is_admitted_but_recorded_unverified(tmp_path):
    pair,context=fixture(tmp_path,6);p=json.loads(pair.read_text())
    status="unverified_missing_original_vendor_pagination_receipts"
    p["version"]="tape_source_pair_v2"
    for name,stream in p["streams"].items():
        stream["terminal_complete"]=False;stream["retrieval_completeness"]=status
        coverage=tmp_path/"evidence"/f"{name}-coverage.json";body=json.loads(coverage.read_text())
        body.update(version="source_coverage_v2",terminal_complete=False,retrieval_completeness=status)
        coverage.write_text(json.dumps(body));stream["coverage_evidence_sha256"]=sha256_file(coverage)[0]
    pair.write_text(json.dumps(p))
    inventory=tmp_path/"inventory.json";inventory.write_text(json.dumps({"members":[{"session_date":"2026-09-02","symbol":"SYN"}]}))
    evidence=tmp_path/"admission.json";evidence.write_text(json.dumps({"members":{"2026-09-02/SYN":{"quote_units":True,"trade_representation":True,"terminal_coverage":False,"historical_retrieval_completeness":status,"halt_context":True,"continuity":True,"source_pair_path":str(pair),"member_context_path":str(context)}}}))
    result=admit_inventory(inventory,evidence,tmp_path/"admitted")
    assert result["metadata_admitted"]==1 and result["blocked"]==0
    assert result["findings"][0]["retrieval_completeness"]=="unverified"
    base=tmp_path/"base";build_base_partition(result["findings"][0]["source_pair_path"],result["findings"][0]["member_context_path"],base)
    stored=json.loads((base/"manifest.json").read_text())["inputs"]["streams"]
    assert all(not stream["terminal_complete"] and stream["retrieval_completeness"]==status for stream in stored.values())


def test_historical_retrieval_requires_explicit_unverified_descriptor_status(tmp_path):
    pair,context=fixture(tmp_path,6);p=json.loads(pair.read_text());p["version"]="tape_source_pair_v2"
    for name,stream in p["streams"].items():
        stream["terminal_complete"]=False;stream["retrieval_completeness"]="verified"
    pair.write_text(json.dumps(p))
    with pytest.raises(ValueError,match="explicitly unverified"):
        build_base_partition(pair,context,tmp_path/"base")

def test_admission_rejects_interval_short_of_prefix(tmp_path):
    pair,context=fixture(tmp_path,6);c=json.loads(context.read_text());start=c["coverage"]["session_start_ns"];c["observation_intervals"]["quotes"]=[[start,start+5*NS]];context.write_text(json.dumps(c))
    p=json.loads(pair.read_text());coverage=tmp_path/"evidence"/"quotes-coverage.json";body=json.loads(coverage.read_text());body["intervals"]=c["observation_intervals"]["quotes"];coverage.write_text(json.dumps(body));p["streams"]["quotes"]["coverage_evidence_sha256"]=sha256_file(coverage)[0];pair.write_text(json.dumps(p))
    with pytest.raises(ValueError,match="do not cover declared prefix"):build_base_partition(pair,context,tmp_path/"base")

def test_numeric_and_contradictory_locks(tmp_path):
    pair,context=fixture(tmp_path,2);start,_=session_bounds("2026-09-02");raw=tmp_path/"raw"
    quotes=[{"sip_timestamp":start,"sequence_number":1,"bid_price":100.,"ask_price":100.,"bid_size":10.,"ask_size":20.,"conditions":[],"indicators":[]},{"sip_timestamp":start+NS,"sequence_number":2,"bid_price":99.,"ask_price":101.,"bid_size":10.,"ask_size":20.,"conditions":[85],"indicators":[]}]
    _write(raw/"quotes.parquet",QSCHEMA,quotes);p=json.loads(pair.read_text());p["streams"]["quotes"].update(_records(raw/"quotes.parquet"));unit=tmp_path/"evidence"/"quote-units.json";body=json.loads(unit.read_text());body["object_sha256"]=p["streams"]["quotes"]["sha256"];unit.write_text(json.dumps(body));p["source_units"]["quote_size_evidence_sha256"]=sha256_file(unit)[0];pair.write_text(json.dumps(p))
    base=tmp_path/"base";build_base_partition(pair,context,base);rows=pq.read_table(base/"base.parquet").to_pylist()
    assert rows[0]["spread_integral_bps_seconds"]==0 and rows[0]["spread_valid_duration_ns"]==NS
    assert rows[1]["bid_end_usd"]==99 and rows[1]["price_end_reason_mask"]==0 and rows[1]["spread_valid_duration_ns"]==0

def test_verified_seed_origin_starts_at_session_and_requires_evidence(tmp_path):
    pair,context=fixture(tmp_path,6);start,_=session_bounds("2026-09-02");raw=tmp_path/"raw"
    quotes=[{"sip_timestamp":start-NS,"sequence_number":1,"bid_price":99.,"ask_price":101.,"bid_size":10.,"ask_size":20.,"conditions":[],"indicators":[]}]
    _write(raw/"quotes.parquet",QSCHEMA,quotes);p=json.loads(pair.read_text());p["streams"]["quotes"].update(_records(raw/"quotes.parquet"));unit=tmp_path/"evidence"/"quote-units.json";body=json.loads(unit.read_text());body["object_sha256"]=p["streams"]["quotes"]["sha256"];unit.write_text(json.dumps(body));p["source_units"]["quote_size_evidence_sha256"]=sha256_file(unit)[0];pair.write_text(json.dumps(p))
    c=json.loads(context.read_text());c["seed"]={"basis":"verified_interval"};context.write_text(json.dumps(c))
    with pytest.raises(ValueError,match="identity-bound evidence"):build_base_partition(pair,context,tmp_path/"rejected")
    seed=tmp_path/"evidence"/"seed.json";seed.write_text(json.dumps({"version":"pre_session_seed_v1","member":"2026-09-02/SYN","start_ns":start-300*NS,"end_ns":start,"quote_object_sha256":p["streams"]["quotes"]["sha256"],"terminal_complete":True,"latest_event":{"sip_timestamp":start-NS,"sequence_number":1}}));c["seed"]={"basis":"verified_interval","path":seed.name,"sha256":sha256_file(seed)[0]};context.write_text(json.dumps(c))
    base=tmp_path/"base";build_base_partition(pair,context,base);row=pq.read_table(base/"base.parquet").to_pylist()[0]
    assert row["midpoint_observation_start_ns"]==start and row["midpoint_age_lower_bound_seconds"]==1

def test_manifest_declared_rows_are_verified(tmp_path):
    pair,context=fixture(tmp_path,6);base=tmp_path/"base";build_base_partition(pair,context,base)
    manifest=json.loads((base/"manifest.json").read_text());manifest["outputs"][0]["rows"]+=1;write_atomic_json(base/"manifest.json",manifest)
    with pytest.raises(ValueError,match="output declaration"):verify_base_partition(base)

def test_one_worker_plan_executes_base_then_features(tmp_path):
    pair,context=fixture(tmp_path,70);wheel=tmp_path/"candidate.whl";wheel.write_bytes(b"synthetic-wheel-identity")
    pair_body=json.loads(pair.read_text());transfer_manifest=tmp_path/"transfer.jsonl";transfer_records=[]
    for stream in ("quotes","trades"):
        declared=pair_body["streams"][stream];transfer_records.append({"version":"raw_migration_object_v1","kind":"canonical_tq","session_date":"2026-09-02","symbol":"SYN","stream":stream,"key":declared["path"],"relative_path":declared["path"],"sha256":declared["sha256"],"size_bytes":declared["bytes"],"rows":declared["rows"],"verify_mode":"tq_parquet_sip_order","reuse_path":None})
    transfer_manifest.write_text("".join(json.dumps(x)+"\n" for x in transfer_records));transfer_manifest_sha=sha256_file(transfer_manifest)[0];transfer_bytes=sum(x["size_bytes"] for x in transfer_records)
    member={"session_date":"2026-09-02","symbol":"SYN"}
    release={"source_revision":"a"*40,"wheel_path":str(wheel),"wheel_sha256":sha256_file(wheel)[0],"executable":sys.executable,"contract_identity":contract_identity(),"base_implementation_identity":base_implementation_identity()["sha256"],"feature_implementation_identity":feature_implementation_identity()["sha256"]}
    kind_summary={"canonical_tq":{"objects":2,"bytes":transfer_bytes},"discovery_reference":{"objects":0,"bytes":0},"halt_support":{"objects":0,"bytes":0}}
    completion=tmp_path/"transfer-complete.json";completion.write_text(json.dumps({"version":"raw_migration_completion_v1","status":"complete","manifest_sha256":transfer_manifest_sha,"expected_objects":2,"expected_bytes":transfer_bytes,"states":{"verified":{"objects":2,"bytes":transfer_bytes},"reused":{"objects":0,"bytes":0}},"reserved_download_bytes":transfer_bytes,"delivered_payload_bytes":transfer_bytes,"attempts":2,"elapsed_seconds":1.0,"scope":"transport identity only; production source admission is separate"}))
    guards={"workers":1,"threads":1,"batch_size":4096,"cpu_quota_percent":200,"tasks_max":64,"memory_max_bytes":1610612736,"memory_swap_max_bytes":0,"process_tree_rss_stop_bytes":1073741824,"runtime_max_seconds":600,"read_limit_bytes":1073741824,"output_scratch_limit_bytes":2147483648,"max_decoded_raw_rows":2000000};phase={"decoded_raw_rows":100,"read_bytes":1000,"peak_rss_bytes":1000000,"wall_seconds":1.25,"disk_bytes":{"output":2000,"scratch_peak":3000},"guards":guards};artifacts=[{"member":member,"source_pair_sha256":"1"*64,"context_sha256":"2"*64,"base_manifest_sha256":"3"*64,"feature_manifest_sha256":"4"*64,"rows":720} for member in ("2026-09-02/KDP","2026-09-02/NVDA")]
    measurement_body={"version":"tape_representative_measurement_v1","status":"accepted","kind":"representative_measurement","source_revision":release["source_revision"],"wheel_sha256":release["wheel_sha256"],"config_sha256":digest(DEFAULT_CONFIG.to_dict()),"sample":{"members":["2026-09-02/KDP","2026-09-02/NVDA"],"coverage_seconds_per_member":720},"rows":{"base":1440,"features":1440,"support":1440},"artifacts":artifacts,"phases":{"build":phase,"verification":phase},"independent_reconstruction":{"base_all_fields":"passed","features_explicit_histories":"passed","support_all_fields":"passed"}};measurement_id=digest(measurement_body);decision_body={"version":"tape_representativeness_decision_v1","status":"reviewed_accepted","population_sha256":digest([member]),"expected_members":1,"source_revision":release["source_revision"],"wheel_sha256":release["wheel_sha256"],"config_sha256":digest(DEFAULT_CONFIG.to_dict()),"measurement_ids":[measurement_id]};decision=tmp_path/"decision.json";decision.write_text(json.dumps(decision_body));decision_ref={"path":str(decision),"sha256":sha256_file(decision)[0]};measurement_body.update(measurement_id=measurement_id,readiness_decision_sha256=decision_ref["sha256"])
    measurement=tmp_path/"measurement.json";measurement.write_text(json.dumps(measurement_body))
    inventory={"members":[member],"transfer_complete":True,"transfer_manifest_path":str(transfer_manifest),"transfer_manifest_sha256":transfer_manifest_sha,"transfer_expected_objects":2,"transfer_expected_bytes":transfer_bytes,"transfer_kind_summary":kind_summary,"transfer_completion":{"path":str(completion),"sha256":sha256_file(completion)[0]},"readiness_decision":decision_ref,"measurement_references":[{"path":str(measurement),"sha256":sha256_file(measurement)[0]}],"release":release,"base_root":str(tmp_path/"run-base"),"feature_root":str(tmp_path/"run-features"),"ledger_path":str(tmp_path/"run-ledger.sqlite")}
    admissions={"findings":[{"member":"2026-09-02/SYN","state":"metadata_admitted","source_pair_path":str(pair),"member_context_path":str(context)}]};limits={"workers":1,"batch_size":7,"disk_reserve_bytes":0,"scratch_cap_bytes":0}
    for name,value in (("inventory-run.json",inventory),("admissions-run.json",admissions),("config-run.json",DEFAULT_CONFIG.to_dict()),("limits-run.json",limits)):(tmp_path/name).write_text(json.dumps(value))
    plan=create_plan(tmp_path/"inventory-run.json",tmp_path/"admissions-run.json",tmp_path/"config-run.json",tmp_path/"limits-run.json",tmp_path/"run-plan")
    result=run_plan(plan["plan"],plan["sha256"]);assert result["status"]=="complete" and result["members"]==1
    assert (tmp_path/"run-base/session_date=2026-09-02/symbol=SYN/manifest.json").exists() and (tmp_path/"run-features/session_date=2026-09-02/symbol=SYN/manifest.json").exists()


def test_plan_rejects_descriptor_member_substitution_before_ledger(tmp_path):
    descriptor_root=tmp_path/"descriptors";descriptor_root.mkdir();pair,context=fixture(descriptor_root,2)
    inventory={"members":[{"session_date":"2026-09-02","symbol":"OTHER"}],"transfer_complete":False,"measurement_references":[],"base_root":str(tmp_path/"base"),"feature_root":str(tmp_path/"features"),"ledger_path":str(tmp_path/"ledger.sqlite")}
    admissions={"findings":[{"member":"2026-09-02/OTHER","state":"metadata_admitted","source_pair_path":str(pair),"member_context_path":str(context)}]}
    for name,value in (("inventory.json",inventory),("admissions.json",admissions),("config.json",DEFAULT_CONFIG.to_dict()),("limits.json",{"workers":1,"disk_reserve_bytes":0,"scratch_cap_bytes":0})):(tmp_path/name).write_text(json.dumps(value))
    plan=create_plan(tmp_path/"inventory.json",tmp_path/"admissions.json",tmp_path/"config.json",tmp_path/"limits.json",tmp_path/"plan")
    with pytest.raises(ValueError,match="admitted_descriptor_identity_mismatch"):run_plan(plan["plan"],plan["sha256"])
    assert not (tmp_path/"ledger.sqlite").exists()


def test_transfer_and_measurement_bodies_require_exact_identities(tmp_path):
    descriptor_root=tmp_path/"descriptors";descriptor_root.mkdir();pair,context=fixture(descriptor_root,2);p=json.loads(pair.read_text())
    records=[]
    for stream in ("quotes","trades"):
        x=p["streams"][stream];records.append({"version":"raw_migration_object_v1","kind":"canonical_tq","session_date":"2026-09-02","symbol":"SYN","stream":stream,"key":x["path"],"relative_path":x["path"],"sha256":x["sha256"],"size_bytes":x["bytes"],"rows":x["rows"],"verify_mode":"tq_parquet_sip_order","reuse_path":None})
    duplicate=tmp_path/"duplicate.jsonl";duplicate.write_text("".join(json.dumps(x)+"\n" for x in records+[records[0]]))
    from tape_data_product.calculate import _transfer_summary, _validate_measurement
    with pytest.raises(ValueError,match="duplicate transfer"):_transfer_summary(duplicate)
    release={"source_revision":"a"*40,"wheel_sha256":"b"*64};guards={"workers":1,"threads":1,"batch_size":4096,"cpu_quota_percent":200,"tasks_max":64,"memory_max_bytes":1610612736,"memory_swap_max_bytes":0,"process_tree_rss_stop_bytes":1073741824,"runtime_max_seconds":600,"read_limit_bytes":1073741824,"output_scratch_limit_bytes":2147483648,"max_decoded_raw_rows":2000000};phase={"decoded_raw_rows":1,"read_bytes":1,"peak_rss_bytes":1,"wall_seconds":.1,"disk_bytes":{"output":1,"scratch_peak":1},"guards":guards};artifacts=[{"member":member,"source_pair_sha256":"1"*64,"context_sha256":"2"*64,"base_manifest_sha256":"3"*64,"feature_manifest_sha256":"4"*64,"rows":720} for member in ("2026-09-02/KDP","2026-09-02/NVDA")]
    core={"version":"tape_representative_measurement_v1","status":"accepted","kind":"representative_measurement","source_revision":release["source_revision"],"wheel_sha256":release["wheel_sha256"],"config_sha256":digest(DEFAULT_CONFIG.to_dict()),"sample":{"members":["2026-09-02/KDP","2026-09-02/NVDA"],"coverage_seconds_per_member":720},"rows":{"base":1440,"features":1440,"support":1440},"artifacts":artifacts,"phases":{"build":phase,"verification":phase},"independent_reconstruction":{"base_all_fields":"passed","features_explicit_histories":"passed","support_all_fields":"passed"}};body={**core,"measurement_id":digest(core),"readiness_decision_sha256":"e"*64}
    _validate_measurement(body,{"config":DEFAULT_CONFIG.to_dict()},release)
    body["sample"]={"members":["2026-01-01/ZZZ"],"coverage_seconds_per_member":999}
    body["measurement_id"]=digest({k:v for k,v in body.items() if k not in {"measurement_id","readiness_decision_sha256"}})
    with pytest.raises(ValueError,match="measurement sample"):_validate_measurement(body,{"config":DEFAULT_CONFIG.to_dict()},release)
    body["sample"]={"members":["2026-09-02/KDP","2026-09-02/NVDA"],"coverage_seconds_per_member":720}
    body["phases"]["build"]["peak_rss_bytes"]=2_000_000_000;body["phases"]["build"]["wall_seconds"]=700
    body["measurement_id"]=digest({k:v for k,v in body.items() if k not in {"measurement_id","readiness_decision_sha256"}})
    with pytest.raises(ValueError,match="resources/guards"):_validate_measurement(body,{"config":DEFAULT_CONFIG.to_dict()},release)
    body["phases"]["build"]["peak_rss_bytes"]=1;body["phases"]["build"]["wall_seconds"]=.1
    body["measurement_id"]=digest({k:v for k,v in body.items() if k not in {"measurement_id","readiness_decision_sha256"}})
    body["wheel_sha256"]="c"*64
    with pytest.raises(ValueError,match="measurement identity"):_validate_measurement(body,{"config":DEFAULT_CONFIG.to_dict()},release)


def test_raw_migration_completion_reconciles_verified_and_reused_manifest_objects(tmp_path):
    pair,_=fixture(tmp_path,2);p=json.loads(pair.read_text());records=[]
    for stream in ("quotes","trades"):
        x=p["streams"][stream]
        records.append({"version":"raw_migration_object_v1","kind":"canonical_tq","session_date":"2026-09-02","symbol":"SYN","stream":stream,"key":x["path"],"relative_path":x["path"],"sha256":x["sha256"],"size_bytes":x["bytes"],"rows":x["rows"],"verify_mode":"tq_parquet_sip_order","reuse_path":None})
    records[1]["reuse_path"]="/srv/retained/trades.parquet"
    manifest=tmp_path/"transfer.jsonl";manifest.write_text("".join(json.dumps(x)+"\n" for x in records));manifest_sha=sha256_file(manifest)[0]
    from tape_data_product.calculate import _transfer_summary,_validate_transfer_completion
    _,objects,size,_,_,states=_transfer_summary(manifest)
    body={"version":"raw_migration_completion_v1","status":"complete","manifest_sha256":manifest_sha,
          "expected_objects":objects,"expected_bytes":size,"states":states,
          "reserved_download_bytes":states["verified"]["bytes"],
          "delivered_payload_bytes":states["verified"]["bytes"],"attempts":states["verified"]["objects"],
          "elapsed_seconds":1.25,"scope":"transport identity only; production source admission is separate"}
    assert _validate_transfer_completion(body,manifest_sha,objects,size,states)==body


@pytest.mark.parametrize("mutation",["manifest","totals","states","incomplete"])
def test_raw_migration_completion_rejects_mismatch_or_incomplete_state(tmp_path,mutation):
    pair,_=fixture(tmp_path,2);p=json.loads(pair.read_text());records=[]
    for stream in ("quotes","trades"):
        x=p["streams"][stream]
        records.append({"version":"raw_migration_object_v1","kind":"canonical_tq","session_date":"2026-09-02","symbol":"SYN","stream":stream,"key":x["path"],"relative_path":x["path"],"sha256":x["sha256"],"size_bytes":x["bytes"],"rows":x["rows"],"verify_mode":"tq_parquet_sip_order","reuse_path":None})
    manifest=tmp_path/"transfer.jsonl";manifest.write_text("".join(json.dumps(x)+"\n" for x in records));manifest_sha=sha256_file(manifest)[0]
    from tape_data_product.calculate import _transfer_summary,_validate_transfer_completion
    _,objects,size,_,_,states=_transfer_summary(manifest)
    body={"version":"raw_migration_completion_v1","status":"complete","manifest_sha256":manifest_sha,
          "expected_objects":objects,"expected_bytes":size,"states":states,
          "reserved_download_bytes":size,"delivered_payload_bytes":size,"attempts":objects,
          "elapsed_seconds":1.25,"scope":"transport identity only; production source admission is separate"}
    if mutation=="manifest":body["manifest_sha256"]="0"*64
    elif mutation=="totals":body["expected_bytes"]+=1
    elif mutation=="states":body["states"]={**states,"verified":{**states["verified"],"objects":1}}
    else:body["status"]="running"
    with pytest.raises(ValueError,match="transfer completion"):
        _validate_transfer_completion(body,manifest_sha,objects,size,states)

def test_ambiguous_whole_second_halts_require_canonical_union(tmp_path):
    pair,context=fixture(tmp_path,3);start,_=session_bounds("2026-09-02");c=json.loads(context.read_text())
    c["halts"]=[{"start_ns":start+100,"end_ns":start+200,"id":"h1"},{"start_ns":start+300,"end_ns":start+400,"id":"h2"}];context.write_text(json.dumps(c))
    with pytest.raises(ValueError,match="canonical union"):build_base_partition(pair,context,tmp_path/"base")

def test_feature_build_rechecks_frozen_base_companions(tmp_path,monkeypatch):
    pair,context=fixture(tmp_path,70);base=tmp_path/"base";build_base_partition(pair,context,base);original=endpoint_module._calculate
    def mutate(*args,**kwargs):
        result=original(*args,**kwargs)
        with (base/"context.json").open("ab") as handle:handle.write(b" ")
        return result
    monkeypatch.setattr(endpoint_module,"_calculate",mutate)

    with pytest.raises(ValueError,match="changed during feature calculation"):build_from_base(base,tmp_path/"features")

def _parallel_fixture(root, symbol, seconds, quote_events=2):
    root.mkdir()
    pair, context = fixture(root, seconds=seconds, symbol=symbol)
    raw = root / "raw"
    member_raw = raw / symbol
    member_raw.mkdir()
    start, _ = session_bounds("2026-09-02")
    if abs(quote_events) > 2:
        event_count = abs(quote_events)
        rows = [
            {
                "sip_timestamp": start + i * max(1, seconds * NS // event_count),
                "sequence_number": i + 1,
                "bid_price": 99.0 + i / 10000,
                "ask_price": 101.0 + i / 10000,
                "bid_size": 10.0 + i % 7,
                "ask_size": 20.0 + i % 11,
                "conditions": [],
                "indicators": [],
            }
            for i in range(event_count)
        ]
        if quote_events < 0:
            rows[1]["sip_timestamp"] = rows[0]["sip_timestamp"]
            rows[1]["sequence_number"] = rows[0]["sequence_number"]
        pq.write_table(pa.Table.from_pylist(rows, schema=QSCHEMA),
                       raw / "quotes.parquet", row_group_size=4096)
    pair_body = json.loads(pair.read_text())
    for stream in ("quotes", "trades"):
        source = raw / f"{stream}.parquet"
        target = member_raw / source.name
        source.rename(target)
        pair_body["streams"][stream].update(_records(target))
        pair_body["streams"][stream]["path"] = f"{symbol}/{target.name}"
    quote_units = root / "evidence" / "quote-units.json"
    quote_body = json.loads(quote_units.read_text())
    quote_body["object_sha256"] = pair_body["streams"]["quotes"]["sha256"]
    quote_units.write_text(json.dumps(quote_body))
    pair_body["source_units"]["quote_size_evidence_sha256"] = sha256_file(quote_units)[0]
    pair.write_text(json.dumps(pair_body))
    return pair, context


def _parallel_plan(tmp_path, specs, workers, label):
    sources = {}
    records = []
    members = []
    admissions_findings = []
    for symbol, seconds, quote_events in specs:
        pair, context = _parallel_fixture(
            tmp_path / f"source-{label}-{symbol}", symbol, seconds, quote_events)
        sources[symbol] = (pair, context)
        pair_body = json.loads(pair.read_text())
        member = {"session_date": "2026-09-02", "symbol": symbol}
        members.append(member)
        admissions_findings.append({
            "member": f"2026-09-02/{symbol}",
            "state": "metadata_admitted",
            "source_pair_path": str(pair),
            "member_context_path": str(context),
        })
        for stream in ("quotes", "trades"):
            declared = pair_body["streams"][stream]
            records.append({
                "version": "raw_migration_object_v1",
                "kind": "canonical_tq",
                "session_date": "2026-09-02",
                "symbol": symbol,
                "stream": stream,
                "key": declared["path"],
                "relative_path": declared["path"],
                "sha256": declared["sha256"],
                "size_bytes": declared["bytes"],
                "rows": declared["rows"],
                "verify_mode": "tq_parquet_sip_order",
                "reuse_path": None,
            })
    transfer_manifest = tmp_path / f"{label}-transfer.jsonl"
    transfer_manifest.write_text("".join(json.dumps(x) + "\n" for x in records))
    transfer_sha = sha256_file(transfer_manifest)[0]
    transfer_bytes = sum(x["size_bytes"] for x in records)
    completion = tmp_path / f"{label}-transfer-complete.json"
    completion.write_text(json.dumps({
        "version": "raw_migration_completion_v1",
        "status": "complete",
        "manifest_sha256": transfer_sha,
        "expected_objects": len(records),
        "expected_bytes": transfer_bytes,
        "states": {
            "verified": {"objects": len(records), "bytes": transfer_bytes},
            "reused": {"objects": 0, "bytes": 0},
        },
        "reserved_download_bytes": transfer_bytes,
        "delivered_payload_bytes": transfer_bytes,
        "attempts": len(records),
        "elapsed_seconds": 1.0,
        "scope": "transport identity only; production source admission is separate",
    }))
    wheel = tmp_path / "candidate.whl"
    if not wheel.exists():
        wheel.write_bytes(b"synthetic-wheel-identity")
    release = {
        "source_revision": "a" * 40,
        "wheel_path": str(wheel),
        "wheel_sha256": sha256_file(wheel)[0],
        "executable": sys.executable,
        "contract_identity": contract_identity(),
        "base_implementation_identity": base_implementation_identity()["sha256"],
        "feature_implementation_identity": feature_implementation_identity()["sha256"],
    }
    measurement_guards = {
        "workers": 1, "threads": 1, "batch_size": 4096,
        "cpu_quota_percent": 200, "tasks_max": 64,
        "memory_max_bytes": 1610612736, "memory_swap_max_bytes": 0,
        "process_tree_rss_stop_bytes": 1073741824,
        "runtime_max_seconds": 600, "read_limit_bytes": 1073741824,
        "output_scratch_limit_bytes": 2147483648,
        "max_decoded_raw_rows": 2000000,
    }
    phase = {
        "decoded_raw_rows": 100, "read_bytes": 1000,
        "peak_rss_bytes": 1000000, "wall_seconds": 1.0,
        "disk_bytes": {"output": 2000, "scratch_peak": 3000},
        "guards": measurement_guards,
    }
    artifacts = [{
        "member": member, "source_pair_sha256": "1" * 64,
        "context_sha256": "2" * 64, "base_manifest_sha256": "3" * 64,
        "feature_manifest_sha256": "4" * 64, "rows": 720,
    } for member in ("2026-09-02/KDP", "2026-09-02/NVDA")]
    measurement_core = {
        "version": "tape_representative_measurement_v1",
        "status": "accepted", "kind": "representative_measurement",
        "source_revision": release["source_revision"],
        "wheel_sha256": release["wheel_sha256"],
        "config_sha256": digest(DEFAULT_CONFIG.to_dict()),
        "sample": {
            "members": ["2026-09-02/KDP", "2026-09-02/NVDA"],
            "coverage_seconds_per_member": 720,
        },
        "rows": {"base": 1440, "features": 1440, "support": 1440},
        "artifacts": artifacts,
        "phases": {"build": phase, "verification": phase},
        "independent_reconstruction": {
            "base_all_fields": "passed",
            "features_explicit_histories": "passed",
            "support_all_fields": "passed",
        },
    }
    measurement_id = digest(measurement_core)
    decision_body = {
        "version": "tape_representativeness_decision_v1",
        "status": "reviewed_accepted",
        "population_sha256": digest(members),
        "expected_members": len(members),
        "source_revision": release["source_revision"],
        "wheel_sha256": release["wheel_sha256"],
        "config_sha256": digest(DEFAULT_CONFIG.to_dict()),
        "measurement_ids": [measurement_id],
    }
    decision = tmp_path / f"{label}-decision.json"
    decision.write_text(json.dumps(decision_body))
    decision_ref = {"path": str(decision), "sha256": sha256_file(decision)[0]}
    measurement_body = {
        **measurement_core,
        "measurement_id": measurement_id,
        "readiness_decision_sha256": decision_ref["sha256"],
    }
    measurement = tmp_path / f"{label}-measurement.json"
    measurement.write_text(json.dumps(measurement_body))
    transfer_kind_summary = {
        "canonical_tq": {"objects": len(records), "bytes": transfer_bytes},
        "discovery_reference": {"objects": 0, "bytes": 0},
        "halt_support": {"objects": 0, "bytes": 0},
    }
    inventory = {
        "members": members, "transfer_complete": True,
        "transfer_manifest_path": str(transfer_manifest),
        "transfer_manifest_sha256": transfer_sha,
        "transfer_expected_objects": len(records),
        "transfer_expected_bytes": transfer_bytes,
        "transfer_kind_summary": transfer_kind_summary,
        "transfer_completion": {
            "path": str(completion), "sha256": sha256_file(completion)[0]},
        "readiness_decision": decision_ref,
        "measurement_references": [{
            "path": str(measurement), "sha256": sha256_file(measurement)[0]}],
        "release": release,
        "base_root": str(tmp_path / f"{label}-base"),
        "feature_root": str(tmp_path / f"{label}-features"),
        "ledger_path": str(tmp_path / f"{label}-ledger.sqlite"),
    }
    limits = {
        "workers": workers, "batch_size": 7, "disk_reserve_bytes": 0,
        "scratch_cap_bytes": 1024 ** 3,
        "process_tree_rss_stop_bytes": 2 * 1024 ** 3,
        "runtime_max_seconds": 120,
    }
    paths = {}
    for name, value in (
        ("inventory", inventory),
        ("admissions", {"findings": admissions_findings}),
        ("config", DEFAULT_CONFIG.to_dict()),
        ("limits", limits),
    ):
        path = tmp_path / f"{label}-{name}.json"
        path.write_text(json.dumps(value))
        paths[name] = path
    plan = create_plan(
        paths["inventory"], paths["admissions"], paths["config"],
        paths["limits"], tmp_path / f"{label}-plan")
    return plan, inventory, sources


def _decoded_member(root, symbol, name):
    path = Path(root) / "session_date=2026-09-02" / f"symbol={symbol}" / name
    return pq.ParquetFile(path).read().to_pylist()



def test_parallel_runner_matches_sequential_and_processes_mixed_members_once(tmp_path):
    specs = [("S00", 70, 3), ("S01", 90, 40), ("S02", 310, 120),
             ("S03", 120, 12), ("S04", 180, 70)]
    sequential, sequential_inventory, _ = _parallel_plan(
        tmp_path, specs, 1, "sequential")
    parallel, parallel_inventory, _ = _parallel_plan(
        tmp_path, specs, 4, "parallel")
    sequential_result = run_plan(sequential["plan"], sequential["sha256"])
    parallel_result = run_plan(parallel["plan"], parallel["sha256"])
    assert parallel_result["workers"] == 4
    assert parallel_result["scheduling"] == "largest_first_admitted_stream_rows"
    ledger = sqlite3.connect(parallel_inventory["ledger_path"])
    rows = ledger.execute(
        "SELECT member,status,COUNT(*) FROM members GROUP BY member,status"
    ).fetchall()
    ledger.close()
    assert len(rows) == len(specs)
    assert all(status == "complete" and count == 1 for _, status, count in rows)
    for symbol, _, _ in specs:
        for name in ("base.parquet",):
            assert _decoded_member(sequential_inventory["base_root"], symbol, name) == _decoded_member(
                parallel_inventory["base_root"], symbol, name)
        for name in ("features.parquet", "support.parquet"):
            assert _decoded_member(sequential_inventory["feature_root"], symbol, name) == _decoded_member(
                parallel_inventory["feature_root"], symbol, name)
    assert sequential_result["members"] == parallel_result["members"] == len(specs)


def test_parallel_runner_reuses_completed_base_features_and_recovers_running_ledger(tmp_path):
    plan, inventory, sources = _parallel_plan(
        tmp_path, [("RST", 90, 30)], 2, "restart")
    base = Path(inventory["base_root"]) / "session_date=2026-09-02" / "symbol=RST"
    pair, context = sources["RST"]
    base_result = build_base_partition(pair, context, base, batch_size=7)
    base_sha = sha256_file(base_result.manifest_path)[0]
    connection = sqlite3.connect(inventory["ledger_path"])
    connection.execute(
        "CREATE TABLE members (member TEXT PRIMARY KEY,status TEXT NOT NULL,"
        "base_manifest TEXT,feature_manifest TEXT,error TEXT,updated_ns INTEGER NOT NULL)")
    connection.execute(
        "INSERT INTO members VALUES (?,?,?,?,?,?)",
        ("2026-09-02/RST", "running", None, None, None, 1))
    connection.commit()
    connection.close()
    first = run_plan(plan["plan"], plan["sha256"])
    assert first["members"] == 1
    assert sha256_file(base_result.manifest_path)[0] == base_sha
    feature = (Path(inventory["feature_root"]) / "session_date=2026-09-02"
               / "symbol=RST" / "manifest.json")
    feature_sha = sha256_file(feature)[0]
    second = run_plan(plan["plan"], plan["sha256"])
    assert second["members"] == 1
    assert sha256_file(base_result.manifest_path)[0] == base_sha
    assert sha256_file(feature)[0] == feature_sha


@pytest.mark.parametrize("workers", [0, -1, 1.5, True, 9])
def test_plan_rejects_invalid_worker_counts(tmp_path, workers):
    inventory = {
        "members": [], "base_root": str(tmp_path / "base"),
        "feature_root": str(tmp_path / "features"),
        "ledger_path": str(tmp_path / "ledger.sqlite"),
    }
    for name, value in (
        ("inventory.json", inventory), ("admissions.json", {"findings": []}),
        ("config.json", DEFAULT_CONFIG.to_dict()),
        ("limits.json", {"workers": workers}),
    ):
        (tmp_path / name).write_text(json.dumps(value))
    with pytest.raises(ValueError, match="integer from 1 through 8"):
        create_plan(tmp_path / "inventory.json", tmp_path / "admissions.json",
                    tmp_path / "config.json", tmp_path / "limits.json",
                    tmp_path / "plan")


def test_plan_rejects_duplicate_members(tmp_path):
    member = {"session_date": "2026-09-02", "symbol": "DUP"}
    inventory = {"members": [member, member]}
    for name, value in (
        ("inventory.json", inventory), ("admissions.json", {"findings": []}),
        ("config.json", DEFAULT_CONFIG.to_dict()),
        ("limits.json", {"workers": 2}),
    ):
        (tmp_path / name).write_text(json.dumps(value))
    with pytest.raises(ValueError, match="duplicate calculation member"):
        create_plan(tmp_path / "inventory.json", tmp_path / "admissions.json",
                    tmp_path / "config.json", tmp_path / "limits.json",
                    tmp_path / "plan")


def test_aggregate_rss_limit_stops_all_active_members(tmp_path):
    plan, inventory, _ = _parallel_plan(
        tmp_path, [("M00", 2000, 10), ("M01", 2000, 10)], 2, "rss-stop")
    body = json.loads(Path(plan["plan"]).read_text())
    body["limits"]["process_tree_rss_stop_bytes"] = 1
    write_atomic_json(plan["plan"], body)
    expected = sha256_file(plan["plan"])[0]
    with pytest.raises(ValueError, match="aggregate process-tree RSS"):
        run_plan(plan["plan"], expected)
    connection = sqlite3.connect(inventory["ledger_path"])
    states = dict(connection.execute("SELECT member,status FROM members").fetchall())
    connection.close()
    assert states and set(states.values()) == {"interrupted"}


def test_worker_failure_stops_dispatch_and_interrupts_active_members(tmp_path):
    specs = [("BAD", 500, -200), ("SLOW", 10000, 100), ("UNSENT", 10000, 90)]
    plan, inventory, _ = _parallel_plan(tmp_path, specs, 2, "worker-failure")
    with pytest.raises(ValueError, match="member failed: 2026-09-02/BAD"):
        run_plan(plan["plan"], plan["sha256"])
    connection = sqlite3.connect(inventory["ledger_path"])
    states = dict(connection.execute("SELECT member,status FROM members").fetchall())
    connection.close()
    assert states["2026-09-02/BAD"] == "failed"
    assert states["2026-09-02/SLOW"] == "interrupted"
    assert "2026-09-02/UNSENT" not in states
    for member, status in states.items():
        if status == "complete":
            symbol = member.split("/")[1]
            assert (Path(inventory["base_root"]) / "session_date=2026-09-02"
                    / f"symbol={symbol}" / "manifest.json").is_file()
            assert (Path(inventory["feature_root"]) / "session_date=2026-09-02"
                    / f"symbol={symbol}" / "manifest.json").is_file()


def _abrupt_worker(*args):
    os._exit(17)


def test_abrupt_worker_death_fails_member_without_orphans(tmp_path, monkeypatch):
    import tape_data_product.calculate_runtime as runtime

    plan, inventory, _ = _parallel_plan(tmp_path, [("DIE", 1000, 10)], 1, "abrupt")
    monkeypatch.setattr(runtime, "_worker_main", _abrupt_worker)
    with pytest.raises(ValueError, match="worker exited abruptly.*exit code 17"):
        run_plan(plan["plan"], plan["sha256"])
    connection = sqlite3.connect(inventory["ledger_path"])
    state = connection.execute("SELECT status FROM members WHERE member=?",
                               ("2026-09-02/DIE",)).fetchone()[0]
    connection.close()
    assert state == "failed"
    assert not [child for child in __import__("psutil").Process().children(recursive=True)
                if child.is_running() and "spawn_main" in " ".join(child.cmdline())]


def test_parent_interruption_terminates_workers_and_reconciles_on_restart(tmp_path):
    plan, inventory, _ = _parallel_plan(
        tmp_path, [("INT", 15000, 600)], 1, "interrupt")
    code = (
        "import sys; from tape_data_product.calculate import run_plan; "
        "run_plan(sys.argv[1], sys.argv[2])"
    )
    parent = subprocess.Popen(
        [sys.executable, "-c", code, plan["plan"], plan["sha256"]],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 10
    worker_pids = []
    while time.monotonic() < deadline:
        if Path(inventory["ledger_path"]).exists():
            connection = sqlite3.connect(inventory["ledger_path"])
            row = connection.execute("SELECT status FROM members").fetchone()
            connection.close()
            descendants = __import__("psutil").Process(parent.pid).children(recursive=True)
            worker_pids = [child.pid for child in descendants
                           if "spawn_main" in " ".join(child.cmdline())]
            if row == ("running",) and worker_pids:
                break
        time.sleep(0.05)
    assert worker_pids
    os.kill(parent.pid, signal.SIGTERM)
    assert parent.wait(timeout=10) != 0
    reap_deadline = time.monotonic() + 5
    while time.monotonic() < reap_deadline:
        alive = [pid for pid in worker_pids
                 if __import__("psutil").pid_exists(pid)
                 and __import__("psutil").Process(pid).status()
                 != __import__("psutil").STATUS_ZOMBIE]
        if not alive:
            break
    connection = sqlite3.connect(inventory["ledger_path"])
    assert connection.execute("SELECT status FROM members").fetchone() == ("running",)
    connection.close()
    # A new parent reconciles ledger state and validates/rebuilds outputs.
    result = run_plan(plan["plan"], plan["sha256"])
    assert result["status"] == "complete"
