from __future__ import annotations
import hashlib,json
from dataclasses import replace
import math
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import tape_data_product.features.endpoint_ew as endpoint_module

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

def fixture(tmp_path,seconds=310):
    day="2026-09-02";start,_=session_bounds(day);raw=tmp_path/"raw";raw.mkdir();evidence=tmp_path/"evidence";evidence.mkdir()
    quotes=[{"sip_timestamp":start,"sequence_number":1,"bid_price":99.,"ask_price":101.,"bid_size":10.,"ask_size":20.,"conditions":[],"indicators":[]},{"sip_timestamp":start+NS//4,"sequence_number":2,"bid_price":100.,"ask_price":102.,"bid_size":30.,"ask_size":40.,"conditions":[],"indicators":[]}]
    trades=[{"sip_timestamp":start+100_000_000,"sequence_number":1,"participant_timestamp":start+100_000_000,"price":100.,"decimal_size":"0.1","size":.1,"conditions":[],"correction":0},{"sip_timestamp":start+200_000_000,"sequence_number":2,"participant_timestamp":start+200_000_000,"price":102.,"decimal_size":"0.2","size":.2,"conditions":[],"correction":0}]
    _write(raw/"quotes.parquet",QSCHEMA,quotes);_write(raw/"trades.parquet",TSCHEMA,trades)
    interval=[[start,start+seconds*NS]]
    streams={}
    for name,path in (("quotes",raw/"quotes.parquet"),("trades",raw/"trades.parquet")):
        provenance=evidence/f"{name}-provenance.json";provenance.write_text(json.dumps({"member":f"{day}/SYN","source":"synthetic"}))
        coverage=evidence/f"{name}-coverage.json";coverage.write_text(json.dumps({"version":"source_coverage_v1","member":f"{day}/SYN","stream":name,"intervals":interval,"terminal_complete":True}))
        streams[name]={**_records(path),"provenance_path":provenance.name,"provenance_sha256":sha256_file(provenance)[0],"coverage_evidence_path":coverage.name,"coverage_evidence_sha256":sha256_file(coverage)[0]}
    quote_units=evidence/"quote-units.json";quote_units.write_text(json.dumps({"version":"source_units_v1","member":f"{day}/SYN","stream":"quotes","unit":"shares","multiplier":1,"object_sha256":streams["quotes"]["sha256"]}))
    trade_units=evidence/"trade-units.json";trade_units.write_text(json.dumps({"version":"source_units_v1","member":f"{day}/SYN","stream":"trades","quantity_precedence":"decimal_size_then_size","scale":9,"object_sha256":streams["trades"]["sha256"]}))
    pair={"version":"tape_source_pair_v1","symbol":"SYN","session_date":day,"currency":"USD","adapter":"massive_canonical_tq_v1","root":str(raw),"evidence_root":str(evidence),"streams":streams,"source_units":{"quote_size_unit":"shares","quote_size_evidence_sha256":sha256_file(quote_units)[0],"quote_size_evidence_path":quote_units.name,"trade_quantity_evidence_sha256":sha256_file(trade_units)[0],"trade_quantity_evidence_path":trade_units.name,"round_lot_shares":None}}
    halt=evidence/"halts.json";halt.write_text(json.dumps({"member":f"{day}/SYN","status":"verified_empty","halts":[]}))
    continuity=evidence/"continuity.json";continuity.write_text(json.dumps({"version":"source_continuity_v1","member":f"{day}/SYN","gaps":{"quotes":[],"trades":[]},"instantaneous_breaks":{"quotes":[],"trades":[]}}))
    context={"version":"tape_member_context_v1","member":f"{day}/SYN","coverage":{"kind":"prefix","session_start_ns":start,"end_ns":start+seconds*NS,"expected_rows":seconds},"observation_intervals":{"quotes":interval,"trades":interval},"gaps":{"quotes":[],"trades":[]},"instantaneous_breaks":{"quotes":[],"trades":[]},"halts":[],"halt_evidence":{"status":"verified_empty","path":halt.name,"sha256":sha256_file(halt)[0]},"continuity_evidence":{"path":continuity.name,"sha256":sha256_file(continuity)[0]},"seed":{"basis":"verified_empty"},"selection":{"basis":"synthetic"},"discovery":{"eligibility_basis":"nominal"}}
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

def test_admission_emits_validated_member_descriptors(tmp_path):
    pair,context=fixture(tmp_path,6)
    inventory={"members":[{"session_date":"2026-09-02","symbol":"SYN"}]}
    evidence={"members":{"2026-09-02/SYN":{"quote_units":True,"trade_representation":True,"terminal_coverage":True,"halt_context":True,"continuity":True,"source_pair_path":str(pair),"member_context_path":str(context)}}}
    for name,value in (("inventory.json",inventory),("evidence.json",evidence)):(tmp_path/name).write_text(json.dumps(value))
    result=admit_inventory(tmp_path/"inventory.json",tmp_path/"evidence.json",tmp_path/"admitted")
    assert result["metadata_admitted"]==1 and result["blocked"]==0
    assert (tmp_path/"admitted/members/2026-09-02/SYN/source-pair.json").exists()

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
    completion=tmp_path/"transfer-complete.json";completion.write_text(json.dumps({"status":"complete","expected_members":1,"manifest_sha256":transfer_manifest_sha,"objects":2,"bytes":8}))
    member={"session_date":"2026-09-02","symbol":"SYN"}
    release={"source_revision":"a"*40,"wheel_path":str(wheel),"wheel_sha256":sha256_file(wheel)[0],"executable":sys.executable,"contract_identity":contract_identity(),"base_implementation_identity":base_implementation_identity()["sha256"],"feature_implementation_identity":feature_implementation_identity()["sha256"]}
    completion.write_text(json.dumps({"status":"complete","expected_members":1,"manifest_sha256":transfer_manifest_sha,"objects":2,"bytes":transfer_bytes}))
    measurement=tmp_path/"measurement.json";measurement.write_text(json.dumps({"version":"tape_representative_measurement_v1","status":"accepted","kind":"representative_measurement","source_revision":release["source_revision"],"wheel_sha256":release["wheel_sha256"],"config_sha256":digest(DEFAULT_CONFIG.to_dict()),"sample":{"members":["2026-09-02/SYN"],"coverage_seconds":70},"rows":{"base":70,"features":70,"support":70},"read_bytes":1000,"peak_rss_bytes":1000000,"wall_seconds":1.25,"disk_bytes":{"base":1000,"features":2000,"scratch_peak":3000}}))
    inventory={"members":[member],"transfer_complete":True,"transfer_manifest_path":str(transfer_manifest),"transfer_manifest_sha256":transfer_manifest_sha,"transfer_expected_objects":2,"transfer_expected_bytes":transfer_bytes,"transfer_completion":{"path":str(completion),"sha256":sha256_file(completion)[0]},"measurement_references":[{"path":str(measurement),"sha256":sha256_file(measurement)[0]}],"release":release,"base_root":str(tmp_path/"run-base"),"feature_root":str(tmp_path/"run-features"),"ledger_path":str(tmp_path/"run-ledger.sqlite")}
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
    release={"source_revision":"a"*40,"wheel_sha256":"b"*64}
    body={"version":"tape_representative_measurement_v1","status":"accepted","kind":"representative_measurement","source_revision":release["source_revision"],"wheel_sha256":release["wheel_sha256"],"config_sha256":digest(DEFAULT_CONFIG.to_dict()),"sample":{"members":["2026-09-02/SYN"],"coverage_seconds":2},"rows":{"base":2,"features":2,"support":2},"read_bytes":1,"peak_rss_bytes":1,"wall_seconds":.1,"disk_bytes":{"base":1,"features":1,"scratch_peak":1}}
    _validate_measurement(body,{"config":DEFAULT_CONFIG.to_dict()},release)
    body["wheel_sha256"]="c"*64
    with pytest.raises(ValueError,match="measurement identity"):_validate_measurement(body,{"config":DEFAULT_CONFIG.to_dict()},release)

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
