from __future__ import annotations
import hashlib,json
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tape_data_product.contracts import DEFAULT_CONFIG
from tape_data_product.contracts.config import digest
from tape_data_product.contracts.policy import NS,session_bounds
from tape_data_product.features.endpoint_ew import AgeWindow,ScaledSum,build_from_base
from tape_data_product.integrity import sha256_file
from tape_data_product.replay.builder import build_base_partition,verify_base_partition

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
    context={"version":"tape_member_context_v1","member":f"{day}/SYN","coverage":{"kind":"prefix","session_start_ns":start,"end_ns":start+seconds*NS,"expected_rows":seconds},"observation_intervals":{"quotes":interval,"trades":interval},"gaps":{"quotes":[],"trades":[]},"instantaneous_breaks":{"quotes":[],"trades":[]},"halts":[],"halt_evidence":{"status":"verified_empty","path":halt.name,"sha256":sha256_file(halt)[0]},"seed":{"basis":"verified_empty"},"selection":{"basis":"synthetic"},"discovery":{"eligibility_basis":"nominal"}}
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
    c=json.loads(context.read_text());c["gaps"]["quotes"]=[[start+NS//4,start+3*NS//4]];context.write_text(json.dumps(c))
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
