from __future__ import annotations
import hashlib,json
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
    return {"path":path.name,"sha256":sha256_file(path)[0],"bytes":path.stat().st_size,"rows":pq.ParquetFile(path).metadata.num_rows,"schema_sha256":digest({"schema":str(pq.ParquetFile(path).schema_arrow)}),"clock":"sip_timestamp_utc_ns","provenance_sha256":"1"*64,"coverage_evidence_sha256":"2"*64,"terminal_complete":True}

def fixture(tmp_path,seconds=310):
    day="2026-09-02";start,_=session_bounds(day);raw=tmp_path/"raw";raw.mkdir()
    quotes=[{"sip_timestamp":start,"sequence_number":1,"bid_price":99.,"ask_price":101.,"bid_size":10.,"ask_size":20.,"conditions":[],"indicators":[]},{"sip_timestamp":start+NS//4,"sequence_number":2,"bid_price":100.,"ask_price":102.,"bid_size":30.,"ask_size":40.,"conditions":[],"indicators":[]}]
    trades=[{"sip_timestamp":start+100_000_000,"sequence_number":1,"participant_timestamp":start+100_000_000,"price":100.,"decimal_size":"0.1","size":.1,"conditions":[],"correction":0},{"sip_timestamp":start+200_000_000,"sequence_number":2,"participant_timestamp":start+200_000_000,"price":102.,"decimal_size":"0.2","size":.2,"conditions":[],"correction":0}]
    _write(raw/"quotes.parquet",QSCHEMA,quotes);_write(raw/"trades.parquet",TSCHEMA,trades)
    pair={"version":"tape_source_pair_v1","symbol":"SYN","session_date":day,"currency":"USD","adapter":"massive_canonical_tq_v1","root":str(raw),"streams":{"quotes":_records(raw/"quotes.parquet"),"trades":_records(raw/"trades.parquet")},"source_units":{"quote_size_unit":"shares","quote_size_evidence_sha256":"3"*64,"trade_quantity_evidence_sha256":"4"*64,"round_lot_shares":None}}
    context={"version":"tape_member_context_v1","member":f"{day}/SYN","coverage":{"kind":"prefix","session_start_ns":start,"end_ns":start+seconds*NS,"expected_rows":seconds},"observation_intervals":{"quotes":[[start,start+seconds*NS]],"trades":[[start,start+seconds*NS]]},"gaps":{"quotes":[],"trades":[]},"instantaneous_breaks":{"quotes":[],"trades":[]},"halts":[],"halt_evidence":{"status":"verified_empty","sha256":"5"*64},"seed":{"basis":"verified_empty"},"selection":{"basis":"synthetic"},"discovery":{"eligibility_basis":"nominal"}}
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
