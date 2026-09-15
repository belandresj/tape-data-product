from __future__ import annotations

import json
import math

import numpy as np
import pyarrow.parquet as pq
import pytest

from endpoint_ew_reference import calculate_reference
from tape_data_product.contracts import DEFAULT_CONFIG
from tape_data_product.contracts.config import EWView, FeatureConfig
from tape_data_product.contracts.policy import NS, session_bounds
from tape_data_product.contracts.reasons import Reason
from tape_data_product.features.endpoint_ew import _calculate
from tape_data_product.features._endpoint_ew_kernel import (
    EndpointEWKernel,
    PARTICIPATION,
    RETURN_USABLE,
    RMS,
    run_ew_batch,
)
from tape_data_product.integrity import sha256_file
from tape_data_product.replay.builder import build_base_partition
from test_endpoint_pipeline import QSCHEMA, TSCHEMA, _records, _write, fixture


def _changing_base(tmp_path, seconds=330):
    pair_path, context_path = fixture(tmp_path, seconds)
    start, _ = session_bounds("2026-09-02")
    raw, evidence = tmp_path / "raw", tmp_path / "evidence"
    quotes=[]
    for i in range(seconds):
        midpoint=100+math.sin(i/11)/2+(i%19)*.002
        quotes.append({"sip_timestamp":start+i*NS+50_000_000,"sequence_number":i+1,
                       "bid_price":midpoint-.01,"ask_price":midpoint+.01,
                       "bid_size":10.+i%17,"ask_size":20.+(i*3)%23,
                       "conditions":[],"indicators":[]})
        if i and i%71==0:
            quotes.append({"sip_timestamp":start+i*NS+700_000_000,"sequence_number":seconds+i,
                           "bid_price":midpoint+1,"ask_price":midpoint,
                           "bid_size":1.,"ask_size":1.,"conditions":[],"indicators":[]})
    quotes.sort(key=lambda row:(row["sip_timestamp"],row["sequence_number"]))
    trades=[]
    for sequence,i in enumerate(range(0,seconds,2),1):
        timestamp=start+i*NS+300_000_000;size=1+i%31
        trades.append({"sip_timestamp":timestamp,"sequence_number":sequence,
                       "participant_timestamp":timestamp-(i%3)*10_000_000,
                       "price":100+math.sin(i/11)/2,"decimal_size":str(size),
                       "size":float(size),"conditions":[],"correction":0})
    _write(raw/"quotes.parquet",QSCHEMA,quotes);_write(raw/"trades.parquet",TSCHEMA,trades)
    pair=json.loads(pair_path.read_text())
    for stream in ("quotes","trades"):pair["streams"][stream].update(_records(raw/f"{stream}.parquet"))
    for stream,name,key in (("quotes","quote-units.json","quote_size_evidence_sha256"),
                            ("trades","trade-units.json","trade_quantity_evidence_sha256")):
        path=evidence/name;body=json.loads(path.read_text());body["object_sha256"]=pair["streams"][stream]["sha256"]
        path.write_text(json.dumps(body));pair["source_units"][key]=sha256_file(path)[0]
    pair_path.write_text(json.dumps(pair))
    context=json.loads(context_path.read_text())
    context["gaps"]={"quotes":[[start+123*NS+250_000_000,start+124*NS+250_000_000]],
                     "trades":[[start+177*NS+500_000_000,start+178*NS+200_000_000]]}
    continuity=evidence/"continuity.json"
    continuity.write_text(json.dumps({"version":"source_continuity_v1","member":"2026-09-02/SYN",
                                      "gaps":context["gaps"],"instantaneous_breaks":context["instantaneous_breaks"]}))
    context["continuity_evidence"]["sha256"]=sha256_file(continuity)[0]
    context["halts"]=[{"start_ns":start+240*NS+200_000_000,
                       "end_ns":start+241*NS+100_000_000,"id":"halt-1"}]
    halt=evidence/"halts.json"
    halt.write_text(json.dumps({"member":"2026-09-02/SYN","status":"accepted_intervals",
                                "halts":context["halts"]}))
    context["halt_evidence"].update(status="accepted_intervals",sha256=sha256_file(halt)[0])
    context_path.write_text(json.dumps(context))
    base=tmp_path/"base";build_base_partition(pair_path,context_path,base,batch_size=7)
    return base


def _run(calculator,base,root,config,batch_size):
    root.mkdir()
    result=calculator(base/"base.parquet",base/"context.json",root/"features.parquet",
                      root/"support.parquet",config,batch_size)
    return result,pq.read_table(root/"features.parquet").to_pylist(),pq.read_table(root/"support.parquet").to_pylist()


def _assert_reference(actual,reference):
    assert actual[0]==reference[0]
    for actual_rows,reference_rows in zip(actual[1:],reference[1:]):
        assert len(actual_rows)==len(reference_rows)
        for actual_row,reference_row in zip(actual_rows,reference_rows):
            assert actual_row.keys()==reference_row.keys()
            for name,expected in reference_row.items():
                value=actual_row[name]
                if isinstance(expected,float):assert value==pytest.approx(expected,rel=1e-10,abs=1e-12)
                else:assert value==expected


def test_columnar_calculator_matches_row_reference_across_batches_and_configs(tmp_path):
    base=_changing_base(tmp_path)
    reference=_run(calculate_reference,base,tmp_path/"reference-default",DEFAULT_CONFIG,7)
    optimized=[]
    for batch_size in (1,7,4096,25000):
        actual=_run(_calculate,base,tmp_path/f"optimized-{batch_size}",DEFAULT_CONFIG,batch_size)
        _assert_reference(actual,reference);optimized.append(actual)
    assert all(actual==optimized[0] for actual in optimized[1:])
    alternate=FeatureConfig(views=(EWView(1,6),EWView(45,90),EWView(300,320)),
                            age_windows_seconds=(1,17,300),spread_min_coverage=.9,
                            other_min_coverage=.8,age_min_coverage=.8,
                            max_trade_reporting_age_ns=1_000_000_000)
    alternate_reference=_run(calculate_reference,base,tmp_path/"reference-alternate",alternate,7)
    _assert_reference(_run(_calculate,base,tmp_path/"optimized-alternate",alternate,25000),alternate_reference)
    assert run_ew_batch.signatures, "production EW batch loop did not compile"


def _kernel_inputs(midpoints, valid):
    rows=len(midpoints);midpoints=np.asarray(midpoints,dtype=np.float64);valid=np.asarray(valid,dtype=np.bool_)
    return dict(
        halt=np.zeros(rows,dtype=np.int8),quote_status=np.ones(rows,dtype=np.int8),
        trade_status=np.ones(rows,dtype=np.int8),bid=midpoints-.01,bid_valid=valid,
        ask=midpoints+.01,ask_valid=valid,price_reason=np.zeros(rows,dtype=np.int64),
        continuity=np.zeros(rows,dtype=np.int64),spread_numerator=np.full(rows,2.),
        spread_exposure=np.ones(rows),activity_count=np.zeros(rows),activity_share=np.zeros(rows),
        activity_dollar=np.zeros(rows),activity_exposure=np.ones(rows),
        bid_size_numerator=np.full(rows,10.),bid_size_exposure=np.ones(rows),
        ask_size_numerator=np.full(rows,20.),ask_size_exposure=np.ones(rows),
    )


def test_compiled_kernel_preserves_zero_and_long_decay_state():
    zero=EndpointEWKernel((EWView(1,6),),.9,.8)
    values,masks,supports,*_=zero.process(**_kernel_inputs([100.]*6,[True]*6))
    assert values[-1,0,RMS]==0
    assert masks[-1,0,RMS]==0
    assert masks[-1,0,PARTICIPATION]==Reason.ZERO_RETURN_VARIATION
    assert supports[-1,0,0]==1

    rows=1110
    midpoint=[100.]*5+[101.]+[101.]*(rows-6)
    valid=[True]*6+[False]*(rows-6)
    decayed=EndpointEWKernel((EWView(1,6),),.9,.8)
    _,masks,supports,*_=decayed.process(**_kernel_inputs(midpoint,valid))
    assert decayed.mantissas[0,RETURN_USABLE]!=0
    assert supports[-1,0,0]==0
    assert masks[-1,0,RMS]&Reason.LOW_COVERAGE
    assert not masks[-1,0,RMS]&Reason.NO_SUPPORTED_DATA
    assert not masks[-1,0,PARTICIPATION]&Reason.ZERO_RETURN_VARIATION
