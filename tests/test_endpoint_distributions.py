"""Literal checkpoint-C reducer examples independent of production readers."""
import json
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tape_data_product.analysis.endpoint_distributions import FixedAxis,JointHistogram,exact_marginal,DistributionError
from tape_data_product.analysis.endpoint_rendering import render_marginal,render_joint

def batch(rows):
    return pa.RecordBatch.from_pylist(rows,pa.schema([("session_date",pa.string()),("symbol",pa.string()),("x",pa.float64()),("x_mask",pa.uint16()),("y",pa.float64()),("y_mask",pa.uint16())]))

def test_exact_ties_zeros_nulls_empty_and_equal_member_weighting(tmp_path):
    # A has [0,1,1,2], B has [2]; pooled F(1)=3/5 while equal-member F(1)=3/8.
    rs=[dict(session_date="2026-01-01",symbol="A",x=x,x_mask=0,y=0.,y_mask=0) for x in (0.,1.)]
    rs2=[dict(session_date="2026-01-01",symbol="A",x=x,x_mask=0,y=0.,y_mask=0) for x in (1.,2.)]
    rs2 += [dict(session_date="2026-01-02",symbol="B",x=2.,x_mask=0,y=0.,y_mask=0),dict(session_date="2026-01-03",symbol="C",x=None,x_mask=8,y=0.,y_mask=0)]
    s=exact_marginal([batch(rs),batch(rs2)],tmp_path/"m",field="x",unit="bps",mask_column="x_mask",expected_members=[("2026-01-04","D")],batch_size=1)
    assert (s["selected"],s["valid"],s["unavailable"],s["zeros"],s["zero_valid_members"])==(6,5,1,1,2)
    t=pq.read_table(tmp_path/"m/ecdf.parquet").to_pydict(); assert t["value"]==[0.,1.,2.]
    assert t["pooled_count"]==[1,2,2]; assert t["pooled_ecdf"][1]==pytest.approx(3/5); assert t["equal_member_ecdf"][1]==pytest.approx(3/8)
    empty=exact_marginal([batch([dict(session_date="d",symbol="Z",x=None,x_mask=32,y=0.,y_mask=0)])],tmp_path/"e",field="x",unit="bps",mask_column="x_mask")
    assert empty["reason_bit_counts"]=={"32":1}
    assert empty["valid"]==0 and pq.read_table(tmp_path/"e/ecdf.parquet").num_rows==0

def test_joint_pair_valid_edges_tails_and_weights(tmp_path):
    axis=FixedAxis("x","u",(1.,2.,4.)); yaxis=FixedAxis("y","v",(10.,20.,40.))
    a=[(0.,0.,0,0),(.5,5.,0,0),(1.,10.,0,0),(2.,20.,0,0),(4.,40.,0,0),(5.,50.,0,0),(9.,None,0,16)]
    b=[(5.,50.,0,0)]
    rs=[]
    for symbol,values in (("A",a),("B",b)):
        for x,y,xm,ym in values: rs.append(dict(session_date="d",symbol=symbol,x=x,x_mask=xm,y=y,y_mask=ym))
    # One member has selected data but no pair-valid row.
    rs.append(dict(session_date="d",symbol="C",x=None,x_mask=8,y=1.,y_mask=0))
    j=JointHistogram(tmp_path/"j.sqlite",axis,yaxis,expected_members=[("d","A"),("d","B"),("d","C"),("d","D")]); j.add_arrow(batch(rs),x_column="x",x_mask_column="x_mask",y_column="y",y_mask_column="y_mask"); s=j.finish(); j.close()
    assert (s["selected"],s["x_valid"],s["y_valid"],s["pair_valid"])==(9,8,8,7)
    assert s["zero_pair_valid_members"]==2 and s["selected_members"]==4
    assert s["x_reason_bit_counts"]=={"8":1} and s["y_reason_bit_counts"]=={"16":1} and s["zero_zero"]==1
    assert np.asarray(s["pooled_counts"]).sum()==7 and np.asarray(s["equal_member_probability"]).sum()==pytest.approx(1)
    # B's sole overflow cell gets half equal-member mass but only 1/7 pooled mass.
    assert s["equal_member_probability"][-1][-1]==pytest.approx(.5+1/12)
    assert s["pooled_probability"][-1][-1]==pytest.approx(2/7)
    assert axis.labels()==["0","0–<1","1–<2","2–4",">4"]

def test_rejects_mask_contradiction_and_bad_axis(tmp_path):
    with pytest.raises(DistributionError): FixedAxis("x","u",(0.,1.))
    with pytest.raises(DistributionError): exact_marginal([batch([dict(session_date="d",symbol="A",x=1.,x_mask=8,y=0.,y_mask=0)])],tmp_path/"bad",field="x",unit="u",mask_column="x_mask")

def test_synthetic_figures_are_labeled_and_not_clipped(tmp_path):
    rs=[dict(session_date="d",symbol="A",x=x,x_mask=0,y=y,y_mask=0) for x,y in [(0.,0.),(1.,10.),(5.,50.)]]
    m=exact_marginal([batch(rs)],tmp_path/"m",field="x",unit="bps",mask_column="x_mask")
    render_marginal(tmp_path/"m/ecdf.parquet",m,tmp_path/"m.png",title="Synthetic marginal",estimator="30-second half-life",population="RTH historical membership",scope="synthetic",lineage="literal-fixture-v1")
    j=JointHistogram(tmp_path/"j.sqlite",FixedAxis("x","bps",(1.,4.)),FixedAxis("y","trades/s",(10.,40.))); j.add_arrow(batch(rs),x_column="x",x_mask_column="x_mask",y_column="y",y_mask_column="y_mask"); s=j.finish(); j.close()
    render_joint(s,tmp_path/"j.png",title="Synthetic joint",estimator="matching 30-second half-life",population="RTH historical membership",scope="synthetic",lineage="literal-fixture-v1")
    from PIL import Image
    for name in ("m.png","j.png"):
        image=Image.open(tmp_path/name); assert image.width>=1000 and image.height>=800
