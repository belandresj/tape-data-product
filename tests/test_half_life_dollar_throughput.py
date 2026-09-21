import importlib.util
from pathlib import Path
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT=Path(__file__).resolve().parents[1]
SCRIPT=ROOT/"scripts/research/analyze_half_life_dollar_throughput.py"
def module():
 s=importlib.util.spec_from_file_location("dollar",SCRIPT); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
def row(i=0,symbol="X",fast=25000.0,slow=25000.0):
 return {"session_date":"2026-06-01","symbol":symbol,"session":"rth","interval_end_ns":1_800_000_000_000_000_000+(i+1)*1_000_000_000,
 "midpoint_rms_5s_bps_hl30s":11.0,"quoted_spread_bps_hl30s":99.0,"midpoint_rms_5s_to_spread_hl30s":3.0,"movement_participation_hl30s":.4,"trade_rate_per_second_hl30s":10.0,"dollar_rate_usd_per_second_hl30s":fast,
 "midpoint_rms_5s_bps_hl120s":11.0,"quoted_spread_bps_hl120s":99.0,"midpoint_rms_5s_to_spread_hl120s":3.0,"movement_participation_hl120s":.4,"trade_rate_per_second_hl120s":10.0,"dollar_rate_usd_per_second_hl120s":slow,
 "quote_age_p90_seconds_window60s":2.0,"trade_age_p90_seconds_window60s":2.0}
def connection(m,tmp_path,items,floor=25000):
 p=tmp_path/"p.parquet"; pq.write_table(pa.Table.from_pylist(items),p); c=duckdb.connect(":memory:"); m.create_views(c,p,m.Decimal(str(floor))); return c

def test_null_zero_and_threshold_mapping(tmp_path):
 m=module(); items=[row(0,"NULL",None,30000),row(1,"ZERO",0,30000),row(2,"BELOW",24999,30000),row(3,"EXACT",25000,30000),row(4,"ABOVE",25001,20000)]
 c=connection(m,tmp_path,items)
 got={r[0]:r[1:] for r in c.execute("SELECT symbol,fast_dollar_valid,slow_dollar_valid,common_dollar_eligible,fast_constrained_match,slow_constrained_match FROM source ORDER BY symbol").fetchall()}; c.close()
 assert got["NULL"]==(False,True,False,False,False)
 assert got["ZERO"]==(True,True,True,False,True)
 assert got["BELOW"][-2:]==(False,True)
 assert got["EXACT"][-2:]==(True,True)
 assert got["ABOVE"][-2:]==(True,False)

def test_unavailable_breaks_but_29_below_floor_bridges_and_30_splits(tmp_path,monkeypatch):
 m=module(); monkeypatch.setattr(m.BASE,"MINIMUM_EPISODE_SECONDS",2); monkeypatch.setattr(m.BASE,"MINIMUM_OCCUPANCY",m.Decimal("0"))
 items=[]
 for symbol,gap in (("G29",29),("G30",30)):
  items += [row(i,symbol,30000,30000) for i in range(2)]
  items += [row(i+2,symbol,0,0) for i in range(gap)]
  items += [row(i+2+gap,symbol,30000,30000) for i in range(2)]
 items += [row(0,"NULLBREAK",30000,30000),row(1,"NULLBREAK",None,30000),row(2,"NULLBREAK",30000,30000)]
 c=connection(m,tmp_path,items); p=m.reduce_periods(c,[("fast_constrained","common_segment","fast_constrained_match")],"common_dollar_eligible",("2026-06-01",))["fast_constrained"].to_pylist(); c.close()
 assert len([r for r in p if r["symbol"]=="G29"])==1
 assert len([r for r in p if r["symbol"]=="G30"])==2
 assert len([r for r in p if r["symbol"]=="NULLBREAK"])==0

def test_exact_occupancy_boundaries_and_grid_session_member_separation(tmp_path,monkeypatch):
 m=module(); monkeypatch.setattr(m.BASE,"MINIMUM_EPISODE_SECONDS",10); monkeypatch.setattr(m.BASE,"MINIMUM_OCCUPANCY",m.Decimal("0.80"))
 items=[]
 for i in range(10): items.append(row(i,"OCC",30000 if i not in (2,7) else 0,30000))
 for i in range(6): items.append(row(i,"GRID",30000,30000))
 items[-1]["interval_end_ns"]+=1_000_000_000
 for i in range(6):
  x=row(i,"SESSION",30000,30000); x["session"]="premarket" if i<3 else "rth"; items.append(x)
 c=connection(m,tmp_path,items); p=m.reduce_periods(c,[("fast_constrained","common_segment","fast_constrained_match")],"common_dollar_eligible",("2026-06-01",))["fast_constrained"].to_pylist(); c.close()
 occ=next(r for r in p if r["symbol"]=="OCC"); assert occ["occupancy"]==pytest.approx(.8)
 assert not any(r["symbol"] in {"GRID","SESSION"} for r in p)

def test_optimized_constrained_reduction_matches_sql_oracle(tmp_path,monkeypatch):
 m=module(); monkeypatch.setattr(m.BASE,"MINIMUM_EPISODE_SECONDS",5); monkeypatch.setattr(m.BASE,"MINIMUM_OCCUPANCY",m.Decimal("0.4"))
 items=[row(i,"A",30000 if i%7 else 0,30000) for i in range(80)]; items[35]["dollar_rate_usd_per_second_hl30s"]=None
 c=connection(m,tmp_path,items); optimized=m.reduce_periods(c,[("fast_constrained","common_segment","fast_constrained_match")],"common_dollar_eligible",("2026-06-01",))["fast_constrained"]
 c.execute("CREATE TEMP VIEW evaluated AS SELECT *,common_dollar_eligible AS common_eligible FROM source")
 reducer=m.BASE._load_reducer(); oracle=c.execute(reducer._episode_sql(m.BASE.OFF_DELAY_SECONDS,m.BASE.MINIMUM_EPISODE_SECONDS,m.BASE.MINIMUM_OCCUPANCY,source_sql="SELECT session_date,symbol,session,interval_end_ns,common_dollar_eligible eligible,fast_constrained_match matching FROM evaluated"),["2026-06-01"]).to_arrow_table(); c.close()
 assert optimized.to_pylist()==oracle.to_pylist()

def test_scope_guard_and_deterministic_output(tmp_path):
 m=module(); a=m.arguments(["--start-date","2026-05-31","--end-date","2026-06-05","--output",str(tmp_path/"o")]); a.catalog=a.identity=a.base_root=a.feature_root="x"; a.baseline_study=str(tmp_path)
 with pytest.raises(ValueError,match="expanded"): m.validate(a)
 rows=[{"b":2,"a":1}]; one=tmp_path/"one"; two=tmp_path/"two"; one.mkdir(); two.mkdir(); m.write(one,"x",rows); m.write(two,"x",rows)
 assert (one/"x.json").read_bytes()==(two/"x.json").read_bytes()
