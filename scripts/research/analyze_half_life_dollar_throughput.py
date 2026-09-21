#!/usr/bin/env python3
"""Five-day EW dollar-throughput extension of the half-life comparison."""
from __future__ import annotations
import argparse, csv, hashlib, importlib.util, json, os, shutil, subprocess, time
from datetime import date
from decimal import Decimal
from pathlib import Path
import duckdb
import pyarrow as pa
from tape_data_product.query import open_tape_database
from tape_data_product.query.endpoint_catalog import read_endpoint_query_catalog

START, END = date(2026,6,1), date(2026,6,5)
DATES = tuple(date.fromordinal(START.toordinal()+i).isoformat() for i in range(5))
RESERVE = 20*1024**3
BASE_STEMS = ("availability","membership","membership_summary","stock_membership",
 "matching_endpoint_overlap","retained_period_overlap","threshold_disagreements",
 "filter_contribution","example_stretches","retained_periods","baseline_sanity")

def _baseline():
    p=Path(__file__).with_name("compare_half_life_selection.py")
    s=importlib.util.spec_from_file_location("half_life_baseline",p); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
BASE=_baseline()

def arguments(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    for flag,env in (("catalog","QUERY_CATALOG"),("identity","QUERY_CATALOG_IDENTITY"),("base-root","BASE_ROOT"),("feature-root","FEATURE_ROOT"),("baseline-study","BASELINE_STUDY")):
        p.add_argument("--"+flag,default=os.environ.get(env))
    p.add_argument("--start-date",required=True); p.add_argument("--end-date",required=True)
    p.add_argument("--output",required=True,type=Path); p.add_argument("--dollar-floor",default="25000")
    p.add_argument("--threads",type=int,default=4); p.add_argument("--memory-limit",default="4GiB")
    p.add_argument("--temp-directory",type=Path); p.add_argument("--max-temp-directory-size",default="8GiB")
    p.add_argument("--batch-size",type=int,default=4096); p.add_argument("--allow-expanded-scope",action="store_true")
    return p.parse_args(argv)

def validate(a):
    if any(not x for x in (a.catalog,a.identity,a.base_root,a.feature_root,a.baseline_study)): raise ValueError("missing product configuration")
    start,end=date.fromisoformat(a.start_date),date.fromisoformat(a.end_date)
    if start>end: raise ValueError("reversed dates")
    if (start<START or end>END) and not a.allow_expanded_scope: raise ValueError("expanded dates require --allow-expanded-scope")
    floor=Decimal(a.dollar_floor)
    if not floor.is_finite() or floor<0: raise ValueError("invalid dollar floor")
    if not 1<=a.threads<=8 or not 1<=a.batch_size<=25000: raise ValueError("invalid threads or batch size")
    if not Path(a.baseline_study).is_dir(): raise ValueError("missing baseline study")
    return start,end,floor

def sha(p):
    h=hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda:f.read(1<<20),b""): h.update(b)
    return h.hexdigest()

def rows(c,q,args=()): return c.execute(q,args).to_arrow_table().to_pylist()
def write(root,name,data):
    data=[{k:str(v) if isinstance(v,Decimal) else v for k,v in r.items()} for r in data]
    (root/f"{name}.json").write_text(json.dumps(data,indent=2,sort_keys=True,default=str)+"\n")
    cols=list(data[0]) if data else []
    with (root/f"{name}.csv").open("x",newline="") as f:
        w=csv.DictWriter(f,fieldnames=cols)
        if cols: w.writeheader(); w.writerows(data)

def create_views(c,projection,floor):
    p=str(projection).replace("'","''"); cc=[]
    for h,label in ((30,"fast"),(120,"slow")):
        for cond in BASE.CONDITIONS: cc.append(f"coalesce({BASE._predicate(cond,h)},false) AS {label}_pass_{cond.key}")
    c.execute(f"""CREATE TEMP TABLE source AS WITH x AS (
      SELECT *,{BASE._availability(30)} AS fast_available,{BASE._availability(120)} AS slow_available,
       dollar_rate_usd_per_second_hl30s IS NOT NULL AND isfinite(dollar_rate_usd_per_second_hl30s) AS fast_dollar_valid,
       dollar_rate_usd_per_second_hl120s IS NOT NULL AND isfinite(dollar_rate_usd_per_second_hl120s) AS slow_dollar_valid,{','.join(cc)}
      FROM read_parquet('{p}',hive_partitioning=false)), y AS (
      SELECT *,fast_available AND slow_available AS original_common_eligible,
       coalesce(fast_available AND ({BASE._pass_all('fast')}),false) AS fast_own_match,
       coalesce(slow_available AND ({BASE._pass_all('slow')}),false) AS slow_own_match,
       coalesce(fast_available AND slow_available AND ({BASE._pass_all('fast')}),false) AS fast_original_match,
       coalesce(fast_available AND slow_available AND ({BASE._pass_all('slow')}),false) AS slow_original_match FROM x)
      SELECT *,original_common_eligible AND fast_dollar_valid AND slow_dollar_valid AS common_dollar_eligible,
       fast_original_match AND fast_dollar_valid AND slow_dollar_valid AND dollar_rate_usd_per_second_hl30s>=? AS fast_constrained_match,
       slow_original_match AND fast_dollar_valid AND slow_dollar_valid AND dollar_rate_usd_per_second_hl120s>=? AS slow_constrained_match FROM y""",[float(floor)]*2)

def reduce_periods(c,variants,eligibility,dates):
    c.execute(f"CREATE TEMP VIEW evaluated AS SELECT *,{eligibility} AS common_eligible FROM source")
    old=BASE._variant_matches; BASE._variant_matches=lambda:variants
    try: return BASE._period_variants(c,None,Path("unused"),dates)
    finally: BASE._variant_matches=old; c.execute("DROP VIEW evaluated")

def profiles(c,periods):
    pr=[]
    for view in ("fast","slow"): pr += [{"view":view,**r} for r in periods[f"{view}_original"].to_pylist()]
    c.register("periods",pa.Table.from_pylist(pr)); out=[]
    inside="EXISTS(SELECT 1 FROM periods p WHERE p.view=z.view_name AND p.session_date=z.session_date AND p.symbol=z.symbol AND p.session=z.session AND z.interval_end_ns>p.episode_start_ns AND z.interval_end_ns<=p.episode_end_ns)"
    for view,h in (("fast",30),("slow",120)):
      f=f"dollar_rate_usd_per_second_hl{h}s"
      for pop,where in (("all_original_matches","TRUE"),("original_retained_periods",inside)):
       r=rows(c,f"""WITH z AS (SELECT '{view}' AS view_name,session_date,symbol,session,interval_end_ns,{f} v,{view}_original_match m,{view}_dollar_valid dollar_valid FROM source)
        SELECT count(*) FILTER(WHERE m)::BIGINT original_matching_seconds,count(*) FILTER(WHERE m AND dollar_valid)::BIGINT dollar_valid_matching_seconds,
        count(*) FILTER(WHERE m AND NOT dollar_valid)::BIGINT dollar_unavailable_matching_seconds,count(*) FILTER(WHERE m AND dollar_valid AND v=0)::BIGINT valid_zero_dollar_matching_seconds,
        min(v) FILTER(WHERE m AND dollar_valid) minimum,quantile_disc(v,.01) FILTER(WHERE m AND dollar_valid) p1,quantile_disc(v,.05) FILTER(WHERE m AND dollar_valid) p5,
        quantile_disc(v,.1) FILTER(WHERE m AND dollar_valid) p10,quantile_disc(v,.25) FILTER(WHERE m AND dollar_valid) p25,quantile_disc(v,.5) FILTER(WHERE m AND dollar_valid) median,
        count(*) FILTER(WHERE m AND dollar_valid AND v<10000)::DOUBLE/nullif(count(*) FILTER(WHERE m AND dollar_valid),0) pct_below_10000,
        count(*) FILTER(WHERE m AND dollar_valid AND v<25000)::DOUBLE/nullif(count(*) FILTER(WHERE m AND dollar_valid),0) pct_below_25000,
        count(*) FILTER(WHERE m AND dollar_valid AND v<50000)::DOUBLE/nullif(count(*) FILTER(WHERE m AND dollar_valid),0) pct_below_50000,
        count(*) FILTER(WHERE m AND dollar_valid AND v<100000)::DOUBLE/nullif(count(*) FILTER(WHERE m AND dollar_valid),0) pct_below_100000 FROM z WHERE {where}""")[0]
       out.append({"view":view,"population":pop,**r})
    return out

def gate(c,floor):
    out=[]; scopes=(("overall","NULL","NULL","NULL",""),("date","CAST(session_date AS VARCHAR)","NULL","NULL","GROUP BY session_date"),("session","NULL","NULL","session","GROUP BY session"),("member","CAST(session_date AS VARCHAR)","symbol","NULL","GROUP BY session_date,symbol"))
    for view,h in (("fast",30),("slow",120)):
      d=f"dollar_rate_usd_per_second_hl{h}s"
      for scope,day,symbol,session,group in scopes:
       result=rows(c,f"""SELECT '{scope}' AS scope,{day} AS session_date,{symbol} AS symbol,{session} AS session,
        count(*) FILTER(WHERE original_common_eligible)::BIGINT common_eligible_seconds,
        count(*) FILTER(WHERE original_common_eligible AND NOT(fast_dollar_valid AND slow_dollar_valid))::BIGINT dollar_unavailable_seconds,
        count(*) FILTER(WHERE {view}_original_match)::BIGINT original_matching_seconds,
        count(*) FILTER(WHERE {view}_original_match AND NOT(fast_dollar_valid AND slow_dollar_valid))::BIGINT original_matching_dollar_unavailable_seconds,
        count(*) FILTER(WHERE {view}_original_match AND fast_dollar_valid AND slow_dollar_valid AND {d}<?)::BIGINT matching_seconds_below_floor,
        count(*) FILTER(WHERE {view}_constrained_match)::BIGINT remaining_matching_seconds,
        count(*) FILTER(WHERE {view}_constrained_match)::DOUBLE/nullif(count(*) FILTER(WHERE {view}_original_match AND fast_dollar_valid AND slow_dollar_valid),0) pct_dollar_valid_original_matches_retained,
        count(*) FILTER(WHERE {view}_own_match)::BIGINT own_original_matching_seconds,count(*) FILTER(WHERE {view}_own_match AND {view}_dollar_valid)::BIGINT own_dollar_valid_matching_seconds,
        count(*) FILTER(WHERE {view}_own_match AND NOT {view}_dollar_valid)::BIGINT own_dollar_unavailable_matching_seconds FROM source {group} ORDER BY session_date,symbol,session""",[float(floor)])
       out += [{"view":view,**r} for r in result]
    return out

def membership(c,periods):
    out=[]
    for view in ("fast","slow"):
      for cohort in ("original","constrained"):
       rr=periods[f"{view}_{cohort}"].to_pylist(); ds=sorted(r["elapsed_seconds"] for r in rr); elapsed=sum(ds); matching=sum(r["matching_seconds"] for r in rr); mid=len(ds)//2
       ep=c.execute(f"SELECT count(*) FILTER(WHERE {view}_{cohort}_match),count(DISTINCT(session_date,symbol)) FILTER(WHERE {view}_{cohort}_match),count(DISTINCT symbol) FILTER(WHERE {view}_{cohort}_match) FROM source").fetchone()
       out.append({"view":view,"cohort":cohort,"matching_seconds":ep[0],"symbol_days_with_any_match":ep[1],"distinct_stocks_with_any_match":ep[2],"retained_periods":len(rr),"retained_symbol_days":len({(str(r['session_date']),r['symbol']) for r in rr}),"distinct_retained_stocks":len({r['symbol'] for r in rr}),"retained_period_seconds":elapsed,"matching_seconds_inside_retained_periods":matching,"mean_retained_period_duration":elapsed/len(rr) if rr else None,"median_retained_period_duration":None if not ds else ds[mid] if len(ds)%2 else (ds[mid-1]+ds[mid])/2,"weighted_retained_period_occupancy":matching/elapsed if elapsed else None,"premarket_periods":sum(r['session']=='premarket' for r in rr),"rth_periods":sum(r['session']=='rth' for r in rr),"after_hours_periods":sum(r['session']=='after_hours' for r in rr)})
    return out

def analyze(projection,root,floor,a,dates):
    scratch=(a.temp_directory.resolve() if a.temp_directory else root/"scratch")/"analysis"; scratch.mkdir(parents=True,exist_ok=True)
    c=duckdb.connect(":memory:",config={"threads":str(a.threads),"memory_limit":a.memory_limit,"temp_directory":str(scratch),"max_temp_directory_size":a.max_temp_directory_size}); phase={}
    try:
      t=time.perf_counter(); create_views(c,projection,floor); phase["condition_evaluation"]=time.perf_counter()-t
      t=time.perf_counter(); original=reduce_periods(c,[("fast_original","common_segment","fast_original_match"),("slow_original","common_segment","slow_original_match")],"original_common_eligible",dates); constrained=reduce_periods(c,[("fast_constrained","common_segment","fast_constrained_match"),("slow_constrained","common_segment","slow_constrained_match")],"common_dollar_eligible",dates); periods={**original,**constrained}; phase["period_reduction"]=time.perf_counter()-t
      t=time.perf_counter(); profile=profiles(c,original); phase["dollar_profile_aggregation"]=time.perf_counter()-t
      t=time.perf_counter(); gates=gate(c,floor); members=membership(c,periods); keys=sorted((str(x),y) for x,y in c.execute("SELECT DISTINCT session_date,symbol FROM source").fetchall()); overlap=BASE._period_overlap_records({"fast":constrained["fast_constrained"],"slow":constrained["slow_constrained"]},keys)
      match=BASE._endpoint_overlap_records.__name__
      matchrows=rows(c,"""WITH m AS(SELECT session_date,symbol,count(*)FILTER(WHERE fast_constrained_match)::BIGINT fast_seconds,count(*)FILTER(WHERE slow_constrained_match)::BIGINT slow_seconds,count(*)FILTER(WHERE fast_constrained_match AND slow_constrained_match)::BIGINT shared_seconds,count(*)FILTER(WHERE fast_constrained_match AND NOT slow_constrained_match)::BIGINT fast_only_seconds,count(*)FILTER(WHERE slow_constrained_match AND NOT fast_constrained_match)::BIGINT slow_only_seconds,count(*)FILTER(WHERE fast_constrained_match OR slow_constrained_match)::BIGINT union_seconds FROM source GROUP BY session_date,symbol),s AS(SELECT 'member' AS scope,CAST(session_date AS VARCHAR) AS session_date,symbol,*EXCLUDE(session_date,symbol) FROM m UNION ALL SELECT 'date',CAST(session_date AS VARCHAR),NULL,sum(fast_seconds),sum(slow_seconds),sum(shared_seconds),sum(fast_only_seconds),sum(slow_only_seconds),sum(union_seconds) FROM m GROUP BY session_date UNION ALL SELECT 'overall',NULL,NULL,sum(fast_seconds),sum(slow_seconds),sum(shared_seconds),sum(fast_only_seconds),sum(slow_only_seconds),sum(union_seconds) FROM m)SELECT *,shared_seconds::DOUBLE/nullif(union_seconds,0) AS shared_over_union,shared_seconds::DOUBLE/nullif(fast_seconds,0) AS shared_over_fast,shared_seconds::DOUBLE/nullif(slow_seconds,0) AS shared_over_slow FROM s ORDER BY scope,session_date,symbol"""); phase["aggregate_statistics"]=time.perf_counter()-t
      t=time.perf_counter()
      for n,d in (("dollar_profile",profile),("dollar_gate_accounting",gates),("constrained_matching_overlap",matchrows),("constrained_membership_summary",members),("constrained_retained_periods",BASE._period_records(periods)),("constrained_retained_overlap",overlap)): write(root,n,d)
      phase["output_writing"]=time.perf_counter()-t
      return {"rows":c.execute("SELECT count(*) FROM source").fetchone()[0],"members":len(keys),"phase":phase}
    finally: c.close()

def compare_baseline(reproduced,saved):
    out=[]; exact=True
    for stem in BASE_STEMS:
        left,right=reproduced/f"{stem}.json",saved/f"{stem}.json"
        same=json.loads(left.read_text())==json.loads(right.read_text()); exact &= same
        out.append({"path":f"{stem}.json","identical":same,"reproduced_sha256":sha(left),"saved_sha256":sha(right)})
    return {"exact":exact,"artifacts":out}

def run(a):
    start,end,floor=validate(a); output=a.output.resolve()
    if output.exists(): raise FileExistsError(output)
    output.parent.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(output.parent).free<RESERVE: raise ValueError("20 GiB disk reserve violated")
    attempt=output.parent/f".{output.name}.in-progress"
    if attempt.exists(): raise FileExistsError(attempt)
    attempt.mkdir(); checkpoints=attempt/"checkpoints"; checkpoints.mkdir(); begun=time.perf_counter()
    query=(Path(__file__).with_name("half_life_dollar_throughput")/"projection.sql").read_text().strip()
    t=time.perf_counter(); catalog,records,_,_,catalog_bytes=read_endpoint_query_catalog(a.catalog,expected_identity=a.identity); catalog_s=time.perf_counter()-t
    selected=[r for r in records if str(start)<=r["session_date"]<=str(end)]; dates=tuple(sorted({r["session_date"] for r in selected}))
    if not dates: raise ValueError("no selected members")
    validation_s=validation_bytes=projection_s=projection_bytes=0; identity=None
    try:
      for position,current in enumerate(dates,1):
        members=tuple(r["member"] for r in selected if r["session_date"]==current); tmp=checkpoints/f".{current}.attempt-{os.getpid()}"; tmp.mkdir()
        scratch=(a.temp_directory.resolve() if a.temp_directory else attempt/"scratch")/current
        print(f"dollar pilot: {position}/{len(dates)} {current} members={len(members)}",flush=True)
        db=open_tape_database(a.catalog,expected_identity=a.identity,data_roots={"base":a.base_root,"features":a.feature_root},start_date=current,end_date=current,members=members,memory_limit=a.memory_limit,threads=a.threads,temp_directory=scratch/"reader",max_temp_directory_size=a.max_temp_directory_size)
        validation_s+=db.validation_seconds; validation_bytes+=db.validation_bytes
        current_id={"query_catalog_identity":db.catalog_identity,"release_source_revision":db.release_source_revision,"release_wheel_sha256":db.release_wheel_sha256}
        if identity and identity!=current_id: raise ValueError("source identity changed")
        identity=current_id; t=time.perf_counter(); count,size=BASE._write_projection(db.sql(query),tmp/"projection.parquet",a.batch_size); db._check_inputs(); db.close(); elapsed=time.perf_counter()-t
        projection_s+=elapsed; projection_bytes+=size
        manifest={"schema":"half_life_dollar_projection_checkpoint_v1","date":current,"members":list(members),"rows":count,"bytes":size,"sha256":sha(tmp/"projection.parquet"),"query_sha256":hashlib.sha256(query.encode()).hexdigest(),"source":identity}
        (tmp/"checkpoint.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n"); os.rename(tmp,checkpoints/current)
        print(f"dollar pilot: {current} rows={count:,} projection={elapsed:.1f}s",flush=True)
      reproduction=attempt/"original_reproduction"; reproduction.mkdir(); chunks=[]
      for current in dates:
        chunk=reproduction/current; chunk.mkdir(); BASE._analyze_projection(checkpoints/current/"projection.parquet",chunk,(current,),memory_limit=a.memory_limit,threads=a.threads,temp_directory=(a.temp_directory.resolve() if a.temp_directory else attempt/"scratch")/current/"baseline",max_temp_directory_size=a.max_temp_directory_size,all_failure_combinations=True); chunks.append(chunk)
      BASE._merge_chunk_outputs(chunks,reproduction,dates); comparison=compare_baseline(reproduction,Path(a.baseline_study)); (attempt/"baseline_reproduction.json").write_text(json.dumps(comparison,indent=2,sort_keys=True)+"\n")
      if not comparison["exact"]: raise AssertionError("unconstrained pilot did not reproduce")
      result=analyze(checkpoints/"*"/"projection.parquet",attempt,floor,a,dates)
      (attempt/"query.sql").write_text(query+"\n"); config=json.loads((Path(__file__).with_name("half_life_dollar_throughput")/"pilot_config.json").read_text()); config["dollar_floor_usd_per_second"]=str(floor); (attempt/"config.json").write_text(json.dumps(config,indent=2,sort_keys=True)+"\n")
      prof=json.loads((attempt/"dollar_profile.json").read_text()); mem=json.loads((attempt/"constrained_membership_summary.json").read_text()); gates=json.loads((attempt/"dollar_gate_accounting.json").read_text())
      lines=["# EW dollar-throughput five-day pilot","",f"The unconstrained pilot reproduced exactly. The sole constrained cohort is ${floor:,.0f}/s.","","## Dollar profile",""]
      lines += [f"- {r['view']} / {r['population']}: p10 ${r['p10']:,.2f}/s; median ${r['median']:,.2f}/s; below $25k {r['pct_below_25000']:.2%}; unavailable {r['dollar_unavailable_matching_seconds']:,}/{r['original_matching_seconds']:,}." for r in prof]
      lines += ["","## Selection effect",""]+[f"- {r['view']} {r['cohort']}: {r['matching_seconds']:,} matching seconds, {r['retained_periods']} periods, {r['retained_symbol_days']} retained symbol-days, {r['retained_period_seconds']:,} period-seconds." for r in mem]
      lines += ["", "This is retrospective descriptive selection, not executable capacity, accessible liquidity, or a validated participation limit. No routing, depth, fill, cost, latency, adverse-excursion, or expectancy model is present.",""]; (attempt/"summary.md").write_text("\n".join(lines))
      revision=subprocess.run(["git","rev-parse","HEAD"],cwd=Path(__file__).parents[2],check=True,capture_output=True,text=True).stdout.strip()
      runtime={"catalog_and_file_verification":catalog_s+validation_s,"projection_query":projection_s,**result["phase"],"total":time.perf_counter()-begun}
      basehash=[{"path":p.name,"bytes":p.stat().st_size,"sha256":sha(p)} for p in sorted(Path(a.baseline_study).iterdir()) if p.is_file()]
      artifacts=[{"path":p.name,"bytes":p.stat().st_size,"sha256":sha(p)} for p in sorted(attempt.iterdir()) if p.is_file() and p.name!="run_metadata.json"]
      metadata={"schema":"half_life_dollar_throughput_study_v1","dates":{"start":str(start),"end":str(end),"inclusive":True},"dollar_floor_usd_per_second":str(floor),"runner_source_revision":revision,"source":{**identity,"catalog_identity":catalog["catalog_identity"],"catalog_bytes":catalog_bytes,"validation_bytes":validation_bytes,"projection_bytes":projection_bytes},"baseline_study":{"path":str(Path(a.baseline_study).resolve()),"exact_reproduction":True,"artifact_hashes":basehash},"settings":{"threads":a.threads,"memory_limit":a.memory_limit,"temp_directory":str(a.temp_directory.resolve() if a.temp_directory else attempt/"scratch"),"max_temp_directory_size":a.max_temp_directory_size,"free_disk_reserve_bytes":RESERVE},"represented_rows":result["rows"],"represented_members":result["members"],"runtime_seconds":runtime,"resource_measurement":{"peak_process_tree_rss_bytes":None,"peak_temporary_disk_bytes":None,"note":"filled from external bounded monitor"},"reproducible_commands":{"pilot":"python scripts/research/analyze_half_life_dollar_throughput.py --start-date 2026-06-01 --end-date 2026-06-05 --dollar-floor 25000 --output PRIVATE_OUTPUT --threads 4 --memory-limit 4GiB --temp-directory PRIVATE_SCRATCH --max-temp-directory-size 8GiB","full_proposed_not_executed":"python scripts/research/analyze_half_life_dollar_throughput.py --start-date FULL_START --end-date FULL_END --dollar-floor 25000 --output PRIVATE_OUTPUT --threads 4 --memory-limit 4GiB --temp-directory PRIVATE_SCRATCH --max-temp-directory-size 8GiB --allow-expanded-scope"},"artifacts":artifacts}
      (attempt/"run_metadata.json").write_text(json.dumps(metadata,indent=2,sort_keys=True)+"\n"); os.rename(attempt,output); return {**metadata,"output":str(output)}
    except Exception:
      print(f"dollar pilot: preserving work at {attempt}",file=os.sys.stderr); raise

def main(argv=None):
    try: result=run(arguments(argv))
    except Exception as e: print(f"dollar pilot failed: {e}",file=os.sys.stderr); return 2
    print(json.dumps(result,sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
