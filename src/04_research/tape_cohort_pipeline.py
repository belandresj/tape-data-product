"""Deterministic planning, checkpointing, date transactions, resume, and queries."""
from __future__ import annotations

from contextlib import contextmanager
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import tempfile
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

import compact_product as PRODUCT
import all_feature_month_core as CORE
import report_release_inventory as RELEASE
from tape_cohort_cache import FeatureCache
from tape_cohort_config import normalize_config, query_hash
from tape_cohort_outputs import DateSinks, OUTPUT_SCHEMA_HASHES, atomic_json, file_identity, iter_parts, verify_date, WINDOW_SCHEMA
from tape_cohort_reader import ValidationStats, iter_query_batches, projected_columns
from tape_cohort_state import CohortMachine

# Complete local import and dynamic-file closure; changes require a new plan.
QUERY_FILES = (
    '../01_data/r2_tq_storage.py',
    '../02_preprocessing/build_market_state.py',
    '../03_features/build_rolling_tape_state_v2.py',
    '../03_features/direct_frozen_product.py',
    '../03_features/economic_tape_state_v3.py',
    '../03_features/rolling_tape_state_v2_halt_clock.py',
    'all_feature_month_core.py',
    'all_feature_month_inventory.py',
    'all_feature_month_moments.py',
    'all_feature_month_runtime.py',
    'all_feature_month_schema.py',
    'all_feature_month_verify.py',
    'compact_preview_inventory.py',
    'compact_preview_reader.py',
    'compact_product.py',
    'compact_product_runtime.py',
    'compact_product_schema.py',
    'compact_product_storage.py',
    'report_release_inventory.py',
    'run_tape_cohort_query.py',
    'snapshot_feature_pipeline.py',
    'tape_cohort_cache.py',
    'tape_cohort_config.py',
    'tape_cohort_outputs.py',
    'tape_cohort_pipeline.py',
    'tape_cohort_reader.py',
    'tape_cohort_reliability.py',
    'tape_cohort_render.py',
    'tape_cohort_state.py',
    'tape_feature_store.py',
    'tape_snapshot_inventory.py',
    'verify_tape_cohort_query.py',
    '../../docs/rolling_tape/rolling_tape_state_v2_feature_spec.md',
    '../../docs/tape_characterization_v3/tape_characterization_v3_model.md',
    '../../docs/tape_characterization_v3/pilot_implementation_spec.md',
    '../../config/tape_snapshot_feature_semantics_v1.json',
)
PREFIX_ROWS = 12_288
MEMBER_SCHEMA = pa.schema([
    pa.field("partition_identity",pa.string(),False),pa.field("session_date",pa.string(),False),pa.field("symbol",pa.string(),False),
    pa.field("feature_sha",pa.string(),False),pa.field("manifest_sha",pa.string(),False),pa.field("validation_mode",pa.string(),False),
    pa.field("expected_rows",pa.int64(),False),pa.field("validated_rows",pa.int64(),False),pa.field("observed_available",pa.int64(),False),
    pa.field("observed_unavailable",pa.int64(),False),pa.field("observed_strict",pa.int64(),False),pa.field("decision_slot_available",pa.int64(),False),
    pa.field("window_count",pa.int64(),False),pa.field("strict_run_count",pa.int64(),False),pa.field("active_seconds",pa.int64(),False),
    pa.field("premarket_active_seconds",pa.int64(),False),pa.field("rth_active_seconds",pa.int64(),False),pa.field("after_hours_active_seconds",pa.int64(),False),
    pa.field("entry_candidates_started",pa.int64(),False),pa.field("entry_candidates_confirmed",pa.int64(),False),
    pa.field("entry_candidates_cancelled_economic",pa.int64(),False),pa.field("entry_candidates_cancelled_boundary",pa.int64(),False),
    pa.field("entry_boundary_cancellations_json",pa.string(),False),pa.field("completion_state",pa.string(),False)])


def _sha(path): return file_identity(path)["sha256"]
def _digest(value): return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":"),allow_nan=False).encode()).hexdigest()


def implementation_identity():
    root = Path(__file__).parent; hashes = {}
    for name in sorted(QUERY_FILES):
        path = root/name
        if not path.exists(): raise ValueError("missing transitive query dependency: " + name)
        hashes[name] = _sha(path)
    versions = {name:importlib.metadata.version(name) for name in ("numpy","pyarrow","duckdb","boto3","psutil")}
    return {"files":hashes,"runtime":{"python":os.sys.version.split()[0],**versions},"output_schema_hashes":OUTPUT_SCHEMA_HASHES,
            "identity":_digest({"files":hashes,"runtime":{"python":os.sys.version.split()[0],**versions},"output_schema_hashes":OUTPUT_SCHEMA_HASHES})}


def _controls(release):
    release = Path(release)
    return {name:_sha(release/name) for name in ("manifest.json","accepted.jsonl","reconciliation.jsonl","capture_exclusions.jsonl")}


def _pilot(members):
    counts = Counter(m["session_date"] for m in members); ordered = sorted(counts.items())
    values = sorted(counts.values()); median = (values[(len(values)-1)//2]+values[len(values)//2])/2
    selected = min(ordered,key=lambda x:(abs(x[1]-median),x[0]))
    return selected[0], {"rule":"member count closest to release median; earliest tie", "median_member_count":median,
                         "selected_member_count":selected[1], "date_counts":dict(ordered)}


def plan_query(release, calculations, config, date_from, date_to, cache_root, result_root, *, output_path=None):
    config = normalize_config(config); accepted = json.loads(Path(calculations).read_text()) if isinstance(calculations,(str,Path)) else calculations
    manifest, members = RELEASE.read(release, accepted_calculations=accepted, require_complete=True)
    if manifest["release_identity"] != "c9abea6e3faea62e2471e8b8b883494c612cc4d48b4e44e82d9a7b47dd9aae50":
        raise ValueError("unexpected authoritative release identity")
    start, end = date.fromisoformat(str(date_from)), date.fromisoformat(str(date_to))
    if start > end: raise ValueError("date_from exceeds date_to")
    routed = sorted([m for m in members if start <= date.fromisoformat(m["session_date"]) <= end], key=lambda m:(m["session_date"],m["symbol"],m["partition_identity"]))
    if not routed: raise ValueError("requested range is entirely not_in_release")
    pilot_date, pilot_rule = _pilot(routed); pilot_members = [m for m in routed if m["session_date"]==pilot_date]
    representatives = sorted({min(pilot_members,key=lambda m:(m["objects"]["features"]["size_bytes"],m["symbol"],m["partition_identity"]))["partition_identity"],
                              max(pilot_members,key=lambda m:(m["objects"]["features"]["size_bytes"],m["symbol"],m["partition_identity"]))["partition_identity"]})
    ocg = next((m for m in members if m["session_date"]=="2026-03-12" and m["symbol"]=="OCG"),None)
    if ocg and ocg["partition_identity"] not in representatives: representatives.append(ocg["partition_identity"])
    impl = implementation_identity(); qhash = query_hash(config); controls = _controls(release)
    membership = [{"partition_identity":m["partition_identity"],"session_date":m["session_date"],"symbol":m["symbol"],
                   "feature_sha":m["objects"]["features"]["sha256"],"feature_bytes":m["objects"]["features"]["size_bytes"],
                   "manifest_sha":m["objects"]["manifest"]["sha256"],"manifest_bytes":m["objects"]["manifest"]["size_bytes"]} for m in routed]
    run_binding = {"query_hash":qhash,"release_identity":manifest["release_identity"],"controls":controls,"membership":membership,
                   "mode":"range","output_schemas":OUTPUT_SCHEMA_HASHES,"implementation_identity":impl["identity"],"prefix_rows":None}
    plan = {"schema":"tape_cohort_run_plan_v1","state":"planned","query_hash":qhash,"query":config,
            "source_release":str(Path(release).resolve()),"accepted_calculations":str(Path(calculations).resolve()) if isinstance(calculations,(str,Path)) else accepted,
            "release_identity":manifest["release_identity"],"control_hashes":controls,"date_from":str(start),"date_to":str(end),
            "routed_dates":sorted({m["session_date"] for m in routed}),"routed_members":membership,"pilot_date":pilot_date,"pilot_selection":pilot_rule,
            "checkpoint":{"prefix_rows":PREFIX_ROWS,"representative_partition_identities":representatives,"required":True,"evidence":None},
            "settings":{"workers":1,"batch_size":4096,"output_buffer_rows":1024,"cache_limit_bytes":1024**3,"scratch_limit_bytes":512*1024**2,
                        "result_limit_bytes":1024**3,"duckdb_memory_bytes":256*1024**2,"rss_stop_bytes":1024**3,"minimum_available_bytes":768*1024**2,"minimum_free_disk_bytes":3*1024**3},
            "cache_root":str(Path(cache_root).resolve()),"result_root":str(Path(result_root).resolve()),"implementation":impl,
            "query_run_hash":_digest(run_binding),"approval":{"confirmed":False,"scope":None,"recorded_at":None},
            "source_totals":{"members":len(routed),"dates":len({m["session_date"] for m in routed}),"rows":sum(m["expected_rows"] for m in routed),
                             "feature_bytes":sum(m["objects"]["features"]["size_bytes"] for m in routed),"manifest_bytes":sum(m["objects"]["manifest"]["size_bytes"] for m in routed)},
            "disk_at_plan":{"cache_free_bytes":shutil.disk_usage(Path(cache_root).resolve().parent).free,
                            "result_free_bytes":shutil.disk_usage(Path(result_root).resolve().parent).free}}
    if output_path:
        Path(output_path).parent.mkdir(parents=True,exist_ok=True); atomic_json(output_path,plan)
    return plan


def _load_plan(path):
    plan=json.loads(Path(path).read_text())
    if plan.get("schema")!="tape_cohort_run_plan_v1" or query_hash(plan["query"])!=plan["query_hash"]: raise ValueError("invalid plan/config identity")
    impl=implementation_identity()
    if impl["identity"]!=plan["implementation"]["identity"]: raise ValueError("query implementation changed; re-plan")
    if _controls(plan["source_release"])!=plan["control_hashes"]: raise ValueError("source controls changed")
    return plan


def _member_map(plan, all_members):
    selected={m["partition_identity"]:m for m in all_members}; result=[]
    for frozen in plan["routed_members"]:
        member=selected.get(frozen["partition_identity"])
        if member is None or member["objects"]["features"]["sha256"]!=frozen["feature_sha"]: raise ValueError("routed membership changed")
        result.append(member)
    return result


def _member_record(member, counters, rows):
    return {"partition_identity":member["partition_identity"],"session_date":member["session_date"],"symbol":member["symbol"],
        "feature_sha":member["objects"]["features"]["sha256"],"manifest_sha":member["objects"]["manifest"]["sha256"],
        "validation_mode":member["validation"]["mode"],"expected_rows":member["expected_rows"],"validated_rows":rows,**counters}


def _process_members(members, plan, directory, cache, accepted, *, max_rows=None, checkpoint=False, part_rows=25000):
    directory=Path(directory); directory.mkdir(parents=True,exist_ok=False); sinks=DateSinks(directory,part_rows=part_rows); member_rows=[]
    try:
        for member in members:
            atomic_json(directory/"processing.json", {"symbol":member["symbol"],"partition_identity":member["partition_identity"]})
            with cache.acquire(member,accepted) as cached:
                stats=ValidationStats(); machine=CohortMachine(plan["query"],member["partition_identity"],session_date=member["session_date"],symbol=member["symbol"],checkpoint=checkpoint,emit_trace=False)
                for batch in iter_query_batches(cached.path,member,cached.manifest["metadata"],plan["query"],plan["settings"]["batch_size"],max_rows=max_rows,stats=stats):
                    for _ in machine.consume(batch,sinks): pass
                counters=machine.finish("selection_boundary" if checkpoint else "session_close",sinks)
                if counters["observed_available"]+counters["observed_unavailable"]!=stats.rows: raise AssertionError("availability accounting")
                member_rows.append(_member_record(member,counters,stats.rows))
    except BaseException:
        for writer in (sinks.windows_writer,sinks.features_writer,sinks.strict_writer):
            if writer.writer is not None: writer.writer.close(); writer.writer=None
            writer.buffer.clear()
        raise
    outputs=sinks.close(); pq.write_table(pa.Table.from_pylist(member_rows,schema=MEMBER_SCHEMA),directory/"members.parquet",compression="zstd",row_group_size=1024)
    manifest={"schema":"tape_cohort_date_v1","state":"checkpoint" if checkpoint else "complete","query_hash":plan["query_hash"],
              "query_run_hash":plan["query_run_hash"],"validation_scope":"prefix" if checkpoint else "full",
              "prefix_offset":0,"prefix_count":max_rows,"members":member_rows,"outputs":outputs,
              "members_file":{**file_identity(directory/"members.parquet"),"rows":len(member_rows)},"output_schema_hashes":OUTPUT_SCHEMA_HASHES}
    manifest["verification"]=verify_date(directory,manifest,len(plan["query"]["conditions"])); atomic_json(directory/"manifest.json",manifest)
    daily={"session_date":member_rows[0]["session_date"] if member_rows and len({m["session_date"] for m in member_rows})==1 else None,
           "planned_members":len(member_rows),"completed_members":len(member_rows),
           "qualifying_stocks":len({m["symbol"] for m in member_rows if m["window_count"]>0}),
           "windows":outputs["window_count"],"strict_runs":sum(m["strict_run_count"] for m in member_rows),
           "total_active_seconds":outputs["active_seconds"],"mean_duration":outputs["active_seconds"]/outputs["window_count"] if outputs["window_count"] else None,
           "median_duration":outputs["median_duration"],"p90_duration":outputs["p90_duration"],
           "strict_passing_observed_endpoints":sum(m["observed_strict"] for m in member_rows),
           "observed_available":sum(m["observed_available"] for m in member_rows),
           "decision_slot_available":sum(m["decision_slot_available"] for m in member_rows),
           "active_strict_seconds":sum(m["active_strict_seconds"] for m in member_rows),
           "active_continuation_seconds":sum(m["active_continuation_seconds"] for m in member_rows),
           "active_pending_exit_seconds":sum(m["active_pending_exit_seconds"] for m in member_rows),
           "premarket_active_seconds":sum(m["premarket_active_seconds"] for m in member_rows),
           "rth_active_seconds":sum(m["rth_active_seconds"] for m in member_rows),
           "after_hours_active_seconds":sum(m["after_hours_active_seconds"] for m in member_rows)}
    atomic_json(directory/"daily.json",daily)
    return manifest


def run_checkpoint(plan_path, *, part_rows=25000):
    plan=_load_plan(plan_path); accepted=json.loads(Path(plan["accepted_calculations"]).read_text())
    release_manifest,all_members=RELEASE.read(plan["source_release"],accepted_calculations=accepted,require_complete=True)
    selected={m["partition_identity"]:m for m in all_members}; members=[selected[x] for x in plan["checkpoint"]["representative_partition_identities"]]
    out=Path(plan["result_root"])/(plan["query_run_hash"]+"-checkpoint"); out.mkdir(parents=True,exist_ok=False)
    atomic_json(out/"owner.json",{"schema":"tape_cohort_results_v1","query_run_hash":plan["query_run_hash"]})
    with FeatureCache(plan["cache_root"],limit_bytes=plan["settings"]["cache_limit_bytes"],run_id=plan["query_run_hash"]) as cache:
        cache.register_release(release_manifest,all_members)
        tick=time.perf_counter()
        cold=_process_members(members,plan,out/"cold",cache,accepted,max_rows=PREFIX_ROWS,checkpoint=True,part_rows=part_rows)
        cold_seconds=time.perf_counter()-tick
        before=dict(cache.metrics); warm=_process_members(members,plan,out/"warm",cache,accepted,max_rows=PREFIX_ROWS,checkpoint=True,part_rows=part_rows)
        warm_seconds=time.perf_counter()-tick-cold_seconds
        if cache.metrics["get_requests"]!=before["get_requests"]: raise AssertionError("warm checkpoint made remote GET")
        changed=json.loads(json.dumps(plan)); changed["query"]["conditions"][0]["entry"]["lower"]=11.0
        changed["query"]=normalize_config(changed["query"]);changed["query_hash"]=query_hash(changed["query"])
        before_changed=cache.metrics["get_requests"]
        tick=time.perf_counter()
        changed_result=_process_members(members,changed,out/"changed-query",cache,accepted,max_rows=PREFIX_ROWS,checkpoint=True,part_rows=part_rows)
        changed_seconds=time.perf_counter()-tick
        if cache.metrics["get_requests"]!=before_changed: raise AssertionError("changed query did not reuse cached bytes")
        metrics={**cache.metrics,"cache_status":cache.status(),"warm_get_requests":0}
    # Adversarial output uses the identical machine/writer/verifier path with
    # immediate confirmation solely to force >1024 windows and reduced part rolls.
    synthetic=json.loads(json.dumps(plan)); synthetic["query"]={"schema":"tape_cohort_config_v1","semantics_version":"tape_cohort_hysteresis_v1",
        "eligibility":"post_discovery_and_requested_zero_masks_v1","decision_session":"extended_0400_2000_ET","entry_confirm_seconds":0,"exit_confirm_seconds":0,
        "conditions":[{"feature":"trade_rate_60s","unit":"trades/second","entry":{"lower":2.0,"lower_inclusive":True,"upper":None,"upper_inclusive":True},
                       "continuation":{"lower":1.0,"lower_inclusive":True,"upper":None,"upper_inclusive":True}}]}
    synthetic["query"]=normalize_config(synthetic["query"]);synthetic["query_hash"]=query_hash(synthetic["query"])
    tick=time.perf_counter();sroot=out/"synthetic-adversarial";sroot.mkdir();sinks=DateSinks(sroot,buffer_rows=1024,part_rows=257)
    machine=CohortMachine(synthetic["query"],"f"*64,session_date="synthetic",symbol="SYN",checkpoint=True)
    for t in range(1,2051):
        machine.consume_row({"interval_end_ns":t*1_000_000_000,"continuity_segment_id":1,"halt_interval_active":False,
            "post_discovery_eligible":True,"trade_rate_60s":2.0 if t%2 else 0.0,"trade_rate_60s_reason_mask":0},sinks)
    counters=machine.finish("selection_boundary",sinks);soutputs=sinks.close()
    synthetic_manifest={"outputs":soutputs,"members":[{"observed_strict":counters["observed_strict"],"strict_run_count":counters["strict_run_count"]}]}
    synthetic_verification=verify_date(sroot,synthetic_manifest,1)
    synthetic_seconds=time.perf_counter()-tick
    # Simulate crash after immutable date rename but before catalog registration;
    # recovery trusts only a verified marker at the exact committed path.
    transaction=out/"transaction-recovery";transaction.mkdir();private=transaction/"date.partial";private.mkdir()
    atomic_json(private/"manifest.json",{"schema":"synthetic_date_commit_v1","state":"complete","identity":_digest({"date":"A"})})
    committed=transaction/"date";private.replace(committed);saved=json.loads((committed/"manifest.json").read_text())
    recovery={"crash_point":"after_date_rename_before_catalog","recovered":saved.get("state")=="complete","duplicate_windows":0}
    manifest={"schema":"tape_cohort_checkpoint_v1","state":"complete","query_hash":plan["query_hash"],"query_run_hash":plan["query_run_hash"],
              "prefix_rows_per_member":PREFIX_ROWS,"members":[{"date":m["session_date"],"symbol":m["symbol"],"partition_identity":m["partition_identity"],"feature_bytes":m["objects"]["features"]["size_bytes"]} for m in members],
              "cold":cold["verification"],"warm":warm["verification"],"changed_query":{"query_hash":changed["query_hash"],"verification":changed_result["verification"],"get_requests":0},
              "synthetic_adversarial":{"rows":2050,"verification":synthetic_verification,"window_parts":len(soutputs["parts"]["windows"]),"max_buffer_rows":1024,"test_roll_rows":257},
              "phase_measurements":{"cold":{"seconds":cold_seconds,"decoded_rows":len(members)*PREFIX_ROWS,"bytes_transferred":metrics["bytes_transferred"]},
                  "warm":{"seconds":warm_seconds,"decoded_rows":len(members)*PREFIX_ROWS,"bytes_transferred":0},
                  "changed_query":{"seconds":changed_seconds,"decoded_rows":len(members)*PREFIX_ROWS,"bytes_transferred":0},
                  "synthetic":{"seconds":synthetic_seconds,"decoded_rows":2050}},
              "interrupted_commit_recovery":recovery,"metrics":metrics}
    atomic_json(out/"manifest.json",manifest); return {**manifest,"path":str(out)}


def run_query(release, calculations, config, date_from, date_to, cache_root, result_root, mode, approved_plan, *, resume=False):
    plan=_load_plan(approved_plan)
    if mode not in ("pilot","range") or not plan["approval"].get("confirmed") or not plan["approval"].get("scope"):
        raise PermissionError("full-date execution requires explicit recorded user approval")
    accepted=json.loads(Path(calculations).read_text()) if isinstance(calculations,(str,Path)) else calculations
    release_manifest,all_members=RELEASE.read(release,accepted_calculations=accepted,require_complete=True); members=_member_map(plan,all_members)
    if mode=="pilot": members=[m for m in members if m["session_date"]==plan["pilot_date"]]
    root=Path(result_root)/plan["query_run_hash"]
    if root.exists() and not resume: raise FileExistsError(root)
    root.mkdir(parents=True,exist_ok=True); atomic_json(root/"owner.json",{"schema":"tape_cohort_results_v1","query_run_hash":plan["query_run_hash"]})
    atomic_json(root/"run_plan.json",plan);atomic_json(root/"query.json",plan["query"])
    atomic_json(root/"study_lock.json",{"schema":"tape_cohort_study_lock_v1","query_hash":plan["query_hash"],"release_identity":plan["release_identity"],"config_status":"candidate","pilot_date":plan["pilot_date"],"approval":plan["approval"]})
    controls=root/"source_release";controls.mkdir(exist_ok=True)
    for name in ("manifest.json","accepted.jsonl","reconciliation.jsonl","capture_exclusions.jsonl"):
        target=controls/name;source=Path(release)/name
        if not target.exists(): shutil.copy2(source,target)
        if _sha(target)!=plan["control_hashes"][name]: raise ValueError("copied source control identity mismatch")
    calculations_target=root/"accepted_calculations.json"
    if not calculations_target.exists(): shutil.copy2(calculations,calculations_target)
    by_date=defaultdict(list)
    for m in members: by_date[m["session_date"]].append(m)
    committed=[]; failed=[]; consecutive_failures=0
    from tape_cohort_reliability import journal, fatal
    atomic_json(root/"manifest.json", {"schema":"tape_cohort_run_v1","state":"running","query_run_hash":plan["query_run_hash"]})
    with FeatureCache(cache_root,limit_bytes=plan["settings"]["cache_limit_bytes"],run_id=plan["query_run_hash"]) as cache:
        cache.register_release(release_manifest,all_members)
        for day in sorted(by_date):
            final=root/"dates"/day
            if final.exists() and resume:
                saved=json.loads((final/"manifest.json").read_text())
                if saved.get("query_run_hash")!=plan["query_run_hash"] or saved.get("query_hash")!=plan["query_hash"]: raise ValueError("resume identity mismatch")
                verify_date(final,saved,len(plan["query"]["conditions"])); committed.append(saved); consecutive_failures=0; continue
            (root/"attempts").mkdir(exist_ok=True); private=Path(tempfile.mkdtemp(prefix=day+"-",dir=root/"attempts"))
            atomic_json(root/"progress.json",{"state":"running","current_date":day,"completed_dates":len(committed),"failed_dates":failed,"planned_dates":len(by_date)})
            try:
                result=_process_members(by_date[day],plan,private/"date",cache,accepted)
            except Exception as exc:
                failed.append(day); consecutive_failures+=1
                journal(root/"failures.jsonl",{"date":day,"error_type":type(exc).__name__,"attempt":str(private),"fatal":fatal(exc)})
                atomic_json(root/"progress.json",{"state":"incomplete","completed_dates":len(committed),"failed_dates":failed,"planned_dates":len(by_date)})
                if fatal(exc) or consecutive_failures>=3:
                    atomic_json(root/"manifest.json",{"schema":"tape_cohort_run_v1","state":"incomplete","query_run_hash":plan["query_run_hash"],"failed_dates":failed,"completed_dates":len(committed),"planned_dates":len(by_date),"stop_reason":"fatal_error" if fatal(exc) else "consecutive_failures"})
                    raise
                continue
            final.parent.mkdir(exist_ok=True); (private/"date").replace(final); committed.append(result); consecutive_failures=0
            journal(root/"progress.jsonl",{"date":day,"state":"complete"})
    if failed:
        incomplete={"schema":"tape_cohort_run_v1","state":"incomplete","query_run_hash":plan["query_run_hash"],"failed_dates":failed,"completed_dates":len(committed),"planned_dates":len(by_date)}
        atomic_json(root/"manifest.json",incomplete)
        return incomplete
    if len(committed)!=len(by_date): raise AssertionError("incomplete date set")
    summary={"dates":len(committed),"members":sum(len(x["members"]) for x in committed),"qualifying_stocks_sum":sum(len({m["symbol"] for m in x["members"] if m["window_count"]>0}) for x in committed),
             "windows":sum(x["outputs"]["window_count"] for x in committed),"active_seconds":sum(x["outputs"]["active_seconds"] for x in committed)}
    summary["mean_stocks_per_date"]=summary["qualifying_stocks_sum"]/summary["dates"] if summary["dates"] else None
    daily_rows=[];symbol_totals=defaultdict(lambda:[0,0,0,0]);month_totals=defaultdict(lambda:[0,0,0,0])
    for result in committed:
        daily=json.loads((root/"dates"/result["members"][0]["session_date"]/"daily.json").read_text());daily_rows.append(daily)
        for member in result["members"]:
            for key in (member["symbol"],member["session_date"][:7]):
                target=symbol_totals[key] if key==member["symbol"] else month_totals[key]
                target[0]+=member["active_seconds"];target[1]+=member["window_count"];target[2]+=int(member["window_count"]>0);target[3]+=1
    summary_root=root/"summary";summary_root.mkdir(exist_ok=True)
    daily_schema=pa.schema([pa.field("session_date",pa.string(),False),pa.field("qualifying_stocks",pa.int64(),False),pa.field("windows",pa.int64(),False),pa.field("total_active_seconds",pa.int64(),False)])
    pq.write_table(pa.Table.from_pylist([{k:r[k] for k in daily_schema.names} for r in daily_rows],schema=daily_schema),summary_root/"daily.parquet",compression="zstd")
    group_schema=pa.schema([pa.field("key",pa.string(),False),pa.field("active_seconds",pa.int64(),False),pa.field("windows",pa.int64(),False),pa.field("qualifying_stock_days",pa.int64(),False),pa.field("covered_stock_days",pa.int64(),False)])
    for name,values in (("symbols",symbol_totals),("months",month_totals)):
        rows=[dict(key=k,active_seconds=v[0],windows=v[1],qualifying_stock_days=v[2],covered_stock_days=v[3]) for k,v in sorted(values.items())]
        pq.write_table(pa.Table.from_pylist(rows,schema=group_schema),summary_root/(name+".parquet"),compression="zstd")
    atomic_json(summary_root/"summary.json",summary);atomic_json(root/"summary.json",summary)
    completion={"schema":"tape_cohort_run_v1","state":"complete","query_run_hash":plan["query_run_hash"],"query_hash":plan["query_hash"],"mode":mode,"summary":summary}
    atomic_json(root/"manifest.json",completion); atomic_json(root/"progress.json",completion); return completion


def iter_windows(run, batch_size=1024):
    root=Path(run)
    for day in sorted((root/"dates").iterdir()):
        manifest=json.loads((day/"manifest.json").read_text())
        yield from iter_parts(day/"windows",manifest["outputs"]["parts"]["windows"],WINDOW_SCHEMA,batch_size)


@contextmanager
def open_date(plan_path, requested_date):
    plan=_load_plan(plan_path)
    if not plan["approval"].get("confirmed"): raise PermissionError("complete-date staging requires approved scope")
    accepted=json.loads(Path(plan["accepted_calculations"]).read_text()); release_manifest,members=RELEASE.read(plan["source_release"],accepted_calculations=accepted,require_complete=True)
    selected=[m for m in members if m["session_date"]==requested_date]
    if not selected: raise ValueError("date not in release")
    with FeatureCache(plan["cache_root"],run_id=plan["query_run_hash"]) as cache:
        contexts=[]; paths=[]
        try:
            for member in selected:
                cm=cache.acquire(member,accepted); value=cm.__enter__(); contexts.append(cm); paths.append(str(value.path))
            con=cache.db
            con.execute("CREATE OR REPLACE TEMP VIEW tape_endpoints AS SELECT * FROM read_parquet(?, union_by_name=false)",[paths])
            yield con
        finally:
            try: cache.db.execute("DROP VIEW IF EXISTS tape_endpoints")
            except Exception: pass
            for cm in reversed(contexts): cm.__exit__(None,None,None)


def extract_trace(run, window_id):
    root=Path(run);completion=json.loads((root/"manifest.json").read_text())
    if completion.get("state")!="complete": raise ValueError("trace requires a committed run")
    selected=None
    for batch in iter_windows(root):
        for row in batch.to_pylist():
            if row["window_id"]==window_id: selected=row;break
        if selected:break
    if selected is None: raise KeyError("window_id is not in committed run")
    plan=json.loads((root/"run_plan.json").read_text());accepted=json.loads((root/"accepted_calculations.json").read_text())
    _,members=RELEASE.read(root/"source_release",accepted_calculations=accepted,require_complete=True)
    member=next(m for m in members if m["partition_identity"]==selected["partition_identity"])
    start=max(CORE.session_start(member["session_date"])+1_000_000_000,selected["candidate_start_endpoint_ns"]-300_000_000_000)
    end=min(CORE.session_start(member["session_date"])+57_600_000_000_000,selected["exit_effective_at_ns"]+60_000_000_000)
    trace_root=root/"traces"/window_id;trace_root.mkdir(parents=True,exist_ok=False);path=trace_root/"endpoints.parquet"
    conditions=[c["feature"] for c in plan["query"]["conditions"]]
    fields=[pa.field("session_date",pa.string(),False),pa.field("symbol",pa.string(),False),pa.field("interval_end_ns",pa.int64(),False)]
    fields += [pa.field(f,pa.float64(),True) for f in conditions]+[pa.field(f+"_reason_mask",pa.int64(),False) for f in conditions]
    fields += [pa.field("continuity_segment_id",pa.int64(),False),pa.field("halt_interval_active",pa.bool_(),False),pa.field("post_discovery_eligible",pa.bool_(),False),
               pa.field("primitive_quote_source_file_accepted",pa.bool_(),False),pa.field("primitive_trade_source_file_accepted",pa.bool_(),False),pa.field("midpoint",pa.float64(),True),
               pa.field("available",pa.bool_(),False),pa.field("strict",pa.bool_(),False),pa.field("continuation",pa.bool_(),False),pa.field("source_stratum",pa.string(),False),pa.field("decision_stratum",pa.string(),False),
               pa.field("state_before",pa.string(),False),pa.field("state_after",pa.string(),False),pa.field("entry_count",pa.int64(),False),pa.field("exit_count",pa.int64(),False),
               pa.field("candidate_start_endpoint_ns",pa.int64(),True),pa.field("pending_exit_trigger_endpoint_ns",pa.int64(),True),pa.field("window_id",pa.string(),True),
               pa.field("strict_failed_features",pa.int64(),False),pa.field("continuation_failed_features",pa.int64(),False),pa.field("unavailable_features",pa.int64(),False),
               pa.field("reason_union_mask",pa.int64(),False),pa.field("boundary_causes_json",pa.string(),False),pa.field("is_member",pa.bool_(),False)]
    schema=pa.schema(fields);writer=None;buffer=[];written=0;matched=False
    with FeatureCache(plan["cache_root"],run_id=plan["query_run_hash"]+"-trace") as cache:
        with cache.acquire(member,accepted) as cached:
            machine=CohortMachine(plan["query"],member["partition_identity"],session_date=member["session_date"],symbol=member["symbol"])
            for batch in iter_query_batches(cached.path,member,cached.manifest["metadata"],plan["query"],include_midpoint=True):
                for source_row in batch.to_pylist():
                    trace=machine.consume_row(source_row,None);t=source_row["interval_end_ns"]
                    if start<=t<=end:
                        buffer.append(trace);matched |= trace["window_id"]==window_id
                        if len(buffer)==1024:
                            if writer is None:writer=pq.ParquetWriter(path,schema,compression="zstd")
                            writer.write_table(pa.Table.from_pylist(buffer,schema=schema));written+=len(buffer);buffer.clear()
                    if t>=end:break
                if source_row["interval_end_ns"]>=end:break
    if buffer:
        if writer is None:writer=pq.ParquetWriter(path,schema,compression="zstd")
        writer.write_table(pa.Table.from_pylist(buffer,schema=schema));written+=len(buffer)
    if writer is not None:writer.close()
    if not matched: raise AssertionError("trace replay did not reproduce selected window")
    manifest={"schema":"tape_cohort_trace_v1","state":"complete","window_id":window_id,"rows":written,"trace_start_ns":start,"trace_end_ns":end,
              "source_replayed_from_session_start":True,"file":file_identity(path),"schema_hash":hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()}
    atomic_json(trace_root/"manifest.json",manifest);return manifest
