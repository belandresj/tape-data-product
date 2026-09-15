"""Immutable calculation plans and bounded fail-closed member execution."""
from __future__ import annotations
import os
import re
import json
import sqlite3
import sys
import time
import math
import fcntl
from pathlib import Path

from .contracts import DEFAULT_CONFIG, contract_identity
from .contracts.config import ContractError, FeatureConfig, digest
from .integrity import read_json, sha256_file, write_atomic_json

def create_plan(inventory_path,admissions_path,config_path,limits_path,output):
    inventory=read_json(inventory_path);admissions=read_json(admissions_path);limits=read_json(limits_path)
    config=FeatureConfig.from_dict(read_json(config_path));output=Path(output)
    workers=limits.get("workers")
    if type(workers) is not int or not 1<=workers<=8:raise ContractError("workers must be an integer from 1 through 8")
    members=inventory.get("members",[]);keys=[f"{member['session_date']}/{member['symbol']}" for member in members]
    if len(keys)!=len(set(keys)):raise ContractError("duplicate calculation member")
    if output.exists():raise FileExistsError(output)
    output.mkdir(parents=True)
    findings={x["member"]:x for x in admissions.get("findings",[])}
    unresolved=[f"{m['session_date']}/{m['symbol']}" for m in members if findings.get(f"{m['session_date']}/{m['symbol']}",{}).get("state")!="metadata_admitted"]
    index_path=output/"admitted-members.sqlite";connection=sqlite3.connect(index_path)
    connection.execute("CREATE TABLE members (member TEXT PRIMARY KEY,source_pair_path TEXT NOT NULL,source_pair_sha256 TEXT NOT NULL,context_path TEXT NOT NULL,context_sha256 TEXT NOT NULL)")
    for member in members:
        key=f"{member['session_date']}/{member['symbol']}";finding=findings.get(key,{})
        if finding.get("state")=="metadata_admitted":
            pair=Path(finding["source_pair_path"]).resolve();context=Path(finding["member_context_path"]).resolve()
            connection.execute("INSERT INTO members VALUES (?,?,?,?,?)",(key,str(pair),sha256_file(pair)[0],str(context),sha256_file(context)[0]))
    connection.commit();connection.close()
    plan={"version":"tape_calculation_plan_v1","inventory_path":str(Path(inventory_path).resolve()),"inventory_sha256":sha256_file(inventory_path)[0],
          "admissions_path":str(Path(admissions_path).resolve()),"admissions_sha256":sha256_file(admissions_path)[0],"config":config.to_dict(),"contract_identity":contract_identity(config),
          "limits":limits,"members":members,"expected_members":len(members),"unresolved_members":unresolved,"transfer_complete":bool(inventory.get("transfer_complete",False)),
          "admitted_index_path":str(index_path.resolve()),"admitted_index_sha256":sha256_file(index_path)[0],"admitted_members":len(members)-len(unresolved),
          "measurement_references":inventory.get("measurement_references",[]),"transfer_completion":inventory.get("transfer_completion"),
          "readiness_decision":inventory.get("readiness_decision"),
          "transfer_manifest_path":inventory.get("transfer_manifest_path"),"transfer_manifest_sha256":inventory.get("transfer_manifest_sha256"),
          "transfer_expected_objects":inventory.get("transfer_expected_objects"),"transfer_expected_bytes":inventory.get("transfer_expected_bytes"),
          "transfer_kind_summary":inventory.get("transfer_kind_summary"),
          "release":inventory.get("release"),"base_root":inventory.get("base_root"),"feature_root":inventory.get("feature_root"),"ledger_path":inventory.get("ledger_path")}
    write_atomic_json(output/"plan.json",plan);sha=sha256_file(output/"plan.json")[0]
    return {"plan":str((output/"plan.json").resolve()),"sha256":sha,"members":len(members),"unresolved":len(unresolved),"ready":not unresolved and plan["transfer_complete"] and bool(plan["transfer_completion"]) and bool(plan["measurement_references"]) and bool(plan["readiness_decision"])}


def _nearest_existing(path):
    path=Path(path)
    while not path.exists() and path!=path.parent:path=path.parent
    return path


TRANSFER_FIELDS={"version","kind","session_date","symbol","stream","key","relative_path",
                 "sha256","size_bytes","rows","verify_mode","reuse_path"}


def _transfer_summary(path):
    """Read the immutable transfer catalog and retain every object identity.

    Aggregate counts alone are not admission: a duplicate stream can otherwise
    replace the intended object while preserving member/object/byte totals.
    """
    members={};records={};keys=set();paths=set();objects=bytes_total=0
    kind_summary={kind:{"objects":0,"bytes":0} for kind in ("canonical_tq","discovery_reference","halt_support")}
    state_summary={state:{"objects":0,"bytes":0} for state in ("verified","reused")}
    with Path(path).open() as handle:
        for line in handle:
            if not line.strip():continue
            record=json.loads(line)
            kind=record.get("kind")
            if kind not in kind_summary:raise ContractError("unknown transfer object kind")
            day=record.get("session_date");symbol=record.get("symbol");stream=record.get("stream");member=f"{day}/{symbol}"
            if (set(record)!=TRANSFER_FIELDS or record.get("version")!="raw_migration_object_v1"
                    or type(record.get("key")) is not str or type(record.get("relative_path")) is not str
                    or Path(record["key"]).is_absolute() or Path(record["relative_path"]).is_absolute()
                    or ".." in Path(record["key"]).parts or ".." in Path(record["relative_path"]).parts
                    or type(record.get("sha256")) is not str or not re.fullmatch(r"[0-9a-f]{64}",record["sha256"])
                    or type(record.get("size_bytes")) is not int or record["size_bytes"]<0
                    or record.get("reuse_path") is not None and type(record["reuse_path"]) is not str):
                raise ContractError("malformed transfer inventory")
            if record["key"] in keys or record["relative_path"] in paths:
                raise ContractError("duplicate transfer object/stream")
            keys.add(record["key"]);paths.add(record["relative_path"]);objects+=1;bytes_total+=record["size_bytes"]
            kind_summary[kind]["objects"]+=1;kind_summary[kind]["bytes"]+=record["size_bytes"]
            state="reused" if record["reuse_path"] is not None else "verified"
            state_summary[state]["objects"]+=1;state_summary[state]["bytes"]+=record["size_bytes"]
            if kind=="canonical_tq":
                if (stream not in ("quotes","trades") or record.get("verify_mode")!="tq_parquet_sip_order"
                        or record["key"]!=record["relative_path"] or type(record.get("rows")) is not int or record["rows"]<0):
                    raise ContractError("malformed canonical transfer object")
                object_id=(member,stream)
                if object_id in records:raise ContractError("duplicate transfer object/stream")
                records[object_id]=record;key=(day,symbol);members.setdefault(key,set()).add(stream)
            elif kind=="discovery_reference":
                if (day is not None or symbol is not None or stream is not None or record["verify_mode"]!="parquet_rows"
                        or type(record.get("rows")) is not int or record["rows"]<0):raise ContractError("malformed discovery reference")
            elif (day is not None or symbol is not None or stream is not None
                    or not ((record["verify_mode"]=="bytes" and record.get("rows") is None)
                            or (record["verify_mode"]=="parquet_rows" and type(record.get("rows")) is int and record["rows"]>=0))):
                raise ContractError("malformed halt support")
    paired={f"{d}/{s}" for (d,s),streams in members.items() if streams=={"quotes","trades"}}
    if any(streams!={"quotes","trades"} for streams in members.values()):raise ContractError("unpaired transfer member")
    return paired,objects,bytes_total,records,kind_summary,state_summary


TRANSFER_COMPLETION_FIELDS={"version","status","manifest_sha256","expected_objects","expected_bytes",
                            "states","reserved_download_bytes","delivered_payload_bytes","attempts",
                            "elapsed_seconds","scope"}


def _validate_transfer_completion(body,manifest_sha256,expected_objects,expected_bytes,manifest_states):
    """Reconcile the raw-migration completion record with its immutable manifest."""
    if (type(body) is not dict or set(body)!=TRANSFER_COMPLETION_FIELDS
            or body.get("version")!="raw_migration_completion_v1" or body.get("status")!="complete"
            or body.get("manifest_sha256")!=manifest_sha256
            or body.get("expected_objects")!=expected_objects or body.get("expected_bytes")!=expected_bytes
            or body.get("scope")!="transport identity only; production source admission is separate"):
        raise ContractError("transfer completion identity/totals mismatch")
    states=body.get("states")
    if (type(states) is not dict or set(states)!={"verified","reused"}
            or any(type(value) is not dict or set(value)!={"objects","bytes"}
                   or type(value["objects"]) is not int or value["objects"]<0
                   or type(value["bytes"]) is not int or value["bytes"]<0
                   for value in states.values())
            or states!=manifest_states
            or sum(value["objects"] for value in states.values())!=expected_objects
            or sum(value["bytes"] for value in states.values())!=expected_bytes):
        raise ContractError("transfer completion states mismatch")
    verified=states["verified"]
    if (type(body.get("attempts")) is not int or body["attempts"]<verified["objects"]
            or type(body.get("reserved_download_bytes")) is not int
            or type(body.get("delivered_payload_bytes")) is not int
            or body["reserved_download_bytes"]<body["delivered_payload_bytes"]
            or body["delivered_payload_bytes"]<verified["bytes"]
            or type(body.get("elapsed_seconds")) not in (int,float)
            or not math.isfinite(body["elapsed_seconds"]) or body["elapsed_seconds"]<0):
        raise ContractError("transfer completion accounting is incomplete")
    return body


def _validate_measurement(body,plan,release):
    fields={"version","status","kind","measurement_id","source_revision","wheel_sha256","config_sha256",
            "sample","rows","artifacts","phases","independent_reconstruction","readiness_decision_sha256"}
    if (type(body) is not dict or set(body)!=fields
            or body["version"] not in {"tape_representative_measurement_v1","tape_representative_measurement_v2"}
            or body["status"]!="accepted" or body["kind"]!="representative_measurement"
            or body["source_revision"]!=release.get("source_revision")
            or body["wheel_sha256"]!=release.get("wheel_sha256")
            or body["config_sha256"]!=digest(plan["config"])
            or type(body["measurement_id"]) is not str or not re.fullmatch(r"[0-9a-f]{64}",body["measurement_id"])
            or body["measurement_id"]!=digest({k:v for k,v in body.items() if k not in {"measurement_id","readiness_decision_sha256"}})
            or type(body["readiness_decision_sha256"]) is not str or not re.fullmatch(r"[0-9a-f]{64}",body["readiness_decision_sha256"])):
        raise ContractError("measurement identity")
    sample=body["sample"]
    if body["version"]=="tape_representative_measurement_v1":
        if sample!={"members":["2026-09-02/KDP","2026-09-02/NVDA"],"coverage_seconds_per_member":720}:
            raise ContractError("measurement sample")
    else:
        plan_members={f"{member['session_date']}/{member['symbol']}" for member in plan["members"]}
        if (type(sample) is not dict or set(sample)!={"members","coverage_seconds_per_member"}
                or type(sample["members"]) is not list or not 1<=len(sample["members"])<=8
                or len(sample["members"])!=len(set(sample["members"]))
                or any(type(member) is not str or member not in plan_members for member in sample["members"])
                or type(sample["coverage_seconds_per_member"]) is not int
                or not 1<=sample["coverage_seconds_per_member"]<=57_600):
            raise ContractError("measurement sample")
    rows=body["rows"]
    expected_rows=len(sample["members"])*sample["coverage_seconds_per_member"]
    if (type(rows) is not dict or set(rows)!={"base","features","support"}
            or rows!={"base":expected_rows,"features":expected_rows,"support":expected_rows}):
        raise ContractError("measurement rows")
    if body["independent_reconstruction"]!={"base_all_fields":"passed","features_explicit_histories":"passed","support_all_fields":"passed"}:
        raise ContractError("measurement independent reconstruction")
    artifacts=body["artifacts"]
    if type(artifacts) is not list or any(type(x) is not dict for x in artifacts) or [x.get("member") for x in artifacts] != sample["members"]:
        raise ContractError("measurement artifacts")
    for artifact in artifacts:
        if (set(artifact)!={"member","source_pair_sha256","context_sha256","base_manifest_sha256","feature_manifest_sha256","rows"}
                or artifact["rows"]!=sample["coverage_seconds_per_member"]
                or any(type(artifact[x]) is not str or not re.fullmatch(r"[0-9a-f]{64}",artifact[x]) for x in
                       ("source_pair_sha256","context_sha256","base_manifest_sha256","feature_manifest_sha256"))):
            raise ContractError("measurement artifacts")
    phases=body["phases"]
    if type(phases) is not dict or set(phases)!={"build","verification"}:raise ContractError("measurement phases")
    for phase in phases.values():
        if type(phase) is not dict or set(phase)!={"decoded_raw_rows","read_bytes","peak_rss_bytes","wall_seconds","disk_bytes","guards"}:raise ContractError("measurement phase")
        disk=phase["disk_bytes"];guards=phase["guards"]
        guard_fields={"workers","threads","batch_size","cpu_quota_percent","tasks_max",
                      "memory_max_bytes","memory_swap_max_bytes","process_tree_rss_stop_bytes",
                      "runtime_max_seconds","read_limit_bytes","output_scratch_limit_bytes",
                      "max_decoded_raw_rows"}
        v1_guards={"workers":1,"threads":1,"batch_size":4096,"cpu_quota_percent":200,"tasks_max":64,
            "memory_max_bytes":1610612736,"memory_swap_max_bytes":0,"process_tree_rss_stop_bytes":1073741824,
            "runtime_max_seconds":600,"read_limit_bytes":1073741824,"output_scratch_limit_bytes":2147483648,
            "max_decoded_raw_rows":2000000}
        guards_valid=(guards==v1_guards if body["version"]=="tape_representative_measurement_v1" else
            type(guards) is dict and set(guards)==guard_fields
            and type(guards["workers"]) is int and 1<=guards["workers"]<=8
            and guards["threads"]==1 and guards["batch_size"]==4096
            and type(guards["cpu_quota_percent"]) is int and 1<=guards["cpu_quota_percent"]<=800
            and type(guards["tasks_max"]) is int and guards["tasks_max"]>=guards["workers"]
            and type(guards["memory_max_bytes"]) is int and 0<guards["memory_max_bytes"]<=8*1024**3
            and guards["memory_swap_max_bytes"]==0
            and type(guards["process_tree_rss_stop_bytes"]) is int and 0<guards["process_tree_rss_stop_bytes"]<=guards["memory_max_bytes"]
            and type(guards["runtime_max_seconds"]) in (int,float) and 0<guards["runtime_max_seconds"]<=2700
            and type(guards["read_limit_bytes"]) is int and guards["read_limit_bytes"]>0
            and type(guards["output_scratch_limit_bytes"]) is int and 0<guards["output_scratch_limit_bytes"]<=8*1024**3
            and type(guards["max_decoded_raw_rows"]) is int and guards["max_decoded_raw_rows"]>0)
        if (type(disk) is not dict or set(disk)!={"output","scratch_peak"} or any(type(disk[x]) is not int or disk[x]<0 for x in disk)
                or type(phase["read_bytes"]) is not int or phase["read_bytes"]<=0
                or type(phase["peak_rss_bytes"]) is not int or phase["peak_rss_bytes"]<=0
                or type(phase["wall_seconds"]) not in (int,float) or not math.isfinite(phase["wall_seconds"]) or phase["wall_seconds"]<=0
                or not guards_valid
                or type(phase["decoded_raw_rows"]) is not int or phase["decoded_raw_rows"]<0
                or phase["decoded_raw_rows"]>guards["max_decoded_raw_rows"]
                or phase["read_bytes"]>guards["read_limit_bytes"]
                or phase["peak_rss_bytes"]>guards["process_tree_rss_stop_bytes"]
                or phase["peak_rss_bytes"]>guards["memory_max_bytes"]
                or phase["wall_seconds"]>guards["runtime_max_seconds"]
                or phase["disk_bytes"]["output"]+phase["disk_bytes"]["scratch_peak"]>guards["output_scratch_limit_bytes"]):
            raise ContractError("measurement resources/guards")


def _preflight(plan):
    from .replay.builder import _implementation_identity as base_identity
    from .features.endpoint_ew import _implementation_identity as feature_identity
    blockers=[];transfer_records=None
    inventory=read_json(plan["inventory_path"]);admissions=read_json(plan["admissions_path"])
    if inventory.get("members")!=plan["members"] or len(plan["members"])!=plan["expected_members"]:
        blockers.append("expected_member_reconciliation_failed")
    if not plan["transfer_complete"]:blockers.append("transfer_incomplete")
    completion=plan.get("transfer_completion")
    if (type(plan.get("transfer_manifest_sha256")) is not str or not re.fullmatch(r"[0-9a-f]{64}",plan["transfer_manifest_sha256"])
            or not plan.get("transfer_manifest_path")):
        blockers.append("transfer_manifest_identity_missing")
    else:
        try:
            if sha256_file(plan["transfer_manifest_path"])[0]!=plan["transfer_manifest_sha256"]:raise ContractError("changed")
            transfer_members,objects,transfer_bytes,transfer_records,kind_summary,transfer_states=_transfer_summary(plan["transfer_manifest_path"])
            expected={f"{m['session_date']}/{m['symbol']}" for m in plan["members"]}
            if (not expected.issubset(transfer_members) or objects!=plan.get("transfer_expected_objects") or transfer_bytes!=plan.get("transfer_expected_bytes")
                    or kind_summary!=plan.get("transfer_kind_summary")):
                blockers.append("transfer_inventory_reconciliation_failed")
        except (OSError,ValueError,ContractError,json.JSONDecodeError):blockers.append("transfer_inventory_reconciliation_failed")
    if not completion:
        blockers.append("transfer_completion_missing")
    else:
        try:
            body=read_json(completion["path"])
            if (set(completion)!={"path","sha256"} or sha256_file(completion["path"])[0]!=completion["sha256"]
                    or transfer_records is None):
                raise ContractError("transfer completion reference mismatch")
            _validate_transfer_completion(body,plan.get("transfer_manifest_sha256"),
                                          plan.get("transfer_expected_objects"),
                                          plan.get("transfer_expected_bytes"),transfer_states)
        except (KeyError,FileNotFoundError,TypeError,ContractError):blockers.append("transfer_completion_mismatch")
    if plan["unresolved_members"]:blockers.append(f"unresolved_admission:{len(plan['unresolved_members'])}")
    findings={x.get("member"):x for x in admissions.get("findings",[]) if type(x) is dict}
    if any(findings.get(f"{m['session_date']}/{m['symbol']}",{}).get("state")!="metadata_admitted" for m in plan["members"]):
        blockers.append("admission_descriptor_reconciliation_failed")
    descriptors={};descriptor_objects={}
    try:
        if sha256_file(plan["admitted_index_path"])[0]!=plan["admitted_index_sha256"]:raise ContractError("index changed")
        index=sqlite3.connect(f'file:{plan["admitted_index_path"]}?mode=ro',uri=True)
        rows=index.execute("SELECT member,source_pair_path,source_pair_sha256,context_path,context_sha256 FROM members ORDER BY member").fetchall();index.close()
        if len(rows)!=plan["admitted_members"]:raise ContractError("index count")
        from .replay.admission import load_member_descriptors
        for key,pair,pair_sha,context,context_sha in rows:
            if sha256_file(pair)[0]!=pair_sha or sha256_file(context)[0]!=context_sha:raise ContractError("descriptor changed")
            pair_body,context_body,_,_=load_member_descriptors(pair,context)
            canonical=f'{pair_body["session_date"]}/{pair_body["symbol"]}'
            if canonical!=key or context_body["member"]!=key:raise ContractError("descriptor member mismatch")
            descriptors[key]=(pair,context);descriptor_objects[key]=pair_body["streams"]
    except (KeyError,OSError,sqlite3.Error,ContractError):blockers.append("admitted_descriptor_identity_mismatch")
    if transfer_records is not None and "admitted_descriptor_identity_mismatch" not in blockers:
        try:
            for member,streams in descriptor_objects.items():
                for stream,declared in streams.items():
                    record=transfer_records[(member,stream)]
                    retrieval_admitted=(declared.get("terminal_complete") is True
                        or (declared.get("terminal_complete") is False
                            and declared.get("retrieval_completeness")=="unverified_missing_original_vendor_pagination_receipts"))
                    if (record["key"]!=declared["path"] or record["relative_path"]!=declared["path"]
                            or record["sha256"]!=declared["sha256"] or record["size_bytes"]!=declared["bytes"]
                            or record["rows"]!=declared["rows"] or not retrieval_admitted):
                        raise ContractError("transfer/source descriptor mismatch")
        except (KeyError,ContractError):blockers.append("transfer_object_identity_mismatch")
    decision=plan.get("readiness_decision");decision_body=None
    if not decision:blockers.append("reviewed_readiness_decision_missing")
    else:
        try:
            decision_body=read_json(decision["path"])
            if (set(decision)!={"path","sha256"} or sha256_file(decision["path"])[0]!=decision["sha256"]
                    or set(decision_body)!={"version","status","population_sha256","expected_members","source_revision","wheel_sha256","config_sha256","measurement_ids"}
                    or decision_body["version"]!="tape_representativeness_decision_v1" or decision_body["status"]!="reviewed_accepted"
                    or decision_body["population_sha256"]!=digest(plan["members"]) or decision_body["expected_members"]!=plan["expected_members"]
                    or decision_body["source_revision"]!=(plan.get("release") or {}).get("source_revision")
                    or decision_body["wheel_sha256"]!=(plan.get("release") or {}).get("wheel_sha256")
                    or decision_body["config_sha256"]!=digest(plan["config"])
                    or type(decision_body["measurement_ids"]) is not list or not decision_body["measurement_ids"]
                    or any(type(x) is not str or not re.fullmatch(r"[0-9a-f]{64}",x) for x in decision_body["measurement_ids"])):
                raise ContractError("decision")
        except (KeyError,TypeError,OSError,ContractError):blockers.append("reviewed_readiness_decision_identity_mismatch")
    if not plan["measurement_references"]:blockers.append("representative_measurement_missing")
    else:
        try:
            measurement_ids=[]
            for reference in plan["measurement_references"]:
                if set(reference)!={"path","sha256"} or sha256_file(reference["path"])[0]!=reference["sha256"]:raise ContractError("measurement")
                body=read_json(reference["path"]);_validate_measurement(body,plan,plan.get("release") or {})
                if decision is None or body["readiness_decision_sha256"]!=decision["sha256"]:raise ContractError("measurement decision")
                measurement_ids.append(body["measurement_id"])
            if decision_body is None or measurement_ids!=decision_body["measurement_ids"]:raise ContractError("measurement decision set")
        except (KeyError,TypeError,OSError,ContractError):blockers.append("representative_measurement_identity_mismatch")
    release=plan.get("release") or {}
    expected_release={"source_revision","wheel_path","wheel_sha256","executable","contract_identity","base_implementation_identity","feature_implementation_identity"}
    if set(release)!=expected_release:
        blockers.append("release_identity_missing")
    else:
        try:
            executable=Path(release["executable"])
            if (not executable.is_file() or executable.parent!=Path(sys.executable).parent
                    or sha256_file(release["wheel_path"])[0]!=release["wheel_sha256"]
                    or release["contract_identity"]!=plan["contract_identity"]
                    or release["base_implementation_identity"]!=base_identity()["sha256"]
                    or release["feature_implementation_identity"]!=feature_identity()["sha256"]):
                blockers.append("release_identity_mismatch")
        except (FileNotFoundError,PermissionError):blockers.append("release_identity_mismatch")
    limits=plan.get("limits",{});workers=limits.get("workers")
    if type(workers) is not int or not 1<=workers<=8:blockers.append("worker_limit_must_be_integer_1_through_8")
    keys=[f"{member['session_date']}/{member['symbol']}" for member in plan["members"]]
    if len(keys)!=len(set(keys)):blockers.append("duplicate_calculation_member")
    for key in ("base_root","feature_root","ledger_path"):
        if not plan.get(key):blockers.append(f"missing_{key}")
    if plan.get("base_root") and plan.get("feature_root"):
        reserve=limits.get("disk_reserve_bytes");scratch=limits.get("scratch_cap_bytes")
        if type(reserve) is not int or type(scratch) is not int or reserve<0 or scratch<0:
            blockers.append("disk_limits_missing")
        else:
            available=os.statvfs(_nearest_existing(plan["base_root"])).f_bavail*os.statvfs(_nearest_existing(plan["base_root"])).f_frsize
            if available<reserve+scratch:blockers.append("disk_capacity_insufficient")
    for member in plan["members"]:
        for root_name in ("base_root","feature_root"):
            if not plan.get(root_name):continue
            path=Path(plan[root_name])/f'session_date={member["session_date"]}'/f'symbol={member["symbol"]}'
            if path.exists() and any(path.iterdir()) and not (path/"manifest.json").is_file():blockers.append(f"conflicting_incomplete_output:{member['session_date']}/{member['symbol']}:{root_name}")
    return blockers,descriptors

def _run_plan_locked(plan_path,expected):
    sha=sha256_file(plan_path)[0]
    run_started=time.monotonic()
    if sha!=expected:raise ContractError("plan identity mismatch")
    plan=read_json(plan_path)
    if plan.get("version")!="tape_calculation_plan_v1":raise ContractError("unknown plan version")
    if sha256_file(plan["inventory_path"])[0]!=plan["inventory_sha256"] or sha256_file(plan["admissions_path"])[0]!=plan["admissions_sha256"]:raise ContractError("plan input identity changed")
    if contract_identity(FeatureConfig.from_dict(plan["config"]))!=plan["contract_identity"]:raise ContractError("plan config identity mismatch")
    blockers,descriptors=_preflight(plan)
    if blockers:raise ContractError("calculation preflight blocked: "+",".join(blockers))
    ledger_path=Path(plan["ledger_path"]);ledger_path.parent.mkdir(parents=True,exist_ok=True)
    connection=sqlite3.connect(ledger_path)
    connection.execute("CREATE TABLE IF NOT EXISTS members (member TEXT PRIMARY KEY,status TEXT NOT NULL,base_manifest TEXT,feature_manifest TEXT,error TEXT,updated_ns INTEGER NOT NULL)")
    connection.execute("CREATE TABLE IF NOT EXISTS run_attempts (attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,plan_sha256 TEXT NOT NULL,runner_identity TEXT NOT NULL,start_method TEXT NOT NULL,workers INTEGER NOT NULL,scheduling TEXT NOT NULL,started_ns INTEGER NOT NULL)")
    connection.execute("UPDATE members SET status='interrupted',error='previous parent exited before reconciliation',updated_ns=? WHERE status='running'",(time.time_ns(),));connection.commit()
    try:
        from .calculate_runtime import run_members
        result=run_members(plan,descriptors,sha,connection,run_started)
    finally:
        connection.close()
    return {**result,"ledger":str(ledger_path),"plan_sha256":sha}


def run_plan(plan_path,expected):
    """Hold a nonblocking plan lock across preflight and the complete ledger run."""
    plan_path=Path(plan_path).resolve();lock_path=plan_path.parent/f".{plan_path.name}.run.lock"
    with lock_path.open("a+b") as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as error:raise ContractError("calculation plan already running") from error
        return _run_plan_locked(plan_path,expected)

def register_commands(commands):
    calculate=commands.add_parser("calculate",help="Prepare or manually run immutable calculation plans").add_subparsers(dest="calculate_command",required=True)
    plan=calculate.add_parser("plan");plan.add_argument("--inventory",required=True);plan.add_argument("--admissions",required=True);plan.add_argument("--config",required=True);plan.add_argument("--limits",required=True);plan.add_argument("--output",required=True);plan.set_defaults(func=lambda a:create_plan(a.inventory,a.admissions,a.config,a.limits,a.output))
    run=calculate.add_parser("run");run.add_argument("--plan",required=True);run.add_argument("--expected-plan-sha256",required=True);run.set_defaults(func=lambda a:run_plan(a.plan,a.expected_plan_sha256))
