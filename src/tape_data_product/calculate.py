"""Immutable one-worker calculation plans and fail-closed manual execution."""
from __future__ import annotations
import os
import re
import json
import sqlite3
import sys
import time
import math
from pathlib import Path

from .contracts import DEFAULT_CONFIG, contract_identity
from .contracts.config import ContractError, FeatureConfig, digest
from .integrity import read_json, sha256_file, write_atomic_json

def create_plan(inventory_path,admissions_path,config_path,limits_path,output):
    inventory=read_json(inventory_path);admissions=read_json(admissions_path);limits=read_json(limits_path)
    config=FeatureConfig.from_dict(read_json(config_path));output=Path(output)
    if output.exists():raise FileExistsError(output)
    output.mkdir(parents=True)
    members=inventory.get("members",[]);findings={x["member"]:x for x in admissions.get("findings",[])}
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
          "transfer_manifest_path":inventory.get("transfer_manifest_path"),"transfer_manifest_sha256":inventory.get("transfer_manifest_sha256"),
          "transfer_expected_objects":inventory.get("transfer_expected_objects"),"transfer_expected_bytes":inventory.get("transfer_expected_bytes"),
          "release":inventory.get("release"),"base_root":inventory.get("base_root"),"feature_root":inventory.get("feature_root"),"ledger_path":inventory.get("ledger_path")}
    write_atomic_json(output/"plan.json",plan);sha=sha256_file(output/"plan.json")[0]
    return {"plan":str((output/"plan.json").resolve()),"sha256":sha,"members":len(members),"unresolved":len(unresolved),"ready":not unresolved and plan["transfer_complete"] and bool(plan["transfer_completion"]) and bool(plan["measurement_references"])}


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
    with Path(path).open() as handle:
        for line in handle:
            if not line.strip():continue
            record=json.loads(line)
            if record.get("kind")!="canonical_tq":continue
            day=record.get("session_date");symbol=record.get("symbol");stream=record.get("stream")
            member=f"{day}/{symbol}"
            if (set(record)!=TRANSFER_FIELDS or record.get("version")!="raw_migration_object_v1"
                    or stream not in ("quotes","trades") or record.get("verify_mode")!="tq_parquet_sip_order"
                    or type(record.get("key")) is not str or type(record.get("relative_path")) is not str
                    or record["key"]!=record["relative_path"] or Path(record["key"]).is_absolute()
                    or ".." in Path(record["key"]).parts
                    or type(record.get("sha256")) is not str or not re.fullmatch(r"[0-9a-f]{64}",record["sha256"])
                    or type(record.get("size_bytes")) is not int or record["size_bytes"]<0
                    or type(record.get("rows")) is not int or record["rows"]<0
                    or record.get("reuse_path") is not None and type(record["reuse_path"]) is not str):
                raise ContractError("malformed transfer inventory")
            object_id=(member,stream)
            if object_id in records or record["key"] in keys or record["relative_path"] in paths:
                raise ContractError("duplicate transfer object/stream")
            records[object_id]=record;keys.add(record["key"]);paths.add(record["relative_path"])
            key=(day,symbol);members.setdefault(key,set()).add(stream);objects+=1;bytes_total+=record["size_bytes"]
    paired={f"{d}/{s}" for (d,s),streams in members.items() if streams=={"quotes","trades"}}
    if any(streams!={"quotes","trades"} for streams in members.values()):raise ContractError("unpaired transfer member")
    return paired,objects,bytes_total,records


def _validate_measurement(body,plan,release):
    fields={"version","status","kind","source_revision","wheel_sha256","config_sha256",
            "sample","rows","read_bytes","peak_rss_bytes","wall_seconds","disk_bytes"}
    if (type(body) is not dict or set(body)!=fields
            or body["version"]!="tape_representative_measurement_v1"
            or body["status"]!="accepted" or body["kind"]!="representative_measurement"
            or body["source_revision"]!=release.get("source_revision")
            or body["wheel_sha256"]!=release.get("wheel_sha256")
            or body["config_sha256"]!=digest(plan["config"])):
        raise ContractError("measurement identity")
    sample=body["sample"]
    if (type(sample) is not dict or set(sample)!={"members","coverage_seconds"}
            or type(sample["members"]) is not list or not sample["members"]
            or any(type(x) is not str or not re.fullmatch(r"\d{4}-\d{2}-\d{2}/[A-Z0-9][A-Z0-9._-]{0,31}",x) for x in sample["members"])
            or len(set(sample["members"]))!=len(sample["members"])
            or type(sample["coverage_seconds"]) is not int or sample["coverage_seconds"]<=0):
        raise ContractError("measurement sample")
    rows=body["rows"]
    if (type(rows) is not dict or set(rows)!={"base","features","support"}
            or any(type(rows[x]) is not int or rows[x]<=0 for x in rows)
            or rows["base"]!=rows["features"] or rows["base"]!=rows["support"]):
        raise ContractError("measurement rows")
    disk=body["disk_bytes"]
    if (type(disk) is not dict or set(disk)!={"base","features","scratch_peak"}
            or any(type(disk[x]) is not int or disk[x]<0 for x in disk)):
        raise ContractError("measurement disk")
    if (type(body["read_bytes"]) is not int or body["read_bytes"]<=0
            or type(body["peak_rss_bytes"]) is not int or body["peak_rss_bytes"]<=0
            or type(body["wall_seconds"]) not in (int,float) or not math.isfinite(body["wall_seconds"])
            or body["wall_seconds"]<=0):
        raise ContractError("measurement resources")


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
            transfer_members,objects,transfer_bytes,transfer_records=_transfer_summary(plan["transfer_manifest_path"])
            expected={f"{m['session_date']}/{m['symbol']}" for m in plan["members"]}
            if (transfer_members!=expected or objects!=plan.get("transfer_expected_objects") or transfer_bytes!=plan.get("transfer_expected_bytes")):
                blockers.append("transfer_inventory_reconciliation_failed")
        except (OSError,ValueError,ContractError,json.JSONDecodeError):blockers.append("transfer_inventory_reconciliation_failed")
    if not completion:
        blockers.append("transfer_completion_missing")
    else:
        try:
            body=read_json(completion["path"])
            if (set(completion)!={"path","sha256"} or sha256_file(completion["path"])[0]!=completion["sha256"]
                    or body!={"status":"complete","expected_members":plan["expected_members"],"manifest_sha256":plan.get("transfer_manifest_sha256"),"objects":plan.get("transfer_expected_objects"),"bytes":plan.get("transfer_expected_bytes")}):
                blockers.append("transfer_completion_mismatch")
        except (KeyError,FileNotFoundError,ContractError):blockers.append("transfer_completion_mismatch")
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
                    if (record["key"]!=declared["path"] or record["relative_path"]!=declared["path"]
                            or record["sha256"]!=declared["sha256"] or record["size_bytes"]!=declared["bytes"]
                            or record["rows"]!=declared["rows"] or declared["terminal_complete"] is not True):
                        raise ContractError("transfer/source descriptor mismatch")
        except (KeyError,ContractError):blockers.append("transfer_object_identity_mismatch")
    if not plan["measurement_references"]:blockers.append("representative_measurement_missing")
    else:
        try:
            for reference in plan["measurement_references"]:
                if set(reference)!={"path","sha256"} or sha256_file(reference["path"])[0]!=reference["sha256"]:raise ContractError("measurement")
                _validate_measurement(read_json(reference["path"]),plan,plan.get("release") or {})
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
    limits=plan.get("limits",{})
    if limits.get("workers")!=1:blockers.append("worker_limit_must_be_one")
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

def run_plan(plan_path,expected):
    sha=sha256_file(plan_path)[0]
    if sha!=expected:raise ContractError("plan identity mismatch")
    plan=read_json(plan_path)
    if plan.get("version")!="tape_calculation_plan_v1":raise ContractError("unknown plan version")
    if sha256_file(plan["inventory_path"])[0]!=plan["inventory_sha256"] or sha256_file(plan["admissions_path"])[0]!=plan["admissions_sha256"]:raise ContractError("plan input identity changed")
    if contract_identity(FeatureConfig.from_dict(plan["config"]))!=plan["contract_identity"]:raise ContractError("plan config identity mismatch")
    blockers,descriptors=_preflight(plan)
    if blockers:raise ContractError("calculation preflight blocked: "+",".join(blockers))
    from .replay.builder import build_base_partition
    from .features.endpoint_ew import build_from_base
    config=FeatureConfig.from_dict(plan["config"]);ledger_path=Path(plan["ledger_path"]);ledger_path.parent.mkdir(parents=True,exist_ok=True)
    connection=sqlite3.connect(ledger_path)
    connection.execute("CREATE TABLE IF NOT EXISTS members (member TEXT PRIMARY KEY,status TEXT NOT NULL,base_manifest TEXT,feature_manifest TEXT,error TEXT,updated_ns INTEGER NOT NULL)")
    completed=0
    try:
        for member in plan["members"]:
            key=f'{member["session_date"]}/{member["symbol"]}';pair_path,context_path=descriptors[key]
            base=Path(plan["base_root"])/f'session_date={member["session_date"]}'/f'symbol={member["symbol"]}'
            features=Path(plan["feature_root"])/f'session_date={member["session_date"]}'/f'symbol={member["symbol"]}'
            try:
                base_result=build_base_partition(pair_path,context_path,base,config=config,batch_size=plan["limits"].get("batch_size",4096))
                feature_result=build_from_base(base,features,config=config,batch_size=plan["limits"].get("batch_size",4096))
                connection.execute("INSERT OR REPLACE INTO members VALUES (?,?,?,?,?,?)",(key,"complete",str(base_result.manifest_path),str(feature_result.manifest_path),None,time.time_ns()));connection.commit();completed+=1
            except Exception as error:
                connection.execute("INSERT OR REPLACE INTO members VALUES (?,?,?,?,?,?)",(key,"failed",None,None,str(error)[:4096],time.time_ns()));connection.commit()
                raise
    finally:connection.close()
    return {"status":"complete","members":completed,"ledger":str(ledger_path),"plan_sha256":sha}

def register_commands(commands):
    calculate=commands.add_parser("calculate",help="Prepare or manually run immutable calculation plans").add_subparsers(dest="calculate_command",required=True)
    plan=calculate.add_parser("plan");plan.add_argument("--inventory",required=True);plan.add_argument("--admissions",required=True);plan.add_argument("--config",required=True);plan.add_argument("--limits",required=True);plan.add_argument("--output",required=True);plan.set_defaults(func=lambda a:create_plan(a.inventory,a.admissions,a.config,a.limits,a.output))
    run=calculate.add_parser("run");run.add_argument("--plan",required=True);run.add_argument("--expected-plan-sha256",required=True);run.set_defaults(func=lambda a:run_plan(a.plan,a.expected_plan_sha256))
