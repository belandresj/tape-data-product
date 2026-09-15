"""Immutable one-worker calculation plans and fail-closed manual execution."""
from __future__ import annotations
import os
import re
import sqlite3
import sys
import time
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
    plan={"version":"tape_calculation_plan_v1","inventory_path":str(Path(inventory_path).resolve()),"inventory_sha256":sha256_file(inventory_path)[0],
          "admissions_path":str(Path(admissions_path).resolve()),"admissions_sha256":sha256_file(admissions_path)[0],"config":config.to_dict(),"contract_identity":contract_identity(config),
          "limits":limits,"members":members,"expected_members":len(members),"unresolved_members":unresolved,"transfer_complete":bool(inventory.get("transfer_complete",False)),
          "measurement_references":inventory.get("measurement_references",[]),"transfer_completion":inventory.get("transfer_completion"),
          "transfer_manifest_sha256":inventory.get("transfer_manifest_sha256"),
          "release":inventory.get("release"),"base_root":inventory.get("base_root"),"feature_root":inventory.get("feature_root"),"ledger_path":inventory.get("ledger_path")}
    write_atomic_json(output/"plan.json",plan);sha=sha256_file(output/"plan.json")[0]
    return {"plan":str((output/"plan.json").resolve()),"sha256":sha,"members":len(members),"unresolved":len(unresolved),"ready":not unresolved and plan["transfer_complete"] and bool(plan["transfer_completion"]) and bool(plan["measurement_references"])}


def _nearest_existing(path):
    path=Path(path)
    while not path.exists() and path!=path.parent:path=path.parent
    return path


def _preflight(plan):
    from .replay.builder import _implementation_identity as base_identity
    from .features.endpoint_ew import _implementation_identity as feature_identity
    blockers=[]
    inventory=read_json(plan["inventory_path"]);admissions=read_json(plan["admissions_path"])
    if inventory.get("members")!=plan["members"] or len(plan["members"])!=plan["expected_members"]:
        blockers.append("expected_member_reconciliation_failed")
    if not plan["transfer_complete"]:blockers.append("transfer_incomplete")
    completion=plan.get("transfer_completion")
    if type(plan.get("transfer_manifest_sha256")) is not str or not re.fullmatch(r"[0-9a-f]{64}",plan["transfer_manifest_sha256"]):
        blockers.append("transfer_manifest_identity_missing")
    if not completion:
        blockers.append("transfer_completion_missing")
    else:
        try:
            body=read_json(completion["path"])
            if (set(completion)!={"path","sha256"} or sha256_file(completion["path"])[0]!=completion["sha256"]
                    or body!={"status":"complete","expected_members":plan["expected_members"],"manifest_sha256":plan.get("transfer_manifest_sha256")}):
                blockers.append("transfer_completion_mismatch")
        except (KeyError,FileNotFoundError,ContractError):blockers.append("transfer_completion_mismatch")
    if plan["unresolved_members"]:blockers.append(f"unresolved_admission:{len(plan['unresolved_members'])}")
    findings={x.get("member"):x for x in admissions.get("findings",[]) if type(x) is dict}
    if any(findings.get(f"{m['session_date']}/{m['symbol']}",{}).get("state")!="metadata_admitted" for m in plan["members"]):
        blockers.append("admission_descriptor_reconciliation_failed")
    if not plan["measurement_references"]:blockers.append("representative_measurement_missing")
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
    return blockers,findings

def run_plan(plan_path,expected):
    sha=sha256_file(plan_path)[0]
    if sha!=expected:raise ContractError("plan identity mismatch")
    plan=read_json(plan_path)
    if plan.get("version")!="tape_calculation_plan_v1":raise ContractError("unknown plan version")
    if sha256_file(plan["inventory_path"])[0]!=plan["inventory_sha256"] or sha256_file(plan["admissions_path"])[0]!=plan["admissions_sha256"]:raise ContractError("plan input identity changed")
    if contract_identity(FeatureConfig.from_dict(plan["config"]))!=plan["contract_identity"]:raise ContractError("plan config identity mismatch")
    blockers,findings=_preflight(plan)
    if blockers:raise ContractError("calculation preflight blocked: "+",".join(blockers))
    from .replay.builder import build_base_partition
    from .features.endpoint_ew import build_from_base
    config=FeatureConfig.from_dict(plan["config"]);ledger_path=Path(plan["ledger_path"]);ledger_path.parent.mkdir(parents=True,exist_ok=True)
    connection=sqlite3.connect(ledger_path)
    connection.execute("CREATE TABLE IF NOT EXISTS members (member TEXT PRIMARY KEY,status TEXT NOT NULL,base_manifest TEXT,feature_manifest TEXT,error TEXT,updated_ns INTEGER NOT NULL)")
    completed=0
    try:
        for member in plan["members"]:
            key=f'{member["session_date"]}/{member["symbol"]}';finding=findings[key]
            base=Path(plan["base_root"])/f'session_date={member["session_date"]}'/f'symbol={member["symbol"]}'
            features=Path(plan["feature_root"])/f'session_date={member["session_date"]}'/f'symbol={member["symbol"]}'
            try:
                base_result=build_base_partition(finding["source_pair_path"],finding["member_context_path"],base,config=config,batch_size=plan["limits"].get("batch_size",4096))
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
