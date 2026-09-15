"""Immutable one-worker calculation plans and fail-closed manual execution."""
from __future__ import annotations
import sqlite3
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
          "measurement_references":inventory.get("measurement_references",[]),"release":inventory.get("release"),"base_root":inventory.get("base_root"),"feature_root":inventory.get("feature_root"),"ledger_path":inventory.get("ledger_path")}
    write_atomic_json(output/"plan.json",plan);sha=sha256_file(output/"plan.json")[0]
    return {"plan":str((output/"plan.json").resolve()),"sha256":sha,"members":len(members),"unresolved":len(unresolved),"ready":not unresolved and plan["transfer_complete"] and bool(plan["measurement_references"])}

def run_plan(plan_path,expected):
    sha=sha256_file(plan_path)[0]
    if sha!=expected:raise ContractError("plan identity mismatch")
    plan=read_json(plan_path)
    if plan.get("version")!="tape_calculation_plan_v1":raise ContractError("unknown plan version")
    if sha256_file(plan["inventory_path"])[0]!=plan["inventory_sha256"] or sha256_file(plan["admissions_path"])[0]!=plan["admissions_sha256"]:raise ContractError("plan input identity changed")
    blockers=[]
    if not plan["transfer_complete"]:blockers.append("transfer_incomplete")
    if plan["unresolved_members"]:blockers.append(f"unresolved_admission:{len(plan['unresolved_members'])}")
    if not plan["measurement_references"]:blockers.append("representative_measurement_missing")
    release=plan.get("release") or {}
    if set(release)!={"source_revision","wheel_path","wheel_sha256","executable","contract_identity","base_implementation_identity","feature_implementation_identity"}:
        blockers.append("release_identity_missing")
    elif not Path(release["executable"]).is_file() or sha256_file(release["wheel_path"])[0]!=release["wheel_sha256"]:
        blockers.append("release_identity_mismatch")
    for key in ("base_root","feature_root","ledger_path"):
        if not plan.get(key):blockers.append(f"missing_{key}")
    if blockers:raise ContractError("calculation preflight blocked: "+",".join(blockers))
    raise ContractError("external calculation requires explicit member descriptors; plan is validated but no corpus launch occurred")

def register_commands(commands):
    calculate=commands.add_parser("calculate",help="Prepare or manually run immutable calculation plans").add_subparsers(dest="calculate_command",required=True)
    plan=calculate.add_parser("plan");plan.add_argument("--inventory",required=True);plan.add_argument("--admissions",required=True);plan.add_argument("--config",required=True);plan.add_argument("--limits",required=True);plan.add_argument("--output",required=True);plan.set_defaults(func=lambda a:create_plan(a.inventory,a.admissions,a.config,a.limits,a.output))
    run=calculate.add_parser("run");run.add_argument("--plan",required=True);run.add_argument("--expected-plan-sha256",required=True);run.set_defaults(func=lambda a:run_plan(a.plan,a.expected_plan_sha256))
