"""Versioned source/context descriptor admission without raw consumption."""
from __future__ import annotations

import re
from pathlib import Path

from ..contracts.config import ContractError
from ..contracts.policy import NS, session_bounds
from ..contracts.source import SourceUnits
from ..integrity import read_json, safe_relative, sha256_file, write_atomic_json

PAIR_FIELDS = {"version", "symbol", "session_date", "currency", "adapter", "root",
               "evidence_root", "streams", "source_units"}
CONTEXT_FIELDS = {"version", "member", "coverage", "observation_intervals", "gaps",
                  "instantaneous_breaks", "halts", "halt_evidence", "seed",
                  "continuity_evidence", "selection", "discovery"}
HISTORICAL_RETRIEVAL_UNVERIFIED = "unverified_missing_original_vendor_pagination_receipts"


def _member(symbol, day):
    if type(symbol) is not str or not re.fullmatch(r"[A-Z0-9][A-Z0-9._-]{0,31}", symbol):
        raise ContractError("invalid symbol")
    session_bounds(day)
    return f"{day}/{symbol}"


def _intervals(values, name, start, end):
    if type(values) is not list or len(values) > 1024:
        raise ContractError(f"invalid {name}")
    previous = start
    result = []
    for item in values:
        if type(item) is not list or len(item) != 2 or any(type(x) is not int for x in item):
            raise ContractError(f"invalid {name} interval")
        a, b = item
        if not start <= a < b <= end or a < previous:
            raise ContractError(f"overlapping/out-of-range {name}")
        result.append((a, b)); previous = b
    return tuple(result)


def load_member_descriptors(source_pair, member_context):
    pair, context = read_json(source_pair), read_json(member_context)
    if set(pair) != PAIR_FIELDS or pair["version"] not in ("tape_source_pair_v1", "tape_source_pair_v2"):
        raise ContractError("unknown/missing source-pair fields or version")
    if set(context) != CONTEXT_FIELDS or context["version"] != "tape_member_context_v1":
        raise ContractError("unknown/missing member-context fields or version")
    member = _member(pair["symbol"], pair["session_date"])
    if pair["currency"] != "USD" or context["member"] != member:
        raise ContractError("source/context member mismatch")
    if pair["adapter"] != "massive_canonical_tq_v1":
        raise ContractError("unsupported source adapter")
    root = Path(pair["root"]); evidence_root=Path(pair["evidence_root"])
    if not root.is_absolute() or not evidence_root.is_absolute():
        raise ContractError("source/evidence roots must be absolute")
    if set(pair["streams"]) != {"quotes", "trades"}:
        raise ContractError("complete T/Q pair required")
    for name, stream in pair["streams"].items():
        required = {"path", "sha256", "bytes", "rows", "schema_sha256", "clock",
                    "provenance_sha256", "coverage_evidence_sha256",
                    "provenance_path", "coverage_evidence_path", "terminal_complete"}
        if pair["version"] == "tape_source_pair_v2":
            required.add("retrieval_completeness")
        if set(stream) != required or stream["clock"] != "sip_timestamp_utc_ns":
            raise ContractError(f"malformed {name} descriptor")
        safe_relative(stream["path"])
        for path_key,hash_key in (("provenance_path","provenance_sha256"),("coverage_evidence_path","coverage_evidence_sha256")):
            evidence_path=evidence_root/safe_relative(stream[path_key])
            if sha256_file(evidence_path)[0]!=stream[hash_key]:raise ContractError(f"{name} evidence identity mismatch")
        if pair["version"] == "tape_source_pair_v1":
            if stream["terminal_complete"] is not True:
                raise ContractError(f"{name} terminal coverage is unresolved")
        elif (stream["terminal_complete"] is not False
                or stream["retrieval_completeness"] != HISTORICAL_RETRIEVAL_UNVERIFIED):
            raise ContractError(f"{name} historical retrieval completeness is not explicitly unverified")
        for key in ("sha256", "schema_sha256", "provenance_sha256", "coverage_evidence_sha256"):
            if type(stream[key]) is not str or not re.fullmatch(r"[0-9a-f]{64}", stream[key]):
                raise ContractError(f"invalid {name} {key}")
        if type(stream["bytes"]) is not int or stream["bytes"] < 0 or type(stream["rows"]) is not int or stream["rows"] < 0:
            raise ContractError(f"invalid {name} counts")
    unit_value=dict(pair["source_units"])
    if set(unit_value)!={"quote_size_unit","quote_size_evidence_sha256","quote_size_evidence_path","trade_quantity_evidence_sha256","trade_quantity_evidence_path","round_lot_shares"}:
        raise ContractError("malformed source units")
    quote_unit_path=evidence_root/safe_relative(unit_value.pop("quote_size_evidence_path"));trade_unit_path=evidence_root/safe_relative(unit_value.pop("trade_quantity_evidence_path"))
    if sha256_file(quote_unit_path)[0]!=unit_value["quote_size_evidence_sha256"] or sha256_file(trade_unit_path)[0]!=unit_value["trade_quantity_evidence_sha256"]:raise ContractError("source-unit evidence identity mismatch")
    try:
        units = SourceUnits(**unit_value)
    except TypeError as error:
        raise ContractError("malformed source units") from error
    quote_unit_body=read_json(quote_unit_path);trade_unit_body=read_json(trade_unit_path)
    if (quote_unit_body!={"version":"source_units_v1","member":member,"stream":"quotes","object_sha256":pair["streams"]["quotes"]["sha256"],"unit":units.quote_size_unit,"multiplier":units.multiplier}
            or trade_unit_body!={"version":"source_units_v1","member":member,"stream":"trades","object_sha256":pair["streams"]["trades"]["sha256"],"quantity_precedence":"decimal_size_then_size","scale":9}):
        raise ContractError("source-unit evidence does not support declared objects/units")
    cov = context["coverage"]
    if set(cov) != {"kind", "session_start_ns", "end_ns", "expected_rows"}:
        raise ContractError("malformed coverage")
    session_start, session_end = session_bounds(pair["session_date"])
    if cov["kind"] not in ("full", "prefix") or cov["session_start_ns"] != session_start:
        raise ContractError("unsupported coverage")
    if type(cov["end_ns"]) is not int or not session_start < cov["end_ns"] <= session_end or (cov["end_ns"]-session_start) % NS:
        raise ContractError("coverage end is off grid")
    if cov["expected_rows"] != (cov["end_ns"]-session_start)//NS:
        raise ContractError("coverage row count mismatch")
    if cov["kind"] == "full" and cov["end_ns"] != session_end:
        raise ContractError("full coverage must end at 20:00 ET")
    if set(context["observation_intervals"]) != {"quotes", "trades"}:
        raise ContractError("observation intervals required")
    for source in ("quotes", "trades"):
        declared=_intervals(context["observation_intervals"][source], source, session_start, cov["end_ns"])
        if (not declared or declared[0][0]!=session_start or declared[-1][1]!=cov["end_ns"]
                or any(left[1]!=right[0] for left,right in zip(declared,declared[1:]))):
            raise ContractError(f"{source} observation intervals do not cover declared prefix")
        coverage=read_json(evidence_root/safe_relative(pair["streams"][source]["coverage_evidence_path"]))
        expected={"version","member","stream","intervals","terminal_complete"}
        if pair["version"] == "tape_source_pair_v2":
            expected.add("retrieval_completeness")
        if (set(coverage)!=expected or coverage["member"]!=member or coverage["stream"]!=source
                or tuple(map(tuple,coverage["intervals"]))!=declared):
            raise ContractError(f"{source} coverage evidence does not support declared intervals")
        if pair["version"] == "tape_source_pair_v1":
            if coverage["version"]!="source_coverage_v1" or coverage["terminal_complete"] is not True:
                raise ContractError(f"{source} terminal coverage is unresolved")
        elif (coverage["version"]!="source_coverage_v2" or coverage["terminal_complete"] is not False
                or coverage["retrieval_completeness"] != HISTORICAL_RETRIEVAL_UNVERIFIED):
            raise ContractError(f"{source} historical retrieval completeness is not explicitly unverified")
        _intervals(context["gaps"].get(source, []), source + " gaps", session_start, cov["end_ns"])
        points = context["instantaneous_breaks"].get(source, [])
        if type(points) is not list or any(type(x) is not int or not session_start <= x < cov["end_ns"] for x in points):
            raise ContractError("invalid instantaneous breaks")
    if set(context["halt_evidence"])!={"status","path","sha256"} or context["halt_evidence"].get("status") not in ("verified_empty", "accepted_intervals"):
        raise ContractError("halt context unresolved")
    if type(context["halts"]) is not list or len(context["halts"])>1024:
        raise ContractError("invalid halts")
    previous_closed_end=None;halt_ids=set()
    for halt in context["halts"]:
        if (type(halt) is not dict or set(halt)!={"start_ns","end_ns","id"}
                or type(halt["start_ns"]) is not int or type(halt["end_ns"]) is not int
                or not session_start<=halt["start_ns"]<halt["end_ns"]<=cov["end_ns"]
                or type(halt["id"]) is not str or not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}",halt["id"])
                or halt["id"] in halt_ids):
            raise ContractError("invalid halt interval/identity")
        closed_start=halt["start_ns"]//NS;closed_end=(halt["end_ns"]-1)//NS
        if previous_closed_end is not None and closed_start<=previous_closed_end+1:
            raise ContractError("overlapping/touching whole-second halt closures require canonical union")
        previous_closed_end=closed_end;halt_ids.add(halt["id"])
    halt_path=evidence_root/safe_relative(context["halt_evidence"]["path"])
    if sha256_file(halt_path)[0]!=context["halt_evidence"]["sha256"]:raise ContractError("halt evidence identity mismatch")
    halt_body=read_json(halt_path)
    if halt_body.get("member")!=member or halt_body.get("status")!=context["halt_evidence"]["status"] or halt_body.get("halts")!=context["halts"]:raise ContractError("halt evidence does not support context")
    continuity=context["continuity_evidence"]
    if set(continuity)!={"path","sha256"}:raise ContractError("continuity evidence unresolved")
    continuity_path=evidence_root/safe_relative(continuity["path"])
    if sha256_file(continuity_path)[0]!=continuity["sha256"]:raise ContractError("continuity evidence identity mismatch")
    continuity_body=read_json(continuity_path)
    if continuity_body!={"version":"source_continuity_v1","member":member,"gaps":context["gaps"],"instantaneous_breaks":context["instantaneous_breaks"]}:
        raise ContractError("continuity evidence does not support context")
    seed=context["seed"]
    if seed=={"basis":"unavailable"} or seed=={"basis":"verified_empty"}:
        pass
    elif type(seed) is dict and set(seed)=={"basis","path","sha256"} and seed["basis"]=="verified_interval":
        seed_path=evidence_root/safe_relative(seed["path"])
        if sha256_file(seed_path)[0]!=seed["sha256"]:raise ContractError("seed evidence identity mismatch")
        body=read_json(seed_path)
        if (set(body)!={"version","member","start_ns","end_ns","quote_object_sha256","terminal_complete","latest_event"}
                or body["version"]!="pre_session_seed_v1" or body["member"]!=member
                or body["start_ns"]!=session_start-300*NS or body["end_ns"]!=session_start
                or body["quote_object_sha256"]!=pair["streams"]["quotes"]["sha256"] or body["terminal_complete"] is not True
                or type(body["latest_event"]) is not dict or set(body["latest_event"])!={"sip_timestamp","sequence_number"}
                or any(type(body["latest_event"][x]) is not int for x in body["latest_event"])):
            raise ContractError("seed evidence does not support declared interval/object")
    else:
        raise ContractError("verified seed requires identity-bound evidence")
    return pair, context, root, units


def admit_inventory(inventory, evidence, output):
    inventory, evidence = read_json(inventory), read_json(evidence)
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    output.mkdir(parents=True)
    records = inventory.get("members")
    if type(records) is not list or len(records) > 1_000_000:
        raise ContractError("bounded member inventory required")
    evidence_members = evidence.get("members", {})
    admitted = blocked = 0
    findings = []
    seen=set()
    for record in records:
        if type(record) is not dict:key="<malformed>"
        else:key=_member(record.get("symbol"),record.get("session_date"))
        if key in seen:raise ContractError("duplicate inventory member")
        seen.add(key)
        reasons = []
        item = evidence_members.get(key, {})
        for required in ("quote_units", "trade_representation", "halt_context", "continuity"):
            if not item.get(required): reasons.append(f"missing_{required}")
        terminal_verified=item.get("terminal_coverage") is True
        historical_unverified=item.get("historical_retrieval_completeness")==HISTORICAL_RETRIEVAL_UNVERIFIED
        if not terminal_verified and not historical_unverified:
            reasons.append("missing_terminal_coverage_or_accepted_historical_unverified_status")
        pair_path=item.get("source_pair_path");context_path=item.get("member_context_path")
        if not pair_path or not context_path:
            reasons.append("missing_member_descriptors")
        if not reasons:
            try:
                pair,context,_,_=load_member_descriptors(pair_path,context_path)
                if f'{pair["session_date"]}/{pair["symbol"]}'!=key or context["member"]!=key:
                    raise ContractError("descriptor member does not match inventory member")
            except (ContractError,FileNotFoundError) as error:
                reasons.append(f"descriptor_rejected:{error}")
        state = "metadata_admitted" if not reasons else "blocked"
        admitted += state == "metadata_admitted"; blocked += state == "blocked"
        finding={"member": key, "state": state, "reasons": reasons,
                 "retrieval_completeness":"verified" if terminal_verified else "unverified"}
        if state=="metadata_admitted":
            member_root=output/"members"/record["session_date"]/record["symbol"]
            write_atomic_json(member_root/"source-pair.json",pair);write_atomic_json(member_root/"context.json",context)
            finding["source_pair_path"]=str((member_root/"source-pair.json").resolve());finding["member_context_path"]=str((member_root/"context.json").resolve())
        findings.append(finding)
    result = {"version": "tape_admission_report_v1", "members": len(records),
              "metadata_admitted": admitted, "blocked": blocked, "findings": findings}
    write_atomic_json(output / "admission-report.json", result)
    return result
