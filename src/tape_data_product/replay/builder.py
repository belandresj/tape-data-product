"""Streaming canonical T/Q replay into the accepted one-second base schema."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import fcntl
import hashlib
import math
import os
from pathlib import Path
import shutil
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from ..contracts import BASE_SCHEMA, DEFAULT_CONFIG, contract_identity
from ..contracts.config import ContractError, FeatureConfig, canonical_json, digest, integer
from ..contracts.interfaces import BuildResult
from ..contracts.policy import (NS, RTH_TRADE_CONDITIONS, EXTENDED_TRADE_CONDITIONS,
    CAUSAL_CORRECTIONS, KNOWN_CORRECTIONS, KNOWN_TRADE_CONDITIONS,
    KNOWN_QUOTE_CONDITIONS, KNOWN_QUOTE_INDICATORS, QUOTE_ONE_SIDED_CODES,
    QUOTE_NONFIRM_CODES, QUOTE_CLOSED_OR_NO_QUOTE_CODES, QUOTE_INVALID_CODES,
    QUOTE_EXPLICIT_CROSSED_CODES, QUOTE_EXPLICIT_LOCKED_CODES, timely_trade)
from ..contracts.reasons import Reason, SourceStatus, MidpointAgeStatus
from ..contracts.schemas import schema_hash
from ..contracts.source import share_units, shares_from_units, add_share_units
from ..contracts.validation import validate_batch
from ..integrity import read_json, sha256_file, write_atomic_json, output_record, verify_output
from .admission import load_member_descriptors

QUOTE_COLUMNS = ("sip_timestamp", "sequence_number", "bid_price", "ask_price", "bid_size",
                 "ask_size", "conditions", "indicators")
TRADE_COLUMNS = ("sip_timestamp", "sequence_number", "participant_timestamp", "price",
                 "decimal_size", "size", "conditions", "correction")


def _implementation_identity():
    root = Path(__file__).parent
    files = {str(p.relative_to(root.parent)): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in sorted((*root.glob("*.py"), *Path(__file__).parent.parent.joinpath("contracts").glob("*.py"),
                              Path(__file__).parent.parent / "integrity.py"))}
    return {"files": files, "sha256": digest(files)}


def _schema_identity(path: Path) -> str:
    return digest({"schema": str(pq.ParquetFile(path).schema_arrow)})


def _verify_source(pair, root):
    for name, record in pair["streams"].items():
        path = root / record["path"]
        sha, size = sha256_file(path)
        if (sha, size) != (record["sha256"], record["bytes"]):
            raise ContractError(f"{name} source identity changed")
        meta = pq.ParquetFile(path).metadata
        if (meta.num_rows, _schema_identity(path)) != (record["rows"], record["schema_sha256"]):
            raise ContractError(f"{name} source identity changed")


def _event_rows(path, columns, batch_size, stop_ns, *, seed_start=None):
    parquet = pq.ParquetFile(path)
    missing = set(columns) - set(parquet.schema_arrow.names)
    if missing:
        raise ContractError(f"missing required columns: {sorted(missing)}")
    previous = None
    for batch in parquet.iter_batches(batch_size=batch_size, columns=list(columns), use_threads=False):
        names = batch.schema.names
        for values in zip(*(batch.column(i).to_pylist() for i in range(batch.num_columns))):
            row = dict(zip(names, values))
            sip, seq = row["sip_timestamp"], row["sequence_number"]
            if type(sip) is not int or type(seq) is not int:
                raise ContractError("untrustworthy event key")
            key = (sip, seq)
            if previous is not None and key <= previous:
                raise ContractError("duplicate/decreasing composite event key")
            previous = key
            if sip >= stop_ns:
                return
            if seed_start is None or sip >= seed_start:
                yield row


def _codes(value, known, name):
    if value is None: return (), set()
    if type(value) is not list or len(value) > 64 or any(type(x) is not int for x in value):
        raise ContractError(f"malformed {name} codes")
    unknown = set(value) - set(known)
    return tuple(value), unknown


@dataclass
class QuoteState:
    bid: float | None = None; ask: float | None = None
    bid_size: float | None = None; ask_size: float | None = None
    price_valid: bool = False; spread_valid: bool = False
    bid_size_valid: bool = False; ask_size_valid: bool = False
    last_quote_ns: int | None = None; origin_ns: int | None = None
    last_change_ns: int | None = None; midpoint: float | None = None
    invalid_seen: bool = False


def _apply_quote(state, row, multiplier):
    codes, unknown_c = _codes(row["conditions"], KNOWN_QUOTE_CONDITIONS, "quote condition")
    indicators, unknown_i = _codes(row["indicators"], KNOWN_QUOTE_INDICATORS, "quote indicator")
    state.last_quote_ns = row["sip_timestamp"]
    invalid = bool(unknown_c or unknown_i or set(codes) & set(QUOTE_ONE_SIDED_CODES + QUOTE_NONFIRM_CODES +
                  QUOTE_CLOSED_OR_NO_QUOTE_CODES + QUOTE_INVALID_CODES + QUOTE_EXPLICIT_CROSSED_CODES))
    bid, ask = row["bid_price"], row["ask_price"]
    invalid |= any(type(x) not in (float, int) or isinstance(x, bool) or not math.isfinite(x) or x <= 0 for x in (bid, ask))
    invalid |= not invalid and ask < bid
    if invalid:
        state.bid = state.ask = state.midpoint = None
        state.bid_size = state.ask_size = None
        state.price_valid = state.spread_valid = state.bid_size_valid = state.ask_size_valid = False
        state.origin_ns = state.last_change_ns = None; state.invalid_seen = True
        return
    bid, ask = float(bid), float(ask); midpoint = bid / 2 + ask / 2
    recovering = not state.price_valid
    if recovering:
        state.origin_ns = row["sip_timestamp"]; state.last_change_ns = None
    elif midpoint != state.midpoint:
        state.last_change_ns = row["sip_timestamp"]
    state.bid, state.ask, state.midpoint, state.price_valid = bid, ask, midpoint, True
    locked = bool(set(codes) & set(QUOTE_EXPLICIT_LOCKED_CODES))
    state.spread_valid = not (locked and bid != ask)
    for side in ("bid", "ask"):
        raw = row[f"{side}_size"]
        valid = type(raw) in (float, int) and not isinstance(raw, bool) and math.isfinite(raw) and raw > 0
        setattr(state, f"{side}_size_valid", valid)
        setattr(state, f"{side}_size", float(raw) * multiplier if valid else None)


def _contains(intervals, start, end):
    return sum(max(0, min(end, b)-max(start, a)) for a,b in intervals)


def _point_in(intervals, point):
    return any(a <= point < b for a,b in intervals)


def _source_bit(status):
    return {SourceStatus.ACCEPTED: 0, SourceStatus.UNVERIFIED: int(Reason.SOURCE_UNVERIFIED),
            SourceStatus.UNAVAILABLE: int(Reason.SOURCE_UNAVAILABLE)}[status]


def _mask_supported(duration, *, halt, status, invalid=False, broken=False):
    if duration: return 0
    value = int(Reason.NO_SUPPORTED_DATA)
    if halt: value |= int(Reason.HALT)
    value |= _source_bit(status)
    if not halt and invalid: value |= int(Reason.INVALID_CURRENT_VALUE)
    if not halt and broken: value |= int(Reason.CONTINUITY_BREAK)
    return value


def _current_mask(valid, *, halt, status, broken=False, missing_event=False, midpoint_bound=False):
    if halt: return int(Reason.HALT) | _source_bit(status)
    if status != SourceStatus.ACCEPTED: return _source_bit(status)
    if valid: return 0
    if midpoint_bound: return int(Reason.MIDPOINT_AGE_LOWER_BOUND_ONLY)
    value = int(Reason.NO_OBSERVED_EVENT if missing_event else Reason.INVALID_CURRENT_VALUE)
    if broken: value |= int(Reason.CONTINUITY_BREAK)
    return value


def _trade_class(row, config, session_start):
    correction = row["correction"]
    if correction is None: correction = 0
    if type(correction) is not int or correction not in KNOWN_CORRECTIONS:
        raise ContractError("unknown correction scope")
    if correction not in CAUSAL_CORRECTIONS: return "excluded", None
    codes_result = _codes(row["conditions"], KNOWN_TRADE_CONDITIONS, "trade condition")
    if len(codes_result) == 2: codes, unknown = codes_result
    else: codes, unknown = codes_result, set()
    if unknown: return "uncertain", None
    sip = row["sip_timestamp"]; participant = row["participant_timestamp"]
    if type(participant) is not int: raise ContractError("untrustworthy participant clock")
    elapsed = (sip - session_start) // NS
    allowed = RTH_TRADE_CONDITIONS if 5*3600+30*60 <= elapsed < 12*3600 else EXTENDED_TRADE_CONDITIONS
    if any(c not in allowed for c in codes) or not timely_trade(sip, participant, config): return "excluded", None
    price = row["price"]
    if type(price) not in (int,float) or isinstance(price,bool) or not math.isfinite(price): return "uncertain", None
    if price <= 0: return "excluded", None
    quantity = row["decimal_size"] if row["decimal_size"] is not None else row["size"]
    try: units = share_units(quantity)
    except ContractError:
        if isinstance(quantity, (int,float)) and math.isfinite(quantity) and quantity <= 0: return "excluded", None
        raise
    if units == 0: return "excluded", None
    return "eligible", (float(price), units)


def _base_compatibility(pair, config, implementation):
    value = {"schema": schema_hash(BASE_SCHEMA), "max_trade_reporting_age_ns": config.max_trade_reporting_age_ns,
             "adapter": pair["adapter"], "source_units": pair["source_units"],
             "implementation": implementation["sha256"], "timing": "sip_endpoint_strict_prior_v1"}
    return {"descriptor": value, "sha256": digest(value)}


def build_base_partition(source_pair, member_context, output, *, config=DEFAULT_CONFIG, batch_size=4096):
    integer(batch_size, "batch size", 1, 25000)
    if not isinstance(config, FeatureConfig): raise ContractError("invalid feature config")
    pair, context, root, units = load_member_descriptors(source_pair, member_context)
    source_pair_sha=sha256_file(source_pair)[0];member_context_sha=sha256_file(member_context)[0]
    output = Path(output)
    if (output / "manifest.json").exists():
        manifest = verify_base_partition(output)
        implementation = _implementation_identity()
        expected_inputs = {"source_pair_sha256":source_pair_sha,
                           "context_sha256":member_context_sha,
                           "streams":pair["streams"]}
        expected_compatibility = _base_compatibility(pair, config, implementation)
        if (manifest["member"] != {"symbol":pair["symbol"], "session_date":pair["session_date"]}
                or manifest["coverage"] != context["coverage"]
                or manifest["inputs"] != expected_inputs
                or manifest["source_units"] != pair["source_units"]
                or manifest["base_compatibility"] != expected_compatibility
                or manifest["implementation_identity"] != implementation):
            raise ContractError("completed base does not match requested inputs/implementation")
        _verify_source(pair, root)
        return BuildResult(digest(manifest["member"]), manifest["contract_identity"], output/"manifest.json", manifest["coverage"]["expected_rows"])
    if output.exists() and any(output.iterdir()): raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    lock_path = output.parent / f".{output.name}.lock"
    with lock_path.open("a+b") as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error: raise ContractError("concurrent member writer") from error
        _verify_source(pair, root)
        attempt = Path(tempfile.mkdtemp(prefix=f".{output.name}.attempt-", dir=output.parent))
        try:
            rows = _replay(pair, context, root, units.multiplier, attempt/"base.parquet", config, batch_size)
            write_atomic_json(attempt/"context.json", context)
            implementation = _implementation_identity()
            records = [output_record(attempt/"base.parquet", rows=rows, schema_sha256=schema_hash(BASE_SCHEMA)),
                       output_record(attempt/"context.json", rows=rows, schema_sha256=digest(context))]
            manifest = {"manifest_version":"tape_member_manifest_v1", "member":{"symbol":pair["symbol"],"session_date":pair["session_date"]},
                "coverage":context["coverage"], "inputs":{"source_pair_sha256":source_pair_sha,"context_sha256":member_context_sha,
                    "streams":pair["streams"]}, "source_units":pair["source_units"], "contract_identity":contract_identity(config),
                "base_compatibility":_base_compatibility(pair, config, implementation), "implementation_identity":implementation,
                "contract_config":config.to_dict(), "outputs":records, "validation":{"integrity":"passed","consumption":"consumption_verified","independent_reconstruction":"pending"}, "complete":True}
            write_atomic_json(attempt/"manifest.json", manifest)
            _verify_source(pair, root)
            if sha256_file(source_pair)[0]!=source_pair_sha or sha256_file(member_context)[0]!=member_context_sha:
                raise ContractError("source/context descriptor changed during replay")
            verify_base_partition(attempt)
            os.replace(attempt, output)
        except Exception:
            shutil.rmtree(attempt, ignore_errors=True); raise
    return BuildResult(digest(manifest["member"]), manifest["contract_identity"], output/"manifest.json", rows)


def _replay(pair, context, root, multiplier, path, config, batch_size):
    start, end = context["coverage"]["session_start_ns"], context["coverage"]["end_ns"]
    quote_events = iter(_event_rows(root/pair["streams"]["quotes"]["path"], QUOTE_COLUMNS, batch_size, end, seed_start=start-300*NS))
    trade_events = iter(_event_rows(root/pair["streams"]["trades"]["path"], TRADE_COLUMNS, batch_size, end, seed_start=start))
    qnext, tnext = next(quote_events, None), next(trade_events, None)
    state = QuoteState(); last_trade=None; quote_cont=trade_cont=0; quote_broken=trade_broken=False; was_halt=False
    # Verified seed is explicitly opted into; otherwise consume but do not apply pre-session quotes.
    last_seed_event=None
    while qnext is not None and qnext["sip_timestamp"] < start:
        if context["seed"].get("basis") == "verified_interval": _apply_quote(state,qnext,multiplier)
        if context["seed"].get("basis") == "verified_interval": last_seed_event=qnext
        qnext=next(quote_events,None)
    if context["seed"].get("basis") == "verified_interval":
        evidence=read_json(Path(pair["evidence_root"])/context["seed"]["path"])["latest_event"]
        actual=None if last_seed_event is None else {"sip_timestamp":last_seed_event["sip_timestamp"],"sequence_number":last_seed_event["sequence_number"]}
        if actual!=evidence:raise ContractError("seed evidence/latest event mismatch")
        if state.price_valid:
            state.origin_ns=start;state.last_change_ns=None
    buffers=[]; writer=pq.ParquetWriter(path, BASE_SCHEMA, compression="zstd", compression_level=3)
    try:
        for left in range(start,end,NS):
            right=left+NS; halts=[h for h in context["halts"] if h["start_ns"] < right and h["end_ns"] > left]
            halt=bool(halts); qints=[tuple(x) for x in context["observation_intervals"]["quotes"]]; tints=[tuple(x) for x in context["observation_intervals"]["trades"]]
            qgaps=[tuple(x) for x in context["gaps"].get("quotes",[])]; tgaps=[tuple(x) for x in context["gaps"].get("trades",[])]
            qobs=max(0,_contains(qints,left,right)-_contains(qgaps,left,right)); tobs=max(0,_contains(tints,left,right)-_contains(tgaps,left,right))
            qbreaks=[x for x in context["instantaneous_breaks"].get("quotes",[]) if left<=x<right] + [a for a,b in qgaps if left<=a<right]
            tbreaks=[x for x in context["instantaneous_breaks"].get("trades",[]) if left<=x<right] + [a for a,b in tgaps if left<=a<right]
            quote_break_in_second=bool(qbreaks);trade_break_in_second=bool(tbreaks)
            halt_entry=halt and not was_halt
            if halt_entry:
                state=QuoteState(); last_trade=None; quote_cont+=1; trade_cont+=1; quote_broken=trade_broken=True
            qstatus=SourceStatus.ACCEPTED if _point_in(qints,right-1) and not _point_in(qgaps,right-1) else SourceStatus.UNAVAILABLE
            tstatus=SourceStatus.ACCEPTED if _point_in(tints,right-1) and not _point_in(tgaps,right-1) else SourceStatus.UNAVAILABLE
            # quote integration, transitions before same-time events
            cursor=left; bsum=asum=ssum=bisum=aisum=0.0; pdur=sdur=bidur=aidur=0
            while qnext is not None and qnext["sip_timestamp"] < right:
                t=qnext["sip_timestamp"]
                boundaries=sorted([x for x in qbreaks if cursor<=x<=t])
                for boundary in boundaries:
                    if not halt: bsum,asum,ssum,bisum,aisum,pdur,sdur,bidur,aidur=_integrate(state,cursor,boundary,bsum,asum,ssum,bisum,aisum,pdur,sdur,bidur,aidur,qints,qgaps)
                    state=QuoteState(); quote_cont+=1; quote_broken=True; cursor=boundary
                if not halt:
                    bsum,asum,ssum,bisum,aisum,pdur,sdur,bidur,aidur=_integrate(state,cursor,t,bsum,asum,ssum,bisum,aisum,pdur,sdur,bidur,aidur,qints,qgaps)
                    if not _point_in(qgaps,t):
                        _apply_quote(state,qnext,multiplier); quote_broken=False
                cursor=t; qnext=next(quote_events,None); qbreaks=[x for x in qbreaks if x>t]
            for boundary in sorted(x for x in qbreaks if cursor<=x<right):
                if not halt: bsum,asum,ssum,bisum,aisum,pdur,sdur,bidur,aidur=_integrate(state,cursor,boundary,bsum,asum,ssum,bisum,aisum,pdur,sdur,bidur,aidur,qints,qgaps)
                state=QuoteState(); quote_cont+=1; quote_broken=True; cursor=boundary
            if not halt: bsum,asum,ssum,bisum,aisum,pdur,sdur,bidur,aidur=_integrate(state,cursor,right,bsum,asum,ssum,bisum,aisum,pdur,sdur,bidur,aidur,qints,qgaps)
            count=0; total=0; dollars=0.0; uncertain=False
            pending_trade_breaks=iter(sorted(tbreaks));next_trade_break=next(pending_trade_breaks,None)
            while tnext is not None and tnext["sip_timestamp"] < right:
                while next_trade_break is not None and next_trade_break <= tnext["sip_timestamp"]:
                    last_trade=None;trade_cont+=1;trade_broken=True;next_trade_break=next(pending_trade_breaks,None)
                if not halt and not _point_in(tgaps,tnext["sip_timestamp"]):
                    kind,payload=_trade_class(tnext,config,start)
                    if kind=="uncertain": uncertain=True; last_trade=None
                    elif kind=="eligible":
                        price,quantity=payload; count+=1; total=add_share_units(total,shares_from_units(quantity)); dollars+=price*(quantity/1e9); last_trade=tnext["sip_timestamp"];trade_broken=False
                tnext=next(trade_events,None)
            while next_trade_break is not None:
                last_trade=None;trade_cont+=1;trade_broken=True;next_trade_break=next(pending_trade_breaks,None)
            if uncertain: count=total=dollars=None; activity=0
            else: activity=0 if halt else tobs
            if activity == 0:
                count=total=dollars=None
            midpoint_status=MidpointAgeStatus.UNOBSERVABLE
            if state.price_valid:
                midpoint_status=MidpointAgeStatus.KNOWN if state.last_change_ns is not None else MidpointAgeStatus.NO_CHANGE_OBSERVED
            row={"session_date":pair["session_date"],"symbol":pair["symbol"],"interval_end_ns":right,
                 "bid_twap_usd":bsum/(pdur/NS) if pdur else None,"ask_twap_usd":asum/(pdur/NS) if pdur else None,
                 "midpoint_twap_usd":((bsum/(pdur/NS))/2+(asum/(pdur/NS))/2) if pdur else None,
                 "price_valid_duration_ns":pdur,"price_twap_reason_mask":_mask_supported(pdur,halt=halt,status=qstatus,invalid=state.invalid_seen,broken=quote_broken),
                 "bid_end_usd":state.bid if state.price_valid and not halt else None,"ask_end_usd":state.ask if state.price_valid and not halt else None,
                 "price_end_reason_mask":_current_mask(state.price_valid and not halt,halt=halt,status=qstatus,broken=quote_broken),
                 "spread_integral_bps_seconds":ssum if sdur else None,"spread_valid_duration_ns":sdur,"spread_integral_reason_mask":_mask_supported(sdur,halt=halt,status=qstatus,invalid=state.invalid_seen,broken=quote_broken),
                 "bid_size_integral_shares_seconds":bisum if bidur else None,"bid_size_valid_duration_ns":bidur,"bid_size_integral_reason_mask":_mask_supported(bidur,halt=halt,status=qstatus,invalid=state.invalid_seen,broken=quote_broken),
                 "ask_size_integral_shares_seconds":aisum if aidur else None,"ask_size_valid_duration_ns":aidur,"ask_size_integral_reason_mask":_mask_supported(aidur,halt=halt,status=qstatus,invalid=state.invalid_seen,broken=quote_broken),
                 "bid_size_end_shares":state.bid_size if state.bid_size_valid and not halt else None,"ask_size_end_shares":state.ask_size if state.ask_size_valid and not halt else None,
                 "bid_size_end_reason_mask":_current_mask(state.bid_size_valid and not halt,halt=halt,status=qstatus,broken=quote_broken),"ask_size_end_reason_mask":_current_mask(state.ask_size_valid and not halt,halt=halt,status=qstatus,broken=quote_broken),
                 "trade_count_1s":count,"share_volume_1s":shares_from_units(total) if total is not None else None,"dollar_volume_1s_usd":dollars,"activity_valid_duration_ns":activity,
                 "activity_reason_mask":_mask_supported(activity,halt=halt,status=tstatus,invalid=uncertain,broken=trade_broken),
                 "trade_age_seconds":(right-last_trade)/NS if last_trade is not None and not halt else None,
                 "trade_age_reason_mask":_current_mask(last_trade is not None and not halt,halt=halt,status=tstatus,broken=trade_broken,missing_event=True),
                 "quote_age_seconds":(right-state.last_quote_ns)/NS if state.last_quote_ns is not None and not halt else None,
                 "quote_age_reason_mask":_current_mask(state.last_quote_ns is not None and not halt,halt=halt,status=qstatus,broken=quote_broken,missing_event=True),
                 "midpoint_change_age_seconds":(right-state.last_change_ns)/NS if midpoint_status==MidpointAgeStatus.KNOWN and not halt else None,
                 "midpoint_change_age_reason_mask":_current_mask(midpoint_status==MidpointAgeStatus.KNOWN and not halt,halt=halt,status=qstatus,broken=quote_broken,midpoint_bound=midpoint_status==MidpointAgeStatus.NO_CHANGE_OBSERVED),
                 "midpoint_age_status":int(midpoint_status if not halt else MidpointAgeStatus.UNOBSERVABLE),
                 "midpoint_observation_start_ns":state.origin_ns if midpoint_status!=MidpointAgeStatus.UNOBSERVABLE and not halt else None,
                 "midpoint_age_lower_bound_seconds":(right-state.origin_ns)/NS if midpoint_status==MidpointAgeStatus.NO_CHANGE_OBSERVED and not halt else None,
                 "quote_source_status":int(qstatus),"trade_source_status":int(tstatus),"quote_observed_duration_ns":qobs,"trade_observed_duration_ns":tobs,
                 "quote_continuity_id":quote_cont,"trade_continuity_id":trade_cont,"quote_continuity_break_in_second":bool(quote_break_in_second or halt_entry),"trade_continuity_break_in_second":bool(trade_break_in_second or halt_entry),
                 "halt_active":halt,"halt_id":halts[0]["id"] if halt else None}
            buffers.append(row)
            was_halt=halt
            if len(buffers)>=4096:
                batch=pa.RecordBatch.from_pylist(buffers,schema=BASE_SCHEMA); validate_batch(batch,"base"); writer.write_batch(batch); buffers=[]
        if buffers:
            batch=pa.RecordBatch.from_pylist(buffers,schema=BASE_SCHEMA); validate_batch(batch,"base"); writer.write_batch(batch)
    finally: writer.close()
    return context["coverage"]["expected_rows"]


def _integrate(state,a,b,bs,asks,ss,bis,ais,pd,sd,bid,aid,observed,gaps):
    duration=_contains(observed,a,b)-_contains(gaps,a,b)
    if duration<=0: return bs,asks,ss,bis,ais,pd,sd,bid,aid
    sec=duration/NS
    if state.price_valid:
        bs+=state.bid*sec; asks+=state.ask*sec; pd+=duration
        if state.spread_valid: ss+=10000*(state.ask-state.bid)/(state.bid/2+state.ask/2)*sec; sd+=duration
        if state.bid_size_valid: bis+=state.bid_size*sec; bid+=duration
        if state.ask_size_valid: ais+=state.ask_size*sec; aid+=duration
    return bs,asks,ss,bis,ais,pd,sd,bid,aid


def verify_base_partition(root):
    root=Path(root); manifest=read_json(root/"manifest.json")
    required={"manifest_version","member","coverage","inputs","source_units","contract_identity","contract_config","base_compatibility","implementation_identity","outputs","validation","complete"}
    if set(manifest)!=required or manifest["manifest_version"]!="tape_member_manifest_v1" or manifest["complete"] is not True:
        raise ContractError("invalid base manifest")
    records={r["path"]:r for r in manifest["outputs"]}
    if set(records)!={"base.parquet","context.json"}: raise ContractError("base companions missing")
    context_path=verify_output(root,records["context.json"]);context=read_json(context_path)
    stored_config=FeatureConfig.from_dict(manifest["contract_config"])
    if (context["member"]!=f'{manifest["member"]["session_date"]}/{manifest["member"]["symbol"]}'
            or context["coverage"]!=manifest["coverage"]
            or manifest["contract_identity"]!=contract_identity(stored_config)
            or records["context.json"]["schema_sha256"]!=digest(context)
            or manifest["implementation_identity"]!=_implementation_identity()):
        raise ContractError("base manifest/context/implementation mismatch")
    base=verify_output(root,records["base.parquet"])
    if (records["base.parquet"]["schema_sha256"]!=schema_hash(BASE_SCHEMA)
            or records["base.parquet"]["rows"]!=manifest["coverage"]["expected_rows"]
            or records["context.json"]["rows"]!=manifest["coverage"]["expected_rows"]):raise ContractError("base output declaration mismatch")
    pf=pq.ParquetFile(base)
    if not pf.schema_arrow.equals(BASE_SCHEMA,check_metadata=True) or pf.metadata.num_rows!=manifest["coverage"]["expected_rows"]:
        raise ContractError("base schema/row count mismatch")
    previous=None;first=None;last=None
    for batch in pf.iter_batches(batch_size=4096,use_threads=False):
        previous=validate_batch(batch,"base",previous_key=previous)
        if batch.num_rows:
            batch_first=(batch.column(0)[0].as_py(),batch.column(1)[0].as_py(),batch.column(2)[0].as_py())
            batch_last=(batch.column(0)[-1].as_py(),batch.column(1)[-1].as_py(),batch.column(2)[-1].as_py())
            if first is None:first=batch_first
            last=batch_last
    expected_first=(manifest["member"]["session_date"],manifest["member"]["symbol"],manifest["coverage"]["session_start_ns"]+NS)
    expected_last=(manifest["member"]["session_date"],manifest["member"]["symbol"],manifest["coverage"]["end_ns"])
    if first!=expected_first or last!=expected_last:raise ContractError("base coverage boundary mismatch")
    return manifest
