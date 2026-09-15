"""Prepare identity-bound descriptors for the authorized KDP/NVDA real prefix.

This reads retained local objects only. It records, but does not cure, the absence
of original vendor-pagination receipts and never downloads or rewrites raw data.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow.parquet as pq

from tape_data_product.calculate import _transfer_summary, _validate_transfer_completion
from tape_data_product.contracts.config import digest
from tape_data_product.contracts.policy import NS, session_bounds
from tape_data_product.integrity import read_json, sha256_file, write_atomic_json
from tape_data_product.replay.admission import HISTORICAL_RETRIEVAL_UNVERIFIED


DAY = "2026-09-02"
SYMBOLS = ("KDP", "NVDA")
PREFIX_SECONDS = 720


def _arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--completion", type=Path, required=True)
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--head-root", type=Path, required=True)
    parser.add_argument("--halt-evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def _json_hash(path: Path) -> str:
    return sha256_file(path)[0]


def main() -> int:
    args = _arguments()
    if args.output.exists():
        raise FileExistsError(args.output)
    manifest_sha = sha256_file(args.manifest)[0]
    _, objects, byte_total, records, _, states = _transfer_summary(args.manifest)
    _validate_transfer_completion(read_json(args.completion), manifest_sha, objects, byte_total, states)
    start, _ = session_bounds(DAY); end = start + PREFIX_SECONDS * NS
    evidence_root = args.output / "evidence"
    findings = {}; inventory = []
    for symbol in SYMBOLS:
        member = f"{DAY}/{symbol}"; member_evidence = evidence_root / symbol
        streams = {}
        for stream in ("quotes", "trades"):
            record = records[(member, stream)]; raw_path = args.raw_root / record["relative_path"]
            if (sha256_file(raw_path)[0] != record["sha256"] or raw_path.stat().st_size != record["size_bytes"]
                    or pq.ParquetFile(raw_path).metadata.num_rows != record["rows"]):
                raise ValueError(f"raw identity mismatch: {member}/{stream}")
            head_path = args.head_root / f"{symbol}-{stream}-head.json"; head = read_json(head_path)
            if (head.get("key") != record["key"] or head.get("bytes") != record["size_bytes"]
                    or head.get("metadata", {}).get("sha256") != record["sha256"]
                    or int(head.get("metadata", {}).get("rows", -1)) != record["rows"]):
                raise ValueError(f"R2 HEAD identity mismatch: {member}/{stream}")
            provenance_path = member_evidence / f"{stream}-provenance.json"
            provenance = {"version":"historical_object_provenance_v1","member":member,"stream":stream,
                          "object_sha256":record["sha256"],"r2_head_sha256":_json_hash(head_path),
                          "provider":"massive","method":"rest",
                          "retrieval_completeness":HISTORICAL_RETRIEVAL_UNVERIFIED}
            if stream == "quotes":
                provenance["quote_size_unit_basis"] = {
                    "unit":"shares","effective_from":"2025-11-03",
                    "url":"https://www.massive.com/blog/change-stocks-quotes-round-lots-to-shares"}
            write_atomic_json(provenance_path, provenance)
            coverage_path = member_evidence / f"{stream}-coverage.json"
            coverage = {"version":"source_coverage_v2","member":member,"stream":stream,
                        "intervals":[[start,end]],"terminal_complete":False,
                        "retrieval_completeness":HISTORICAL_RETRIEVAL_UNVERIFIED}
            write_atomic_json(coverage_path, coverage)
            streams[stream] = {"path":record["relative_path"],"sha256":record["sha256"],
                "bytes":record["size_bytes"],"rows":record["rows"],
                "schema_sha256":digest({"schema":str(pq.ParquetFile(raw_path).schema_arrow)}),
                "clock":"sip_timestamp_utc_ns","provenance_path":str(provenance_path.relative_to(evidence_root)),
                "provenance_sha256":_json_hash(provenance_path),
                "coverage_evidence_path":str(coverage_path.relative_to(evidence_root)),
                "coverage_evidence_sha256":_json_hash(coverage_path),"terminal_complete":False,
                "retrieval_completeness":HISTORICAL_RETRIEVAL_UNVERIFIED}
        quote_units_path = member_evidence / "quote-units.json"
        write_atomic_json(quote_units_path,{"version":"source_units_v1","member":member,"stream":"quotes",
            "object_sha256":streams["quotes"]["sha256"],"unit":"shares","multiplier":1})
        trade_units_path = member_evidence / "trade-units.json"
        write_atomic_json(trade_units_path,{"version":"source_units_v1","member":member,"stream":"trades",
            "object_sha256":streams["trades"]["sha256"],"quantity_precedence":"decimal_size_then_size","scale":9})
        halt_path = member_evidence / "halts.json"
        write_atomic_json(halt_path,{"version":"historical_halt_context_v1","member":member,"status":"verified_empty",
            "halts":[],"source_evidence_sha256":_json_hash(args.halt_evidence)})
        continuity_path = member_evidence / "continuity.json"
        write_atomic_json(continuity_path,{"version":"source_continuity_v1","member":member,
            "gaps":{"quotes":[],"trades":[]},"instantaneous_breaks":{"quotes":[],"trades":[]}})
        pair = {"version":"tape_source_pair_v2","symbol":symbol,"session_date":DAY,"currency":"USD",
            "adapter":"massive_canonical_tq_v1","root":str(args.raw_root.resolve()),
            "evidence_root":str(evidence_root.resolve()),"streams":streams,
            "source_units":{"quote_size_unit":"shares","quote_size_evidence_sha256":_json_hash(quote_units_path),
                "quote_size_evidence_path":str(quote_units_path.relative_to(evidence_root)),
                "trade_quantity_evidence_sha256":_json_hash(trade_units_path),
                "trade_quantity_evidence_path":str(trade_units_path.relative_to(evidence_root)),"round_lot_shares":None}}
        context = {"version":"tape_member_context_v1","member":member,
            "coverage":{"kind":"prefix","session_start_ns":start,"end_ns":end,"expected_rows":PREFIX_SECONDS},
            "observation_intervals":{"quotes":[[start,end]],"trades":[[start,end]]},
            "gaps":{"quotes":[],"trades":[]},"instantaneous_breaks":{"quotes":[],"trades":[]},
            "halts":[],"halt_evidence":{"status":"verified_empty","path":str(halt_path.relative_to(evidence_root)),"sha256":_json_hash(halt_path)},
            "seed":{"basis":"unavailable"},
            "continuity_evidence":{"path":str(continuity_path.relative_to(evidence_root)),"sha256":_json_hash(continuity_path)},
            "selection":{"basis":"recovered_historical_job_list","source_evidence_sha256":_json_hash(args.halt_evidence)},
            "discovery":{"eligibility_basis":"nominal_historical","receipt_known_at":"unavailable"}}
        pair_path=args.output/f"{symbol}-source-pair.json";context_path=args.output/f"{symbol}-context.json"
        write_atomic_json(pair_path,pair);write_atomic_json(context_path,context)
        inventory.append({"session_date":DAY,"symbol":symbol})
        findings[member]={"quote_units":True,"trade_representation":True,"terminal_coverage":False,
            "historical_retrieval_completeness":HISTORICAL_RETRIEVAL_UNVERIFIED,"halt_context":True,
            "continuity":True,"source_pair_path":str(pair_path.resolve()),"member_context_path":str(context_path.resolve())}
    write_atomic_json(args.output/"inventory.json",{"members":inventory})
    write_atomic_json(args.output/"admission-evidence.json",{"members":findings})
    write_atomic_json(args.output/"preparation.json",{"status":"prepared","sample":[f"{DAY}/{s}" for s in SYMBOLS],
        "prefix_seconds":PREFIX_SECONDS,"manifest_sha256":manifest_sha,"completion_sha256":_json_hash(args.completion),
        "retrieval_completeness":"unverified","network_reads":0})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
