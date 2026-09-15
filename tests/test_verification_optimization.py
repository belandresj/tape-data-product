from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import tape_data_product.features.endpoint_ew as endpoint_module
from tape_data_product.calculate_runtime import run_members
from tape_data_product.cli import parser
from tape_data_product.features.endpoint_ew import (
    build_from_base,
    verify_member_partitions,
)
from tape_data_product.integrity import sha256_file, write_atomic_json
from tape_data_product.replay.builder import build_base_partition
from test_endpoint_pipeline import fixture


def _member_outputs(root, symbol="SYN", seconds=10):
    source=root/"source"
    source.mkdir(parents=True)
    pair,context=fixture(source,seconds,symbol=symbol)
    suffix=f"session_date=2026-09-02/symbol={symbol}"
    base=root/"base"/suffix
    features=root/"features"/suffix
    build_base_partition(pair,context,base)
    build_from_base(base,features)
    return pair,context,base,features


def _refresh_output_record(root,name):
    manifest=json.loads((root/"manifest.json").read_text())
    record=next(item for item in manifest["outputs"] if item["path"]==name)
    record["sha256"],record["bytes"]=sha256_file(root/name)
    write_atomic_json(root/"manifest.json",manifest)


def test_combined_verifier_passes_and_scans_each_table_once(tmp_path,monkeypatch):
    _,_,base,features=_member_outputs(tmp_path)
    calls={"base":0,"features":0,"support":0}
    original=endpoint_module.validate_batch

    def counted(batch,kind,**kwargs):
        calls[kind]+=1
        return original(batch,kind,**kwargs)

    monkeypatch.setattr(endpoint_module,"validate_batch",counted)
    result=verify_member_partitions(base,features)
    assert result["base"]["coverage"]["expected_rows"]==10
    assert calls=={"base":1,"features":1,"support":1}


def test_combined_verifier_rejects_corruption_and_columnar_key_mismatch(tmp_path):
    _,_,base,features=_member_outputs(tmp_path/"corrupt")
    with (features/"support.parquet").open("ab") as handle:
        handle.write(b"corrupt")
    with pytest.raises(ValueError,match="output identity mismatch"):
        verify_member_partitions(base,features)

    _,_,base,features=_member_outputs(tmp_path/"keys")
    support=pq.ParquetFile(features/"support.parquet").read()
    symbols=pa.array(["ALT"]*support.num_rows,type=support.schema.field("symbol").type)
    support=support.set_column(support.schema.get_field_index("symbol"),support.schema.field("symbol"),symbols)
    pq.write_table(support,features/"support.parquet")
    _refresh_output_record(features,"support.parquet")
    with pytest.raises(ValueError,match="base/feature/support key mismatch"):
        verify_member_partitions(base,features)


def test_combined_verification_cli_dispatch(tmp_path):
    _,_,base,features=_member_outputs(tmp_path)
    arguments=parser().parse_args([
        "calculate","verify-member","--base",str(base),"--features",str(features)])
    result=arguments.func(arguments)
    assert result["integrity"]=="passed"
    assert result["row_validation"]=="passed"
    assert result["key_alignment"]=="passed"


def test_multi_member_runner_records_verified_and_verification_failed(tmp_path):
    pairs={}
    for symbol in ("SYN","ALT"):
        pair,context,base,features=_member_outputs(tmp_path/symbol,symbol=symbol)
        pairs[f"2026-09-02/{symbol}"]=(str(pair),str(context))
    plan={
        "members":[{"session_date":"2026-09-02","symbol":"SYN"},
                   {"session_date":"2026-09-02","symbol":"ALT"}],
        "base_root":str(tmp_path/"run-base"),
        "feature_root":str(tmp_path/"run-features"),
        "ledger_path":str(tmp_path/"ledger.sqlite"),
        "config":endpoint_module.DEFAULT_CONFIG.to_dict(),
        "limits":{"workers":1,"batch_size":7,"disk_reserve_bytes":0,
                  "scratch_cap_bytes":0,"runtime_max_seconds":60},
    }
    for symbol in ("SYN","ALT"):
        source=tmp_path/symbol
        suffix=Path("session_date=2026-09-02")/f"symbol={symbol}"
        target_base=Path(plan["base_root"])/suffix
        target_features=Path(plan["feature_root"])/suffix
        target_base.parent.mkdir(parents=True,exist_ok=True)
        target_features.parent.mkdir(parents=True,exist_ok=True)
        (source/"base"/suffix).rename(target_base)
        (source/"features"/suffix).rename(target_features)

    ledger=sqlite3.connect(tmp_path/"ledger.sqlite")
    ledger.execute("CREATE TABLE members (member TEXT PRIMARY KEY,status TEXT NOT NULL,base_manifest TEXT,feature_manifest TEXT,error TEXT,updated_ns INTEGER NOT NULL,verification_status TEXT NOT NULL DEFAULT 'not_run')")
    ledger.execute("CREATE TABLE run_attempts (attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,plan_sha256 TEXT NOT NULL,runner_identity TEXT NOT NULL,start_method TEXT NOT NULL,workers INTEGER NOT NULL,scheduling TEXT NOT NULL,started_ns INTEGER NOT NULL)")
    result=run_members(plan,pairs,"a"*64,ledger,time.monotonic())
    assert result["built_members"]==result["verified_members"]==2
    assert result["verification_failed_members"]==0
    bad=Path(plan["feature_root"])/"session_date=2026-09-02"/"symbol=ALT"/"support.parquet"
    with bad.open("ab") as handle:
        handle.write(b"corrupt")
    with pytest.raises(ValueError,match="member verification failed"):
        run_members(plan,pairs,"a"*64,ledger,time.monotonic())
    rows=dict(ledger.execute(
        "SELECT member,status FROM members ORDER BY member").fetchall())
    manifests=ledger.execute(
        "SELECT base_manifest,feature_manifest FROM members WHERE member=?",
        ("2026-09-02/ALT",)).fetchone()
    ledger.close()
    assert rows=={"2026-09-02/ALT":"verification_failed",
                  "2026-09-02/SYN":"complete"}
    check=sqlite3.connect(tmp_path/"ledger.sqlite")
    verification=dict(check.execute(
        "SELECT member,verification_status FROM members").fetchall())
    check.close()
    assert verification=={"2026-09-02/ALT":"failed",
                         "2026-09-02/SYN":"passed"}
    assert all(manifests)
    assert Path(manifests[0]).is_file() and Path(manifests[1]).is_file()
