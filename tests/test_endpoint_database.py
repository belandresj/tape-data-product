from pathlib import Path
import json
import sys

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parent))
from test_endpoint_reader import _open, _write_partition  # noqa: E402

from tape_data_product.contracts.config import ContractError
from tape_data_product.contracts.config import canonical_json, digest
from tape_data_product.integrity import sha256_file, write_atomic_json
from tape_data_product.query import (
    build_endpoint_query_catalog,
    open_tape_database,
)
from tape_data_product.query import endpoint_database as endpoint_database_module
from tape_data_product.cli import main


def _catalog(tmp_path):
    partition = _write_partition(tmp_path)
    handle, _, _ = _open(tmp_path, partition)
    output = tmp_path / "query-catalog"
    result = build_endpoint_query_catalog(handle, output)
    roots = {
        "base": partition["base_root"],
        "features": partition["feature_root"],
    }
    return partition, output, result, roots


def test_scoped_database_preserves_threshold_boundaries_null_zero_and_all_sessions(tmp_path):
    partition, catalog, result, roots = _catalog(tmp_path)
    member = f"{partition['day']}/{partition['symbol']}"
    with open_tape_database(
        catalog,
        expected_identity=result["catalog_identity"],
        data_roots=roots,
        start_date=partition["day"],
        end_date=partition["day"],
        members=(member,),
    ) as database:
        assert database.selected_members == (member,)
        strict = database.sql(
            "SELECT interval_end_ns, midpoint_rms_5s_bps_hl30s "
            "FROM features WHERE midpoint_rms_5s_bps_hl30s > 2 "
            "ORDER BY interval_end_ns"
        ).fetchall()
        inclusive = database.sql(
            "SELECT count(*) FROM features "
            "WHERE midpoint_rms_5s_bps_hl30s >= 2"
        ).fetchone()[0]
        zero = database.sql(
            "SELECT count(*) FROM features "
            "WHERE midpoint_rms_5s_bps_hl30s = 0"
        ).fetchone()[0]
        unavailable = database.sql(
            "SELECT count(*) FROM features "
            "WHERE midpoint_rms_5s_bps_hl30s IS NULL "
            "AND midpoint_rms_5s_bps_hl30s_reason_mask <> 0"
        ).fetchone()[0]
        assert [value for _, value in strict] == [3.0, 4.0, 5.0, 6.0, 7.0]
        assert inclusive == 6
        assert zero == 1
        assert unavailable == 1
        assert database.sql(
            "SELECT session, count(*) FROM features GROUP BY session"
        ).fetchall() == [("premarket", 8)]

        fields = database.sql(
            "SELECT name, table_name, reason_mask, unit FROM feature_catalog"
        ).fetchall()
        assert len(fields) == 27
        assert (
            "midpoint_rms_5s_bps_hl30s",
            "features",
            "midpoint_rms_5s_bps_hl30s_reason_mask",
            "bps",
        ) in fields
        assert (
            "trade_age_seconds",
            "current_ages",
            "trade_age_reason_mask",
            "s",
        ) in fields


def test_current_ages_are_keyed_separately_and_feature_only_scan_skips_base(tmp_path):
    partition, catalog, _, roots = _catalog(tmp_path)
    with open_tape_database(catalog, data_roots=roots) as database:
        ages = database.sql(
            "SELECT midpoint_change_age_seconds, midpoint_change_age_reason_mask "
            "FROM current_ages ORDER BY interval_end_ns LIMIT 2"
        ).fetchall()
        assert ages == [(0.0, 0), (None, 256)]
        assert database.sql(
            "SELECT count(*) FROM features AS f "
            "JOIN current_ages AS a USING(session_date, symbol, interval_end_ns)"
        ).fetchone()[0] == 8

        plan = database.sql(
            "EXPLAIN SELECT midpoint_rms_5s_bps_hl30s FROM features"
        ).fetchone()[1]
        assert plan.count("Function:") == 1
        assert "midpoint_rms_5s_bps_hl30s" in plan
        assert "trade_age_seconds" not in plan
        tables = {
            row[0]
            for row in database.sql("SHOW TABLES").fetchall()
        }
        assert {"features", "current_ages", "members", "feature_catalog"}.issubset(tables)
        assert "support" not in tables

        feature_path = (
            partition["feature_root"]
            / partition["relative"]
            / "features.parquet"
        )
        with feature_path.open("ab") as stream:
            stream.write(b"changed-during-session")
        with pytest.raises(ContractError, match="changed during database session"):
            database.sql("SELECT count(*) FROM features")


def test_scope_and_input_failures_are_explicit(tmp_path):
    partition, catalog, result, roots = _catalog(tmp_path)
    with pytest.raises(ContractError, match="unexpected query catalog identity"):
        open_tape_database(
            catalog,
            expected_identity="0" * 64,
            data_roots=roots,
        )
    with pytest.raises(ContractError, match="absent completed members"):
        open_tape_database(
            catalog,
            data_roots=roots,
            members=(f"{partition['day']}/MISSING",),
        )
    with pytest.raises(ContractError, match="scope contains no completed members"):
        open_tape_database(
            catalog,
            data_roots=roots,
            start_date="2026-04-01",
            end_date="2026-04-02",
        )

    feature_path = (
        partition["feature_root"]
        / partition["relative"]
        / "features.parquet"
    )
    with feature_path.open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(ContractError, match="content identity mismatch"):
        open_tape_database(
            catalog,
            expected_identity=result["catalog_identity"],
            data_roots=roots,
        )


def test_query_result_bounds_arrow_batches_and_dataframe_materialization(tmp_path):
    _, catalog, _, roots = _catalog(tmp_path)
    with open_tape_database(catalog, data_roots=roots) as database:
        frame = database.sql(
            "SELECT interval_end_ns FROM features ORDER BY interval_end_ns LIMIT 2"
        ).df()
        assert len(frame) == 2
        with pytest.raises(ContractError, match="DataFrame limit"):
            database.sql("SELECT interval_end_ns FROM features").df(max_rows=3)
        batches = list(
            database.sql("SELECT interval_end_ns FROM features ORDER BY interval_end_ns")
            .arrow_batches(batch_size=3)
        )
        assert [batch.num_rows for batch in batches] == [3, 3, 2]
        with pytest.raises(ContractError, match="Arrow batch size"):
            list(database.sql("SELECT 1").arrow_batches(batch_size=25_001))


def test_streamed_export_records_sql_scope_source_and_rows(tmp_path, monkeypatch):
    partition, catalog, result, roots = _catalog(tmp_path)
    output = tmp_path / "export"
    query = (
        "SELECT symbol, session_date, endpoint_time, midpoint_rms_5s_bps_hl30s "
        "FROM features WHERE midpoint_rms_5s_bps_hl30s = 0;"
    )
    usage = type("Usage", (), {"free": 21 * 1024**3})()
    monkeypatch.setattr(
        "tape_data_product.query.endpoint_database.shutil.disk_usage",
        lambda _: usage,
    )
    with open_tape_database(
        catalog,
        expected_identity=result["catalog_identity"],
        data_roots=roots,
        start_date=partition["day"],
        end_date=partition["day"],
    ) as database:
        exported = database.export_parquet(query, output)

    manifest = json.loads((output / "manifest.json").read_text())
    scope = json.loads((output / "scope.json").read_text())
    assert manifest["result"]["rows"] == 1
    assert exported["result"]["rows"] == 1
    assert pq.ParquetFile(output / "results.parquet").metadata.num_rows == 1
    assert (output / "query.sql").read_text() == query + "\n"
    assert scope["start_date"] == partition["day"]
    assert scope["end_date"] == partition["day"]
    assert scope["members"] == [f"{partition['day']}/{partition['symbol']}"]
    assert scope["sessions"] == "all_represented"
    assert scope["source_identity"]["query_catalog_identity"] == result["catalog_identity"]


def test_cli_query_fields_preview_zero_match_and_empty_date(tmp_path, capsys):
    partition, catalog, result, roots = _catalog(tmp_path)
    common = [
        "endpoint-data",
        "query-fields",
        "--catalog",
        str(catalog),
        "--identity",
        result["catalog_identity"],
        "--base-root",
        str(roots["base"]),
        "--feature-root",
        str(roots["features"]),
        "--start-date",
        partition["day"],
        "--end-date",
        partition["day"],
    ]
    assert main(common) == 0
    fields = json.loads(capsys.readouterr().out)
    assert len(fields["fields"]) == 27
    assert {field["table_name"] for field in fields["fields"]} == {
        "features",
        "current_ages",
    }

    sql_file = tmp_path / "zero.sql"
    sql_file.write_text(
        "SELECT symbol, endpoint_time FROM features "
        "WHERE midpoint_rms_5s_bps_hl30s > 1000000"
    )
    query_args = common.copy()
    query_args[1] = "sql"
    query_args.extend(["--sql-file", str(sql_file), "--preview-limit", "3"])
    assert main(query_args) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["displayed_rows"] == 0
    assert preview["truncated"] is False

    empty_args = query_args.copy()
    start_index = empty_args.index("--start-date") + 1
    end_index = empty_args.index("--end-date") + 1
    empty_args[start_index] = "2026-04-01"
    empty_args[end_index] = "2026-04-01"
    assert main(empty_args) == 2
    assert "scope contains no completed members" in capsys.readouterr().err


def test_date_scope_does_not_touch_unrelated_member_files(tmp_path):
    first = _write_partition(tmp_path, day="2026-03-09", symbol="ONE")
    second = _write_partition(tmp_path, day="2026-03-10", symbol="TWO")
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()
    first_handle, _, _ = _open(first_root, first)
    second_handle, _, _ = _open(second_root, second)
    first_catalog = tmp_path / "first-catalog"
    second_catalog = tmp_path / "second-catalog"
    build_endpoint_query_catalog(first_handle, first_catalog)
    build_endpoint_query_catalog(second_handle, second_catalog)

    records = []
    for catalog in (first_catalog, second_catalog):
        records.extend(
            json.loads(line)
            for line in (catalog / "members.jsonl").read_text().splitlines()
        )
    members_path = first_catalog / "members.jsonl"
    members_path.write_text("".join(canonical_json(record) + "\n" for record in records))
    members_sha, members_bytes = sha256_file(members_path)
    manifest = json.loads((first_catalog / "manifest.json").read_text())
    manifest["members"] = {
        "path": "members.jsonl",
        "sha256": members_sha,
        "bytes": members_bytes,
        "count": 2,
        "rows_per_table": 16,
    }
    manifest["validation"]["rows"] = 16
    manifest.pop("catalog_identity")
    manifest["catalog_identity"] = digest(manifest)
    write_atomic_json(first_catalog / "manifest.json", manifest)

    unrelated = second["feature_root"] / second["relative"] / "features.parquet"
    unrelated.unlink()
    with open_tape_database(
        first_catalog,
        expected_identity=manifest["catalog_identity"],
        data_roots={"base": first["base_root"], "features": first["feature_root"]},
        start_date="2026-03-09",
        end_date="2026-03-09",
    ) as database:
        assert database.selected_members == ("2026-03-09/ONE",)
        assert database.sql("SELECT DISTINCT symbol FROM features").fetchall() == [
            ("ONE",)
        ]


def test_scoped_database_applies_explicit_threads_and_spill_bound(tmp_path, monkeypatch):
    _, catalog, result, roots = _catalog(tmp_path)
    scratch = tmp_path / "duckdb-scratch"
    monkeypatch.setattr(
        endpoint_database_module.shutil,
        "disk_usage",
        lambda _path: type("Usage", (), {"free": 21 * 1024**3})(),
    )
    with open_tape_database(
        catalog,
        expected_identity=result["catalog_identity"],
        data_roots=roots,
        threads=2,
        memory_limit="384MiB",
        temp_directory=scratch,
        max_temp_directory_size="2GiB",
    ) as database:
        assert database.sql("SELECT current_setting('threads')").fetchone()[0] == 2
        assert database.sql("SELECT current_setting('memory_limit')").fetchone()[0]
        assert database.sql(
            "SELECT current_setting('max_temp_directory_size')"
        ).fetchone()[0]

    with pytest.raises(ContractError, match="requires an explicit temp_directory"):
        open_tape_database(
            catalog,
            expected_identity=result["catalog_identity"],
            data_roots=roots,
            max_temp_directory_size="2GiB",
        )
