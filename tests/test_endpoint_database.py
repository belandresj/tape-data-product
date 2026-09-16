from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from test_endpoint_reader import _open, _write_partition  # noqa: E402

from tape_data_product.contracts.config import ContractError
from tape_data_product.query import (
    build_endpoint_query_catalog,
    open_tape_database,
)


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
