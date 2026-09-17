from copy import deepcopy
import json
from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tape_data_product.contracts import DEFAULT_CONFIG, contract_identity
from tape_data_product.contracts.config import ContractError, canonical_json, digest
from tape_data_product.contracts.policy import session_bounds
from tape_data_product.contracts.reasons import Reason
from tape_data_product.contracts.schemas import (
    BASE_SCHEMA,
    feature_schema,
    schema_hash,
    support_schema,
)
from tape_data_product.integrity import output_record, sha256_file, write_atomic_json
from tape_data_product.query import (
    EndpointSelection,
    describe_endpoint_fields,
    iter_endpoint_batches,
    open_endpoint_reference,
)
from tape_data_product.query import endpoint_release
from tape_data_product.query.endpoint_release import select_pilot_members
from tape_data_product.query.endpoint_selection import NS, selected_ranges, session_ranges


def _literal_table(schema, day, symbol, endpoints):
    values = {}
    for field in schema:
        if field.name == "session_date":
            values[field.name] = [day] * len(endpoints)
        elif field.name == "symbol":
            values[field.name] = [symbol] * len(endpoints)
        elif field.name == "interval_end_ns":
            values[field.name] = endpoints
        elif field.name.endswith("_reason_mask"):
            values[field.name] = [int(Reason.SOURCE_UNAVAILABLE | Reason.NO_SUPPORTED_DATA)] * len(endpoints)
        elif field.name.endswith("_source_status"):
            values[field.name] = [2] * len(endpoints)
        elif field.name == "midpoint_age_status":
            values[field.name] = [0] * len(endpoints)
        elif field.name == "quote_continuity_id":
            values[field.name] = [7 if index < 3 else 8 for index in range(len(endpoints))]
        elif field.name == "trade_continuity_id":
            values[field.name] = [20 if index < 5 else 21 for index in range(len(endpoints))]
        elif field.name.endswith("_duration_ns"):
            values[field.name] = [0] * len(endpoints)
        elif field.name == "quote_continuity_break_in_second":
            values[field.name] = [index == 3 for index in range(len(endpoints))]
        elif field.name == "trade_continuity_break_in_second":
            values[field.name] = [index == 5 for index in range(len(endpoints))]
        elif field.name == "halt_active":
            values[field.name] = [False] * len(endpoints)
        elif not field.nullable:
            values[field.name] = [0] * len(endpoints)
        else:
            values[field.name] = [None] * len(endpoints)
    return pa.Table.from_pydict(values, schema=schema)


def _write_partition(root, day="2026-03-09", symbol="SYN", rows=8, row_groups=(1, 7, 2)):
    base_root, feature_root = root / "base", root / "features"
    relative = Path(f"session_date={day}") / f"symbol={symbol}"
    base, feature = base_root / relative, feature_root / relative
    base.mkdir(parents=True)
    feature.mkdir(parents=True)
    start = session_ranges(day, ("premarket",))[0][0]
    endpoints = [start + index * NS for index in range(rows)]
    base_table = _literal_table(BASE_SCHEMA, day, symbol, endpoints)
    feature_table = _literal_table(feature_schema(), day, symbol, endpoints)
    support_table = _literal_table(support_schema(), day, symbol, endpoints)

    midpoint = base_table.schema.get_field_index("midpoint_change_age_seconds")
    midpoint_mask = base_table.schema.get_field_index("midpoint_change_age_reason_mask")
    midpoint_status = base_table.schema.get_field_index("midpoint_age_status")
    midpoint_origin = base_table.schema.get_field_index("midpoint_observation_start_ns")
    midpoint_bound = base_table.schema.get_field_index("midpoint_age_lower_bound_seconds")
    columns = list(base_table.columns)
    columns[midpoint] = pa.array([0.0, None] + [None] * (rows - 2), type=pa.float64())
    columns[midpoint_mask] = pa.array([0, int(Reason.MIDPOINT_AGE_LOWER_BOUND_ONLY)] + [int(Reason.SOURCE_UNAVAILABLE | Reason.NO_SUPPORTED_DATA)] * (rows - 2), type=pa.uint16())
    columns[midpoint_status] = pa.array([2, 1] + [0] * (rows - 2), type=pa.uint8())
    columns[midpoint_origin] = pa.array([endpoints[0] - NS, endpoints[1] - 2 * NS] + [None] * (rows - 2), type=pa.int64())
    columns[midpoint_bound] = pa.array([None, 2.0] + [None] * (rows - 2), type=pa.float64())
    base_table = pa.Table.from_arrays(columns, schema=BASE_SCHEMA)

    rms = feature_table.schema.get_field_index("midpoint_rms_5s_bps_hl30s")
    rms_mask = feature_table.schema.get_field_index("midpoint_rms_5s_bps_hl30s_reason_mask")
    other = feature_table.schema.get_field_index("quoted_spread_bps_hl30s")
    other_mask = feature_table.schema.get_field_index("quoted_spread_bps_hl30s_reason_mask")
    columns = list(feature_table.columns)
    columns[rms] = pa.array([0.0, None] + [float(index) for index in range(2, rows)])
    columns[rms_mask] = pa.array([0, int(Reason.STARTUP)] + [0] * (rows - 2), type=pa.uint16())
    columns[other] = pa.array([None] * rows, type=pa.float64())
    columns[other_mask] = pa.array([int(Reason.STARTUP)] * rows, type=pa.uint16())
    feature_table = pa.Table.from_arrays(columns, schema=feature_schema())

    pq.write_table(base_table, base / "base.parquet", row_group_size=row_groups[0])
    pq.write_table(feature_table, feature / "features.parquet", row_group_size=row_groups[1])
    pq.write_table(support_table, feature / "support.parquet", row_group_size=row_groups[2])
    context = {
        "member": f"{day}/{symbol}",
        "coverage": {
            "kind": "prefix",
            "session_start_ns": endpoints[0] - NS,
            "end_ns": endpoints[-1],
            "expected_rows": rows,
        },
        "discovery": {
            "eligibility_basis": "nominal_historical",
            "receipt_known_at": "unavailable",
        },
    }
    write_atomic_json(base / "context.json", context)
    base_outputs = [
        output_record(base / "base.parquet", rows=rows, schema_sha256=schema_hash(BASE_SCHEMA)),
        output_record(base / "context.json", rows=rows, schema_sha256=digest(context)),
    ]
    base_manifest = {
        "manifest_version": "tape_member_manifest_v1",
        "member": {"session_date": day, "symbol": symbol},
        "coverage": context["coverage"],
        "inputs": {},
        "source_units": {},
        "contract_identity": contract_identity(),
        "contract_config": DEFAULT_CONFIG.to_dict(),
        "base_compatibility": {"sha256": "compat"},
        "implementation_identity": {"sha256": "base-producer"},
        "outputs": base_outputs,
        "validation": {"integrity": "passed"},
        "complete": True,
    }
    write_atomic_json(base / "manifest.json", base_manifest)
    base_manifest_sha, base_manifest_bytes = sha256_file(base / "manifest.json")
    feature_outputs = [
        output_record(feature / "features.parquet", rows=rows, schema_sha256=schema_hash(feature_schema())),
        output_record(feature / "support.parquet", rows=rows, schema_sha256=schema_hash(support_schema())),
    ]
    feature_manifest = {
        "manifest_version": "tape_member_manifest_v1",
        "member": {"session_date": day, "symbol": symbol},
        "coverage": context["coverage"],
        "inputs": {"base_manifest_sha256": base_manifest_sha},
        "source_units": {},
        "contract_identity": contract_identity(),
        "contract_config": DEFAULT_CONFIG.to_dict(),
        "implementation_identity": {"sha256": "feature-producer"},
        "outputs": feature_outputs,
        "validation": {"integrity": "passed"},
        "complete": True,
    }
    write_atomic_json(feature / "manifest.json", feature_manifest)
    feature_manifest_sha, feature_manifest_bytes = sha256_file(feature / "manifest.json")
    return {
        "base_root": base_root,
        "feature_root": feature_root,
        "relative": str(relative),
        "day": day,
        "symbol": symbol,
        "endpoints": endpoints,
        "coverage": context["coverage"],
        "context": context,
        "base_manifest": {
            "sha256": base_manifest_sha,
            "bytes": base_manifest_bytes,
            "outputs": {row["path"]: row for row in base_outputs},
            "implementation_identity": "base-producer",
        },
        "feature_manifest": {
            "sha256": feature_manifest_sha,
            "bytes": feature_manifest_bytes,
            "outputs": {row["path"]: row for row in feature_outputs},
            "implementation_identity": "feature-producer",
            "consumed_base_manifest_sha256": base_manifest_sha,
        },
    }


def _write_reference(root, partition):
    reference = root / "reference"
    reference.mkdir()
    member = {
        "member": f"{partition['day']}/{partition['symbol']}",
        "session_date": partition["day"],
        "symbol": partition["symbol"],
        "base_path": partition["relative"],
        "feature_path": partition["relative"],
        "coverage": partition["coverage"],
        "lineage": {
            "selection": {"basis": "test"},
            "discovery": partition["context"]["discovery"],
            "halt_evidence": {"status": "synthetic"},
            "continuity_evidence": {"status": "synthetic"},
            "source_streams": {"quotes": {}, "trades": {}},
        },
        "base_manifest": partition["base_manifest"],
        "feature_manifest": partition["feature_manifest"],
    }
    members = reference / "members.jsonl"
    members.write_text(canonical_json(member) + "\n")
    members_sha, members_bytes = sha256_file(members)
    manifest = {
        "version": "endpoint_ew_reference_v1",
        "reference_kind": "synthetic",
        "synthetic": True,
        "release": {
            "contract_identity": contract_identity(),
            "base_implementation_identity": partition["base_manifest"][
                "implementation_identity"
            ],
            "feature_implementation_identity": partition["feature_manifest"][
                "implementation_identity"
            ],
        },
        "contract_config": DEFAULT_CONFIG.to_dict(),
        "schemas": {
            "base": schema_hash(BASE_SCHEMA),
            "features": schema_hash(feature_schema()),
            "support": schema_hash(support_schema()),
        },
        "members": {
            "path": "members.jsonl",
            "sha256": members_sha,
            "bytes": members_bytes,
            "count": 1,
            "rows_per_table": partition["coverage"]["expected_rows"],
        },
    }
    manifest["reference_identity"] = digest(manifest)
    write_atomic_json(reference / "manifest.json", manifest)
    return reference, manifest["reference_identity"]


def _open(root, partition):
    reference, identity = _write_reference(root, partition)
    handle = open_endpoint_reference(
        reference,
        expected_identity=identity,
        data_roots={"base": partition["base_root"], "features": partition["feature_root"]},
    )
    return handle, reference, identity


def test_registry_maps_all_27_measurements_and_age_mask_exceptions():
    rows = describe_endpoint_fields(DEFAULT_CONFIG)
    actual = [
        (r["name"], r["table"], r["reason_mask"], r["unit"], r["family"],
         r["half_life_seconds"], r["window_seconds"], tuple(r["sources"]),
         tuple(r["support_dependencies"]), tuple(r["inspection_dependencies"]))
        for r in rows
    ]
    expected = []
    ew = (
        ("midpoint_rms_5s_bps", "bps", "return", "quote", "return"),
        ("movement_participation", "1", "participation", "quote", "return"),
        ("quoted_spread_bps", "bps", "spread", "quote", "spread"),
        ("trade_rate_per_second", "trades/s", "activity", "trade", "activity"),
        ("share_rate_per_second", "shares/s", "activity", "trade", "activity"),
        ("dollar_rate_usd_per_second", "USD/s", "activity", "trade", "activity"),
        ("bid_size_mean_shares", "shares", "bid_size", "quote", "bid_size"),
        ("ask_size_mean_shares", "shares", "ask_size", "quote", "ask_size"),
        ("midpoint_rms_5s_to_spread", "1", "ratio", "quote", "ratio"),
    )
    for half_life in (30, 120):
        for stem, unit, family, source, support_family in ew:
            name = f"{stem}_hl{half_life}s"
            if support_family == "return":
                support = (f"return_usable_weight_hl{half_life}s", f"return_possible_weight_hl{half_life}s")
            elif support_family == "ratio":
                support = (
                    f"return_usable_weight_hl{half_life}s", f"return_possible_weight_hl{half_life}s",
                    f"spread_usable_exposure_seconds_hl{half_life}s", f"spread_possible_exposure_seconds_hl{half_life}s",
                )
            else:
                support = (
                    f"{support_family}_usable_exposure_seconds_hl{half_life}s",
                    f"{support_family}_possible_exposure_seconds_hl{half_life}s",
                )
            expected.append((name, "features", name + "_reason_mask", unit, family,
                             half_life, None, (source,), support + (f"{source}_ew_startup_elapsed_seconds",), ()))
    for window in (60, 300):
        for stem, source in (("trade", "trade"), ("quote", "quote"), ("midpoint_change", "quote")):
            name = f"{stem}_age_p90_seconds_window{window}s"
            expected.append((name, "features", name + "_reason_mask", "s", stem + "_age",
                             None, window, (source,),
                             (f"{stem}_age_sample_count_window{window}s", f"{stem}_age_elapsed_slots_window{window}s"), ()))
    expected.extend((
        ("trade_age_seconds", "base", "trade_age_reason_mask", "s", "trade_current_age", None, None, ("trade",), (), ()),
        ("quote_age_seconds", "base", "quote_age_reason_mask", "s", "quote_current_age", None, None, ("quote",), (), ()),
        ("midpoint_change_age_seconds", "base", "midpoint_change_age_reason_mask", "s", "midpoint_change_current_age", None, None, ("quote",), (),
         ("midpoint_age_status", "midpoint_observation_start_ns", "midpoint_age_lower_bound_seconds")),
    ))
    assert actual == expected
    assert all(row["value_column"] == row["name"] for row in rows)


def test_session_and_discovery_boundaries_are_literal_and_date_aware():
    summer = session_ranges("2026-07-01", ("premarket", "rth", "after_hours"))
    winter = session_ranges("2026-03-09", ("premarket", "rth", "after_hours"))
    assert len(summer) == len(winter) == 3
    assert summer[0][0] - NS == 1_782_892_800_000_000_000
    assert winter[0][0] - NS == 1_773_043_200_000_000_000
    rth = session_ranges("2026-07-01", ("rth",))[0]
    assert rth[1] - rth[0] + NS == 23_400 * NS
    selection = EndpointSelection("nominal_post_discovery")
    discovery = {
        "nominal_discovery_endpoint_ns": rth[0] + NS // 2,
        "nominal_provenance": "validated:test",
    }
    assert selected_ranges("2026-07-01", selection, discovery)[0][0] == rth[0] + NS
    receipt = EndpointSelection("receipt_post_discovery")
    clocks = {
        "nominal_discovery_endpoint_ns": rth[0] + NS,
        "receipt_known_at_ns": rth[0] + NS // 2,
        "nominal_provenance": "n",
        "receipt_provenance": "r",
    }
    assert selected_ranges("2026-07-01", receipt, clocks)[0][0] == rth[0] + 2 * NS
    clocks["receipt_known_at_ns"] = rth[0] + 2 * NS
    assert selected_ranges("2026-07-01", receipt, clocks)[0][0] == rth[0] + 3 * NS


def test_nominal_discovery_requires_provenance_even_when_clock_is_present():
    selection = EndpointSelection("nominal_post_discovery")
    with pytest.raises(ContractError, match="nominal discovery provenance unavailable"):
        selected_ranges("2026-07-01", selection, {"nominal_discovery_endpoint_ns": 1})


def test_pilot_selection_is_shuffle_invariant_and_records_repeat_fallback():
    records = []
    for month in range(3, 9):
        for index in range(8):
            day = f"2026-{month:02d}-{index + 1:02d}"
            records.append(
                {
                    "session_date": day,
                    "symbol": "REPEAT",
                    "event_count": index // 2,
                }
            )
    forward = select_pilot_members(records)
    reverse = select_pilot_members(list(reversed(records)))
    assert [(r["session_date"], r["symbol"]) for r in forward] == [
        (r["session_date"], r["symbol"]) for r in reverse
    ]
    assert len(forward) == 24
    assert any(row["symbol_repeat_fallback"] for row in forward)


def test_pilot_selection_fails_missing_stratum():
    rows = [
        {"session_date": f"2026-{month:02d}-09", "symbol": f"S{month}", "event_count": 1}
        for month in range(3, 9)
    ]
    with pytest.raises(ContractError, match="missing .* stratum"):
        select_pilot_members(rows)


def test_reader_aligns_unequal_row_groups_and_preserves_zero_null_and_lower_bound(tmp_path):
    partition = _write_partition(tmp_path)
    handle, _, _ = _open(tmp_path, partition)
    selection = EndpointSelection(
        "historical_membership",
        sessions=("premarket",),
        members=(f"{partition['day']}/{partition['symbol']}",),
    )
    field_names = ("midpoint_rms_5s_bps_hl30s", "midpoint_change_age_seconds")
    results = []
    for size in (1, 7, 4096):
        batches = list(
            iter_endpoint_batches(
                handle,
                fields=field_names,
                selection=selection,
                include_support=True,
                include_run_boundaries=True,
                batch_size=size,
            )
        )
        table = pa.Table.from_batches(batches)
        results.append(table.to_pylist())
        assert table.schema.field("midpoint_rms_5s_bps_hl30s").type == pa.float64()
    assert results[0] == results[1] == results[2]
    assert results[0][0]["midpoint_rms_5s_bps_hl30s"] == 0.0
    assert results[0][1]["midpoint_rms_5s_bps_hl30s"] is None
    assert results[0][1]["midpoint_age_status"] == 1
    assert results[0][1]["midpoint_age_lower_bound_seconds"] == 2.0
    assert "trade_continuity_id" not in results[0][0]
    assert "quote_continuity_id" in results[0][0]
    assert results[0][0]["selection_segment_start"] is True
    assert results[0][-1]["selection_segment_end"] is True


def test_run_boundaries_preserve_only_required_source_interruptions(tmp_path):
    partition = _write_partition(tmp_path)
    handle, _, _ = _open(tmp_path, partition)
    selection = EndpointSelection(
        "historical_membership",
        members=(f"{partition['day']}/{partition['symbol']}",),
    )
    quote = pa.Table.from_batches(list(iter_endpoint_batches(
        handle,
        fields=("midpoint_rms_5s_bps_hl30s",),
        selection=selection,
        include_run_boundaries=True,
    )))
    assert "trade_continuity_id" not in quote.column_names
    assert quote["quote_continuity_id"].to_pylist() == [7, 7, 7, 8, 8, 8, 8, 8]
    assert quote["quote_continuity_break_in_second"].to_pylist() == [False, False, False, True, False, False, False, False]

    trade = pa.Table.from_batches(list(iter_endpoint_batches(
        handle,
        fields=("trade_rate_per_second_hl30s",),
        selection=selection,
        include_run_boundaries=True,
    )))
    assert "quote_continuity_id" not in trade.column_names
    assert trade["trade_continuity_id"].to_pylist() == [20, 20, 20, 20, 20, 21, 21, 21]
    assert trade["trade_continuity_break_in_second"].to_pylist() == [False, False, False, False, False, True, False, False]


def test_single_feature_projection_ignores_unrelated_invalid_feature(tmp_path):
    partition = _write_partition(tmp_path)
    handle, _, _ = _open(tmp_path, partition)
    selection = EndpointSelection(
        "historical_membership",
        members=(f"{partition['day']}/{partition['symbol']}",),
    )
    table = pa.Table.from_batches(
        list(
            iter_endpoint_batches(
                handle,
                fields=("midpoint_rms_5s_bps_hl30s",),
                selection=selection,
            )
        )
    )
    assert "quoted_spread_bps_hl30s" not in table.column_names
    assert table["midpoint_rms_5s_bps_hl30s"][0].as_py() == 0.0


def test_reader_rejects_contradictory_value_mask_and_unknown_bits(tmp_path):
    for label, mask in (("contradictory", int(Reason.STARTUP)), ("unknown", 4096)):
        root = tmp_path / label
        partition = _write_partition(root)
        path = partition["feature_root"] / partition["relative"] / "features.parquet"
        table = pq.ParquetFile(path).read()
        index = table.schema.get_field_index("midpoint_rms_5s_bps_hl30s_reason_mask")
        columns = list(table.columns)
        columns[index] = pa.array([mask] + columns[index].to_pylist()[1:], type=pa.uint16())
        table = pa.Table.from_arrays(columns, schema=table.schema)
        pq.write_table(table, path)
        record = output_record(path, rows=table.num_rows, schema_sha256=schema_hash(feature_schema()))
        partition["feature_manifest"]["outputs"]["features.parquet"] = record
        manifest_path = partition["feature_root"] / partition["relative"] / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["outputs"] = list(partition["feature_manifest"]["outputs"].values())
        write_atomic_json(manifest_path, manifest)
        sha, size = sha256_file(manifest_path)
        partition["feature_manifest"]["sha256"] = sha
        partition["feature_manifest"]["bytes"] = size
        handle, _, _ = _open(root, partition)
        selection = EndpointSelection(
            "historical_membership", members=(f"{partition['day']}/{partition['symbol']}",)
        )
        with pytest.raises(ContractError, match="invalid projected value/reason mask"):
            list(iter_endpoint_batches(handle, fields=("midpoint_rms_5s_bps_hl30s",), selection=selection))


@pytest.mark.parametrize("mutation,pattern", [
    ("missing", "missing physical endpoint row"),
    ("duplicate", "duplicate or reordered endpoint key"),
    ("reordered", "missing physical endpoint row|duplicate or reordered endpoint key"),
    ("wrong_member", "wrong-member key"),
])
def test_reader_rejects_key_corruption_without_inner_join_loss(tmp_path, mutation, pattern):
    partition = _write_partition(tmp_path)
    feature_path = partition["feature_root"] / partition["relative"] / "features.parquet"
    table = pq.ParquetFile(feature_path).read()
    if mutation == "missing":
        table = table.take(pa.array([0, 1, 2, 4, 5, 6, 7]))
    elif mutation == "duplicate":
        table = table.take(pa.array([0, 1, 2, 2, 4, 5, 6, 7]))
    elif mutation == "reordered":
        table = table.take(pa.array([0, 2, 1, 3, 4, 5, 6, 7]))
    else:
        columns = list(table.columns)
        columns[1] = pa.array(["OTHER"] + [partition["symbol"]] * 7)
        table = pa.Table.from_arrays(columns, schema=table.schema)
    pq.write_table(table, feature_path, row_group_size=7)
    record = output_record(feature_path, rows=table.num_rows, schema_sha256=schema_hash(feature_schema()))
    partition["feature_manifest"]["outputs"]["features.parquet"] = record
    feature_manifest_path = partition["feature_root"] / partition["relative"] / "manifest.json"
    feature_manifest = json.loads(feature_manifest_path.read_text())
    feature_manifest["outputs"] = list(partition["feature_manifest"]["outputs"].values())
    write_atomic_json(feature_manifest_path, feature_manifest)
    sha, size = sha256_file(feature_manifest_path)
    partition["feature_manifest"]["sha256"] = sha
    partition["feature_manifest"]["bytes"] = size
    handle, _, _ = _open(tmp_path, partition)
    selection = EndpointSelection("historical_membership", members=(f"{partition['day']}/{partition['symbol']}",))
    with pytest.raises(ContractError, match=pattern):
        list(iter_endpoint_batches(handle, fields=("midpoint_rms_5s_bps_hl30s",), selection=selection))


def test_unknown_discovery_clock_fails_preflight_but_historical_mode_works(tmp_path):
    partition = _write_partition(tmp_path)
    handle, _, _ = _open(tmp_path, partition)
    member = (f"{partition['day']}/{partition['symbol']}",)
    historical = EndpointSelection("historical_membership", members=member)
    assert sum(batch.num_rows for batch in iter_endpoint_batches(handle, fields=("trade_age_seconds",), selection=historical)) == 8
    unavailable = EndpointSelection("nominal_post_discovery", members=member)
    with pytest.raises(ContractError, match="preflight failed for 1 member"):
        list(iter_endpoint_batches(handle, fields=("trade_age_seconds",), selection=unavailable))


def test_relocation_preserves_identity_and_mutation_invalidates_handle(tmp_path):
    partition = _write_partition(tmp_path / "source")
    handle, reference, identity = _open(tmp_path / "source", partition)
    moved = tmp_path / "moved"
    shutil.copytree(tmp_path / "source" / "base", moved / "base")
    shutil.copytree(tmp_path / "source" / "features", moved / "features")
    relocated = open_endpoint_reference(
        reference,
        expected_identity=identity,
        data_roots={"base": moved / "base", "features": moved / "features"},
    )
    selection = EndpointSelection("historical_membership", members=(f"{partition['day']}/{partition['symbol']}",))
    assert sum(batch.num_rows for batch in iter_endpoint_batches(relocated, fields=("trade_age_seconds",), selection=selection)) == 8
    path = moved / "features" / partition["relative"] / "features.parquet"
    with path.open("ab") as stream:
        stream.write(b"x")
    with pytest.raises(ContractError, match="changed during read session"):
        list(iter_endpoint_batches(relocated, fields=("trade_age_seconds",), selection=selection))


def test_open_rejects_replacement_between_hash_and_parquet_open(tmp_path, monkeypatch):
    partition = _write_partition(tmp_path)
    reference, identity = _write_reference(tmp_path, partition)
    target = partition["base_root"] / partition["relative"] / "base.parquet"
    replacement = tmp_path / "replacement.parquet"
    table = pq.ParquetFile(target).read()
    columns = list(table.columns)
    columns[table.schema.get_field_index("trade_continuity_id")] = pa.array(
        [999] * table.num_rows, type=pa.uint64()
    )
    pq.write_table(pa.Table.from_arrays(columns, schema=table.schema), replacement)
    original = endpoint_release.pq.ParquetFile
    replaced = False

    def replace_during_open(path, *args, **kwargs):
        nonlocal replaced
        if Path(path) == target and not replaced:
            replaced = True
            replacement.replace(target)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(endpoint_release.pq, "ParquetFile", replace_during_open)
    with pytest.raises(ContractError, match="changed during open"):
        open_endpoint_reference(
            reference,
            expected_identity=identity,
            data_roots={"base": partition["base_root"], "features": partition["feature_root"]},
        )
    assert replaced


def test_pilot_open_rejects_prefix_declared_as_full_substitution(tmp_path):
    reference = tmp_path / "pilot-reference"
    reference.mkdir()
    members = []
    for month in range(3, 9):
        day = f"2026-{month:02d}-09"
        start_ns, end_ns = session_bounds(day)
        for stratum in range(4):
            symbol = f"M{month}S{stratum}"
            members.append({
                "member": f"{day}/{symbol}",
                "session_date": day,
                "symbol": symbol,
                "month": day[:7],
                "stratum": stratum,
                "coverage": {
                    "kind": "full", "session_start_ns": start_ns,
                    "end_ns": end_ns, "expected_rows": 57_600,
                },
            })
    members[0]["coverage"] = {
        "kind": "prefix",
        "session_start_ns": members[0]["coverage"]["session_start_ns"],
        "end_ns": members[0]["coverage"]["end_ns"] - NS,
        "expected_rows": 57_600,
    }
    member_path = reference / "members.jsonl"
    member_path.write_text("".join(canonical_json(row) + "\n" for row in members))
    member_sha, member_bytes = sha256_file(member_path)
    manifest = {
        "version": "endpoint_ew_reference_v1",
        "reference_kind": "pilot",
        "synthetic": False,
        "selection": {
            "algorithm": "month_event_rank_quartiles_v1", "seed": "phase4-pilot-v1"
        },
        "release": {"contract_identity": contract_identity()},
        "contract_config": DEFAULT_CONFIG.to_dict(),
        "schemas": {
            "base": schema_hash(BASE_SCHEMA),
            "features": schema_hash(feature_schema()),
            "support": schema_hash(support_schema()),
        },
        "members": {
            "path": "members.jsonl", "sha256": member_sha, "bytes": member_bytes,
            "count": 24, "rows_per_table": 24 * 57_600,
        },
    }
    manifest["reference_identity"] = digest(manifest)
    write_atomic_json(reference / "manifest.json", manifest)
    with pytest.raises(ContractError, match="not an exact full session"):
        endpoint_release._verify_reference_manifest(
            reference, manifest["reference_identity"]
        )


def test_open_rejects_schema_corruption_and_path_escape(tmp_path):
    partition = _write_partition(tmp_path / "schema")
    feature_path = partition["feature_root"] / partition["relative"] / "features.parquet"
    pq.write_table(pa.table({"x": [1]}), feature_path)
    record = output_record(feature_path, rows=1, schema_sha256=schema_hash(feature_schema()))
    partition["feature_manifest"]["outputs"]["features.parquet"] = record
    feature_manifest_path = partition["feature_root"] / partition["relative"] / "manifest.json"
    feature_manifest = json.loads(feature_manifest_path.read_text())
    feature_manifest["outputs"] = list(partition["feature_manifest"]["outputs"].values())
    write_atomic_json(feature_manifest_path, feature_manifest)
    sha, size = sha256_file(feature_manifest_path)
    partition["feature_manifest"]["sha256"] = sha
    partition["feature_manifest"]["bytes"] = size
    reference, identity = _write_reference(tmp_path / "schema", partition)
    with pytest.raises(ContractError, match="schema/count mismatch"):
        open_endpoint_reference(reference, expected_identity=identity, data_roots={"base": partition["base_root"], "features": partition["feature_root"]})

    safe = _write_partition(tmp_path / "escape")
    safe["relative"] = "../outside"
    reference, identity = _write_reference(tmp_path / "escape", safe)
    with pytest.raises(ContractError, match="traversal"):
        open_endpoint_reference(reference, expected_identity=identity, data_roots={"base": safe["base_root"], "features": safe["feature_root"]})


def test_open_rejects_mixed_configuration_and_wrong_base_binding(tmp_path):
    mixed = _write_partition(tmp_path / "mixed")
    manifest_path = mixed["feature_root"] / mixed["relative"] / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["contract_config"]["other_min_coverage"] = 0.75
    write_atomic_json(manifest_path, manifest)
    sha, size = sha256_file(manifest_path)
    mixed["feature_manifest"]["sha256"] = sha
    mixed["feature_manifest"]["bytes"] = size
    reference, identity = _write_reference(tmp_path / "mixed", mixed)
    with pytest.raises(ContractError, match="manifest binding mismatch"):
        open_endpoint_reference(reference, expected_identity=identity, data_roots={"base": mixed["base_root"], "features": mixed["feature_root"]})

    wrong = _write_partition(tmp_path / "wrong")
    wrong["feature_manifest"]["consumed_base_manifest_sha256"] = "0" * 64
    reference, identity = _write_reference(tmp_path / "wrong", wrong)
    with pytest.raises(ContractError, match="feature/base binding mismatch"):
        open_endpoint_reference(reference, expected_identity=identity, data_roots={"base": wrong["base_root"], "features": wrong["feature_root"]})
