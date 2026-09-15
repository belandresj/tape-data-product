"""Validated, projected endpoint/EW reader shared by later research stages."""
from __future__ import annotations

from collections import OrderedDict
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from ..contracts.config import ContractError
from ..contracts.reasons import ALL_REASONS, HISTORY_REASONS, Reason
from ..contracts.registry import query_registry
from ..contracts.schemas import BASE_SCHEMA, feature_schema, support_schema
from .endpoint_release import (
    EndpointReferenceHandle,
    _BatchCursor,
    _open_parquet_checked,
    _within,
    verify_handle_unchanged,
)
from .endpoint_selection import EndpointSelection, selected_ranges


KEYS = ("session_date", "symbol", "interval_end_ns")
_CURRENT_AGE_MASKS = {
    "trade_age_seconds": "trade_age_reason_mask",
    "quote_age_seconds": "quote_age_reason_mask",
    "midpoint_change_age_seconds": "midpoint_change_age_reason_mask",
}


def _support_dependencies(feature):
    if feature.half_life_seconds is not None:
        suffix = f"_hl{feature.half_life_seconds}s"
        families = {
            "return": ("return",),
            "participation": ("return",),
            "spread": ("spread",),
            "activity": ("activity",),
            "bid_size": ("bid_size",),
            "ask_size": ("ask_size",),
            "ratio": ("return", "spread"),
        }[feature.family]
        values = []
        for family in families:
            if family == "return":
                values.extend(
                    (
                        f"return_usable_weight{suffix}",
                        f"return_possible_weight{suffix}",
                    )
                )
            else:
                values.extend(
                    (
                        f"{family}_usable_exposure_seconds{suffix}",
                        f"{family}_possible_exposure_seconds{suffix}",
                    )
                )
        values.append(
            f"{feature.sources[0]}_ew_startup_elapsed_seconds"
        )
        return tuple(values)
    if feature.window_seconds is not None:
        stem = feature.name.split("_age_p90_seconds_", 1)[0]
        return (
            f"{stem}_age_sample_count_window{feature.window_seconds}s",
            f"{stem}_age_elapsed_slots_window{feature.window_seconds}s",
        )
    return ()


def _inspection_dependencies(name):
    if name == "midpoint_change_age_seconds":
        return (
            "midpoint_age_status",
            "midpoint_observation_start_ns",
            "midpoint_age_lower_bound_seconds",
        )
    return ()


def describe_endpoint_fields(config):
    """Return the canonical 27-field table/mask/unit/dependency mapping."""
    result = []
    for feature in query_registry(config):
        current = feature.name in _CURRENT_AGE_MASKS
        result.append(
            {
                "name": feature.name,
                "table": "base" if current else "features",
                "value_column": feature.name,
                "reason_mask": (
                    _CURRENT_AGE_MASKS[feature.name]
                    if current
                    else feature.name + "_reason_mask"
                ),
                "unit": feature.unit,
                "family": feature.family,
                "half_life_seconds": feature.half_life_seconds,
                "window_seconds": feature.window_seconds,
                "sources": list(feature.sources),
                "support_dependencies": list(_support_dependencies(feature)),
                "inspection_dependencies": list(
                    _inspection_dependencies(feature.name)
                ),
            }
        )
    return tuple(result)


def _descriptor_map(config):
    return {row["name"]: row for row in describe_endpoint_fields(config)}


def _selected_members(handle, selection):
    available = {row["member"]: row for row in handle.members}
    selected = list(handle.members)
    if selection.members is not None:
        missing = sorted(set(selection.members) - set(available))
        if missing:
            raise ContractError(f"selection requests absent members: {missing}")
        wanted = set(selection.members)
        selected = [row for row in selected if row["member"] in wanted]
    if selection.dates is not None:
        available_dates = {row["session_date"] for row in handle.members}
        missing = sorted(set(selection.dates) - available_dates)
        if missing:
            raise ContractError(f"selection requests absent dates: {missing}")
        wanted_dates = set(selection.dates)
        selected = [row for row in selected if row["session_date"] in wanted_dates]
    if not selected:
        raise ContractError("selection contains no reference members")
    failures = []
    if selection.population_timing_mode != "historical_membership":
        for row in selected:
            discovery = row["lineage"]["discovery"]
            nominal = discovery.get("nominal_discovery_endpoint_ns")
            if type(nominal) is not int:
                failures.append((row["member"], "nominal discovery endpoint unavailable"))
                continue
            if not discovery.get("nominal_provenance"):
                failures.append((row["member"], "nominal discovery provenance unavailable"))
                continue
            if selection.population_timing_mode == "receipt_post_discovery":
                receipt = discovery.get("receipt_known_at_ns")
                if type(receipt) is not int:
                    failures.append((row["member"], "discovery receipt clock unavailable"))
                elif not discovery.get("receipt_provenance"):
                    failures.append((row["member"], "receipt discovery provenance unavailable"))
    if failures:
        detail = "; ".join(f"{member}: {reason}" for member, reason in failures)
        raise ContractError(
            f"population timing preflight failed for {len(failures)} member(s): {detail}"
        )
    return tuple(selected)


def _schema_field(schemas, table, name):
    index = schemas[table].get_field_index(name)
    if index < 0:
        raise ContractError(f"unknown projected column {table}.{name}")
    return schemas[table].field(index)


def _same_keys(batches):
    reference = batches["base"]
    for table, batch in batches.items():
        if table == "base":
            continue
        for index in range(3):
            if not reference.column(index).equals(batch.column(index)):
                raise ContractError("base/feature/support key mismatch")


def _validate_projected(batches, fields, descriptors):
    zero = pa.scalar(0, pa.uint16())
    unknown = pa.scalar((~ALL_REASONS) & 0xFFFF, pa.uint16())
    for name in fields:
        descriptor = descriptors[name]
        batch = batches[descriptor["table"]]
        value = batch.column(batch.schema.get_field_index(name))
        mask_name = descriptor["reason_mask"]
        mask = batch.column(batch.schema.get_field_index(mask_name))
        if descriptor["table"] == "base":
            allowed = int(
                Reason.SOURCE_UNAVAILABLE
                | Reason.SOURCE_UNVERIFIED
                | Reason.HALT
                | Reason.NO_SUPPORTED_DATA
                | Reason.INVALID_CURRENT_VALUE
                | Reason.NO_OBSERVED_EVENT
                | Reason.CONTINUITY_BREAK
            )
            if name == "midpoint_change_age_seconds":
                allowed |= int(Reason.MIDPOINT_AGE_LOWER_BOUND_ONLY)
        else:
            allowed = HISTORY_REASONS
            if descriptor["family"] == "participation":
                allowed |= int(Reason.ZERO_RETURN_VARIATION)
            if descriptor["family"] == "ratio":
                allowed |= int(Reason.ZERO_SPREAD)
        invalid_reason = pc.not_equal(
            pc.bit_wise_and(mask, pa.scalar((~allowed) & 0xFFFF, pa.uint16())),
            zero,
        )
        unknown_reason = pc.not_equal(pc.bit_wise_and(mask, unknown), zero)
        null_disagrees = pc.not_equal(
            pc.is_null(value), pc.not_equal(mask, zero)
        )
        finite_nonnegative = pc.or_kleene(
            pc.is_null(value),
            pc.and_kleene(pc.is_finite(value), pc.greater_equal(value, 0.0)),
        )
        if (
            pc.any(invalid_reason).as_py()
            or pc.any(unknown_reason).as_py()
            or pc.any(null_disagrees).as_py()
            or not pc.all(pc.fill_null(finite_nonnegative, False)).as_py()
        ):
            raise ContractError(f"invalid projected value/reason mask: {name}")


def _member_batches(handle, member, fields, descriptors, selection, include_support, include_run_boundaries, batch_size):
    base_partition = _within(handle.data_roots["base"], member["base_path"])
    feature_partition = _within(handle.data_roots["features"], member["feature_path"])
    schemas = {
        "base": BASE_SCHEMA,
        "features": feature_schema(handle.config),
        "support": support_schema(handle.config),
    }
    columns = {"base": list(KEYS), "features": list(KEYS), "support": list(KEYS)}
    used_tables = {"base"}
    support_names = []
    inspection_names = []
    sources = set()
    for name in fields:
        descriptor = descriptors[name]
        table = descriptor["table"]
        used_tables.add(table)
        columns[table].extend((descriptor["value_column"], descriptor["reason_mask"]))
        sources.update(descriptor["sources"])
        inspection_names.extend(descriptor["inspection_dependencies"])
        if include_support:
            support_names.extend(descriptor["support_dependencies"])
    columns["base"].extend(inspection_names)
    if include_run_boundaries:
        columns["base"].append("halt_active")
        for source in sorted(sources):
            columns["base"].extend(
                (
                    f"{source}_continuity_id",
                    f"{source}_continuity_break_in_second",
                )
            )
    if include_support and support_names:
        used_tables.add("support")
        columns["support"].extend(support_names)
    for table in columns:
        columns[table] = list(OrderedDict.fromkeys(columns[table]))

    paths = {
        "base": base_partition / "base.parquet",
        "features": feature_partition / "features.parquet",
        "support": feature_partition / "support.parquet",
    }
    parquet = {
        table: _open_parquet_checked(path, handle.snapshots[str(path)])
        for table, path in paths.items()
    }
    cursors = {
        table: _BatchCursor(parquet[table], columns[table])
        for table in used_tables
    }
    declared_first = member["coverage"]["session_start_ns"] + 1_000_000_000
    declared_last = member["coverage"]["end_ns"]
    ranges = tuple(
        (max(start, declared_first), min(stop, declared_last))
        for start, stop in selected_ranges(
            member["session_date"], selection, member["lineage"]["discovery"]
        )
        if max(start, declared_first) <= min(stop, declared_last)
    )
    segment_ids = {
        bounds: f"{member['member']}:{index}"
        for index, bounds in enumerate(ranges)
    }
    seen_rows = 0
    previous_keys = {table: None for table in cursors}
    first_keys = {table: None for table in cursors}
    while not all(cursor.done for cursor in cursors.values()):
        if any(cursor.done for cursor in cursors.values()):
            raise ContractError("projected companion row count mismatch")
        count = min(cursor.available for cursor in cursors.values())
        batches = {table: cursor.take(count) for table, cursor in cursors.items()}
        for table, batch in batches.items():
            dates = batch.column(0).to_pylist()
            symbols = batch.column(1).to_pylist()
            endpoints = batch.column(2).to_pylist()
            previous_key = previous_keys[table]
            for day, symbol, endpoint in zip(dates, symbols, endpoints):
                key = (day, symbol, endpoint)
                if first_keys[table] is None:
                    first_keys[table] = key
                if day != member["session_date"] or symbol != member["symbol"]:
                    raise ContractError("wrong-member key in endpoint companion")
                if previous_key is not None and key <= previous_key:
                    raise ContractError("duplicate or reordered endpoint key")
                if previous_key is not None and endpoint != previous_key[2] + 1_000_000_000:
                    raise ContractError("missing physical endpoint row")
                previous_key = key
            previous_keys[table] = previous_key
        _same_keys(batches)
        _validate_projected(batches, fields, descriptors)
        base = batches["base"]
        endpoints = base.column(2).to_pylist()
        seen_rows += count
        keep = []
        segment_for_row = []
        start_flags = []
        end_flags = []
        for endpoint in endpoints:
            segment = next(
                ((bounds, segment_ids[bounds]) for bounds in ranges if bounds[0] <= endpoint <= bounds[1]),
                None,
            )
            keep.append(segment is not None)
            if segment is None:
                segment_for_row.append("")
                start_flags.append(False)
                end_flags.append(False)
            else:
                bounds, segment_id = segment
                segment_for_row.append(segment_id)
                start_flags.append(endpoint == bounds[0])
                end_flags.append(endpoint == bounds[1])
        if not any(keep):
            continue
        mask = pa.array(keep, type=pa.bool_())
        filtered = {table: batch.filter(mask) for table, batch in batches.items()}
        filtered_segments = [value for value, selected in zip(segment_for_row, keep) if selected]
        filtered_starts = [value for value, selected in zip(start_flags, keep) if selected]
        filtered_ends = [value for value, selected in zip(end_flags, keep) if selected]
        arrays = []
        output_fields = []
        base_filtered = filtered["base"]
        for key in KEYS:
            arrays.append(base_filtered.column(base_filtered.schema.get_field_index(key)))
            output_fields.append(_schema_field(schemas, "base", key))
        for name in fields:
            descriptor = descriptors[name]
            table_batch = filtered[descriptor["table"]]
            for column_name in (descriptor["value_column"], descriptor["reason_mask"]):
                arrays.append(
                    table_batch.column(table_batch.schema.get_field_index(column_name))
                )
                output_fields.append(
                    _schema_field(schemas, descriptor["table"], column_name)
                )
        for name in OrderedDict.fromkeys(support_names):
            arrays.append(filtered["support"].column(filtered["support"].schema.get_field_index(name)))
            output_fields.append(_schema_field(schemas, "support", name))
        for name in OrderedDict.fromkeys(inspection_names):
            arrays.append(base_filtered.column(base_filtered.schema.get_field_index(name)))
            output_fields.append(_schema_field(schemas, "base", name))
        if include_run_boundaries:
            run_names = ["halt_active"]
            for source in sorted(sources):
                run_names.extend(
                    (
                        f"{source}_continuity_id",
                        f"{source}_continuity_break_in_second",
                    )
                )
            for name in run_names:
                arrays.append(base_filtered.column(base_filtered.schema.get_field_index(name)))
                output_fields.append(_schema_field(schemas, "base", name))
        selected_count = len(filtered_segments)
        arrays.extend(
            (
                pa.array([True] * selected_count, type=pa.bool_()),
                pa.array(filtered_segments, type=pa.string()),
                pa.array(filtered_starts, type=pa.bool_()),
                pa.array(filtered_ends, type=pa.bool_()),
            )
        )
        output_fields.extend(
            (
                pa.field("selected", pa.bool_(), nullable=False),
                pa.field("selection_segment_id", pa.string(), nullable=False),
                pa.field("selection_segment_start", pa.bool_(), nullable=False),
                pa.field("selection_segment_end", pa.bool_(), nullable=False),
            )
        )
        output = pa.RecordBatch.from_arrays(arrays, schema=pa.schema(output_fields))
        for result in pa.Table.from_batches([output]).to_batches(max_chunksize=batch_size):
            yield result
    if seen_rows != member["coverage"]["expected_rows"]:
        raise ContractError("projected member row count mismatch")
    expected_first = (
        member["session_date"], member["symbol"],
        member["coverage"]["session_start_ns"] + 1_000_000_000,
    )
    expected_last = (
        member["session_date"], member["symbol"], member["coverage"]["end_ns"]
    )
    if any(first != expected_first for first in first_keys.values()) or any(
        last != expected_last for last in previous_keys.values()
    ):
        raise ContractError("projected member coverage boundary mismatch")


def iter_endpoint_batches(
    handle,
    *,
    fields,
    selection,
    include_support=False,
    include_run_boundaries=False,
    batch_size=4096,
):
    """Yield typed selected observations without recalculating stored histories."""
    if not isinstance(handle, EndpointReferenceHandle):
        raise ContractError("invalid endpoint reference handle")
    if not isinstance(selection, EndpointSelection):
        raise ContractError("selection must be EndpointSelection")
    if type(batch_size) is not int or not 1 <= batch_size <= 4096:
        raise ContractError("batch size must be an integer in 1..4096")
    if (
        not isinstance(fields, (tuple, list))
        or not fields
        or len(set(fields)) != len(fields)
        or any(not isinstance(name, str) for name in fields)
    ):
        raise ContractError("fields must be a nonempty unique sequence")
    descriptors = _descriptor_map(handle.config)
    unknown = sorted(set(fields) - set(descriptors))
    if unknown:
        raise ContractError(f"unknown endpoint fields: {unknown}")
    selected = _selected_members(handle, selection)
    verify_handle_unchanged(handle)
    for member in selected:
        yield from _member_batches(
            handle,
            member,
            tuple(fields),
            descriptors,
            selection,
            bool(include_support),
            bool(include_run_boundaries),
            batch_size,
        )
        verify_handle_unchanged(handle)
