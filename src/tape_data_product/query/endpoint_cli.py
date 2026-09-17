"""Additive tape-product endpoint-data command group."""
from pathlib import Path
import time

from ..contracts import DEFAULT_CONFIG
from ..contracts.config import ContractError, FeatureConfig
from ..integrity import read_json
from .endpoint_reader import describe_endpoint_fields, iter_endpoint_batches
from .endpoint_database import open_tape_database
from .endpoint_release import (
    build_endpoint_full_reference,
    build_endpoint_reference,
    open_endpoint_reference,
)
from .endpoint_selection import EndpointSelection


def register_commands(commands):
    endpoint = commands.add_parser(
        "endpoint-data", help="Build, verify and inspect endpoint/EW references"
    )
    subcommands = endpoint.add_subparsers(dest="endpoint_command", required=True)

    pilot = subcommands.add_parser("pilot", help="Build the deterministic 24-member pilot")
    pilot.add_argument("--plan", required=True)
    pilot.add_argument("--ledger", required=True)
    pilot.add_argument("--completion", required=True)
    pilot.add_argument("--base-root", required=True)
    pilot.add_argument("--feature-root", required=True)
    pilot.add_argument("--output", required=True)
    pilot.add_argument("--expected-plan-sha256", required=True)
    pilot.add_argument("--expected-population-sha256", required=True)
    pilot.set_defaults(func=_pilot)

    full = subcommands.add_parser("full", help="Build the accepted full-population reference")
    full.add_argument("--plan", required=True)
    full.add_argument("--ledger", required=True)
    full.add_argument("--completion", required=True)
    full.add_argument("--base-root", required=True)
    full.add_argument("--feature-root", required=True)
    full.add_argument("--output", required=True)
    full.add_argument("--expected-plan-sha256", required=True)
    full.add_argument("--expected-population-sha256", required=True)
    full.set_defaults(func=_full)

    verify = subcommands.add_parser("verify", help="Verify a reference and consumed bytes")
    _reference_arguments(verify)
    verify.set_defaults(func=_verify)

    fields = subcommands.add_parser("fields", help="Describe all queryable measurements")
    fields.add_argument("--config")
    fields.set_defaults(func=_fields)

    inspect = subcommands.add_parser(
        "inspect", help="Inspect an explicitly selected and capped endpoint projection"
    )
    _reference_arguments(inspect)
    inspect.add_argument("--selection", required=True)
    inspect.add_argument("--field", action="append", required=True, dest="fields")
    inspect.add_argument("--include-support", action="store_true")
    inspect.add_argument("--include-run-boundaries", action="store_true")
    inspect.add_argument("--batch-size", type=int, default=4096)
    inspect.add_argument("--limit", type=int, default=100)
    inspect.set_defaults(func=_inspect)

    query_fields = subcommands.add_parser(
        "query-fields", help="List fields from an installed query catalog"
    )
    _query_arguments(query_fields)
    query_fields.set_defaults(func=_query_fields)

    sql = subcommands.add_parser(
        "sql", help="Execute one SQL file against an explicit trading-date scope"
    )
    _query_arguments(sql)
    sql.add_argument("--sql-file", required=True)
    sql.add_argument("--preview-limit", type=int, default=20)
    sql.add_argument(
        "--export",
        help="New export directory; streams results to Parquet instead of the terminal",
    )
    sql.set_defaults(func=_sql)


def _reference_arguments(parser):
    parser.add_argument("--reference", required=True)
    parser.add_argument("--identity", required=True)
    parser.add_argument("--base-root", required=True)
    parser.add_argument("--feature-root", required=True)


def _query_arguments(parser):
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--identity", required=True)
    parser.add_argument("--base-root", required=True)
    parser.add_argument("--feature-root", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--member", action="append", dest="members")
    parser.add_argument("--temp-directory")


def _roots(args):
    return {"base": args.base_root, "features": args.feature_root}


def _open_database(args):
    return open_tape_database(
        args.catalog,
        expected_identity=args.identity,
        data_roots=_roots(args),
        start_date=args.start_date,
        end_date=args.end_date,
        members=tuple(args.members) if args.members else None,
        temp_directory=args.temp_directory,
    )


def _pilot(args):
    return build_endpoint_reference(
        args.plan,
        args.ledger,
        args.completion,
        args.base_root,
        args.feature_root,
        args.output,
        expected_plan_sha256=args.expected_plan_sha256,
        expected_population_sha256=args.expected_population_sha256,
    )


def _full(args):
    return build_endpoint_full_reference(
        args.plan,
        args.ledger,
        args.completion,
        args.base_root,
        args.feature_root,
        args.output,
        expected_plan_sha256=args.expected_plan_sha256,
        expected_population_sha256=args.expected_population_sha256,
    )


def _open(args):
    return open_endpoint_reference(
        args.reference, expected_identity=args.identity, data_roots=_roots(args)
    )


def _verify(args):
    handle = _open(args)
    return {
        "reference_identity": handle.manifest["reference_identity"],
        "members": len(handle.members),
        "rows_per_table": handle.manifest["members"]["rows_per_table"],
        "integrity": "passed",
        "validation_bytes": handle.validation_bytes,
        "validation_seconds": handle.validation_seconds,
    }

def _fields(args):
    config = (
        DEFAULT_CONFIG
        if args.config is None
        else FeatureConfig.from_dict(read_json(args.config))
    )
    return {"fields": describe_endpoint_fields(config)}


def _inspect(args):
    if type(args.limit) is not int or not 1 <= args.limit <= 1000:
        raise ContractError("inspect limit must be in 1..1000")
    selection = EndpointSelection.from_dict(read_json(args.selection))
    if selection.members is None:
        raise ContractError("inspect requires explicit member selection")
    handle = _open(args)
    rows = []
    scanned = 0
    for batch in iter_endpoint_batches(
        handle,
        fields=tuple(args.fields),
        selection=selection,
        include_support=args.include_support,
        include_run_boundaries=args.include_run_boundaries,
        batch_size=args.batch_size,
    ):
        scanned += batch.num_rows
        remaining = args.limit - len(rows)
        if remaining > 0:
            rows.extend(batch.slice(0, remaining).to_pylist())
    return {
        "reference_identity": handle.manifest["reference_identity"],
        "selection": selection.to_dict(),
        "projected_rows": scanned,
        "displayed_rows": len(rows),
        "display_limit": args.limit,
        "rows": rows,
        "validation_bytes": handle.validation_bytes,
        "validation_seconds": handle.validation_seconds,
    }


def _query_fields(args):
    with _open_database(args) as database:
        started = time.perf_counter()
        fields = database.sql(
            "SELECT name, table_name, reason_mask, unit, family, "
            "half_life_seconds, window_seconds "
            "FROM feature_catalog ORDER BY table_name, name"
        ).fetchall()
        return {
            "catalog_identity": database.catalog_identity,
            "selected_members": list(database.selected_members),
            "setup_seconds": database.validation_seconds,
            "query_seconds": time.perf_counter() - started,
            "fields": [
                dict(
                    zip(
                        (
                            "name",
                            "table_name",
                            "reason_mask",
                            "unit",
                            "family",
                            "half_life_seconds",
                            "window_seconds",
                        ),
                        row,
                    )
                )
                for row in fields
            ],
        }


def _read_sql(path):
    path = Path(path)
    if path.stat().st_size > 1 << 20:
        raise ContractError("SQL file exceeds 1 MiB")
    query = path.read_text(encoding="utf-8")
    if not query.strip():
        raise ContractError("SQL file is empty")
    return query


def _sql(args):
    if type(args.preview_limit) is not int or not 1 <= args.preview_limit <= 100:
        raise ContractError("preview limit must be in 1..100")
    query = _read_sql(args.sql_file)
    with _open_database(args) as database:
        if args.export:
            result = database.export_parquet(query, args.export)
            return {
                "catalog_identity": database.catalog_identity,
                "selected_members": list(database.selected_members),
                "setup_seconds": database.validation_seconds,
                "export": result,
            }
        started = time.perf_counter()
        preview = database.sql(query).preview(args.preview_limit)
        return {
            "catalog_identity": database.catalog_identity,
            "selected_members": list(database.selected_members),
            "setup_seconds": database.validation_seconds,
            "query_seconds": time.perf_counter() - started,
            "preview_limit": args.preview_limit,
            **preview,
        }
