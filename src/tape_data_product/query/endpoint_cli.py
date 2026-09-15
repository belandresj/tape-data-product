"""Additive tape-product endpoint-data command group."""
from pathlib import Path

from ..contracts import DEFAULT_CONFIG
from ..contracts.config import ContractError, FeatureConfig
from ..integrity import read_json
from .endpoint_reader import describe_endpoint_fields, iter_endpoint_batches
from .endpoint_release import build_endpoint_reference, open_endpoint_reference
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


def _reference_arguments(parser):
    parser.add_argument("--reference", required=True)
    parser.add_argument("--identity", required=True)
    parser.add_argument("--base-root", required=True)
    parser.add_argument("--feature-root", required=True)


def _roots(args):
    return {"base": args.base_root, "features": args.feature_root}


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
