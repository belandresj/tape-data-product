"""Thin dispatch for calculation, release, query, and threshold pilot stages."""

from pathlib import Path
import json
from tape_data_product.features.api import (
    build_partition,
    build_inventory,
    verify_partition,
)
from tape_data_product.query.release import build_release, verify_release
from tape_data_product.query.api import run_query


def _read(path):
    return json.loads(Path(path).read_text())


def _build(args):
    direct = (
        "quotes",
        "trades",
        "discovery",
        "session_date",
        "symbol",
        "pair_manifest",
        "halts",
        "continuity_breaks",
    )
    if args.inventory is not None:
        if any(getattr(args, name) is not None for name in direct):
            raise ValueError(
                "--inventory cannot be combined with direct member, discovery or overlay flags; use --contexts"
            )
        index = build_inventory(
            args.inventory,
            args.output,
            seconds=args.seconds,
            synthetic=args.synthetic,
            contexts=args.contexts,
        )
        return {"partitions": str(index)}
    if args.contexts is not None:
        raise ValueError("--contexts requires --inventory")
    required = ("quotes", "trades", "discovery", "session_date", "symbol")
    missing = [
        "--" + name.replace("_", "-")
        for name in required
        if getattr(args, name) is None
    ]
    if missing:
        raise ValueError("Direct feature build requires " + ", ".join(missing))
    return build_partition(
        args.quotes,
        args.trades,
        args.session_date,
        args.symbol,
        _read(args.discovery),
        args.output,
        seconds=args.seconds,
        halts=_read(args.halts) if args.halts else (),
        synthetic=args.synthetic,
        pair_manifest=args.pair_manifest,
        continuity_breaks_ns=(
            _read(args.continuity_breaks) if args.continuity_breaks else ()
        ),
    )


def register_commands(subparsers):
    features = subparsers.add_parser(
        "features", help="Calculate or independently verify compact 60s/300s features"
    )
    commands = features.add_subparsers(dest="feature_command", required=True)
    build = commands.add_parser(
        "build", help="Stream explicit canonical source pairs from session start"
    )
    for name in ("quotes", "trades", "discovery"):
        build.add_argument("--" + name, type=Path)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument(
        "--inventory",
        type=Path,
        help="Sequentially build acquisition inventory and emit partitions.jsonl",
    )
    build.add_argument(
        "--contexts",
        type=Path,
        help="Inventory-only keyed JSONL halt/continuity contexts; omitted means empty overlays",
    )
    build.add_argument("--session-date")
    build.add_argument("--symbol")
    build.add_argument("--seconds", type=int, default=57600)
    build.add_argument(
        "--halts", type=Path, help="JSON array of accepted halt intervals"
    )
    build.add_argument(
        "--continuity-breaks",
        type=Path,
        help="JSON array of UTC nanosecond continuity-break timestamps",
    )
    build.add_argument(
        "--pair-manifest",
        type=Path,
        help="Required canonical pair.json receipt for production",
    )
    build.add_argument("--synthetic", action="store_true")
    build.set_defaults(func=_build)
    verify = commands.add_parser(
        "verify",
        help="Check completion integrity and optionally reconstruct all feature values",
    )
    verify.add_argument("--input", type=Path, required=True)
    verify.add_argument("--reconstruction", action="store_true")
    verify.set_defaults(
        func=lambda args: verify_partition(
            args.input, reconstruction=args.reconstruction
        )
    )
    release = subparsers.add_parser(
        "release", help="Construct immutable releases with expected-member accounting"
    )
    commands = release.add_subparsers(dest="release_command", required=True)
    build = commands.add_parser("build")
    for name in ("expected-members", "partitions", "output"):
        build.add_argument("--" + name, type=Path, required=True)
    build.add_argument("--allow-partial", action="store_true")
    build.set_defaults(
        func=lambda args: build_release(
            args.expected_members,
            args.partitions,
            args.output,
            allow_partial=args.allow_partial,
        )
    )
    verify = commands.add_parser("verify")
    verify.add_argument("--input", type=Path, required=True)
    verify.add_argument("--expected-release-hash")
    verify.set_defaults(
        func=lambda args: verify_release(args.input, args.expected_release_hash)
    )
    query = subparsers.add_parser(
        "query",
        help="Retrieve strict runs and causal intervals from an explicit verified release",
    )
    _query_args(query, False)
    experiment = subparsers.add_parser(
        "experiment",
        help="Implemented descriptive studies; no trading expectancy claim",
    )
    commands = experiment.add_subparsers(dest="experiment_command", required=True)
    pilot = commands.add_parser(
        "threshold-pilot", help="Reproduce fixed five-date participation variants A–D"
    )
    pilot.add_argument("--release", type=Path, required=True)
    pilot.add_argument("--output", type=Path, required=True)
    pilot.add_argument("--expected-release-hash")
    from tape_data_product.experiments.threshold_pilot import run_pilot

    pilot.set_defaults(
        func=lambda args: run_pilot(
            args.release, args.output, expected_release_hash=args.expected_release_hash
        )
    )


def _query_args(parser, pilot):
    for name in ("release", "config", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--expected-release-hash")
    parser.set_defaults(
        func=lambda args: run_query(
            args.release,
            _read(args.config),
            args.output,
            expected_release_hash=args.expected_release_hash,
            pilot=pilot,
        )
    )
