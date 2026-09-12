"""Thin command-line dispatch for numerical report production."""

import json
from pathlib import Path
from .pipeline import aggregate_report, render_report, verify_report


def _aggregate(args):
    config = json.loads(args.config.read_text()) if args.config else None
    tape = json.loads(args.tape_config.read_text()) if args.tape_config else None
    print(
        aggregate_report(
            args.release,
            args.output,
            screen=args.screen,
            config=config,
            synthetic=args.synthetic,
            tape_config=tape,
        )
    )


def register_commands(subparsers):
    report = subparsers.add_parser(
        "report", help="Aggregate verified releases and render figures offline"
    )
    commands = report.add_subparsers(dest="report_command", required=True)
    aggregate = commands.add_parser(
        "aggregate", help="Produce exact ECDFs, histograms and denominator tables"
    )
    aggregate.add_argument("--release", type=Path, required=True)
    aggregate.add_argument(
        "--screen",
        type=Path,
        help="Verified screening stage with reference denominators",
    )
    aggregate.add_argument(
        "--config",
        type=Path,
        help="JSON: batch_size, axes, optional expected_release_hash",
    )
    aggregate.add_argument(
        "--tape-config",
        type=Path,
        help="JSON with exactly two explicit canonical T/Q slices",
    )
    aggregate.add_argument("--output", type=Path, required=True)
    aggregate.add_argument(
        "--synthetic", action="store_true", help="Label invented inputs and outputs"
    )
    aggregate.set_defaults(func=_aggregate)
    render = commands.add_parser(
        "render", help="Render saved numerical artifacts without network access"
    )
    render.add_argument("--aggregate", type=Path, required=True)
    render.add_argument("--output", type=Path, required=True)
    render.add_argument("--dpi", type=int, default=120)
    render.set_defaults(
        func=lambda args: print(
            render_report(args.aggregate, args.output, dpi=args.dpi)
        )
    )
    verify = commands.add_parser(
        "verify", help="Verify exact artifact identities and export accounting"
    )
    verify.add_argument("--input", type=Path, required=True)
    verify.set_defaults(
        func=lambda args: print(
            json.dumps(verify_report(args.input)["validation"], indent=2)
        )
    )
