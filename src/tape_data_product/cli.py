"""Installed command interface for the local research workflow."""

import argparse
import json
import sys


def parser():
    result = argparse.ArgumentParser(
        prog="tape-product",
        description="Acquire, calculate, query and reproduce direction-neutral tape measurements.",
    )
    commands = result.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="Run a complete invented offline workflow")
    demo.add_argument(
        "--output", required=True, help="New output directory; never overwritten"
    )
    demo.set_defaults(func=_demo)
    from tape_data_product.acquisition.cli import register_commands as acquisition
    from tape_data_product.features.cli import register_commands as features
    from tape_data_product.analysis.cli import register_commands as analysis
    from tape_data_product.replay.cli import register_commands as replay
    from tape_data_product.calculate import register_commands as calculate
    from tape_data_product.query.endpoint_cli import register_commands as endpoint

    acquisition(commands)
    features(commands)
    analysis(commands)
    replay(commands)
    calculate(commands)
    endpoint(commands)
    return result


def _demo(args):
    from tape_data_product.demo import run_demo

    return run_demo(args.output)


def main(argv=None):
    arguments = parser().parse_args(argv)
    try:
        result = arguments.func(arguments)
    except (ValueError, FileExistsError, FileNotFoundError, PermissionError) as error:
        print(f"tape-product: {error}", file=sys.stderr)
        return 2
    except KeyError as error:
        print(
            f"tape-product: required configuration/artifact field missing: {error}",
            file=sys.stderr,
        )
        return 2
    if result is not None:
        print(json.dumps(result, indent=2, default=str, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
