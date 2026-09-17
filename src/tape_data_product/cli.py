"""Installed command interface for the endpoint/EW V2 product."""

import argparse
import json
import sys


def parser():
    result = argparse.ArgumentParser(
        prog="tape-product",
        description="Build, query, and reproduce the endpoint/EW tape-data product.",
    )
    commands = result.add_subparsers(dest="command", required=True)
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
