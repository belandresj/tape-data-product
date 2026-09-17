"""Command dispatch for base-only endpoint/EW feature construction."""


def register_commands(subparsers):
    features = subparsers.add_parser(
        "features", help="Build or independently verify endpoint/EW features from base"
    )
    commands = features.add_subparsers(dest="feature_command", required=True)
    from .endpoint_ew_cli import register_commands as endpoint_ew

    endpoint_ew(commands)
