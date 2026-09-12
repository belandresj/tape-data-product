"""Thin acquisition and explicit storage command handlers."""

from pathlib import Path
import json
import os
from .vendor import (
    MassiveHTTPClient,
    FixtureClient,
    acquire_reference,
    acquire_minutes,
    acquire_tq,
)
from .screen import screen
from tape_data_product.storage.catalog import (
    build_inventory,
    verify_pair,
    publish_pair,
    stage_pair,
)


def _client(config):
    if "fixture_pages" in config:
        return FixtureClient(config["fixture_pages"])
    variable = config.get("api_key_env", "MASSIVE_API_KEY")
    return MassiveHTTPClient(os.environ.get(variable), retries=config.get("retries", 3))


def _acquire(args):
    config = json.loads(args.config.read_text())
    client = _client(config)
    options = {"synthetic": config.get("synthetic", False)}
    if args.kind == "reference":
        return acquire_reference(client, config["session_date"], args.output, **options)
    if args.kind == "minutes":
        return acquire_minutes(
            client, config["session_date"], config["symbol"], args.output, **options
        )
    return acquire_tq(
        client,
        Path(config["selection"]),
        args.output,
        coverage=config.get("coverage"),
        **options,
    )


def _storage(args):
    if args.operation == "verify":
        return verify_pair(args.pair, allow_synthetic=args.allow_synthetic)
    if args.operation == "inventory":
        return build_inventory(args.selection, args.pair_root, args.output)
    import boto3
    from botocore.config import Config

    config = json.loads(args.config.read_text())
    endpoint = config["endpoint_url"]
    if not endpoint.startswith("https://") or not endpoint.endswith(
        ".r2.cloudflarestorage.com"
    ):
        raise ValueError("Expected an explicit HTTPS Cloudflare R2 endpoint")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="auto",
        config=Config(
            connect_timeout=10,
            read_timeout=60,
            retries={"max_attempts": 3, "mode": "standard"},
            max_pool_connections=1,
        ),
        aws_access_key_id=os.environ[config.get("access_key_env", "R2_ACCESS_KEY_ID")],
        aws_secret_access_key=os.environ[
            config.get("secret_key_env", "R2_SECRET_ACCESS_KEY")
        ],
    )
    bucket = config.get("bucket", "massive-equities")
    if args.operation == "publish":
        return publish_pair(client, bucket, args.pair)
    return stage_pair(
        client,
        bucket,
        args.manifest,
        args.output,
        max_bytes=config.get("max_stage_bytes", 512 * 1024 * 1024),
    )


def register_commands(subparsers):
    acquire = subparsers.add_parser(
        "acquire",
        help="Historical reference, minute bars or canonical T/Q; explicit provider configuration",
    )
    children = acquire.add_subparsers(dest="kind", required=True)
    for name in ("reference", "minutes", "tq"):
        parser = children.add_parser(
            name, help=f"Acquire {name} from bounded pages or explicit offline fixtures"
        )
        parser.add_argument(
            "--config",
            type=Path,
            required=True,
            help="JSON; paths resolve against command working directory",
        )
        parser.add_argument(
            "--output", type=Path, required=True, help="New output directory"
        )
        parser.set_defaults(func=_acquire)
    parser = subparsers.add_parser(
        "screen", help="Exact consecutive two-minute 700bps/1600 trade-count screen"
    )
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--minutes", type=Path, required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument(
        "--minute-receipt",
        type=Path,
        action="append",
        default=[],
        help="Completed minute acquisition stage, repeated per reference symbol",
    )
    parser.set_defaults(
        func=lambda a: screen(
            a.reference,
            a.minutes,
            a.date,
            a.output,
            synthetic=a.synthetic,
            minute_receipts=a.minute_receipt,
        )
    )
    storage = subparsers.add_parser(
        "storage", help="Canonical local inventory and explicit immutable R2 operations"
    )
    children = storage.add_subparsers(dest="operation", required=True)
    parser = children.add_parser(
        "verify", help="Verify complete local pair bytes, rows, schema and coverage"
    )
    parser.add_argument("--pair", type=Path, required=True)
    parser.add_argument("--allow-synthetic", action="store_true")
    parser.set_defaults(func=_storage)
    parser = children.add_parser(
        "inventory", help="Reconcile all selected symbol-days with verified local pairs"
    )
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--pair-root", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path, required=True, help="New JSONL inventory file"
    )
    parser.set_defaults(func=_storage)
    for name in ("publish", "stage"):
        parser = children.add_parser(
            name,
            help="Explicit R2 operation; credentials read from named environment variables",
        )
        parser.add_argument("--config", type=Path, required=True)
        if name == "publish":
            parser.add_argument("--pair", type=Path, required=True)
        else:
            parser.add_argument("--manifest", type=Path, required=True)
            parser.add_argument("--output", type=Path, required=True)
        parser.set_defaults(func=_storage)
