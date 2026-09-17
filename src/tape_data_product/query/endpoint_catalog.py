"""Identity-bound completed-file catalogs for endpoint/EW SQL access."""
from __future__ import annotations

from datetime import date
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

from ..contracts import contract_identity
from ..contracts.config import ContractError, FeatureConfig, canonical_json, digest
from ..contracts.schemas import BASE_SCHEMA, feature_schema, schema_hash, support_schema
from ..integrity import safe_relative, sha256_file, write_atomic_json
from .endpoint_reader import describe_endpoint_fields, iter_endpoint_batches
from .endpoint_release import (
    EndpointReferenceHandle,
    _read_bytes_stable,
    _read_json_stable,
    verify_handle_unchanged,
)
from .endpoint_selection import EndpointSelection


CATALOG_VERSION = "endpoint_query_catalog_v1"


def _select_reference_members(handle, members):
    available = {record["member"]: record for record in handle.members}
    if members is None:
        return handle.members
    if (
        not isinstance(members, (tuple, list))
        or not members
        or len(set(members)) != len(members)
        or any(not isinstance(member, str) or not member for member in members)
    ):
        raise ContractError("catalog members must be a nonempty unique sequence")
    missing = sorted(set(members) - set(available))
    if missing:
        raise ContractError(f"catalog requests absent completed members: {missing}")
    wanted = set(members)
    return tuple(record for record in handle.members if record["member"] in wanted)


def build_endpoint_query_catalog(handle, output, *, members=None):
    """Create an arbitrary-member catalog from an already verified reference.

    The output format deliberately has no deterministic-pilot shape requirement.
    Each included member is scanned once through the accepted projected reader so
    the catalog binds completed files whose keys, masks, and values passed the
    existing validation path.
    """
    if not isinstance(handle, EndpointReferenceHandle):
        raise ContractError("invalid endpoint reference handle")
    selected = _select_reference_members(handle, members)
    verify_handle_unchanged(handle)
    fields = tuple(row["name"] for row in describe_endpoint_fields(handle.config))
    selection = EndpointSelection(
        "historical_membership",
        members=tuple(record["member"] for record in selected),
    )
    rows_validated = 0
    for batch in iter_endpoint_batches(
        handle,
        fields=fields,
        selection=selection,
        batch_size=4096,
    ):
        rows_validated += batch.num_rows
    expected_rows = sum(record["coverage"]["expected_rows"] for record in selected)
    if rows_validated != expected_rows:
        raise ContractError("catalog row validation count mismatch")
    verify_handle_unchanged(handle)

    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    attempt = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        members_path = attempt / "members.jsonl"
        with members_path.open("x") as stream:
            for record in selected:
                stream.write(canonical_json(record) + "\n")
        members_sha, members_bytes = sha256_file(members_path)
        manifest = {
            "version": CATALOG_VERSION,
            "catalog_kind": "completed_endpoint_members",
            "source_reference_identity": handle.manifest["reference_identity"],
            "synthetic": bool(handle.manifest.get("synthetic")),
            "release": handle.manifest["release"],
            "contract_config": handle.config.to_dict(),
            "schemas": {
                "base": schema_hash(BASE_SCHEMA),
                "features": schema_hash(feature_schema(handle.config)),
                "support": schema_hash(support_schema(handle.config)),
            },
            "members": {
                "path": "members.jsonl",
                "sha256": members_sha,
                "bytes": members_bytes,
                "count": len(selected),
                "rows_per_table": expected_rows,
            },
            "validation": {
                "status": "passed",
                "method": "verified_reference_plus_all_27_projected_value_mask_key_scan",
                "rows": rows_validated,
            },
        }
        manifest["catalog_identity"] = digest(manifest)
        write_atomic_json(attempt / "manifest.json", manifest)
        os.rename(attempt, output)
    except Exception:
        shutil.rmtree(attempt, ignore_errors=True)
        raise
    return {
        "catalog_identity": manifest["catalog_identity"],
        "members": len(selected),
        "rows_per_table": expected_rows,
        "manifest_path": str(output / "manifest.json"),
    }


def read_endpoint_query_catalog(path, *, expected_identity=None):
    """Read and structurally validate a completed-file query catalog."""
    root = Path(path).resolve()
    manifest_path = root / "manifest.json"
    manifest, manifest_snapshot, manifest_bytes, _ = _read_json_stable(manifest_path)
    required = {
        "version",
        "catalog_kind",
        "source_reference_identity",
        "synthetic",
        "release",
        "contract_config",
        "schemas",
        "members",
        "validation",
        "catalog_identity",
    }
    if set(manifest) != required:
        raise ContractError("query catalog manifest is malformed")
    identity = manifest.pop("catalog_identity", None)
    if identity is None or digest(manifest) != identity:
        raise ContractError("query catalog identity mismatch")
    manifest["catalog_identity"] = identity
    if expected_identity is not None and identity != expected_identity:
        raise ContractError("unexpected query catalog identity")
    if (
        manifest["version"] != CATALOG_VERSION
        or manifest["catalog_kind"] != "completed_endpoint_members"
        or manifest["validation"]
        != {
            "status": "passed",
            "method": "verified_reference_plus_all_27_projected_value_mask_key_scan",
            "rows": manifest["members"].get("rows_per_table"),
        }
    ):
        raise ContractError("incompatible or unvalidated query catalog")
    config = FeatureConfig.from_dict(manifest["contract_config"])
    if manifest["release"].get("contract_identity") != contract_identity(config):
        raise ContractError("query catalog contract/config mismatch")
    expected_schemas = {
        "base": schema_hash(BASE_SCHEMA),
        "features": schema_hash(feature_schema(config)),
        "support": schema_hash(support_schema(config)),
    }
    if manifest["schemas"] != expected_schemas:
        raise ContractError("query catalog schema identity mismatch")
    descriptor = manifest["members"]
    if (
        set(descriptor) != {"path", "sha256", "bytes", "count", "rows_per_table"}
        or descriptor["path"] != "members.jsonl"
        or type(descriptor["count"]) is not int
        or descriptor["count"] < 1
    ):
        raise ContractError("query catalog membership descriptor is malformed")
    members_path = root / safe_relative(descriptor["path"])
    payload, members_snapshot = _read_bytes_stable(members_path)
    if (
        hashlib.sha256(payload).hexdigest() != descriptor["sha256"]
        or len(payload) != descriptor["bytes"]
    ):
        raise ContractError("query catalog membership identity mismatch")
    try:
        members = tuple(
            json.loads(line) for line in payload.decode().splitlines() if line.strip()
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError("invalid query catalog membership JSON") from error
    core = {
        "member",
        "session_date",
        "symbol",
        "base_path",
        "feature_path",
        "coverage",
        "lineage",
        "base_manifest",
        "feature_manifest",
    }
    keys = []
    rows = 0
    for record in members:
        if not isinstance(record, dict) or not core.issubset(record):
            raise ContractError("query catalog member record is malformed")
        key = f"{record['session_date']}/{record['symbol']}"
        if record["member"] != key:
            raise ContractError("query catalog member identity mismatch")
        try:
            date.fromisoformat(record["session_date"])
            safe_relative(record["base_path"])
            safe_relative(record["feature_path"])
            expected_rows = record["coverage"]["expected_rows"]
        except (KeyError, TypeError, ValueError) as error:
            raise ContractError("query catalog member metadata is invalid") from error
        if type(expected_rows) is not int or expected_rows < 1:
            raise ContractError("query catalog member row count is invalid")
        keys.append(key)
        rows += expected_rows
    if (
        len(members) != descriptor["count"]
        or len(set(keys)) != len(keys)
        or rows != descriptor["rows_per_table"]
    ):
        raise ContractError("query catalog membership accounting mismatch")
    return (
        manifest,
        members,
        config,
        {
            str(manifest_path): manifest_snapshot,
            str(members_path): members_snapshot,
        },
        manifest_bytes + len(payload),
    )
