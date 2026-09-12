"""Explicit immutable publication/staging using shared R2 identity helpers."""

from pathlib import Path
import json
import sys
import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from tape_data_product.features import compact_product as P
from tape_data_product.features.compact_product_runtime import retry_io, BlockedInput
from tape_data_product.storage import r2_tq_storage as S


def client(settings):
    raw = boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key_id,
        aws_secret_access_key=settings.secret_access_key,
        region_name=settings.region,
        config=Config(
            signature_version="s3v4",
            connect_timeout=15,
            read_timeout=60,
            retries={"total_max_attempts": 1, "mode": "standard"},
        ),
    )
    return TransferClient(raw)


class TransferClient:
    """No hidden transfer retry budget or transfer threads."""

    def __init__(self, raw):
        self.raw = raw

    def __getattr__(self, name):
        return getattr(self.raw, name)

    def download_file(self, *args, **kwargs):
        kwargs["Config"] = TransferConfig(
            max_concurrency=1, use_threads=False, num_download_attempts=1
        )
        return self.raw.download_file(*args, **kwargs)

    def upload_file(self, *args, **kwargs):
        kwargs["Config"] = TransferConfig(
            max_concurrency=1, use_threads=False, num_download_attempts=1
        )
        return self.raw.upload_file(*args, **kwargs)


def stage(client, bucket, obj, path, *, record, check=lambda: None):
    def operation():
        if obj.get("metadata"):
            head = client.head_object(Bucket=bucket, Key=obj["object_key"])
            if any(
                head.get("Metadata", {}).get(k) != v for k, v in obj["metadata"].items()
            ):
                raise ValueError("source coverage/provenance metadata changed")
        actual = S.download_verified_parquet(
            client, bucket, obj["object_key"], Path(path), transfer_workers=1
        )
        S.validate_identities(
            S.ObjectIdentity(
                **{k: obj[k] for k in ("object_key", "size_bytes", "sha256", "rows")}
            ),
            actual,
            require_rows=True,
        )
        check()
        return Path(path)

    return retry_io(operation, record=record)


def prefix(manifest):
    m = manifest["metadata"]
    scope = "partitions" if m["expected_rows"] == 57600 else "partial_prefixes"
    return f"derived/tape_data_product/{P.S.LAYOUT_VERSION}/{scope}/{manifest['partition_identity']}"


def publish(
    directory,
    manifest,
    client,
    bucket,
    *,
    record,
    check=lambda: None,
    committed=lambda x: None,
):
    directory = Path(directory)
    root = prefix(manifest)
    objects = {}
    for name in ("features", "support", "manifest"):
        filename = name + (".json" if name == "manifest" else ".parquet")
        path = directory / filename

        def operation():
            # The helper checks existing exact identity before uploading. This
            # also resolves a lost response without recalculating or overwriting.
            result = S.publish_file_immutable(
                client,
                bucket,
                path,
                root + "/" + filename,
                rows=None if name == "manifest" else manifest["objects"][name]["rows"],
                upload_workers=1,
            )
            check()
            return result

        objects[name] = retry_io(operation, record=record)
        committed(dict(name=name, object=objects[name]))
    return dict(bucket=bucket, manifest_key=root + "/manifest.json", objects=objects)


def verify_remote(client, bucket, published, *, record):
    for obj in published["objects"].values():
        expected = S.ObjectIdentity(
            **{k: obj[k] for k in ("object_key", "size_bytes", "sha256", "rows")}
        )

        def operation():
            S.validate_identities(
                expected,
                S.remote_identity(client, bucket, expected.object_key),
                require_rows=expected.rows is not None,
            )

        retry_io(operation, record=record)
    marker = published["objects"]["manifest"]

    def fetch():
        response = client.get_object(Bucket=bucket, Key=marker["object_key"])
        body = response["Body"]
        try:
            data = body.read(2 * 1024**2 + 1)
        finally:
            body.close()
        if len(data) > 2 * 1024**2:
            raise ValueError("remote manifest exceeds bound")
        import hashlib

        if hashlib.sha256(data).hexdigest() != marker["sha256"]:
            raise ValueError("remote completion marker hash mismatch")
        return json.loads(data)

    return retry_io(fetch, record=record)


def publish_release(directory, client, bucket, *, record, check=lambda: None):
    """Publish an exact streaming catalog and dictionary, then its release marker.

    Partial and complete releases have separate immutable identities. Historical
    partition versions cannot enter the catalog by a prefix scan.
    """
    directory = Path(directory)
    status = P.json.loads((directory / "run_status.json").read_text())
    name = (
        "release.json"
        if (directory / "release.json").exists()
        else "partial_release.json"
    )
    release = P.json.loads((directory / name).read_text())
    if (
        release["catalog_sha256"]
        != P.file_identity(directory / "catalog.jsonl", check)["sha256"]
    ):
        raise ValueError("release catalog hash mismatch")
    dictionary = directory / "dictionary.json"
    if not dictionary.exists():
        P.atomic_json(dictionary, P.S.dictionary())
    identity = P.digest(
        dict(release=release, dictionary=P.file_identity(dictionary, check))
    )
    root = f"derived/tape_data_product/{P.S.LAYOUT_VERSION}/releases/{identity}"
    objects = {}
    for artifact in ("catalog.jsonl", "dictionary.json", name):
        objects[artifact] = retry_io(
            lambda: S.publish_file_immutable(
                client,
                bucket,
                directory / artifact,
                root + "/" + artifact,
                upload_workers=1,
            ),
            record=record,
        )
        check()
    return dict(
        release_identity=identity,
        partial=release["partial"],
        objects=objects,
        manifest_key=root + "/" + name,
    )


def resolve_base_candidate(member, manifest, *, seconds):
    """Admit only exact current corrected code, source and overlay provenance."""
    if manifest is None:
        return None
    from tape_data_product.features import snapshot_feature_pipeline as F

    expected_source = dict(
        session_date=member["session_date"],
        symbol=member["symbol"],
        **{
            k: {
                name: member["inputs"][k][name]
                for name in ("object_key", "sha256", "size_bytes", "rows")
            }
            for k in ("quotes", "trades")
        },
    )
    identity = manifest.get("partition_identity", {})
    if manifest.get("contract") != F.identity():
        return None
    if identity.get("source") != expected_source or identity.get("halts") != sorted(
        member["overlay"].get("halts", [])
    ):
        return None
    rows = manifest.get("counts", {}).get("rows")
    if (
        rows is None
        or rows < seconds
        or manifest.get("verification", {}).get("rows_verified") != rows
    ):
        return None
    key = (
        member["candidate_base_manifest"].removesuffix("/manifest.json")
        + "/features.parquet"
    )
    return dict(
        object_key=key,
        sha256=manifest["sha256"],
        size_bytes=manifest["bytes"],
        rows=rows,
        inputs=member["inputs"],
        overlay=member["overlay"],
        calculation=P.legacy_calculation_identity(),
        completion_manifest=member["candidate_base_manifest"],
    )


def read_partition(
    client, bucket, marker, scratch, *, record, allow_partial=False, check=lambda: None
):
    """Stage exactly one committed compact pair; cleanup only this owned cache."""
    import tempfile
    import shutil
    import hashlib

    def fetch():
        expected = S.ObjectIdentity(
            **{k: marker[k] for k in ("object_key", "sha256", "size_bytes", "rows")}
        )
        S.validate_identities(
            expected,
            S.remote_identity(client, bucket, expected.object_key),
            require_rows=False,
        )
        response = client.get_object(Bucket=bucket, Key=expected.object_key)
        body = response["Body"]
        try:
            data = body.read(2 * 1024**2 + 1)
        finally:
            body.close()
        if (
            len(data) != expected.size_bytes
            or hashlib.sha256(data).hexdigest() != expected.sha256
        ):
            raise ValueError("completion marker identity mismatch")
        return data

    data = retry_io(fetch, record=record)
    manifest = json.loads(data)
    if marker["object_key"] != prefix(manifest) + "/manifest.json":
        raise ValueError("completion namespace mismatch")
    if manifest["metadata"]["expected_rows"] != 57600 and not allow_partial:
        raise ValueError("partial partition")
    needed = sum(o["size_bytes"] for o in manifest["objects"].values())
    scratch = Path(scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(scratch).free < 3 * 1024**3 + 2 * needed:
        raise ValueError("reader disk reserve unavailable")
    with tempfile.TemporaryDirectory(prefix="compact-read-", dir=scratch) as folder:
        folder = Path(folder)
        for name in ("features", "support"):
            obj = manifest["objects"][name]
            if obj["file"] != name + ".parquet":
                raise ValueError("unsafe compact object filename")
            stage(
                client,
                bucket,
                obj | dict(object_key=prefix(manifest) + "/" + obj["file"]),
                folder / obj["file"],
                record=record,
                check=check,
            )
        (folder / "manifest.json").write_bytes(data)
        yield from P.completed_rows(folder, allow_partial=allow_partial, check=check)
