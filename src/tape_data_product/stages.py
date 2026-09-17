"""Versioned local stage receipts with transitive installed-code identities.

Domain manifests retain their stricter semantic validation. These receipts bind
explicit inputs, effective parameters and output bytes; they do not certify
numerical correctness without domain verification evidence.
"""

from importlib import metadata
import hashlib
import json
from pathlib import Path
import platform

SCHEMA_VERSION = "tape_product_stage_v1"


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def implementation_identity():
    root = Path(__file__).resolve().parent
    files = {
        str(path.relative_to(root)): sha256(path)
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.suffix in {".py", ".json"}
    }
    # Resolve the installed runtime requirement closure, including conditional
    # dependencies on this platform. Optional development extras are excluded.
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    dependencies = {}
    pending = ["tape-data-product"]
    while pending:
        name = canonicalize_name(pending.pop())
        if name in dependencies:
            continue
        try:
            distribution = metadata.distribution(name)
        except metadata.PackageNotFoundError:
            dependencies[name] = "not-installed"
            continue
        dependencies[name] = distribution.version
        for raw_requirement in distribution.requires or ():
            requirement = Requirement(raw_requirement)
            if requirement.marker is None or requirement.marker.evaluate({"extra": ""}):
                pending.append(requirement.name)
    body = {
        "files": files,
        "dependencies": dependencies,
        "python": platform.python_version(),
        "platform": platform.system(),
        "machine": platform.machine(),
    }
    return {**body, "sha256": hashlib.sha256(_encode(body)).hexdigest()}


def _encode(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def file_identity(path):
    path = Path(path)
    result = {"sha256": sha256(path), "bytes": path.stat().st_size}
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq

        result["rows"] = pq.ParquetFile(path).metadata.num_rows
    return result


def _contained(root, name):
    relative = Path(name)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Output must be a relative contained path: {name}")
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError(f"Output escapes stage directory: {name}")
    return path


def write_stage(
    output, stage, inputs, parameters, outputs, validation, synthetic=False
):
    """Write stage.json last, exclusively, after domain output validation.

    ``outputs`` is an iterable of paths relative to ``output``. ``inputs`` holds
    explicit artifact identities, not inferred paths or ambient configuration.
    """
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    identities = {}
    for name in outputs:
        name = str(name)
        if name == "stage.json" or name in identities:
            raise ValueError("Duplicate or self-referential output")
        identities[name] = file_identity(_contained(root, name))
    if not identities:
        raise ValueError("A completed stage must contain outputs")
    body = dict(
        schema_version=SCHEMA_VERSION,
        stage=stage,
        inputs=inputs,
        parameters=parameters,
        implementation=implementation_identity(),
        outputs=identities,
        validation=validation,
        synthetic=bool(synthetic),
    )
    receipt = {**body, "identity": hashlib.sha256(_encode(body)).hexdigest()}
    with (root / "stage.json").open("x") as stream:
        json.dump(receipt, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
    return receipt


def verify_stage(path, *, require_current_implementation=False):
    """Validate receipt hash and every declared output before downstream use."""
    path = Path(path)
    if path.is_dir():
        path = path / "stage.json"
    receipt = json.loads(path.read_text())
    body = {key: value for key, value in receipt.items() if key != "identity"}
    if receipt.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Unsupported stage schema")
    if receipt.get("identity") != hashlib.sha256(_encode(body)).hexdigest():
        raise ValueError("Stage receipt identity mismatch")
    if not receipt.get("outputs"):
        raise ValueError("Stage has no completed outputs")
    for name, expected in receipt["outputs"].items():
        if file_identity(_contained(path.parent, name)) != expected:
            raise ValueError(f"Stage output identity mismatch: {name}")
    if (
        require_current_implementation
        and receipt["implementation"] != implementation_identity()
    ):
        raise ValueError("Implementation/runtime identity changed; create a new run")
    return receipt
