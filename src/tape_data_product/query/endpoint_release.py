"""Versioned endpoint/EW references and deterministic Phase 4 pilot selection."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from itertools import zip_longest
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time

import pyarrow.parquet as pq

from ..contracts import contract_identity
from ..contracts.config import ContractError, FeatureConfig, canonical_json, digest
from ..contracts.policy import session_bounds
from ..contracts.schemas import BASE_SCHEMA, feature_schema, schema_hash, support_schema
from ..contracts.validation import validate_batch
from ..integrity import read_json, safe_relative, sha256_file, write_atomic_json

REFERENCE_VERSION = "endpoint_ew_reference_v1"
PILOT_ALGORITHM = "month_event_rank_quartiles_v1"
PILOT_SEED = "phase4-pilot-v1"
PILOT_MONTHS = tuple(f"2026-{month:02d}" for month in range(3, 9))
EXPECTED_ROWS = 57_600
READ_BUDGET_BYTES = 8 * 1024**3
OUTPUT_SCRATCH_CAP_BYTES = 4 * 1024**3
FREE_DISK_RESERVE_BYTES = 20 * 1024**3


@dataclass(frozen=True)
class EndpointReferenceHandle:
    root: Path
    manifest: dict
    members: tuple[dict, ...]
    config: FeatureConfig
    data_roots: dict[str, Path]
    snapshots: dict[str, tuple[int, int, int, int, int]]
    validation_seconds: float
    validation_bytes: int


def _stat_snapshot(stat):
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _snapshot(path: Path):
    return _stat_snapshot(path.stat())


def _jsonl(path):
    with Path(path).open() as stream:
        for line in stream:
            if len(line) > 1 << 20:
                raise ContractError("reference member record exceeds 1 MiB")
            if line.strip():
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ContractError("reference member record must be an object")
                yield value


def _read_bytes_stable(path: Path):
    """Read from one pinned inode and return its stable identity."""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ContractError(f"cannot open verified input: {path.name}") from error
    try:
        before = _stat_snapshot(os.fstat(descriptor))
        chunks = []
        while True:
            chunk = os.read(descriptor, 1 << 20)
            if not chunk:
                break
            chunks.append(chunk)
        after = _stat_snapshot(os.fstat(descriptor))
    finally:
        os.close(descriptor)
    if before != after:
        raise ContractError(f"verified input changed while reading: {path.name}")
    return b"".join(chunks), after


def _read_json_stable(path: Path, expected_sha=None, expected_bytes=None):
    payload, snapshot = _read_bytes_stable(path)
    if expected_sha is not None and hashlib.sha256(payload).hexdigest() != expected_sha:
        raise ContractError(f"content identity mismatch: {path.name}")
    if expected_bytes is not None and len(payload) != expected_bytes:
        raise ContractError(f"content identity mismatch: {path.name}")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError(f"invalid JSON input: {path.name}") from error
    return value, snapshot, len(payload), hashlib.sha256(payload).hexdigest()


def _hash_checked(path: Path, expected_sha: str, expected_bytes: int | None = None):
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ContractError(f"cannot open verified input: {path.name}") from error
    hasher = hashlib.sha256()
    size = 0
    try:
        before = _stat_snapshot(os.fstat(descriptor))
        while True:
            chunk = os.read(descriptor, 1 << 20)
            if not chunk:
                break
            hasher.update(chunk)
            size += len(chunk)
        after = _stat_snapshot(os.fstat(descriptor))
    finally:
        os.close(descriptor)
    if before != after:
        raise ContractError(f"verified input changed while hashing: {path.name}")
    sha = hasher.hexdigest()
    if sha != expected_sha or (expected_bytes is not None and size != expected_bytes):
        raise ContractError(f"content identity mismatch: {path.name}")
    return size, after


def _open_parquet_checked(path: Path, expected_snapshot):
    """Open Parquet while proving the path still names the hashed inode."""
    if _snapshot(path) != expected_snapshot:
        raise ContractError("verified endpoint companion changed before open")
    parquet = pq.ParquetFile(path)
    if _snapshot(path) != expected_snapshot:
        raise ContractError("verified endpoint companion changed during open")
    return parquet


def _within(root: Path, relative: str):
    root = root.resolve()
    path = (root / safe_relative(relative)).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ContractError("reference path escapes trusted data root") from error
    return path


def _output_map(manifest, expected):
    outputs = manifest.get("outputs")
    if not isinstance(outputs, list):
        raise ContractError("member outputs are malformed")
    records = {record.get("path"): record for record in outputs if isinstance(record, dict)}
    if set(records) != set(expected) or len(records) != len(outputs):
        raise ContractError("member companion set mismatch")
    for name, record in records.items():
        if set(record) != {"path", "sha256", "bytes", "rows", "schema_sha256"}:
            raise ContractError("member output record is malformed")
        safe_relative(name)
    return records


class _BatchCursor:
    def __init__(self, parquet, columns):
        self._iterator = iter(
            parquet.iter_batches(batch_size=4096, columns=columns, use_threads=False)
        )
        self.batch = None
        self.offset = 0
        self.done = False
        self._advance()

    def _advance(self):
        try:
            self.batch = next(self._iterator)
            self.offset = 0
        except StopIteration:
            self.batch = None
            self.done = True

    @property
    def available(self):
        return 0 if self.done else self.batch.num_rows - self.offset

    def take(self, count):
        if self.done or count < 1 or count > self.available:
            raise ContractError("invalid aligned batch request")
        result = self.batch.slice(self.offset, count)
        self.offset += count
        if self.offset == self.batch.num_rows:
            self._advance()
        return result


def _scan_member(base_path, feature_path, support_path, member, coverage, config):
    parquet = {
        "base": pq.ParquetFile(base_path),
        "features": pq.ParquetFile(feature_path),
        "support": pq.ParquetFile(support_path),
    }
    cursors = {
        kind: _BatchCursor(parquet[kind], None)
        for kind in ("base", "features", "support")
    }
    previous = {kind: None for kind in cursors}
    first = last = None
    rows = 0
    while not all(cursor.done for cursor in cursors.values()):
        if any(cursor.done for cursor in cursors.values()):
            raise ContractError("base/feature/support row count mismatch")
        count = min(cursor.available for cursor in cursors.values())
        batches = {kind: cursor.take(count) for kind, cursor in cursors.items()}
        for kind, batch in batches.items():
            previous[kind] = validate_batch(
                batch, kind, config=config, previous_key=previous[kind]
            )
        for column in range(3):
            if not batches["base"].column(column).equals(
                batches["features"].column(column)
            ) or not batches["base"].column(column).equals(
                batches["support"].column(column)
            ):
                raise ContractError("base/feature/support key mismatch")
        keys = batches["base"]
        if count:
            current_first = tuple(keys.column(i)[0].as_py() for i in range(3))
            current_last = tuple(keys.column(i)[-1].as_py() for i in range(3))
            first = current_first if first is None else first
            last = current_last
        rows += count
    expected_first = (
        member["session_date"],
        member["symbol"],
        coverage["session_start_ns"] + 1_000_000_000,
    )
    expected_last = (member["session_date"], member["symbol"], coverage["end_ns"])
    if rows != coverage["expected_rows"] or first != expected_first or last != expected_last:
        raise ContractError("member grid/coverage mismatch")
    return rows


def _validate_member(
    base_partition: Path,
    feature_partition: Path,
    *,
    expected_member,
    expected_descriptor,
    expected_release,
    config,
    scan_rows,
):
    base_manifest_path = base_partition / "manifest.json"
    feature_manifest_path = feature_partition / "manifest.json"
    base_manifest, base_manifest_snapshot, base_manifest_bytes, base_manifest_sha = (
        _read_json_stable(base_manifest_path)
    )
    feature_manifest, feature_manifest_snapshot, feature_manifest_bytes, feature_manifest_sha = (
        _read_json_stable(feature_manifest_path)
    )
    stable_inputs = {
        base_manifest_path: base_manifest_snapshot,
        feature_manifest_path: feature_manifest_snapshot,
    }
    base_required = {
        "manifest_version", "member", "coverage", "inputs", "source_units",
        "contract_identity", "contract_config", "base_compatibility",
        "implementation_identity", "outputs", "validation", "complete",
    }
    feature_required = {
        "manifest_version", "member", "coverage", "inputs", "source_units",
        "contract_identity", "contract_config", "implementation_identity",
        "outputs", "validation", "complete",
    }
    if (
        set(base_manifest) != base_required
        or set(feature_manifest) != feature_required
        or base_manifest.get("manifest_version") != "tape_member_manifest_v1"
        or feature_manifest.get("manifest_version") != "tape_member_manifest_v1"
        or base_manifest.get("complete") is not True
        or feature_manifest.get("complete") is not True
        or base_manifest.get("validation", {}).get("integrity") != "passed"
        or feature_manifest.get("validation", {}).get("integrity") != "passed"
    ):
        raise ContractError("member completion/integrity evidence is invalid")
    member = {"session_date": expected_member[0], "symbol": expected_member[1]}
    if base_manifest.get("member") != member or feature_manifest.get("member") != member:
        raise ContractError("member manifest identity mismatch")
    coverage = base_manifest.get("coverage")
    if (
        coverage != feature_manifest.get("coverage")
        or coverage
        != {
            "kind": "full",
            "session_start_ns": coverage.get("session_start_ns") if isinstance(coverage, dict) else None,
            "end_ns": coverage.get("end_ns") if isinstance(coverage, dict) else None,
            "expected_rows": EXPECTED_ROWS,
        }
    ):
        raise ContractError("pilot requires complete 57,600-row sessions")
    stored_config = FeatureConfig.from_dict(base_manifest.get("contract_config"))
    if stored_config != config or FeatureConfig.from_dict(
        feature_manifest.get("contract_config")
    ) != config:
        raise ContractError("mixed endpoint/EW configurations")
    expected_contract = contract_identity(config)
    if (
        base_manifest.get("contract_identity") != expected_contract
        or feature_manifest.get("contract_identity") != expected_contract
        or expected_release.get("contract_identity") != expected_contract
    ):
        raise ContractError("endpoint/EW contract identity mismatch")
    base_implementation = base_manifest.get("implementation_identity", {}).get("sha256")
    feature_implementation = feature_manifest.get("implementation_identity", {}).get(
        "sha256"
    )
    if (
        base_implementation != expected_release.get("base_implementation_identity")
        or feature_implementation != expected_release.get("feature_implementation_identity")
    ):
        raise ContractError("mixed producer implementation identities")
    if (
        base_manifest.get("inputs", {}).get("source_pair_sha256")
        != expected_descriptor["source_pair_sha256"]
        or base_manifest.get("inputs", {}).get("context_sha256")
        != expected_descriptor["context_sha256"]
    ):
        raise ContractError("member descriptor identity mismatch")
    if feature_manifest.get("inputs", {}).get("base_manifest_sha256") != base_manifest_sha:
        raise ContractError("feature/base manifest binding mismatch")
    if (
        feature_manifest.get("inputs", {}).get("base_compatibility_sha256")
        != base_manifest.get("base_compatibility", {}).get("sha256")
        or feature_manifest.get("source_units") != base_manifest.get("source_units")
    ):
        raise ContractError("feature/base compatibility mismatch")

    base_outputs = _output_map(base_manifest, ("base.parquet", "context.json"))
    feature_outputs = _output_map(
        feature_manifest, ("features.parquet", "support.parquet")
    )
    schemas = {
        "base.parquet": BASE_SCHEMA,
        "features.parquet": feature_schema(config),
        "support.parquet": support_schema(config),
    }
    consumed_bytes = base_manifest_bytes + feature_manifest_bytes
    paths = {}
    for root, records in (
        (base_partition, base_outputs),
        (feature_partition, feature_outputs),
    ):
        for name, record in records.items():
            path = root / safe_relative(name)
            size, snapshot = _hash_checked(path, record["sha256"], record["bytes"])
            consumed_bytes += size
            stable_inputs[path] = snapshot
            if record["rows"] != EXPECTED_ROWS:
                raise ContractError("member companion row declaration mismatch")
            if name in schemas:
                parquet = _open_parquet_checked(path, snapshot)
                schema = schemas[name]
                if (
                    record["schema_sha256"] != schema_hash(schema)
                    or parquet.metadata.num_rows != EXPECTED_ROWS
                    or not parquet.schema_arrow.equals(schema, check_metadata=True)
                ):
                    raise ContractError("member companion schema mismatch")
                paths[name] = path
    context_path = paths.get("context.json", base_partition / "context.json")
    context, context_snapshot, _, _ = _read_json_stable(
        context_path,
        base_outputs["context.json"]["sha256"],
        base_outputs["context.json"]["bytes"],
    )
    if stable_inputs[context_path] != context_snapshot:
        raise ContractError("context changed between hash and decode")
    if (
        context.get("member") != f"{member['session_date']}/{member['symbol']}"
        or context.get("coverage") != coverage
        or digest(context) != base_outputs["context.json"]["schema_sha256"]
    ):
        raise ContractError("base context binding mismatch")
    if scan_rows:
        _scan_member(
            paths["base.parquet"],
            paths["features.parquet"],
            paths["support.parquet"],
            member,
            coverage,
            config,
        )
    for path, snapshot in stable_inputs.items():
        if _snapshot(path) != snapshot:
            raise ContractError("member input changed during reference construction")
    discovery = context.get("discovery")
    if not isinstance(discovery, dict):
        raise ContractError("member discovery metadata is missing")
    lineage = {
        "selection": context.get("selection"),
        "discovery": discovery,
        "halt_evidence": context.get("halt_evidence"),
        "continuity_evidence": context.get("continuity_evidence"),
        "source_streams": base_manifest.get("inputs", {}).get("streams"),
    }
    if any(value is None for value in lineage.values()):
        raise ContractError("member source/selection lineage is incomplete")
    return {
        "base_manifest": {
            "sha256": base_manifest_sha,
            "bytes": base_manifest_bytes,
            "outputs": base_outputs,
            "implementation_identity": base_implementation,
        },
        "feature_manifest": {
            "sha256": feature_manifest_sha,
            "bytes": feature_manifest_bytes,
            "outputs": feature_outputs,
            "implementation_identity": feature_implementation,
            "consumed_base_manifest_sha256": base_manifest_sha,
        },
        "coverage": coverage,
        "source_units": base_manifest["source_units"],
        "lineage": lineage,
        "consumed_bytes": consumed_bytes,
    }


def select_pilot_members(records):
    """Select one member from each month/rank stratum independent of features."""
    normalized = []
    seen = set()
    for record in records:
        key = record["session_date"], record["symbol"]
        if key in seen:
            raise ContractError("duplicate population member")
        seen.add(key)
        event_count = record["event_count"]
        if type(event_count) is not int or event_count < 0:
            raise ContractError("invalid admitted raw event count")
        normalized.append(
            {
                **record,
                "session_date": key[0],
                "symbol": key[1],
                "event_count": event_count,
            }
        )
    selected = []
    used_symbols = set()
    for month in PILOT_MONTHS:
        monthly = sorted(
            (row for row in normalized if row["session_date"][:7] == month),
            key=lambda row: (
                row["event_count"],
                row["session_date"],
                row["symbol"],
            ),
        )
        if not monthly:
            raise ContractError(f"pilot selection missing month {month}")
        strata = {value: [] for value in range(4)}
        count = len(monthly)
        for rank, row in enumerate(monthly):
            stratum = min(3, (4 * rank) // count)
            strata[stratum].append((rank, row))
        for stratum in range(4):
            candidates = strata[stratum]
            if not candidates:
                raise ContractError(f"pilot selection missing {month} stratum {stratum}")
            candidates.sort(
                key=lambda item: (
                    hashlib.sha256(
                        f"{PILOT_SEED}|{item[1]['session_date']}|{item[1]['symbol']}".encode()
                    ).hexdigest(),
                    item[1]["session_date"],
                    item[1]["symbol"],
                )
            )
            available = [item for item in candidates if item[1]["symbol"] not in used_symbols]
            fallback = not bool(available)
            rank, row = (available or candidates)[0]
            used_symbols.add(row["symbol"])
            selected.append(
                {
                    **row,
                    "month": month,
                    "stratum": stratum,
                    "monthly_rank": rank,
                    "monthly_members": count,
                    "symbol_repeat_fallback": fallback,
                    "selection_hash": hashlib.sha256(
                        f"{PILOT_SEED}|{row['session_date']}|{row['symbol']}".encode()
                    ).hexdigest(),
                }
            )
    if len(selected) != 24:
        raise ContractError("pilot selection did not produce 24 members")
    return tuple(selected)


def _load_population(plan_path, ledger_path):
    plan_path, ledger_path = Path(plan_path), Path(ledger_path)
    plan = read_json(plan_path)
    plan_sha, metadata_bytes = sha256_file(plan_path)
    admitted_path = Path(plan["admitted_index_path"])
    admitted_sha, admitted_bytes = sha256_file(admitted_path)
    metadata_bytes += admitted_bytes
    if admitted_sha != plan["admitted_index_sha256"]:
        raise ContractError("accepted admitted-index identity mismatch")
    for name in ("admissions", "inventory"):
        path = Path(plan[f"{name}_path"])
        sha, size = sha256_file(path)
        metadata_bytes += size
        if sha != plan[f"{name}_sha256"]:
            raise ContractError(f"accepted {name} identity mismatch")
    planned = [(row["session_date"], row["symbol"]) for row in plan["members"]]
    if len(planned) != plan["expected_members"] or len(set(planned)) != len(planned):
        raise ContractError("accepted plan membership is malformed")

    ledger = sqlite3.connect(f"file:{ledger_path}?mode=ro", uri=True)
    index = sqlite3.connect(f"file:{admitted_path}?mode=ro", uri=True)
    ledger.row_factory = index.row_factory = sqlite3.Row
    try:
        completed = {
            row["member"]: dict(row)
            for row in ledger.execute(
                "SELECT member,status,verification_status,base_manifest,feature_manifest FROM members"
            )
        }
        descriptors = {
            row["member"]: dict(row)
            for row in index.execute(
                "SELECT member,source_pair_path,source_pair_sha256,context_path,context_sha256 FROM members"
            )
        }
    finally:
        ledger.close()
        index.close()
    expected_keys = {f"{day}/{symbol}" for day, symbol in planned}
    if set(completed) != expected_keys or set(descriptors) != expected_keys:
        raise ContractError("plan/ledger/admitted-index membership mismatch")
    bad = [
        key
        for key, row in completed.items()
        if row["status"] != "complete" or row["verification_status"] != "passed"
    ]
    if bad:
        raise ContractError(f"population has {len(bad)} incomplete/unverified members")

    population = []
    for day, symbol in planned:
        key = f"{day}/{symbol}"
        descriptor = descriptors[key]
        source_path, context_path = (
            Path(descriptor["source_pair_path"]),
            Path(descriptor["context_path"]),
        )
        for path_key, path in (("source_pair_sha256", source_path), ("context_sha256", context_path)):
            sha, size = sha256_file(path)
            metadata_bytes += size
            if sha != descriptor[path_key]:
                raise ContractError(f"admitted member descriptor changed: {key}")
        source = read_json(source_path)
        context = read_json(context_path)
        if (
            source.get("session_date") != day
            or source.get("symbol") != symbol
            or context.get("member") != key
        ):
            raise ContractError(f"admitted member descriptor substitution: {key}")
        streams = source.get("streams", {})
        try:
            event_count = streams["quotes"]["rows"] + streams["trades"]["rows"]
        except (KeyError, TypeError) as error:
            raise ContractError(f"missing admitted raw event counts: {key}") from error
        if type(event_count) is not int:
            raise ContractError(f"invalid admitted raw event counts: {key}")
        population.append(
            {
                "session_date": day,
                "symbol": symbol,
                "event_count": event_count,
                "source_pair_sha256": descriptor["source_pair_sha256"],
                "context_sha256": descriptor["context_sha256"],
                "ledger_base_manifest": completed[key]["base_manifest"],
                "ledger_feature_manifest": completed[key]["feature_manifest"],
            }
        )
    return plan, plan_sha, population, metadata_bytes


def build_endpoint_reference(
    plan_path,
    ledger_path,
    completion_path,
    base_root,
    feature_root,
    output,
    *,
    expected_plan_sha256,
    expected_population_sha256,
):
    """Build the immutable 24-member metadata-only Phase 4 pilot reference."""
    started = time.perf_counter()
    base_root, feature_root, output = Path(base_root), Path(feature_root), Path(output)
    plan, plan_sha, population, metadata_bytes = _load_population(plan_path, ledger_path)
    if plan_sha != expected_plan_sha256:
        raise ContractError("accepted plan identity mismatch")
    completion_path = Path(completion_path)
    completion = read_json(completion_path)
    completion_sha, completion_bytes = sha256_file(completion_path)
    metadata_bytes += completion_bytes
    if (
        completion.get("status") != "complete"
        or completion.get("plan_sha256") != plan_sha
        or completion.get("population_sha256") != expected_population_sha256
        or completion.get("companions") != {"members": 5208, "missing": 0}
    ):
        raise ContractError("completed population evidence mismatch")
    release = completion.get("release")
    if release != plan.get("release"):
        raise ContractError("plan/completion release identity mismatch")
    config = FeatureConfig.from_dict(plan["config"])
    selected = select_pilot_members(population)
    projected_validation_bytes = 0
    for row in selected:
        relative = Path(f"session_date={row['session_date']}") / f"symbol={row['symbol']}"
        for path in (
            base_root / relative / "manifest.json",
            base_root / relative / "base.parquet",
            base_root / relative / "context.json",
            feature_root / relative / "manifest.json",
            feature_root / relative / "features.parquet",
            feature_root / relative / "support.parquet",
        ):
            projected_validation_bytes += path.stat().st_size
    projected_read_bytes = metadata_bytes + projected_validation_bytes
    if projected_read_bytes > READ_BUDGET_BYTES:
        raise ContractError(
            f"pilot projected reads {projected_read_bytes} exceed {READ_BUDGET_BYTES}"
        )
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    disk = shutil.disk_usage(output.parent)
    if disk.free - OUTPUT_SCRATCH_CAP_BYTES < FREE_DISK_RESERVE_BYTES:
        raise ContractError("insufficient free disk for pilot scratch cap and reserve")
    attempt = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    validation_bytes = 0
    try:
        records = []
        for selected_row in selected:
            day, symbol = selected_row["session_date"], selected_row["symbol"]
            relative = f"session_date={day}/symbol={symbol}"
            base_partition = _within(base_root, relative)
            feature_partition = _within(feature_root, relative)
            expected_base = str(base_partition / "manifest.json")
            expected_feature = str(feature_partition / "manifest.json")
            if (
                Path(selected_row["ledger_base_manifest"]).resolve()
                != Path(expected_base).resolve()
                or Path(selected_row["ledger_feature_manifest"]).resolve()
                != Path(expected_feature).resolve()
            ):
                raise ContractError("ledger member locator does not match trusted roots")
            validated = _validate_member(
                base_partition,
                feature_partition,
                expected_member=(day, symbol),
                expected_descriptor=selected_row,
                expected_release=release,
                config=config,
                scan_rows=True,
            )
            validation_bytes += validated.pop("consumed_bytes")
            records.append(
                {
                    "member": f"{day}/{symbol}",
                    "session_date": day,
                    "symbol": symbol,
                    "base_path": relative,
                    "feature_path": relative,
                    **{
                        key: selected_row[key]
                        for key in (
                            "month",
                            "stratum",
                            "monthly_rank",
                            "monthly_members",
                            "event_count",
                            "symbol_repeat_fallback",
                            "selection_hash",
                            "source_pair_sha256",
                            "context_sha256",
                        )
                    },
                    **validated,
                }
            )
        members_path = attempt / "members.jsonl"
        with members_path.open("x") as stream:
            for record in records:
                stream.write(canonical_json(record) + "\n")
        members_sha, members_bytes = sha256_file(members_path)
        manifest = {
            "version": REFERENCE_VERSION,
            "reference_kind": "pilot",
            "synthetic": False,
            "selection": {
                "algorithm": PILOT_ALGORITHM,
                "seed": PILOT_SEED,
                "months": list(PILOT_MONTHS),
                "strata_per_month": 4,
                "symbol_repeat_policy": "first_hash_candidate_with_unseen_symbol_else_first_and_record_fallback",
            },
            "parent_population": {
                "members": plan["expected_members"],
                "plan_sha256": plan_sha,
                "population_sha256": expected_population_sha256,
                "admitted_index_sha256": plan["admitted_index_sha256"],
                "admissions_sha256": plan["admissions_sha256"],
                "inventory_sha256": plan["inventory_sha256"],
                "completion_sha256": completion_sha,
            },
            "release": {
                key: release[key]
                for key in (
                    "source_revision",
                    "wheel_sha256",
                    "contract_identity",
                    "base_implementation_identity",
                    "feature_implementation_identity",
                )
            },
            "contract_config": config.to_dict(),
            "schemas": {
                "base": schema_hash(BASE_SCHEMA),
                "features": schema_hash(feature_schema(config)),
                "support": schema_hash(support_schema(config)),
            },
            "members": {
                "path": "members.jsonl",
                "sha256": members_sha,
                "bytes": members_bytes,
                "count": len(records),
                "rows_per_table": len(records) * EXPECTED_ROWS,
            },
            "validation": {
                "status": "passed",
                "method": "full_companion_hash_schema_grid_key_value_mask_validation",
                "metadata_inventory_members": len(population),
                "metadata_bytes": metadata_bytes,
                "consumed_validation_bytes": validation_bytes,
                "independent_feature_reconstruction": "not_claimed",
            },
        }
        manifest["reference_identity"] = digest(manifest)
        write_atomic_json(attempt / "manifest.json", manifest)
        os.rename(attempt, output)
    except Exception:
        shutil.rmtree(attempt, ignore_errors=True)
        raise
    elapsed_seconds = time.perf_counter() - started
    return {
        "reference_identity": manifest["reference_identity"],
        "members": len(records),
        "rows_per_table": len(records) * EXPECTED_ROWS,
        "manifest_path": str(output / "manifest.json"),
        "validation_bytes": validation_bytes,
        "metadata_bytes": metadata_bytes,
        "elapsed_seconds": elapsed_seconds,
    }


def _verify_reference_manifest(root: Path, expected_identity: str):
    manifest_path = root / "manifest.json"
    manifest, manifest_snapshot, manifest_bytes, _ = _read_json_stable(manifest_path)
    identity = manifest.pop("reference_identity", None)
    if identity is None or digest(manifest) != identity:
        raise ContractError("reference identity mismatch")
    manifest["reference_identity"] = identity
    if expected_identity is None or identity != expected_identity:
        raise ContractError("unexpected endpoint reference identity")
    if (
        manifest.get("version") != REFERENCE_VERSION
        or manifest.get("reference_kind") not in ("pilot", "synthetic")
        or manifest.get("members", {}).get("path") != "members.jsonl"
    ):
        raise ContractError("incompatible endpoint reference")
    config = FeatureConfig.from_dict(manifest["contract_config"])
    if manifest["release"]["contract_identity"] != contract_identity(config):
        raise ContractError("reference contract/config mismatch")
    expected_schemas = {
        "base": schema_hash(BASE_SCHEMA),
        "features": schema_hash(feature_schema(config)),
        "support": schema_hash(support_schema(config)),
    }
    if manifest.get("schemas") != expected_schemas:
        raise ContractError("reference schema identity mismatch")
    member_file = root / safe_relative(manifest["members"]["path"])
    member_payload, member_snapshot = _read_bytes_stable(member_file)
    if (
        hashlib.sha256(member_payload).hexdigest() != manifest["members"]["sha256"]
        or len(member_payload) != manifest["members"]["bytes"]
    ):
        raise ContractError("content identity mismatch: members.jsonl")
    try:
        lines = member_payload.decode().splitlines()
        members = tuple(json.loads(line) for line in lines if line.strip())
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError("invalid reference member JSON") from error
    if any(not isinstance(row, dict) for row in members):
        raise ContractError("reference member record must be an object")
    keys = [row.get("member") for row in members]
    if (
        len(members) != manifest["members"]["count"]
        or len(set(keys)) != len(keys)
        or any(key != f"{row.get('session_date')}/{row.get('symbol')}" for key, row in zip(keys, members))
        or manifest["members"]["rows_per_table"]
        != sum(row["coverage"]["expected_rows"] for row in members)
    ):
        raise ContractError("reference membership mismatch")
    if manifest["reference_kind"] == "pilot":
        cells = []
        for row in members:
            try:
                start_ns, end_ns = session_bounds(row["session_date"])
            except (KeyError, TypeError) as error:
                raise ContractError("invalid pilot member coverage") from error
            expected_coverage = {
                "kind": "full",
                "session_start_ns": start_ns,
                "end_ns": end_ns,
                "expected_rows": EXPECTED_ROWS,
            }
            cells.append((row.get("month"), row.get("stratum")))
            if row.get("coverage") != expected_coverage:
                raise ContractError("pilot member is not an exact full session")
        expected_cells = [
            (month, stratum) for month in PILOT_MONTHS for stratum in range(4)
        ]
        if (
            manifest.get("synthetic") is not False
            or len(members) != 24
            or manifest["members"]["rows_per_table"] != 24 * EXPECTED_ROWS
            or manifest.get("selection", {}).get("algorithm") != PILOT_ALGORITHM
            or manifest.get("selection", {}).get("seed") != PILOT_SEED
            or cells != expected_cells
        ):
            raise ContractError("invalid deterministic pilot declaration")
    if manifest["reference_kind"] == "synthetic" and manifest.get("synthetic") is not True:
        raise ContractError("synthetic/real reference mixture")
    return (
        manifest,
        members,
        config,
        {
            str(manifest_path): manifest_snapshot,
            str(member_file): member_snapshot,
        },
        manifest_bytes + len(member_payload),
    )


def open_endpoint_reference(path, *, expected_identity, data_roots):
    """Open a scoped verified handle; full consumed-file hashes run once per handle."""
    started = time.perf_counter()
    root = Path(path).resolve()
    if not isinstance(data_roots, dict) or set(data_roots) != {"base", "features"}:
        raise ContractError("data_roots must explicitly provide base and features")
    roots = {name: Path(value).resolve() for name, value in data_roots.items()}
    manifest, members, config, snapshots, validation_bytes = (
        _verify_reference_manifest(root, expected_identity)
    )
    reference_release = manifest["release"]
    schemas = {
        "base.parquet": BASE_SCHEMA,
        "features.parquet": feature_schema(config),
        "support.parquet": support_schema(config),
    }
    for member in members:
        base_partition = _within(roots["base"], member["base_path"])
        feature_partition = _within(roots["features"], member["feature_path"])
        current_manifests = {}
        for partition, key, expected in (
            (base_partition, "base", member["base_manifest"]),
            (feature_partition, "features", member["feature_manifest"]),
        ):
            manifest_path = partition / "manifest.json"
            size, snapshot = _hash_checked(
                manifest_path, expected["sha256"], expected["bytes"]
            )
            validation_bytes += size
            snapshots[str(manifest_path)] = snapshot
            current, decoded_snapshot, _, _ = _read_json_stable(
                manifest_path, expected["sha256"], expected["bytes"]
            )
            if decoded_snapshot != snapshot:
                raise ContractError("manifest changed between hash and decode")
            current_manifests[key] = current
            implementation = current.get("implementation_identity", {}).get("sha256")
            expected_implementation = reference_release[
                f"{'base' if key == 'base' else 'feature'}_implementation_identity"
            ]
            if implementation != expected_implementation:
                raise ContractError("producer identity changed")
            for name, output_record in expected["outputs"].items():
                companion = partition / safe_relative(name)
                size, snapshot = _hash_checked(
                    companion, output_record["sha256"], output_record["bytes"]
                )
                validation_bytes += size
                snapshots[str(companion)] = snapshot
                if name in schemas:
                    parquet = _open_parquet_checked(companion, snapshot)
                    expected_schema = schemas[name]
                    if (
                        output_record["schema_sha256"] != schema_hash(expected_schema)
                        or parquet.metadata.num_rows != output_record["rows"]
                        or not parquet.schema_arrow.equals(
                            expected_schema, check_metadata=True
                        )
                    ):
                        raise ContractError("reference companion schema/count mismatch")
        if (
            member["feature_manifest"]["consumed_base_manifest_sha256"]
            != member["base_manifest"]["sha256"]
        ):
            raise ContractError("reference feature/base binding mismatch")
        base_manifest = current_manifests["base"]
        feature_manifest = current_manifests["features"]
        expected_member = {
            "session_date": member["session_date"], "symbol": member["symbol"]
        }
        if (
            base_manifest.get("member") != expected_member
            or feature_manifest.get("member") != expected_member
            or base_manifest.get("coverage") != member["coverage"]
            or feature_manifest.get("coverage") != member["coverage"]
            or FeatureConfig.from_dict(base_manifest.get("contract_config")) != config
            or FeatureConfig.from_dict(feature_manifest.get("contract_config")) != config
            or feature_manifest.get("inputs", {}).get("base_manifest_sha256")
            != member["base_manifest"]["sha256"]
        ):
            raise ContractError("reference member manifest binding mismatch")
    for consumed_path, snapshot in snapshots.items():
        if _snapshot(Path(consumed_path)) != snapshot:
            raise ContractError("verified input changed during reference open")
    return EndpointReferenceHandle(
        root=root,
        manifest=manifest,
        members=members,
        config=config,
        data_roots=roots,
        snapshots=snapshots,
        validation_seconds=time.perf_counter() - started,
        validation_bytes=validation_bytes,
    )


def verify_handle_unchanged(handle: EndpointReferenceHandle):
    for path, snapshot in handle.snapshots.items():
        if _snapshot(Path(path)) != snapshot:
            raise ContractError("verified endpoint companion changed during read session")
