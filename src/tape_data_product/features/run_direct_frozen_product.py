"""Compact product CLI: plan inputs, supervised workers, reports and explicit retry.

A sample never silently expands to a week or six months. External full-session
runs require a frozen measured acceptance checkpoint in their run configuration.
"""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import sys
import threading
import time
import traceback
from tape_data_product.features import compact_product_runtime as R

HERE = Path(__file__).resolve().parent


class Progress:
    def __init__(self, stats):
        self.stats = stats
        self.counter = 0
        self.phase = "initialize"
        self.finished = threading.Event()
        self.path = os.environ.get("COMPACT_PROGRESS")
        self.parent = json.loads(os.environ.get("COMPACT_SUPERVISOR_IDENTITY", "null"))
        self.thread = threading.Thread(target=self.watch, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.finished.set()
        self.thread.join(timeout=2)

    def check(self):
        self.counter += 1

    def watch(self):
        while not self.finished.is_set():
            try:
                if self.parent:
                    try:
                        parent = R.psutil.Process(self.parent["pid"])
                        if (
                            abs(parent.create_time() - self.parent["created"]) > 0.01
                            or not parent.is_running()
                            or parent.status() == R.psutil.STATUS_ZOMBIE
                        ):
                            os._exit(72)
                    except R.psutil.NoSuchProcess:
                        os._exit(72)
                if self.path:
                    R.save(
                        self.path,
                        dict(
                            time=time.time(),
                            counter=self.counter,
                            phase=self.phase,
                            stats=dict(self.stats),
                        ),
                    )
            except Exception:
                pass  # Advisory progress-file I/O must not kill calculation.
            self.finished.wait(1)


def execution_identity():
    from tape_data_product.stages import implementation_identity

    return implementation_identity()


def route(member, accepted):
    from tape_data_product.features import compact_product as P

    def compatible(name):
        obj = member.get(name)
        if not obj:
            return False
        try:
            P.require_compatible(obj, accepted)
            if obj.get("inputs") != member.get("inputs") or obj.get(
                "overlay"
            ) != member.get("overlay"):
                return False
            return True
        except ValueError:
            return False

    if compatible("compact"):
        return "compact"
    if compatible("base"):
        if (
            compatible("extension")
            and member["base"]["calculation"] == member["extension"]["calculation"]
        ):
            return "repack"
        if (
            member.get("canonical_coverage_accepted")
            and member.get("overlay_accepted")
            and member.get("inputs", {}).get("quotes")
        ):
            return "quote_extension"
    sources = member.get("inputs", {})
    if (
        all(sources.get(k) for k in ("quotes", "trades"))
        and member.get("canonical_coverage_accepted")
        and member.get("overlay_accepted")
    ):
        return "direct"
    raise R.BlockedInput(
        "missing accepted canonical inputs or unresolved overlay; compatible calculations unavailable"
    )


def worker(member, attempt, config, *, verify_receipt=None):
    import pyarrow as pa

    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    from tape_data_product.features import compact_product as P
    from tape_data_product.features import compact_product_storage as STORE
    from tape_data_product.features import direct_frozen_product as D
    from tape_data_product.features import all_feature_month_core as C

    attempt = Path(attempt)
    stats = Counter()
    io_attempts = []
    remote = []
    with Progress(stats) as progress:

        def record(number, exc):
            io_attempts.append(
                dict(
                    attempt=number,
                    error_class=type(exc).__name__,
                    message=str(exc)[:512],
                )
            )
            journal = attempt / "io_attempts.jsonl"
            line = json.dumps(io_attempts[-1]) + "\n"
            if journal.exists() and journal.stat().st_size + len(line) > 1024**2:
                raise RuntimeError("I/O attempt evidence limit reached")
            with journal.open("a") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            if len(io_attempts) > 64:
                del io_attempts[0]

        try:
            client = None
            bucket = config.get("bucket")

            def get_client():
                nonlocal client, bucket
                if client is None:
                    try:
                        settings = STORE.S.load_r2_settings()
                        client = STORE.client(settings)
                    except (ValueError, KeyError) as exc:
                        raise R.StopRun("invalid global R2 configuration") from exc
                    bucket = bucket or settings.bucket
                    client.raw.meta.events.register(
                        "before-send.s3",
                        lambda **kwargs: stats.update({"http_requests": 1}),
                    )
                return client

            if verify_receipt is not None:
                progress.phase = "verify_completion"
                if verify_receipt["state"] in ("running", "interrupted"):
                    directory = Path(verify_receipt["attempt_path"]) / "product"
                    manifest = P.verify_complete(
                        directory,
                        expected_metadata=dict(
                            inputs=member["inputs"],
                            overlay=member["overlay"],
                            session_date=member["session_date"],
                            symbol=member["symbol"],
                            expected_rows=config.get("seconds", 57600),
                        ),
                        check=progress.check,
                    )
                    P.require_compatible(
                        {"calculation": manifest["metadata"]["calculation"]},
                        config.get(
                            "accepted_calculations",
                            [P.calculation_identity(), P.legacy_calculation_identity()],
                        ),
                    )
                    verify_receipt = dict(
                        verify_receipt,
                        state="complete",
                        manifest_directory=str(directory),
                        manifest=str(directory / "manifest.json"),
                        partition_identity=manifest["partition_identity"],
                        rows_verified=manifest["validation"]["rows_verified"],
                    )
                    if config.get("publish"):
                        objects = {}
                        for name in ("features", "support", "manifest"):
                            path = directory / (
                                name + (".json" if name == "manifest" else ".parquet")
                            )
                            objects[name] = P.file_identity(
                                path, progress.check
                            ) | dict(
                                object_key=STORE.prefix(manifest) + "/" + path.name,
                                rows=(
                                    None
                                    if name == "manifest"
                                    else manifest["validation"]["rows_verified"]
                                ),
                            )
                        verify_receipt["publication"] = dict(
                            bucket=bucket,
                            objects=objects,
                            manifest_key=objects["manifest"]["object_key"],
                        )
                if verify_receipt.get("publication"):
                    manifest = STORE.verify_remote(
                        get_client(),
                        bucket,
                        verify_receipt["publication"],
                        record=record,
                    )
                    if (
                        manifest["partition_identity"]
                        != verify_receipt["partition_identity"]
                    ):
                        raise ValueError("recovered partition identity mismatch")
                else:
                    manifest = P.verify_complete(
                        verify_receipt["manifest_directory"], check=progress.check
                    )
                result = dict(
                    verify_receipt,
                    reused=True,
                    validation_mode=manifest["validation"].get(
                        "mode", "legacy_reconstruction"
                    ),
                    independent_reconstruction=manifest["validation"].get(
                        "independent_reconstruction", "passed"
                    ),
                )
                R.save(attempt / "result.json", result)
                return 0
            if member.get("_release"):
                progress.phase = "publish_release"
                published = STORE.publish_release(
                    member["_release"],
                    get_client(),
                    bucket,
                    record=record,
                    check=progress.check,
                )
                R.save(
                    attempt / "result.json",
                    dict(state="complete", publication=published),
                )
                return 0
            if (
                member.get("candidate_base_manifest")
                and not member.get("base")
                and not member.get("compact")
            ):
                from tape_data_product.features import tape_feature_store as REFERENCE

                progress.phase = "resolve_existing_base"
                candidate = R.retry_io(
                    lambda: REFERENCE.read_manifest(
                        get_client(), bucket, member["candidate_base_manifest"]
                    ),
                    record=record,
                )
                resolved = STORE.resolve_base_candidate(
                    member, candidate, seconds=config.get("seconds", 57600)
                )
                if resolved:
                    member = dict(member, base=resolved)
                else:
                    stats["incompatible_or_missing_base_candidates"] += 1
            for stream, obj in member.get("inputs", {}).items():
                key = obj.get("object_key", "")
                if (
                    key.startswith("tq/")
                    and key
                    != f"tq/session_date={member['session_date']}/symbol={member['symbol']}/{stream}.parquet"
                ):
                    raise R.BlockedInput("canonical object key does not match member")
            selected = route(
                member,
                config.get(
                    "accepted_calculations",
                    [P.calculation_identity(), P.legacy_calculation_identity()],
                ),
            )
            if selected == "compact":
                prior = member["compact"]
                manifest = P.verify_complete(prior["path"], check=progress.check)
                expected = dict(
                    inputs=member["inputs"],
                    overlay=member["overlay"],
                    session_date=member["session_date"],
                    symbol=member["symbol"],
                    expected_rows=config.get("seconds", 57600),
                    calculation=prior["calculation"],
                    discovery_verified=bool(
                        member.get("discovery", {}).get("verified")
                    ),
                )
                if any(manifest["metadata"].get(k) != v for k, v in expected.items()):
                    raise ValueError("compact reuse dependencies mismatch")
                discovery = member.get("discovery", {})
                expected_discovery = dict(
                    zip(
                        P.S.DISCOVERY_FIELDS,
                        (
                            discovery.get("endpoint_ns"),
                            discovery.get("received_at_ns"),
                            discovery.get("timing_basis", "unavailable"),
                            discovery.get("provenance_hash"),
                        ),
                    )
                )
                if manifest["metadata"]["discovery"] != expected_discovery:
                    raise ValueError("compact discovery identity mismatch")
                result = dict(
                    state="complete",
                    route=selected,
                    reused=True,
                    validation_mode=manifest["validation"].get(
                        "mode", "legacy_reconstruction"
                    ),
                    independent_reconstruction=manifest["validation"].get(
                        "independent_reconstruction", "passed"
                    ),
                    manifest_directory=prior["path"],
                    manifest=str(Path(prior["path"]) / "manifest.json"),
                    partition_identity=manifest["partition_identity"],
                    rows_verified=manifest["validation"]["rows_verified"],
                )
                R.save(attempt / "result.json", result)
                return 0

            def local(obj, name):
                progress.phase = "stage_" + name
                if obj.get("path"):
                    path = Path(obj["path"])
                    actual = P.file_identity(path, progress.check)
                    if any(actual[k] != obj[k] for k in ("sha256", "size_bytes")):
                        raise ValueError("local input identity mismatch")
                    if C.pq.ParquetFile(path).metadata.num_rows != obj["rows"]:
                        raise ValueError("input row count mismatch")
                    return path
                return STORE.stage(
                    get_client(),
                    bucket,
                    obj,
                    attempt / (name + ".parquet"),
                    record=record,
                    check=progress.check,
                )

            discovery = member.get("discovery", {})
            meta = dict(
                session_date=member["session_date"],
                symbol=member["symbol"],
                expected_rows=config.get("seconds", 57600),
                inputs=member["inputs"],
                overlay=member["overlay"],
                discovery_verified=bool(discovery.get("verified")),
                calculation=(
                    P.calculation_identity()
                    if selected == "direct"
                    else member["base"]["calculation"]
                ),
                execution=execution_identity(),
                route=selected,
                source_provenance=member.get("source_provenance", {}),
            )
            if selected == "direct":
                q = local(member["inputs"]["quotes"], "quotes")
                t = local(member["inputs"]["trades"], "trades")
                pairs = D.product_pairs(
                    q,
                    t,
                    meta["session_date"],
                    meta["symbol"],
                    discovery,
                    seconds=meta["expected_rows"],
                    halts=member["overlay"].get("halts", ()),
                    continuity_breaks_ns=member.get("continuity_breaks_ns", ()),
                    batch_size=config.get("batch_size", 4096),
                    stats=stats,
                    check=progress.check,
                )
            else:
                base = local(member["base"], "base")
                from tape_data_product.features import (
                    snapshot_feature_pipeline as REFERENCE,
                )

                embedded = json.loads(
                    (C.pq.ParquetFile(base).schema_arrow.metadata or {}).get(
                        b"tape_snapshot", b"{}"
                    )
                )
                if embedded.get("contract") != REFERENCE.identity():
                    raise ValueError(
                        "base embedded corrected calculation identity mismatch"
                    )
                if selected == "quote_extension":
                    quotes = local(member["inputs"]["quotes"], "quotes")
                    extension = attempt / "extension.parquet"
                    C.write_rows(
                        extension,
                        C.extension_rows(
                            base,
                            quotes,
                            meta["session_date"],
                            meta["symbol"],
                            batch_size=config.get("batch_size", 4096),
                            check=progress.check,
                            full=meta["expected_rows"] == 57600,
                            limit=meta["expected_rows"],
                            halts=member["overlay"].get("halts", ()),
                            stats=stats,
                        ),
                        {},
                        check=progress.check,
                        columns=C.extension_columns(),
                    )
                else:
                    extension = local(member["extension"], "extension")
                pairs = P.split_rows(
                    C.joined_rows(
                        base,
                        extension,
                        discovery,
                        check=progress.check,
                        limit=meta["expected_rows"],
                    )
                )
            progress.phase = "calculate_write_verify"
            directory = attempt / "product"
            manifest = P.write_partition(
                directory,
                pairs,
                meta,
                output_rows=config.get("output_rows", 12288),
                check=progress.check,
                validation_mode=config.get("validation_mode", "integrity"),
            )
            result = dict(
                state="complete",
                route=selected,
                reused=selected != "direct",
                rows_verified=manifest["validation"]["rows_verified"],
                validation_mode=manifest["validation"].get(
                    "mode", "legacy_reconstruction"
                ),
                independent_reconstruction=manifest["validation"].get(
                    "independent_reconstruction", "passed"
                ),
                partition_identity=manifest["partition_identity"],
                manifest_directory=str(directory),
                manifest=str(directory / "manifest.json"),
                stats=dict(stats),
                phase_seconds=manifest["phase_seconds"],
                output_bytes={
                    k: v["size_bytes"] for k, v in manifest["objects"].items()
                },
                source_bytes=sum(v["size_bytes"] for v in member["inputs"].values()),
                io_attempts=io_attempts,
            )
            if config.get("publish"):
                progress.phase = "publish"

                def committed(obj):
                    remote.append(obj)
                    R.save(attempt / "remote_objects.json", remote)

                result["publication"] = STORE.publish(
                    directory,
                    manifest,
                    get_client(),
                    bucket,
                    record=record,
                    check=progress.check,
                    committed=committed,
                )
                result["manifest"] = result["publication"]["manifest_key"]
            result["stats"] = dict(stats)
            R.save(attempt / "result.json", result)
            return 0
        except (R.StopRun, MemoryError) as exc:
            R.save(attempt / "result.json", dict(state="interrupted", error=str(exc)))
            return 70
        except Exception as exc:
            if isinstance(exc, OSError) and exc.errno in (12, 28, 122):
                R.save(
                    attempt / "result.json",
                    dict(state="interrupted", error="memory/disk resource failure"),
                )
                return 70
            result = dict(
                state=(
                    "blocked_input"
                    if isinstance(exc, (R.BlockedInput, FileNotFoundError))
                    else "failed"
                ),
                phase=progress.phase,
                error_class=type(exc).__name__,
                error=str(exc)[:2048],
                error_signature=R.signature(progress.phase, exc),
                traceback=traceback.format_exc()[-8192:],
            )
            R.save(attempt / "result.json", result)
            return 1


def require_acceptance(config):
    """An earlier serial acceptance does not authorize a concurrent full run."""
    if config.get("seconds", 57600) != 57600:
        return
    checkpoint = config.get("acceptance_checkpoint", {})
    if not checkpoint.get("user_confirmed"):
        raise R.StopRun(
            "full external-data execution requires the measured checkpoint and user confirmation"
        )
    workers = config.get("workers", 1)
    if workers > 1 and checkpoint.get("workers") != workers:
        raise R.StopRun(
            "full multi-worker execution requires a measured acceptance checkpoint for this worker count"
        )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="mode", required=True)
    for name in ("run", "report", "reconcile"):
        s = sub.add_parser(name)
        s.add_argument("--run-dir", type=Path, required=True)
        s.add_argument("--inventory", type=Path, required=True)
        if name in ("run", "reconcile"):
            s.add_argument("--config", type=Path, required=True)
            s.add_argument("--retry-selected", type=Path)
            s.add_argument(
                "--workers",
                type=int,
                choices=range(1, 8),
                help="maximum concurrent partition workers; frozen into run identity (default: config workers or 1)",
            )
        if name == "reconcile":
            s.add_argument("--member-id", required=True)
    s = sub.add_parser("audit")
    s.add_argument("--directory", type=Path, required=True)
    s = sub.add_parser("freeze")
    s.add_argument("--selection", type=Path, action="append", required=True)
    s.add_argument("--output", type=Path, required=True)
    s.add_argument("--months", nargs="+", required=True)
    s = sub.add_parser("select-week")
    s.add_argument("--inventory", type=Path, required=True)
    s.add_argument("--session-dates", type=Path, required=True)
    s.add_argument("--output", type=Path, required=True)
    s = sub.add_parser("_worker")
    s.add_argument("--payload", type=Path, required=True)
    s.add_argument("--attempt", type=Path, required=True)
    args = p.parse_args()
    if args.mode == "audit":
        from tape_data_product.features import compact_product as P

        try:
            result = P.audit_complete(args.directory)
        except Exception as exc:
            print(
                json.dumps(
                    dict(state="failed", error_class=type(exc).__name__, error=str(exc))
                )
            )
            return 1
        print(json.dumps(result, indent=2))
        return 0
    if args.mode == "freeze":
        from tape_data_product.features import compact_product_inventory as I

        print(
            json.dumps(
                I.freeze(args.selection, args.output, months=args.months), indent=2
            )
        )
        return 0
    if args.mode == "select-week":
        from tape_data_product.features import compact_product_inventory as I

        plan = I.select_week(args.inventory, R.read(args.session_dates), args.output)
        R.save(args.output.with_suffix(".plan.json"), plan)
        print(json.dumps(plan, indent=2))
        return 0 if plan["state"] == "frozen_one_week" else 1
    if args.mode == "_worker":
        payload = R.read(args.payload)
        return worker(
            payload["member"],
            args.attempt,
            payload["config"],
            verify_receipt=payload.get("verify_receipt"),
        )
    if args.mode == "report":
        R.regenerate(args.run_dir, args.inventory)
        return 0
    config = R.read(args.config)
    config["execution"] = execution_identity()
    if args.workers is not None:
        config["workers"] = args.workers
    R.validate_config(config)
    if args.mode == "run":
        try:
            require_acceptance(config)
        except R.StopRun as exc:
            p.error(str(exc))
    retry = []
    if args.retry_selected:
        if args.retry_selected.stat().st_size > 2 * 1024**2:
            raise R.StopRun("retry control file exceeds 2 MiB; split the selection")
        with args.retry_selected.open() as f:
            retry = [json.loads(line)["member_id"] for line in f]
    args.run_dir.mkdir(parents=True, exist_ok=True)

    def invoke(member, receipt, progress, verify=False):
        attempt = (
            Path(receipt["attempt_path"])
            if not verify
            else args.run_dir
            / "verification"
            / R.member_id(member)
            / str(time.time_ns())
        )
        payload = args.run_dir / "payloads" / (R.member_id(member) + ".json")
        R.save(
            payload,
            dict(
                member=member,
                config=config,
                **({"verify_receipt": receipt} if verify else {}),
            ),
        )
        return R.supervise(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "_worker",
                "--payload",
                str(payload.resolve()),
                "--attempt",
                str(attempt.resolve()),
            ],
            attempt,
            config,
            on_progress=progress,
        )

    def verify(member, receipt):
        if R.verify_published_receipt(member, receipt, config):
            return
        result = invoke(member, receipt, lambda p: None, True)
        if result["state"] != "complete":
            raise ValueError(
                "completed partition verification failed: " + str(result.get("error"))
            )

    if args.mode == "reconcile":
        frozen = R.read(args.run_dir / "run_plan.json")
        if frozen["config"] != config or frozen["inventory_sha256"] != R.hash_file(
            args.inventory
        ):
            raise R.StopRun(
                "reconciliation requires unchanged original code/config/inputs"
            )
        for member in R.members(args.inventory):
            if R.member_id(member) == args.member_id:
                result = R.reconcile(
                    args.run_dir,
                    member,
                    lambda m, r: invoke(m, r, lambda p: None, True),
                )
                R.regenerate(args.run_dir, args.inventory)
                return 0 if result else 1
        raise R.StopRun("member not in frozen inventory")
    result = R.run(
        args.run_dir,
        args.inventory,
        config,
        invoke,
        retry_selected=retry,
        verify_completed=verify,
    )
    if config.get("publish") and R.read(args.run_dir / "run_status.json")[
        "state"
    ] not in ("stopped", "drained"):
        with R.run_lock(args.run_dir):
            attempt = args.run_dir / "release_attempts" / str(time.time_ns())
            payload = args.run_dir / "release_payload.json"
            R.save(
                payload,
                dict(member={"_release": str(args.run_dir.resolve())}, config=config),
            )
            publication = R.supervise(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "_worker",
                    "--payload",
                    str(payload.resolve()),
                    "--attempt",
                    str(attempt.resolve()),
                ],
                attempt,
                config,
            )
            if publication["state"] != "complete":
                return 1
            R.save(args.run_dir / "publication.json", publication)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
