#!/usr/bin/env python3
"""Monitored, sequential V3 feature/scaler/neighbor pilot.

Use --sample-seconds for the measured checkpoint. Full raw-data replay requires
--full-run-approved after the measured checkpoint has been presented to the user.
"""
from __future__ import annotations

import argparse
from contextlib import nullcontext
from dataclasses import asdict
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import economic_tape_neighbors_v3 as NN

V3 = NN.V3
ROOT = V3.ROOT
STOP_BYTES = int(2.8*1024**3)


def checkout_revision():
    """Source archives and a fresh checkout may have no Git HEAD yet."""
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT,
                                       text=True, stderr=subprocess.DEVNULL).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None


def atomic_json(path, payload):
    path = Path(path)
    temp = path.with_suffix(".partial.json")
    temp.write_text(json.dumps(payload, indent=2, allow_nan=False)+"\n")
    temp.replace(path)


def file_identity(path):
    stat = Path(path).stat()
    return dict(path=str(Path(path).resolve()), bytes=stat.st_size, mtime_ns=stat.st_mtime_ns,
                rows=V3.pq.ParquetFile(path).metadata.num_rows, sha256=V3.sha256(path))


def load_halts(directory, day, symbol):
    """Verify the frozen registry using the existing authoritative verifier."""
    module = V3.load_module("economic_v3_halts", ROOT/"src/04_research/historical_halt_registry_v1.py")
    directory = Path(directory)
    # The verifier materializes registry metadata, not event data. Bound it explicitly.
    for name in ("registry.parquet", "accepted_intervals.parquet"):
        if V3.pq.ParquetFile(directory/name).metadata.num_rows > 25_000:
            raise ValueError("registry exceeds metadata bound; use a bounded registry verifier")
    manifest = module.verify_frozen_registry(directory)
    intervals = []
    for batch in V3.pq.ParquetFile(directory/"accepted_intervals.parquet").iter_batches(batch_size=1024, use_threads=False):
        for row in batch.to_pylist():
            if str(row["session_date"]) != day or row["symbol"] != symbol:
                continue
            a = int(module._ns(row["halt_interval_start"]))
            b = module._ns(row.get("trade_resume_time"))
            intervals.append((a, int(b) if b is not None else V3.session_bounds(day)[1], row["halt_interval_id"]))
    return intervals, dict(registry_version=manifest["registry_version"],
                           registry_config_hash=manifest["registry_config_hash"],
                           content_sha256=manifest["content_sha256"], accepted_interval_ids=[x[2] for x in intervals],
                           verifier_sha256=V3.sha256(module.__file__))


def require_space(path, required):
    free = shutil.disk_usage(path).free
    if free < required:
        raise OSError(f"insufficient scratch disk: {free} bytes free; {required} required")


def worker(args):
    plan = json.loads(args.plan.read_text())
    if not plan.get("universe_provenance"):
        raise ValueError("source manifest requires universe_provenance")
    sources = plan.get("sources", [])
    if not sources:
        raise ValueError("source manifest is empty")
    if args.sample_seconds is None and not args.full_run_approved:
        raise ValueError("full run requires confirmation after measured checkpoint")
    if args.sample_seconds is not None and not 1 <= args.sample_seconds <= 3600:
        raise ValueError("bounded sample must be 1..3600 seconds")
    if not 1 <= args.batch_size <= 25_000:
        raise ValueError("raw batch size must be 1..25000")
    if args.output.exists():
        raise FileExistsError(args.output)
    args.output.mkdir(parents=True)
    args.scratch_root.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    storage = V3.load_module("economic_v3_r2", ROOT/"src/01_data/r2_tq_storage.py")
    seen = set()
    paths = {NN.REFERENCE_DATE: [], NN.QUERY_DATE: []}
    manifest = dict(version=V3.VERSION, status="running", sample_seconds=args.sample_seconds,
                    universe_provenance=plan["universe_provenance"], plan_sha256=V3.sha256(args.plan),
                    sources=[], full_run_approved_flag=args.full_run_approved)
    atomic_json(args.output/"run_manifest.json", manifest)
    try:
        for source in sources:
            day, symbol = storage._validate_date_symbol(source["session_date"], source["symbol"])
            if day not in paths or (day, symbol) in seen:
                raise ValueError("duplicate source or date outside pilot")
            seen.add((day, symbol))
            coverage = source.get("coverage", {})
            if coverage.get("trades") != "[04:00,20:00) ET" or coverage.get("quotes") != "[03:55,20:00) ET":
                raise ValueError("source lacks canonical coverage declaration")
            if not source.get("coverage_provenance"):
                raise ValueError("source lacks coverage provenance")
            if day not in source.get("halt_coverage_dates", []) or not source.get("halt_coverage_provenance"):
                raise ValueError("source lacks explicit halt registry date coverage/provenance")
            halts, halt_identity = load_halts(source["registry_dir"], day, symbol)
            remote = source.get("r2", False)
            remote_heads = {}
            if remote:
                settings = storage.load_r2_settings()
                client = storage.build_client(settings)
                required = 0
                for stream in ("trades", "quotes"):
                    key = storage.tq_object_key(day, symbol, stream)
                    head = client.head_object(Bucket=settings.bucket, Key=key)
                    identity = storage.object_identity_from_head(key, head)
                    if identity.rows is None:
                        raise ValueError("R2 object lacks row count")
                    required += identity.size_bytes
                    remote_heads[stream] = dict(identity=asdict(identity), metadata=head.get("Metadata", {}))
                require_space(args.scratch_root, required+2*1024**3)
                context = storage.staged_symbol_day(client, settings.bucket, day, symbol, args.scratch_root,
                                                    download_workers=1, transfer_workers=1)
            else:
                context = nullcontext(Path(source["local_dir"]))
            with context as staged:
                directory = staged.directory if remote else staged
                trades, quotes = directory/"trades.parquet", directory/"quotes.parquet"
                identity = V3.contract_identity()
                identity.update(raw_sources={"trades": file_identity(trades), "quotes": file_identity(quotes)},
                                coverage=coverage, coverage_provenance=source["coverage_provenance"],
                                halt_registry=halt_identity, remote_objects=remote_heads,
                                halt_coverage_dates=source["halt_coverage_dates"],
                                halt_coverage_provenance=source["halt_coverage_provenance"],
                                continuity_breaks_ns=source.get("continuity_breaks_ns", []),
                                sample_seconds=args.sample_seconds, source_acceptance="prefix-only" if args.sample_seconds else "complete-file",
                                universe_provenance=plan["universe_provenance"],
                                storage_code_sha256=V3.sha256(storage.__file__),
                                git_revision=checkout_revision(),
                                runner_sha256=V3.sha256(__file__))
                target = args.output/f"{day}_{symbol}.parquet"
                phase_start = time.monotonic()
                result = V3.write_features(quotes, trades, day, symbol, target, identity=identity, halts=halts,
                                           seconds=args.sample_seconds or 57_600, batch_size=args.batch_size,
                                           continuity_breaks_ns=source.get("continuity_breaks_ns", []))
                result.update(session_date=day, symbol=symbol, elapsed_seconds=time.monotonic()-phase_start,
                              identity=identity, output=str(target.resolve()))
                manifest["sources"].append(result)
                paths[day].append(target)
                atomic_json(args.output/"run_manifest.json", manifest)
        if not args.features_only:
            # SQLite scratch shares the chosen scratch volume, not the raw cache.
            reference_rows = sum(s["eligible_rows"] for s in manifest["sources"] if s["session_date"] == NN.REFERENCE_DATE)
            # Conservative metadata/table/index/temp-sort budget, before any DB build.
            projected_search_disk = reference_rows*600 + 2*1024**3
            require_space(args.scratch_root, projected_search_disk)
            manifest["projected_search_scratch_bytes"] = projected_search_disk
            with tempfile.TemporaryDirectory(prefix="v3-search-", dir=args.scratch_root) as work:
                result = NN.run(paths[NN.REFERENCE_DATE], paths[NN.QUERY_DATE], Path(work)/"neighbors",
                                max_queries=args.max_queries, k=args.k)
                shutil.copytree(Path(work)/"neighbors", args.output/"neighbors")
            manifest["retrieval"] = result
        manifest["status"] = "bounded_sample_complete" if args.sample_seconds else "complete"
    except BaseException as exc:
        manifest.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        manifest.update(elapsed_seconds=time.monotonic()-started,
                        worker_peak_rss_bytes=int(value if sys.platform == "darwin" else value*1024))
        atomic_json(args.output/"run_manifest.json", manifest)


def monitored(command, measurement_path):
    """Separate live watchdog, including descendants. Never retries a failed job."""
    import psutil
    env = dict(os.environ, OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", MKL_NUM_THREADS="1")
    process = subprocess.Popen(command, env=env, start_new_session=True)
    peak, worker_peak, supervisor_peak, killed, started = 0, 0, 0, False, time.monotonic()
    try:
        while process.poll() is None:
            try:
                parent = psutil.Process(process.pid)
                rss = parent.memory_info().rss
                for child in parent.children(recursive=True):
                    try:
                        rss += child.memory_info().rss
                    except psutil.NoSuchProcess:
                        pass
                worker_peak = max(worker_peak, rss)
                supervisor = psutil.Process().memory_info().rss
                supervisor_peak = max(supervisor_peak, supervisor)
                peak = max(peak, rss+supervisor)
                if rss+supervisor >= STOP_BYTES:
                    os.killpg(process.pid, signal.SIGTERM)
                    killed = True
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
            except psutil.NoSuchProcess:
                pass
            time.sleep(.05)
        code = process.wait()
    except BaseException:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=5)
        raise
    finally:
        measurement_path = Path(measurement_path)
        measurement_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_json(measurement_path, dict(peak_aggregate_rss_bytes=peak, rss_poll_seconds=.05,
                    peak_worker_tree_rss_bytes=worker_peak, peak_supervisor_rss_bytes=supervisor_peak,
                    stop_threshold_bytes=STOP_BYTES, killed_for_memory=killed,
                    elapsed_seconds=time.monotonic()-started, returncode=process.poll()))
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scratch-root", type=Path, required=True)
    parser.add_argument("--sample-seconds", type=int)
    parser.add_argument("--full-run-approved", action="store_true")
    parser.add_argument("--batch-size", type=int, default=25_000)
    parser.add_argument("--max-queries", type=int, default=128)
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--features-only", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        def terminate(signum, frame):
            raise SystemExit("worker terminated; unwinding private scratch")
        signal.signal(signal.SIGTERM, terminate)
        worker(args)
    else:
        if args.output.exists():
            raise FileExistsError(args.output)
        args.scratch_root.mkdir(parents=True, exist_ok=True)
        # Parent owns this private directory, so even SIGKILL cleanup is bounded
        # to this run's raw/search scratch and never touches source caches.
        with tempfile.TemporaryDirectory(prefix="v3-run-", dir=args.scratch_root) as private:
            command = [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], "--scratch-root", private, "--worker"]
            raise SystemExit(monitored(command, args.output.parent/f"{args.output.name}_resource_measurement.json"))


if __name__ == "__main__":
    main()
