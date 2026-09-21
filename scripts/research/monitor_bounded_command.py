#!/usr/bin/env python3
"""Measure process-tree RSS and owned bytes for one bounded command."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import psutil


def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--measurement", required=True, type=Path)
    parser.add_argument("--watch", action="append", default=[], type=Path)
    parser.add_argument("--study-output", type=Path)
    parser.add_argument("--rss-limit-bytes", type=int, default=8 * 1024**3)
    parser.add_argument("--owned-byte-limit", type=int, default=12 * 1024**3)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command[:1] == ["--"]:
        args.command = args.command[1:]
    if not args.command:
        parser.error("missing command after --")
    return args


def tree_bytes(root: Path) -> int:
    if not root.exists():
        return 0
    if root.is_file():
        return root.stat().st_size
    total = 0
    for base, _, names in os.walk(root):
        for name in names:
            try:
                total += (Path(base) / name).stat().st_size
            except FileNotFoundError:
                pass
    return total


def stop(process: subprocess.Popen) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def main() -> int:
    args = arguments(); args.measurement.parent.mkdir(parents=True, exist_ok=True)
    if args.measurement.exists():
        raise FileExistsError(args.measurement)
    started = time.monotonic(); peak_rss = peak_bytes = 0; stop_reason = None
    log_path = args.measurement.with_suffix(".log")
    with log_path.open("x") as log:
        child = subprocess.Popen(args.command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        process = psutil.Process(child.pid)
        while child.poll() is None:
            try:
                rss = sum(p.memory_info().rss for p in [process, *process.children(recursive=True)] if p.is_running())
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                rss = 0
            owned = sum(tree_bytes(path) for path in args.watch)
            peak_rss = max(peak_rss, rss); peak_bytes = max(peak_bytes, owned)
            if rss > args.rss_limit_bytes:
                stop_reason = "process-tree RSS limit exceeded"; stop(child); break
            if owned > args.owned_byte_limit:
                stop_reason = "owned-byte limit exceeded"; stop(child); break
            time.sleep(.1)
        code = child.wait()
    result = {"command": args.command, "exit_code": code, "stop_reason": stop_reason, "elapsed_seconds": time.monotonic()-started, "sampling_interval_seconds": .1, "peak_process_tree_rss_bytes": peak_rss, "peak_owned_scratch_output_bytes": peak_bytes, "watched_paths": [str(p.resolve()) for p in args.watch]}
    args.measurement.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    if args.study_output and (args.study_output / "run_metadata.json").is_file():
        path = args.study_output / "run_metadata.json"; metadata = json.loads(path.read_text())
        metadata["resource_measurement"] = {"peak_process_tree_rss_bytes": peak_rss, "peak_temporary_and_output_disk_bytes": peak_bytes, "measurement_path": str(args.measurement.resolve()), "sampling_interval_seconds": .1}
        path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return code if stop_reason is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
