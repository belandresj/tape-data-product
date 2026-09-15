"""Bounded process scheduling and aggregate guards for calculation plans."""
from __future__ import annotations

import ctypes
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import queue
import signal
import sys
import time
import traceback

import psutil

from .contracts.config import ContractError, FeatureConfig
from .integrity import read_json

THREAD_LIMIT_ENV = (
    "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS", "ARROW_NUM_THREADS", "VECLIB_MAXIMUM_THREADS",
    "BLIS_NUM_THREADS",
)


def runner_identity():
    path = Path(__file__)
    return {"module": "tape_data_product.calculate_runtime",
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def member_paths(plan, member):
    suffix = Path(f"session_date={member['session_date']}") / f"symbol={member['symbol']}"
    return Path(plan["base_root"]) / suffix, Path(plan["feature_root"]) / suffix


def member_order(plan, descriptors):
    """Largest-first from admitted Parquet row metadata, else stable plan order."""
    weighted = []
    for position, member in enumerate(plan["members"]):
        key = f"{member['session_date']}/{member['symbol']}"
        try:
            streams = read_json(descriptors[key][0])["streams"]
            rows = sum(streams[name]["rows"] for name in ("quotes", "trades"))
            if type(rows) is not int or rows < 0:
                raise ValueError
        except (KeyError, TypeError, ValueError, OSError):
            return list(plan["members"]), "deterministic_plan_order_no_reliable_event_counts"
        weighted.append((-rows, position, member))
    return [item[2] for item in sorted(weighted)], "largest_first_admitted_stream_rows"


def _path_bytes(path):
    path = Path(path)
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for child in path.rglob("*"):
        try:
            if child.is_file():
                total += child.stat().st_size
        except FileNotFoundError:
            pass
    return total


def _active_output_bytes(plan, active, baseline):
    total = 0
    for member in active.values():
        key = f"{member['session_date']}/{member['symbol']}"
        base, features = member_paths(plan, member)
        member_total = 0
        for output in (base, features):
            member_total += _path_bytes(output)
            if output.parent.exists():
                for attempt in output.parent.glob(f".{output.name}.attempt-*"):
                    member_total += _path_bytes(attempt)
        total += max(0, member_total - baseline[key])
    return total


def _nearest_existing(path):
    path = Path(path)
    while not path.exists() and path != path.parent:
        path = path.parent
    return path


def _tree_rss():
    process = psutil.Process()
    total = 0
    for child in (process, *process.children(recursive=True)):
        try:
            total += child.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return total


def _tree_cpu_seconds():
    process = psutil.Process()
    total = 0.0
    for child in (process, *process.children(recursive=True)):
        try:
            cpu = child.cpu_times()
            total += cpu.user + cpu.system
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return total


def _tree_io_bytes():
    process = psutil.Process()
    read_bytes = write_bytes = 0
    for child in (process, *process.children(recursive=True)):
        try:
            counters = child.io_counters()
            read_bytes += counters.read_bytes
            write_bytes += counters.write_bytes
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return read_bytes, write_bytes


def _resource_sample(plan, started, active, committed_bytes, baseline):
    limits = plan["limits"]
    runtime = limits.get("runtime_max_seconds")
    reason = None
    if (type(runtime) in (int, float) and not isinstance(runtime, bool)
            and runtime > 0 and time.monotonic() - started >= runtime):
        reason = "aggregate runtime limit exceeded"
    rss = _tree_rss()
    rss_limits = [limits[name] for name in
                  ("process_tree_rss_stop_bytes", "memory_max_bytes")
                  if type(limits.get(name)) is int and limits[name] > 0]
    if reason is None and rss_limits and rss >= min(rss_limits):
        reason = "aggregate process-tree RSS limit exceeded"
    owned = committed_bytes + _active_output_bytes(plan, active, baseline)
    scratch = limits.get("scratch_cap_bytes")
    # Zero retains the legacy synthetic-plan meaning: reserve no capacity at
    # preflight, without imposing a dynamic zero-byte output ceiling.
    if reason is None and type(scratch) is int and scratch > 0 and owned > scratch:
        reason = "aggregate output/scratch limit exceeded"
    reserve = limits.get("disk_reserve_bytes")
    if reason is None and type(reserve) is int and reserve >= 0:
        for root_name in ("base_root", "feature_root", "ledger_path"):
            candidate = plan[root_name]
            if root_name == "ledger_path":
                candidate = Path(candidate).parent
            stat = os.statvfs(_nearest_existing(candidate))
            if stat.f_bavail * stat.f_frsize < reserve:
                reason = "free-disk reserve breached"
                break
    return reason, rss, owned


def _arm_parent_death(parent_pid):
    if sys.platform.startswith("linux"):
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(1, signal.SIGTERM) != 0:
            raise OSError(ctypes.get_errno(), "prctl(PR_SET_PDEATHSIG) failed")
        if os.getppid() != parent_pid:
            os._exit(70)


def _worker_main(input_queue, result_queue, config_body, batch_size, parent_pid):
    _arm_parent_death(parent_pid)
    # Spawn inherits THREAD_LIMIT_ENV before importing Arrow/numerical builders.
    from .features.endpoint_ew import build_from_base
    from .replay.builder import build_base_partition

    config = FeatureConfig.from_dict(config_body)
    if any(os.environ.get(name) != "1" for name in THREAD_LIMIT_ENV):
        raise ContractError("worker computational thread limits were not applied before import")
    while True:
        task = input_queue.get()
        if task is None:
            return
        task_id, key, pair_path, context_path, base, features = task
        try:
            base_result = build_base_partition(
                pair_path, context_path, base, config=config, batch_size=batch_size)
            feature_result = build_from_base(
                base, features, config=config, batch_size=batch_size)
            result_queue.put((
                task_id, key, True, str(base_result.manifest_path),
                str(feature_result.manifest_path),
                _path_bytes(base) + _path_bytes(features), None,
            ))
        except BaseException as error:
            detail = "".join(
                traceback.format_exception_only(type(error), error)).strip()[:4096]
            result_queue.put((task_id, key, False, None, None, 0, detail))


def _stop_workers(processes, queues, *, graceful=False):
    if graceful:
        for input_queue in queues:
            try:
                input_queue.put_nowait(None)
            except queue.Full:
                pass
        for process in processes:
            process.join(5)
    for process in processes:
        if process.is_alive():
            process.terminate()
    deadline = time.monotonic() + 5
    for process in processes:
        process.join(max(0, deadline - time.monotonic()))
    for process in processes:
        if process.is_alive():
            process.kill()
    for process in processes:
        process.join(1)


def _ledger_record(connection, key, status, base_manifest=None,
                   feature_manifest=None, error=None):
    connection.execute(
        "INSERT OR REPLACE INTO members VALUES (?,?,?,?,?,?)",
        (key, status, base_manifest, feature_manifest, error, time.time_ns()))
    connection.commit()


def run_members(plan, descriptors, plan_sha256, connection, started):
    ordered, scheduling = member_order(plan, descriptors)
    workers = plan["limits"]["workers"]
    cpu_start = _tree_cpu_seconds()
    read_start, write_start = _tree_io_bytes()
    context = multiprocessing.get_context("spawn")
    result_queue = context.Queue(maxsize=max(2, workers * 2))
    input_queues, processes = [], []
    saved_env = {name: os.environ.get(name) for name in THREAD_LIMIT_ENV}
    try:
        for name in THREAD_LIMIT_ENV:
            os.environ[name] = "1"
        for number in range(workers):
            input_queue = context.Queue(maxsize=1)
            process = context.Process(
                target=_worker_main,
                args=(input_queue, result_queue, plan["config"],
                      plan["limits"].get("batch_size", 4096),
                      os.getpid()),
                name=f"tape-member-{number + 1}")
            process.start()
            input_queues.append(input_queue)
            processes.append(process)
    finally:
        for name, value in saved_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    connection.execute(
        "INSERT INTO run_attempts (plan_sha256,runner_identity,start_method,workers,scheduling,started_ns) VALUES (?,?,?,?,?,?)",
        (plan_sha256, json.dumps(runner_identity(), sort_keys=True), "spawn",
         workers, scheduling, time.time_ns()))
    connection.commit()
    pending = iter(ordered)
    active, task_ids = {}, {}
    completed = committed_bytes = 0
    baseline = {
        f"{member['session_date']}/{member['symbol']}":
        sum(_path_bytes(path) for path in member_paths(plan, member))
        for member in plan["members"]
    }
    peak_rss, peak_owned = _tree_rss(), 0
    cpu_seconds = _tree_cpu_seconds()
    read_bytes, write_bytes = _tree_io_bytes()
    try:
        next_id = 0
        for slot in range(workers):
            member = next(pending, None)
            if member is None:
                break
            key = f"{member['session_date']}/{member['symbol']}"
            base, features = member_paths(plan, member)
            pair_path, context_path = descriptors[key]
            next_id += 1
            input_queues[slot].put((
                next_id, key, pair_path, context_path, str(base), str(features)))
            active[slot], task_ids[slot] = member, next_id
            _ledger_record(connection, key, "running")
        while active:
            reason, rss, owned = _resource_sample(
                plan, started, active, committed_bytes, baseline)
            peak_rss, peak_owned = max(peak_rss, rss), max(peak_owned, owned)
            cpu_seconds = max(cpu_seconds, _tree_cpu_seconds())
            current_read, current_write = _tree_io_bytes()
            read_bytes, write_bytes = max(read_bytes, current_read), max(write_bytes, current_write)
            if reason:
                raise ContractError(reason)
            for slot, process in enumerate(processes):
                if slot in active and not process.is_alive():
                    member = active[slot]
                    key = f"{member['session_date']}/{member['symbol']}"
                    _ledger_record(connection, key, "failed", error=f"worker exit code {process.exitcode}")
                    raise ContractError(
                        f"worker exited abruptly for {key} with exit code {process.exitcode}")
            try:
                result = result_queue.get(timeout=0.05)
            except queue.Empty:
                continue
            (task_id, key, ok, base_manifest, feature_manifest,
             output_bytes, error) = result
            slot = next((slot for slot, value in task_ids.items()
                         if value == task_id), None)
            if slot is None:
                raise ContractError("worker returned unknown task")
            active.pop(slot)
            task_ids.pop(slot)
            if not ok:
                _ledger_record(connection, key, "failed", error=error)
                raise ContractError(f"member failed: {key}: {error}")
            _ledger_record(connection, key, "complete",
                           base_manifest, feature_manifest)
            completed += 1
            committed_bytes += max(0, output_bytes - baseline[key])
            following = next(pending, None)
            if following is not None:
                following_key = f"{following['session_date']}/{following['symbol']}"
                base, features = member_paths(plan, following)
                pair_path, context_path = descriptors[following_key]
                next_id += 1
                input_queues[slot].put((
                    next_id, following_key, pair_path, context_path,
                    str(base), str(features)))
                active[slot], task_ids[slot] = following, next_id
                _ledger_record(connection, following_key, "running")
        cpu_seconds = max(cpu_seconds, _tree_cpu_seconds())
        current_read, current_write = _tree_io_bytes()
        read_bytes, write_bytes = max(read_bytes, current_read), max(write_bytes, current_write)
        _stop_workers(processes, input_queues, graceful=True)
    except BaseException as error:
        for member in active.values():
            key = f"{member['session_date']}/{member['symbol']}"
            row = connection.execute(
                "SELECT status FROM members WHERE member=?", (key,)).fetchone()
            if row and row[0] == "running":
                _ledger_record(connection, key, "interrupted",
                               error=str(error)[:4096])
        _stop_workers(processes, input_queues)
        raise
    finally:
        for input_queue in input_queues:
            input_queue.cancel_join_thread()
            input_queue.close()
        result_queue.cancel_join_thread()
        result_queue.close()
    return {
        "status": "complete", "members": completed, "workers": workers,
        "scheduling": scheduling, "start_method": "spawn",
        "wall_seconds": time.monotonic() - started,
        "peak_process_tree_rss_bytes": peak_rss,
        "peak_owned_output_scratch_bytes": peak_owned,
        "process_tree_cpu_seconds": max(0.0, cpu_seconds - cpu_start),
        "process_tree_read_bytes": max(0, read_bytes - read_start),
        "process_tree_write_bytes": max(0, write_bytes - write_start),
    }
