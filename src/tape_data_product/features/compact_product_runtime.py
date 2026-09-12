"""Durable bounded partition supervision; no Arrow imports in the parent.

Inventory/catalog/receipt scans stream one member at a time. Each worker has a
fresh process and private attempt. The parent samples process-tree RSS at 100 ms.
No retries of calculations, retained attempts, resource failures or killed jobs.
"""

from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import psutil
from tape_data_product.features.all_feature_month_runtime import tree, scratch_bytes
from tape_data_product.features.all_feature_month_inventory import digest

GiB = 1024**3
MiB = 1024**2
POOL_TARGET_RSS = 2 * GiB
POOL_HARD_RSS = int(2.5 * GiB)
AVAILABLE_FLOOR = 512 * MiB
AVAILABLE_GRACE_SECONDS = 2.0
DISK_FLOOR = 3 * GiB
POOL_INTERVAL = 0.1
POOL_LATE_GAP = 0.25
DEFAULT_WORKER_RESERVATION = 256 * MiB
_supervision_context = threading.local()


class StopRun(RuntimeError):
    pass


class BlockedInput(ValueError):
    pass


class MemoryPressure:
    """Duration of the current uninterrupted low-memory episode (monotonic)."""

    def __init__(self):
        self.since = None

    def observe(self, available, now):
        if available >= AVAILABLE_FLOOR:
            self.since = None
            return 0.0
        if self.since is None:
            self.since = now
        return now - self.since


class PoolMonitor:
    """One sampler/admission owner for all child processes in a concurrent run."""

    def __init__(self, root, config):
        self.root = Path(root)
        self.config = config
        self.maximum = config["workers"]
        self.reservation = config.get(
            "worker_memory_reservation_bytes", DEFAULT_WORKER_RESERVATION
        )
        self.scratch_reservation = config.get("scratch_required_bytes", 0)
        self.jobs = {}
        self.lock = threading.RLock()
        self.done = threading.Event()
        self.ready = threading.Event()
        self.reason = None
        self.maximum_gap = 0.0
        self.aggregate_peak = 0
        self.last_disk = 0.0
        self.late_samples = 0
        self.peak_active_workers = 0
        self.minimum_available = None
        self.scratch_peak = 0
        self.available = 0
        self.free = 0
        self.parent_rss = 0
        self.last_sample = 0.0
        self.slow_updated = time.monotonic()
        self.slow_error = None
        self.memory_pressure = MemoryPressure()
        self.thread = threading.Thread(
            target=self._watch, name="compact-pool-monitor", daemon=True
        )
        self.slow_thread = threading.Thread(
            target=self._slow_watch, name="compact-pool-slow-watch", daemon=True
        )

    def start(self):
        self.slow_thread.start()
        self.thread.start()
        while not self.ready.wait(0.1):
            if not self.thread.is_alive():
                raise StopRun("essential resource sampler exited")
        if self.reason:
            raise StopRun(self.reason)

    def _stop(self, reason):
        if not self.reason:
            self.reason = reason

    def stop(self, reason):
        with self.lock:
            self._stop(reason)

    def _watch(self):
        previous = time.monotonic()
        while not self.done.is_set():
            tick = time.monotonic()
            gap = tick - previous
            previous = tick
            try:
                with self.lock:
                    self.maximum_gap = max(self.maximum_gap, gap)
                    if gap > POOL_LATE_GAP:
                        self.late_samples += 1
                    self.parent_rss = psutil.Process().memory_info().rss
                    worker_total = 0
                    for job in self.jobs.values():
                        values = tree(job["identity"]) if job["identity"] else {}
                        for pid in values:
                            try:
                                job["owned"][pid] = psutil.Process(pid).create_time()
                            except psutil.NoSuchProcess:
                                pass
                        current = sum(values.values())
                        job["current_rss"] = current
                        job["peak_rss"] = max(
                            job["peak_rss"], current + self.parent_rss
                        )
                        worker_total += current
                    self.available = psutil.virtual_memory().available
                    self.free = shutil.disk_usage(self.root).free
                    aggregate = self.parent_rss + worker_total
                    self.aggregate_peak = max(self.aggregate_peak, aggregate)
                    self.peak_active_workers = max(
                        self.peak_active_workers, len(self.jobs)
                    )
                    self.minimum_available = (
                        self.available
                        if self.minimum_available is None
                        else min(self.minimum_available, self.available)
                    )
                    self.scratch_peak = max(
                        self.scratch_peak,
                        sum(j["current_scratch"] for j in self.jobs.values()),
                    )
                    reason = resource_reason(
                        rss=aggregate,
                        available=self.available,
                        free=self.free,
                        needed=0,
                        memory_pressure_seconds=self.memory_pressure.observe(
                            self.available, tick
                        ),
                    )
                    if reason:
                        self._stop(reason)
                    self.last_sample = time.monotonic()
                    self.ready.set()
            except BaseException as exc:
                with self.lock:
                    self._stop("shared resource monitor failure: " + str(exc))
                self.ready.set()
            self.done.wait(max(0.0, POOL_INTERVAL - (time.monotonic() - tick)))

    def _slow_watch(self):
        while not self.done.is_set():
            try:
                with self.lock:
                    attempts = {key: job["attempt"] for key, job in self.jobs.items()}
                scratch = {
                    key: scratch_bytes(path) if path.exists() else 0
                    for key, path in attempts.items()
                }
                with self.lock:
                    self.slow_error = None
                    for key, current in scratch.items():
                        if key in self.jobs:
                            self.jobs[key]["current_scratch"] = current
                            self.jobs[key]["scratch_peak"] = max(
                                self.jobs[key]["scratch_peak"], current
                            )
                    self.slow_updated = time.monotonic()
            except BaseException as exc:
                with self.lock:
                    self.slow_error = str(exc)
            self.done.wait(1)

    def _capacity(self):
        slots = self.maximum - len(self.jobs)
        if slots <= 0 or self.reason:
            return 0
        committed = sum(
            max(j["current_rss"], self.reservation) for j in self.jobs.values()
        )
        memory = max(
            0, (POOL_TARGET_RSS - self.parent_rss - committed) // self.reservation
        )
        unmaterialized = sum(
            max(0, self.reservation - j["current_rss"]) for j in self.jobs.values()
        )
        available = max(
            0, (self.available - AVAILABLE_FLOOR - unmaterialized) // self.reservation
        )
        scratch_unmaterialized = sum(
            max(0, self.scratch_reservation - j["current_scratch"])
            for j in self.jobs.values()
        )
        disk = (
            slots
            if not self.scratch_reservation
            else max(
                0,
                (self.free - DISK_FLOOR - scratch_unmaterialized)
                // self.scratch_reservation,
            )
        )
        return int(max(0, min(slots, memory, available, disk)))

    def reserve(self, identifier, attempt):
        with self.lock:
            if self._capacity() < 1:
                return False
            self.jobs[identifier] = dict(
                identity=None,
                attempt=Path(attempt),
                current_rss=0,
                peak_rss=0,
                current_scratch=0,
                scratch_peak=0,
                progress={},
                owned={},
            )
            self.peak_active_workers = max(self.peak_active_workers, len(self.jobs))
            return True

    def wait_for_refresh(self, timeout=0.5):
        """Yield while waiting for admission; resource breaches still stop the pool."""
        with self.lock:
            previous = self.last_sample
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                if self.reason or self.last_sample > previous:
                    return
            self.done.wait(0.02)

    def register(self, identifier, identity):
        with self.lock:
            self.jobs[identifier]["identity"] = identity

    def update_progress(self, identifier, progress):
        with self.lock:
            if identifier in self.jobs:
                self.jobs[identifier]["progress"] = dict(progress)

    def sample(self, identifier):
        with self.lock:
            if (
                self.thread.ident is not None
                and not self.thread.is_alive()
                and not self.done.is_set()
            ):
                self._stop("essential resource sampler exited")
            job = self.jobs[identifier]
            return dict(
                reason=self.reason,
                current_rss_bytes=job["current_rss"] + self.parent_rss,
                peak_rss_bytes=job["peak_rss"],
                scratch_peak_bytes=job["scratch_peak"],
                owned=dict(job["owned"]),
                aggregate_rss_bytes=self.parent_rss
                + sum(j["current_rss"] for j in self.jobs.values()),
                aggregate_peak_rss_bytes=self.aggregate_peak,
                admission_cap=self._capacity(),
                maximum_pool_observation_gap_seconds=self.maximum_gap,
            )

    def worker_exited(self, identifier):
        with self.lock:
            if identifier in self.jobs:
                self.jobs[identifier]["identity"] = None

    def release(self, identifier):
        with self.lock:
            return self.jobs.pop(identifier, None)

    def status(self):
        with self.lock:
            return dict(
                state="running",
                workers=self.maximum,
                active_workers=len(self.jobs),
                admission_cap=self._capacity(),
                aggregate_rss_bytes=self.parent_rss
                + sum(j["current_rss"] for j in self.jobs.values()),
                aggregate_peak_rss_bytes=self.aggregate_peak,
                available_bytes=self.available,
                free_bytes=self.free,
                stop_reason=self.reason,
            )

    def resources(self):
        with self.lock:
            return dict(
                aggregate_peak_rss_bytes=self.aggregate_peak,
                peak_active_workers=self.peak_active_workers,
                maximum_observation_gap_seconds=self.maximum_gap,
                late_sample_count=self.late_samples,
                sampling_target_seconds=POOL_INTERVAL,
                late_sample_threshold_seconds=POOL_LATE_GAP,
                monitor_stale_timeout_seconds=None,
                advisory_scratch_error=self.slow_error,
                advisory_scratch_age_seconds=time.monotonic() - self.slow_updated,
                minimum_available_bytes=self.minimum_available,
                total_scratch_peak_bytes=self.scratch_peak,
                worker_memory_reservation_bytes=self.reservation,
                scratch_reservation_bytes_per_worker=self.scratch_reservation,
                target_rss_bytes=POOL_TARGET_RSS,
                hard_rss_bytes=POOL_HARD_RSS,
                available_floor_bytes=AVAILABLE_FLOOR,
                available_grace_seconds=AVAILABLE_GRACE_SECONDS,
                disk_floor_bytes=DISK_FLOOR,
                stop_reason=self.reason,
            )

    def close(self):
        self.done.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=2)
        if self.slow_thread.ident is not None:
            self.slow_thread.join(timeout=2)


def read(path):
    path = Path(path)
    if path.stat().st_size > 2 * 1024**2:
        raise StopRun("metadata exceeds 2 MiB bound")
    return json.loads(path.read_text())


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".partial")
    with temporary.open("w") as f:
        json.dump(value, f, sort_keys=True, indent=2, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def members(path):
    previous = None
    with open(path) as f:
        while line := f.readline(2 * 1024**2 + 1):
            if len(line) > 2 * 1024**2:
                raise StopRun("inventory member exceeds bound")
            m = json.loads(line)
            key = (m["session_date"], m["symbol"])
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", key[0]) or not re.fullmatch(
                r"[A-Z0-9.\-]{1,20}", key[1]
            ):
                raise StopRun("unsafe inventory key")
            if previous is not None and key <= previous:
                raise StopRun("inventory must be sorted unique")
            previous = key
            yield m


def member_id(member):
    return digest(member)


def receipt_path(root, member):
    return Path(root) / "receipts" / (member_id(member) + ".json")


def signature(phase, exc):
    message = str(exc)
    message = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", "<date>", message)
    message = re.sub(r"\b[A-Z][A-Z0-9.\-]{0,19}\b", "<symbol>", message)
    message = re.sub(r"\b[0-9a-f]{8,}\b|\b\d+\b", "<id>", message)
    return phase + "|" + type(exc).__name__ + "|" + message[:512]


@contextmanager
def run_lock(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / "run.lock").open("a") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise StopRun("another invocation owns this run")
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def retry_io(operation, *, record, sleep=time.sleep):
    """Exactly three application attempts; the caller disables SDK retries."""
    from botocore.exceptions import (
        ClientError,
        ConnectionClosedError,
        EndpointConnectionError,
        ReadTimeoutError,
        ConnectTimeoutError,
    )

    for attempt in range(1, 4):
        try:
            return operation()
        except Exception as exc:
            original = exc
            seen = set()
            while (
                not isinstance(
                    exc,
                    (
                        ClientError,
                        ConnectionClosedError,
                        EndpointConnectionError,
                        ReadTimeoutError,
                        ConnectTimeoutError,
                        TimeoutError,
                        ConnectionError,
                    ),
                )
                and (exc.__cause__ or exc.__context__) is not None
                and id(exc) not in seen
            ):
                seen.add(id(exc))
                exc = exc.__cause__ or exc.__context__
            transient = isinstance(
                exc,
                (
                    ConnectionClosedError,
                    EndpointConnectionError,
                    ReadTimeoutError,
                    ConnectTimeoutError,
                    TimeoutError,
                    ConnectionError,
                ),
            )
            if isinstance(exc, ClientError):
                code = str(exc.response.get("Error", {}).get("Code", ""))
                status = exc.response.get("ResponseMetadata", {}).get(
                    "HTTPStatusCode", 0
                )
                if status in (401, 403) or code in (
                    "AccessDenied",
                    "InvalidAccessKeyId",
                    "SignatureDoesNotMatch",
                    "ExpiredToken",
                ):
                    record(attempt, exc)
                    raise original  # Object-specific permissions are partition failures.
                transient = (
                    status >= 500
                    or status == 429
                    or code in ("SlowDown", "Throttling", "RequestTimeout")
                )
            record(attempt, exc)
            if not transient or attempt == 3:
                raise
            sleep((5, 30)[attempt - 1])


def resource_reason(
    *, rss, available, free, needed=0, monitor_gap=0, memory_pressure_seconds=0
):
    if rss >= int(2.5 * GiB):
        return "process-tree RSS termination threshold"
    if (
        available < AVAILABLE_FLOOR
        and memory_pressure_seconds >= AVAILABLE_GRACE_SECONDS
    ):
        return "system available memory below 512 MiB for 2 seconds"
    if free < 3 * GiB + needed:
        return "disk reserve breach"
    return None


def reap(child, owned):
    # Worker owns a new process group; kill descendants even if parent has died.
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    for pid, created in owned.items():
        try:
            p = psutil.Process(pid)
            if abs(p.create_time() - created) < 0.01:
                p.terminate()
        except psutil.NoSuchProcess:
            pass
    try:
        child.wait(timeout=1)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    child.wait()


def supervise(command, attempt, config, *, on_progress=lambda x: None):
    attempt = Path(attempt)
    attempt.mkdir(parents=True, exist_ok=False)
    lease = attempt / "monitor.json"
    heartbeat = attempt / "progress.json"
    save(lease, {"time": time.time()})
    needed = config.get("scratch_required_bytes", 0)
    pool = getattr(_supervision_context, "pool", None)
    pool_job = getattr(_supervision_context, "job", None)
    if pool is None:
        reason = resource_reason(
            rss=psutil.Process().memory_info().rss,
            available=psutil.virtual_memory().available,
            free=shutil.disk_usage(attempt).free,
            needed=needed,
        )
        if psutil.virtual_memory().available < AVAILABLE_FLOOR:
            raise StopRun(
                "insufficient available memory to admit a worker (512 MiB floor)"
            )
        if reason:
            raise StopRun(reason)
    env = os.environ.copy()
    env.update(
        {
            k: "1"
            for k in (
                "OMP_NUM_THREADS",
                "OPENBLAS_NUM_THREADS",
                "MKL_NUM_THREADS",
                "NUMEXPR_NUM_THREADS",
                "VECLIB_MAXIMUM_THREADS",
                "ARROW_NUM_THREADS",
            )
        }
    )
    env["COMPACT_SUPERVISOR_IDENTITY"] = json.dumps(
        dict(pid=os.getpid(), created=psutil.Process().create_time())
    )
    env.pop("COMPACT_MONITOR_LEASE", None)
    env["COMPACT_PROGRESS"] = str(heartbeat)
    started = previous = last_progress = time.monotonic()
    counter = None
    peak = 0
    scratch_peak = 0
    owned = {}
    last_report = 0
    max_gap = 0
    memory_pressure = MemoryPressure()
    with (attempt / "worker.log").open("wb") as log:
        child = subprocess.Popen(
            command,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            identity = {
                "pid": child.pid,
                "created": psutil.Process(child.pid).create_time(),
            }
            if pool is not None:
                pool.register(pool_job, identity)
        except BaseException:
            reap(child, owned)
            raise
        try:
            while child.poll() is None:
                now = time.monotonic()
                gap = now - previous
                previous = now
                max_gap = max(max_gap, gap)
                if pool is None:
                    values = tree(identity)
                    for pid in values:
                        try:
                            owned[pid] = psutil.Process(pid).create_time()
                        except psutil.NoSuchProcess:
                            pass
                    rss = sum(values.values()) + psutil.Process().memory_info().rss
                    peak = max(peak, rss)
                    available = psutil.virtual_memory().available
                    reason = resource_reason(
                        rss=rss,
                        available=available,
                        free=shutil.disk_usage(attempt).free,
                        needed=0,
                        monitor_gap=gap,
                        memory_pressure_seconds=memory_pressure.observe(available, now),
                    )
                    if reason:
                        raise StopRun(reason)
                else:
                    shared = pool.sample(pool_job)
                    owned.update(shared["owned"])
                    if shared["reason"]:
                        raise StopRun(shared["reason"])
                    peak = max(peak, shared["peak_rss_bytes"])
                    scratch_peak = max(scratch_peak, shared["scratch_peak_bytes"])
                    on_progress(shared)
                if now - last_report >= 1:
                    # Progress and scratch accounting are advisory. Actual process exit
                    # and measured resource breaches are enforced independently.
                    try:
                        progress = read(heartbeat) if heartbeat.exists() else {}
                    except Exception as exc:
                        progress = {"monitoring_warning": str(exc)[:512]}
                    if progress.get("counter") != counter:
                        counter = progress.get("counter")
                        last_progress = now
                    progress["advisory_no_progress"] = now - last_progress > config.get(
                        "no_progress_seconds", 900
                    )
                    progress["advisory_long_running"] = now - started > config.get(
                        "worker_timeout_seconds", 7200
                    )
                    on_progress(
                        dict(
                            progress,
                            peak_rss_bytes=peak,
                            scratch_peak_bytes=scratch_peak,
                        )
                        | ({} if pool is None else shared)
                    )
                    # Keep bounded diagnostics, even for a misbehaving dependency.
                    if log.tell() > 1024**2:
                        log.truncate(1024**2)
                        log.seek(0)
                    last_report = now
                time.sleep(max(0, 0.1 - (time.monotonic() - now)))
            if pool is not None:
                shared = pool.sample(pool_job)
                owned.update(shared["owned"])
                if shared["reason"]:
                    raise StopRun(shared["reason"])
            if child.returncode == 70:
                result = read(attempt / "result.json")
                raise StopRun("worker safety failure: " + str(result.get("error")))
            if (
                child.returncode < 0
                or child.returncode in (71, 72)
                or not (attempt / "result.json").exists()
            ):
                result = dict(
                    state="failed",
                    error_class="WorkerExited",
                    error="worker exited without valid completion: "
                    + str(child.returncode),
                    error_signature="worker_exit:" + str(child.returncode),
                )
            else:
                result = read(attempt / "result.json")
            result.update(
                maximum_observation_gap_seconds=max_gap,
                peak_rss_bytes=peak,
                scratch_peak_bytes=scratch_peak,
                elapsed_seconds=time.monotonic() - started,
            )
            return result
        except (KeyboardInterrupt, StopRun) as exc:
            try:
                save(
                    attempt / "resource_failure.json",
                    dict(
                        peak_rss_bytes=peak,
                        maximum_observation_gap_seconds=max_gap,
                        elapsed_seconds=time.monotonic() - started,
                        stop_reason=str(exc),
                        scratch_peak_bytes=scratch_peak,
                    ),
                )
            except OSError:
                pass
            reap(child, owned)
            raise
        except Exception as exc:
            reap(child, owned)
            if isinstance(exc, OSError) and exc.errno in (12, 28, 122):
                raise StopRun(
                    "supervisor memory/disk resource failure: " + str(exc)
                ) from exc
            return dict(
                state="failed",
                error_class=type(exc).__name__,
                error=str(exc)[:2048],
                error_signature=signature("supervision", exc),
                traceback=traceback.format_exc()[-8192:],
                peak_rss_bytes=peak,
                elapsed_seconds=time.monotonic() - started,
            )
        finally:
            # Terminate any surviving worker-owned descendants, including on error return.
            reap(child, owned)
            if pool is not None:
                pool.worker_exited(pool_job)


def regenerate(root, inventory, *, stop_reason=None, started=None):
    """Bounded reports and explicit partial release; never hide unresolved members."""
    root = Path(root)
    counts = Counter()
    validation_counts = Counter()
    months = {}
    verified = 0
    peak = 0
    pool_resources = (
        read(root / "pool_resources.json")
        if (root / "pool_resources.json").exists()
        else None
    )
    catalog_tmp = root / "catalog.jsonl.partial"
    retry_tmp = root / "retry.jsonl.partial"
    with (
        catalog_tmp.open("w") as catalog,
        retry_tmp.open("w") as retry,
        (root / "morning_report.md.partial").open("w") as report,
    ):
        report.write("# Compact product run\n\n")
        if stop_reason:
            report.write("Stopped: " + stop_reason + "\n\n")
        report.write(
            "| Session | Symbol | State | Error / retained attempt |\n|---|---|---|---|\n"
        )
        for m in members(inventory):
            path = receipt_path(root, m)
            r = read(path) if path.exists() else {"state": "pending"}
            state = r["state"]
            counts[state] += 1
            month = m["session_date"][:7]
            months.setdefault(month, Counter())[state] += 1
            peak = max(peak, r.get("peak_rss_bytes", 0))
            if state == "complete":
                verified += r["rows_verified"]
                validation_counts[
                    r.get("validation_mode", "legacy_reconstruction")
                ] += 1
                marker = r.get("publication", {}).get("objects", {}).get("manifest")
                if marker is None and Path(r["manifest"]).is_file():
                    marker = dict(
                        file=r["manifest"],
                        sha256=hash_file(r["manifest"]),
                        size_bytes=Path(r["manifest"]).stat().st_size,
                        rows=None,
                    )
                catalog.write(
                    json.dumps(
                        dict(
                            session_date=m["session_date"],
                            symbol=m["symbol"],
                            partition_identity=r["partition_identity"],
                            manifest=r["manifest"],
                            manifest_object=marker,
                            validation_mode=r.get(
                                "validation_mode", "legacy_reconstruction"
                            ),
                            independent_reconstruction=r.get(
                                "independent_reconstruction", "passed"
                            ),
                        )
                    )
                    + "\n"
                )
            else:
                retry.write(
                    json.dumps(
                        dict(
                            member=m,
                            member_id=member_id(m),
                            state=state,
                            receipt=str(path),
                        )
                    )
                    + "\n"
                )
                detail = (
                    r.get("error_signature", "") + " " + r.get("attempt_path", "")
                ).replace("|", "/")
                report.write(
                    f"| {m['session_date']} | {m['symbol']} | {state} | {detail} |\n"
                )
        aggregate_peak = (
            pool_resources.get("aggregate_peak_rss_bytes", 0) if pool_resources else 0
        )
        peak = max(peak, aggregate_peak)
        report.write(
            "\nCounts: "
            + json.dumps(counts, sort_keys=True)
            + "\n\nIntegrity-checked rows: "
            + str(verified)
            + "\n\nValidation levels: "
            + json.dumps(validation_counts, sort_keys=True)
            + "\n\nPeak aggregate worker pool plus coordinator RSS: "
            + str(peak)
            + " bytes.\n"
        )
        report.flush()
        os.fsync(report.fileno())
        catalog.flush()
        os.fsync(catalog.fileno())
        retry.flush()
        os.fsync(retry.fileno())
    for a, b in [
        (catalog_tmp, root / "catalog.jsonl"),
        (retry_tmp, root / "retry.jsonl"),
        (root / "morning_report.md.partial", root / "morning_report.md"),
    ]:
        os.replace(a, b)
    complete = (
        counts["complete"] == sum(counts.values())
        and bool(counts["complete"])
        and not stop_reason
    )
    status = dict(
        state=(
            "complete"
            if complete
            else (
                "drained"
                if stop_reason == "drain requested"
                else "stopped" if stop_reason else "finished_with_errors"
            )
        ),
        counts=dict(counts),
        months={k: dict(v) for k, v in months.items()},
        verified_rows=verified,
        integrity_checked_rows=verified,
        validation_counts=dict(validation_counts),
        peak_rss_bytes=peak,
        pool_resources=pool_resources,
        stop_reason=stop_reason,
        elapsed_seconds=time.time() - started if started else None,
    )
    save(root / "run_status.json", status)
    plan = read(root / "run_plan.json") if (root / "run_plan.json").exists() else {}
    full_seconds = plan.get("config", {}).get("seconds", 57600) == 57600
    release = dict(
        status,
        partial=not complete or not full_seconds,
        catalog="catalog.jsonl",
        coverage="full_sessions" if full_seconds else "bounded_prefixes",
        inventory_sha256=plan.get("inventory_sha256"),
        catalog_sha256=hash_file(root / "catalog.jsonl"),
    )
    save(
        root
        / ("release.json" if complete and full_seconds else "partial_release.json"),
        release,
    )
    return status


def validate_config(config):
    if config.get("validation_mode", "integrity") not in (
        "integrity",
        "reconstruction",
    ):
        raise StopRun("invalid validation mode")
    for name, low, high, default in [
        ("seconds", 1, 57600, 57600),
        ("batch_size", 1, 25000, 4096),
        ("output_rows", 1, 12288, 12288),
        ("consecutive_failure_limit", 1, 100, 10),
        ("workers", 1, 7, 1),
        (
            "worker_memory_reservation_bytes",
            64 * MiB,
            512 * MiB,
            DEFAULT_WORKER_RESERVATION,
        ),
    ]:
        value = config.get(name, default)
        if type(value) is not int or not low <= value <= high:
            raise StopRun("invalid global configuration: " + name)
    for name, default in [
        ("no_progress_seconds", 900),
        ("worker_timeout_seconds", 7200),
    ]:
        if (
            not isinstance(config.get(name, default), (int, float))
            or config.get(name, default) <= 0
        ):
            raise StopRun("invalid global deadline")
    if config.get("scratch_required_bytes", 0) < 0:
        raise StopRun("invalid scratch budget")


def _verify_before_pool(root, inventory, verify_completed):
    """Drain completion verification before any concurrent calculation starts."""
    for m in members(inventory):
        if (Path(root) / "DRAIN").exists():
            return
        path = receipt_path(root, m)
        receipt = read(path)
        if receipt["state"] == "running":
            receipt.update(
                state="interrupted",
                error="abandoned running receipt requires reviewed reconciliation",
            )
            save(path, receipt)
            continue
        if receipt["state"] == "complete":
            if verify_completed is None:
                raise StopRun("completed partition verification callback required")
            try:
                verify_completed(m, receipt)
            except Exception as exc:
                receipt.update(
                    state="interrupted",
                    error="completion verification failed: " + str(exc),
                )
                save(path, receipt)
                if isinstance(exc, StopRun):
                    raise


def _run_concurrent(root, inventory, config, worker, selected):
    """Bounded orchestration; worker callbacks still own fresh supervised children."""
    pool = PoolMonitor(root, config)
    try:
        pool.start()
    except BaseException:
        pool.close()
        raise
    last_signature = None
    consecutive = 0
    stream = iter(members(inventory))
    next_job = None
    exhausted = False
    active = {}
    sequence = 0

    def find_next():
        nonlocal exhausted
        for member in stream:
            path = receipt_path(root, member)
            receipt = read(path)
            state = receipt["state"]
            identifier = member_id(member)
            if state == "complete":
                continue
            if state != "pending" and identifier not in selected:
                continue
            if state == "interrupted":
                continue
            attempt = receipt["attempt"] + 1
            attempt_path = (root / "attempts" / identifier / str(attempt)).resolve()
            return member, path, receipt, identifier, attempt, attempt_path
        exhausted = True
        return None

    def execute(record):
        member, path, receipt, identifier, attempt, attempt_path, seq = record
        _supervision_context.pool = pool
        _supervision_context.job = identifier
        try:
            return worker(
                member,
                receipt,
                lambda progress: pool.update_progress(identifier, progress),
            )
        except StopRun as exc:
            pool.stop(str(exc))
            raise
        finally:
            _supervision_context.pool = None
            _supervision_context.job = None

    def commit(record, result=None, error=None):
        nonlocal last_signature, consecutive
        member, path, receipt, identifier, attempt, attempt_path, seq = record
        if error is None:
            receipt.update(result)
            if receipt.get("state") not in ("complete", "failed", "blocked_input"):
                error = ValueError("invalid worker terminal receipt")
        if error is not None:
            if isinstance(error, StopRun):
                failure = attempt_path / "resource_failure.json"
                if failure.exists():
                    receipt.update(read(failure))
                receipt.update(state="interrupted", error=str(error))
            else:
                receipt.update(
                    state=(
                        "blocked_input" if isinstance(error, BlockedInput) else "failed"
                    ),
                    error=str(error)[:2048],
                    error_signature=signature(receipt.get("phase", "worker"), error),
                    traceback="".join(
                        traceback.format_exception(
                            type(error), error, error.__traceback__
                        )
                    )[-8192:],
                )
        receipt["finished_at"] = time.time()
        save(path, receipt)
        released = pool.release(identifier)
        if released:
            receipt["pool_peak_rss_bytes"] = released["peak_rss"]
            receipt["scratch_peak_bytes"] = max(
                receipt.get("scratch_peak_bytes", 0), released["scratch_peak"]
            )
            save(path, receipt)
        if receipt["state"] == "complete" and receipt.get("publication"):
            cleanup_after_commit(root, path, receipt)
        sig = receipt.get("error_signature") if receipt["state"] != "complete" else None
        consecutive = (
            consecutive + 1
            if sig is not None and sig == last_signature
            else 1 if sig else 0
        )
        last_signature = sig
        if isinstance(error, StopRun):
            raise error

    executor = ThreadPoolExecutor(
        max_workers=config["workers"], thread_name_prefix="compact-worker"
    )
    stop = None
    last_status_write = 0.0
    try:
        while not exhausted or next_job is not None or active:
            if pool.reason:
                raise StopRun(pool.reason)
            draining = (root / "DRAIN").exists()
            if draining and not active:
                stop = "drain requested"
                break
            while len(active) < config["workers"] and not draining:
                if (root / "DRAIN").exists():
                    draining = True
                    break
                if next_job is None:
                    next_job = find_next()
                if next_job is None:
                    break
                member, path, receipt, identifier, attempt, attempt_path = next_job
                admitted = pool.reserve(identifier, attempt_path)
                if not admitted:
                    if not active:
                        pool.wait_for_refresh()
                        admitted = pool.reserve(identifier, attempt_path)
                        if not admitted:
                            status = pool.status()
                            save(
                                root / "run_status.json",
                                dict(
                                    status,
                                    advisory="waiting for worker admission headroom",
                                ),
                            )
                            break
                    else:
                        break
                receipt.update(
                    state="running",
                    attempt=attempt,
                    phase="launch",
                    started_at=time.time(),
                    attempt_path=str(attempt_path),
                )
                save(path, receipt)
                sequence += 1
                record = (*next_job, sequence)
                future = executor.submit(execute, record)
                active[future] = record
                next_job = None
            if not active:
                pool.wait_for_refresh()
                continue
            done, _ = wait(tuple(active), timeout=0.1, return_when=FIRST_COMPLETED)
            if not done:
                now = time.monotonic()
                if now - last_status_write >= 1:
                    save(
                        root / "run_status.json",
                        dict(
                            pool.status(), state="draining" if draining else "running"
                        ),
                    )
                    last_status_write = now
                continue
            for future in sorted(done, key=lambda item: active[item][-1]):
                record = active.pop(future)
                try:
                    result = future.result()
                except BaseException as exc:
                    commit(record, error=exc)
                else:
                    commit(record, result=result)
    except BaseException as exc:
        stop = str(exc) or "interrupted by user"
        pool.stop(stop)
        for future, record in sorted(active.items(), key=lambda item: item[1][-1]):
            try:
                result = future.result(
                    timeout=config.get("worker_timeout_seconds", 7200) + 2
                )
            except BaseException as worker_exc:
                try:
                    commit(
                        record,
                        error=(
                            worker_exc
                            if isinstance(worker_exc, StopRun)
                            else StopRun(stop)
                        ),
                    )
                except StopRun:
                    pass
            else:
                try:
                    commit(record, result=result)
                except StopRun:
                    pass
        if next_job is not None:
            # It was inspected but never admitted; its receipt remains pending.
            next_job = None
        active.clear()
    finally:
        executor.shutdown(wait=True, cancel_futures=False)
        pool.close()
        save(root / "pool_resources.json", pool.resources())
    return stop


def run(root, inventory, config, worker, *, retry_selected=(), verify_completed=None):
    """Worker callback isolates a partition. Retry selection is explicit after diagnosis."""
    validate_config(config)
    root = Path(root)
    inventory = Path(inventory).resolve()
    started = time.time()
    stop = None
    selected = set()
    selection_bytes = 0
    for identifier in retry_selected:
        selection_bytes += len(identifier)
        if selection_bytes > 2 * 1024**2:
            raise StopRun("explicit retry control exceeds 2 MiB; split the selection")
        selected.add(identifier)
    with run_lock(root):
        frozen = dict(config=config, inventory_sha256=hash_file(inventory))
        plan = root / "run_plan.json"
        if plan.exists() and read(plan) != frozen:
            raise StopRun("run identity changed; use a new run directory")
        if not plan.exists():
            save(plan, frozen)
        for m in members(inventory):
            p = receipt_path(root, m)
            if not p.exists():
                save(
                    p,
                    dict(
                        state="pending",
                        member_id=member_id(m),
                        attempt=0,
                        run_identity=digest(frozen),
                    ),
                )
        if config.get("workers", 1) > 1:
            try:
                _verify_before_pool(root, inventory, verify_completed)
                stop = _run_concurrent(root, inventory, config, worker, selected)
            except (StopRun, KeyboardInterrupt, OSError) as exc:
                stop = str(exc) or "interrupted by user"
            status = regenerate(root, inventory, stop_reason=stop, started=started)
            return 0 if status["state"] == "complete" else 1
        last_signature = None
        consecutive = 0
        try:
            for m in members(inventory):
                if (root / "DRAIN").exists():
                    stop = "drain requested"
                    break
                p = receipt_path(root, m)
                r = read(p)
                state = r["state"]
                if state == "running":
                    r.update(
                        state="interrupted",
                        error="abandoned running receipt requires reviewed reconciliation",
                    )
                    save(p, r)
                    state = "interrupted"
                if state == "complete":
                    if verify_completed is None:
                        raise StopRun(
                            "completed partition verification callback required"
                        )
                    try:
                        verify_completed(m, r)
                    except Exception as exc:
                        r.update(
                            state="interrupted",
                            error="completion verification failed: " + str(exc),
                        )
                        save(p, r)
                        if isinstance(exc, StopRun):
                            raise
                    continue
                if state != "pending" and member_id(m) not in selected:
                    continue
                if state == "interrupted":
                    continue
                r.update(
                    state="running",
                    attempt=r["attempt"] + 1,
                    phase="launch",
                    started_at=time.time(),
                    attempt_path=str(
                        (
                            root / "attempts" / member_id(m) / str(r["attempt"] + 1)
                        ).resolve()
                    ),
                )
                save(p, r)
                try:
                    result = worker(
                        m,
                        r,
                        lambda progress: save(
                            root / "run_status.json",
                            dict(state="running", member_id=member_id(m), **progress),
                        ),
                    )
                    r.update(result)
                    if r["state"] not in ("complete", "failed", "blocked_input"):
                        raise ValueError("invalid worker terminal receipt")
                except StopRun as exc:
                    failure = Path(r["attempt_path"]) / "resource_failure.json"
                    if failure.exists():
                        r.update(read(failure))
                    r.update(state="interrupted", error=str(exc))
                    save(p, r)
                    raise
                except Exception as exc:
                    r.update(
                        state=(
                            "blocked_input"
                            if isinstance(exc, BlockedInput)
                            else "failed"
                        ),
                        error=str(exc)[:2048],
                        error_signature=signature(r.get("phase", "worker"), exc),
                        traceback=traceback.format_exc()[-8192:],
                    )
                r["finished_at"] = time.time()
                save(p, r)
                if r["state"] == "complete" and r.get("publication"):
                    cleanup_after_commit(root, p, r)
                sig = r.get("error_signature") if r["state"] != "complete" else None
                consecutive = (
                    consecutive + 1
                    if sig is not None and sig == last_signature
                    else 1 if sig else 0
                )
                last_signature = sig
        except (StopRun, KeyboardInterrupt, OSError) as exc:
            stop = str(exc) or "interrupted by user"
        status = regenerate(root, inventory, stop_reason=stop, started=started)
        return 0 if status["state"] == "complete" else 1


def hash_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(1024**2):
            h.update(block)
    return h.hexdigest()


def reconcile(root, member, verify):
    """Explicit reviewed recovery, with completion verification before transition.

    The verifier must inspect retained local/remote completion markers under the
    original identities. Absence of a marker leaves the attempt interrupted;
    it never launches another calculation or removes attempt evidence.
    """
    root = Path(root)
    with run_lock(root):
        path = receipt_path(root, member)
        receipt = read(path)
        if receipt["state"] not in ("running", "interrupted"):
            raise StopRun("reconciliation requires an interrupted receipt")
        result = verify(member, receipt)
        if result is None or result.get("state") != "complete":
            receipt.update(
                state="interrupted",
                error="completion marker unresolved; no automatic recalculation",
            )
            save(path, receipt)
            return False
        if result.get("member_id", member_id(member)) != member_id(member):
            raise StopRun("reconciliation member identity mismatch")
        receipt.update(result, state="complete", reconciled_at=time.time())
        save(path, receipt)
        return True


def cleanup_owned(root, receipt):
    """Release only enumerated worker-owned bytes, after durable remote receipt."""
    if receipt.get("state") != "complete" or not receipt.get("publication"):
        raise StopRun("cleanup requires committed remote identity")
    attempt = Path(receipt["attempt_path"]).resolve()
    owned = (Path(root) / "attempts").resolve()
    if not attempt.is_relative_to(owned):
        raise StopRun("attempt outside owned scratch")
    for relative in (
        "quotes.parquet",
        "trades.parquet",
        "base.parquet",
        "extension.parquet",
        "product/features.parquet",
        "product/support.parquet",
    ):
        path = attempt / relative
        if path.is_symlink() or not path.resolve().is_relative_to(attempt):
            raise StopRun("unsafe private cleanup path")
        path.unlink(missing_ok=True)


def cleanup_after_commit(root, path, receipt):
    try:
        cleanup_owned(root, receipt)
    except OSError as exc:
        receipt["cleanup_warning"] = str(exc)[:1024]
        save(path, receipt)


def verify_published_receipt(member, receipt, config):
    """Reuse an immutable, publication-verified receipt without remote re-audit.

    The retained manifest must match its recorded published hash and every object
    identity. Missing local evidence falls back to remote verification; conflicting
    evidence is rejected. No Arrow import or output-file reconstruction is needed.
    """
    if receipt.get("state") != "complete" or not receipt.get("publication"):
        return False
    publication = receipt["publication"]
    objects = publication.get("objects", {})
    directory = receipt.get("manifest_directory")
    if not directory and receipt.get("attempt_path"):
        directory = str(Path(receipt["attempt_path"]) / "product")
    if not directory:
        return False
    path = Path(directory) / "manifest.json"
    if not path.exists():
        return False
    if set(objects) != {"features", "support", "manifest"}:
        raise ValueError("incomplete publication receipt")
    if any(
        obj.get("status") not in ("uploaded_verified", "skipped_verified")
        for obj in objects.values()
    ):
        return False
    if config.get("bucket") and publication.get("bucket") != config["bucket"]:
        raise ValueError("publication bucket mismatch")
    marker = objects["manifest"]
    if (
        path.stat().st_size != marker["size_bytes"]
        or hash_file(path) != marker["sha256"]
    ):
        raise ValueError("retained published manifest hash mismatch")
    manifest = read(path)
    meta = manifest["metadata"]
    identity = manifest["partition_identity"]
    if identity != digest(meta) or identity != receipt["partition_identity"]:
        raise ValueError("published partition identity mismatch")
    expected = dict(
        inputs=member["inputs"],
        overlay=member["overlay"],
        session_date=member["session_date"],
        symbol=member["symbol"],
        expected_rows=config.get("seconds", 57600),
        discovery_verified=bool(member.get("discovery", {}).get("verified")),
    )
    if any(meta.get(k) != v for k, v in expected.items()):
        raise ValueError("published completion dependencies mismatch")
    discovery = member.get("discovery", {})
    expected_discovery = dict(
        first_discovery_endpoint_ns=discovery.get("endpoint_ns"),
        discovery_received_at_ns=discovery.get("received_at_ns"),
        discovery_timing_basis=discovery.get("timing_basis", "unavailable"),
        discovery_provenance_hash=discovery.get("provenance_hash"),
    )
    if meta.get("discovery") != expected_discovery:
        raise ValueError("published discovery identity mismatch")
    rows = expected["expected_rows"]
    if (
        receipt["rows_verified"] != rows
        or manifest["validation"]["rows_verified"] != rows
    ):
        raise ValueError("published row count mismatch")
    prefix = f"derived/tape_data_product/{meta['layout_version']}/{'partitions' if rows==57600 else 'partial_prefixes'}/{identity}"
    if (
        publication.get("manifest_key") != prefix + "/manifest.json"
        or marker["object_key"] != publication["manifest_key"]
    ):
        raise ValueError("published completion namespace mismatch")
    for name in ("features", "support"):
        obj = objects[name]
        expected_object = manifest["objects"][name]
        if (
            obj["object_key"] != prefix + "/" + name + ".parquet"
            or expected_object["file"] != name + ".parquet"
        ):
            raise ValueError("published object key mismatch")
        if obj.get("rows") != rows or any(
            obj.get(k) != expected_object.get(k)
            for k in ("sha256", "size_bytes", "rows")
        ):
            raise ValueError("published object identity mismatch")
    return True
