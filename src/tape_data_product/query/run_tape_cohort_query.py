"""CLI and live process-tree supervisor for the tape cohort query."""

from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from tape_data_product.query.tape_cohort_config import normalize_config
from tape_data_product.query.tape_cohort_outputs import atomic_json
from tape_data_product.query.tape_cohort_pipeline import (
    plan_query,
    run_checkpoint,
    run_query,
    _load_plan,
    open_date,
    extract_trace,
)


def monitored_checkpoint(plan_path):
    import psutil

    plan = _load_plan(plan_path)
    output = Path(plan["result_root"]) / (
        plan["query_run_hash"] + "-checkpoint-resources.json"
    )
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "checkpoint",
        "--plan",
        str(Path(plan_path).resolve()),
        "--worker",
    ]
    start = time.monotonic()
    child = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    parent = psutil.Process()
    peak = 0
    min_available = psutil.virtual_memory().available
    min_free = shutil.disk_usage(plan["result_root"]).free
    max_gap = 0.0
    last = time.monotonic()
    reason = None
    samples = 0
    cache_high = result_high = 0

    def tree_bytes(path):
        path = Path(path)
        return (
            sum(
                p.stat().st_size
                for p in path.rglob("*")
                if p.is_file() and not p.is_symlink()
            )
            if path.exists()
            else 0
        )

    while child.poll() is None:
        now = time.monotonic()
        max_gap = max(max_gap, now - last)
        last = now
        try:
            worker = psutil.Process(child.pid)
            processes = [worker, *worker.children(recursive=True)]
            rss = parent.memory_info().rss + sum(
                p.memory_info().rss for p in processes if p.is_running()
            )
        except psutil.Error:
            rss = parent.memory_info().rss
        available = psutil.virtual_memory().available
        free = shutil.disk_usage(plan["result_root"]).free
        peak = max(peak, rss)
        min_available = min(min_available, available)
        min_free = min(min_free, free)
        samples += 1
        cache_high = max(cache_high, tree_bytes(plan["cache_root"]))
        result_high = max(result_high, tree_bytes(plan["result_root"]))
        if rss >= plan["settings"]["rss_stop_bytes"]:
            reason = "1 GiB process-tree RSS stop"
        elif available < plan["settings"]["minimum_available_bytes"]:
            reason = "available memory floor"
        elif free < plan["settings"]["minimum_free_disk_bytes"]:
            reason = "free disk floor"
        if reason:
            child.kill()
            break
        time.sleep(0.1)
    stdout, stderr = child.communicate()
    measurement = {
        "schema": "tape_cohort_resources_v1",
        "elapsed_seconds": time.monotonic() - start,
        "peak_process_tree_including_supervisor_rss_bytes": peak,
        "minimum_available_memory_bytes": min_available,
        "minimum_free_disk_bytes": min_free,
        "monitor_target_seconds": 0.1,
        "maximum_observed_monitor_gap_seconds": max_gap,
        "cache_high_water_bytes": cache_high,
        "scratch_high_water_bytes": 0,
        "result_high_water_bytes": result_high,
        "samples": samples,
        "stop_reason": reason,
        "exit_code": child.returncode,
    }
    atomic_json(output, measurement)
    if child.returncode or reason:
        raise RuntimeError((reason or stderr[-4000:]) + f"; resources={output}")
    result = json.loads(stdout.strip().splitlines()[-1])
    checkpoint = json.loads((Path(result["path"]) / "manifest.json").read_text())
    phases = checkpoint["phase_measurements"]
    totals = plan["source_totals"]
    cold_rate = (
        phases["cold"]["seconds"] / phases["cold"]["bytes_transferred"]
        if phases["cold"]["bytes_transferred"]
        else None
    )
    row_rate = max(
        phases[x]["seconds"] / phases[x]["decoded_rows"]
        for x in ("warm", "changed_query")
    )
    pilot = [
        m for m in plan["routed_members"] if m["session_date"] == plan["pilot_date"]
    ]
    pilot_bytes = sum(m["feature_bytes"] + m["manifest_bytes"] for m in pilot)
    pilot_rows = len(pilot) * 57600
    checkpoint_bytes = tree_bytes(result["path"])
    real_rows = 2 * len(checkpoint["members"]) * checkpoint["prefix_rows_per_member"]
    bytes_per_endpoint = checkpoint_bytes / max(1, real_rows)
    projection = {
        "schema": "tape_cohort_resource_projection_v1",
        "checkpoint_query_run_hash": plan["query_run_hash"],
        "arithmetic": {
            "cold_seconds_per_source_byte": cold_rate,
            "slowest_warm_query_seconds_per_row": row_rate,
            "time_contingency": 1.5,
            "output_contingency": 1.25,
        },
        "pilot": {
            "date": plan["pilot_date"],
            "members": len(pilot),
            "rows": pilot_rows,
            "source_bytes": pilot_bytes,
            "cold_seconds_projected": (
                1.5 * (cold_rate * pilot_bytes + row_rate * pilot_rows)
                if cold_rate is not None
                else None
            ),
            "warm_seconds_projected": 1.5 * row_rate * pilot_rows,
            "result_bytes_projected": int(1.25 * bytes_per_endpoint * pilot_rows),
        },
        "range": {
            "dates": totals["dates"],
            "members": totals["members"],
            "rows": totals["rows"],
            "source_bytes": totals["feature_bytes"] + totals["manifest_bytes"],
            "cold_seconds_projected": (
                1.5
                * (
                    cold_rate * (totals["feature_bytes"] + totals["manifest_bytes"])
                    + row_rate * totals["rows"]
                )
                if cold_rate is not None
                else None
            ),
            "warm_seconds_projected": 1.5 * row_rate * totals["rows"],
            "result_bytes_projected": int(1.25 * bytes_per_endpoint * totals["rows"]),
        },
        "rss": {
            "measured_peak_bytes": peak,
            "projected_peak_bytes": peak,
            "configured_batch_rows": plan["settings"]["batch_size"],
            "exercised_batch_rows": plan["settings"]["batch_size"],
            "fixed_allocation_difference_bytes": 0,
            "stop_bytes": plan["settings"]["rss_stop_bytes"],
        },
        "disk": {
            "current_free_bytes": shutil.disk_usage(plan["result_root"]).free,
            "cache_ceiling_bytes": plan["settings"]["cache_limit_bytes"],
            "result_ceiling_bytes": plan["settings"]["result_limit_bytes"],
            "spill_ceiling_bytes": plan["settings"]["duckdb_memory_bytes"],
            "reserve_free_bytes": plan["settings"]["minimum_free_disk_bytes"],
        },
        "limitations": {
            "prefix_occupancy_unknown": True,
            "synthetic_worst_case_bytes": tree_bytes(
                Path(result["path"]) / "synthetic-adversarial"
            ),
            "pilot_refines_range_projection": True,
        },
    }
    projection_path = Path(result["path"]) / "resource_projection.json"
    atomic_json(projection_path, projection)
    pilot_plan = {
        "schema": "tape_cohort_pilot_plan_v1",
        "state": "awaiting_user_confirmation",
        "pilot_date": plan["pilot_date"],
        "members": len(pilot),
        "query_hash": plan["query_hash"],
        "release_identity": plan["release_identity"],
        "cache_root": plan["cache_root"],
        "result_root": plan["result_root"],
        "resource_projection": projection["pilot"],
        "approval": {"confirmed": False, "scope": None},
    }
    pilot_path = Path(result["path"]) / "pilot_plan.json"
    atomic_json(pilot_path, pilot_plan)
    result["resources_path"] = str(output)
    result["resource_projection_path"] = str(projection_path)
    result["pilot_plan_path"] = str(pilot_path)
    result["resources"] = measurement
    return result


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="action", required=True)
    q = sub.add_parser("plan")
    q.add_argument("--release", required=True)
    q.add_argument("--calculations", required=True)
    q.add_argument("--query", required=True)
    q.add_argument("--date-from", required=True)
    q.add_argument("--date-to", required=True)
    q.add_argument("--cache", required=True)
    q.add_argument("--output", required=True)
    q = sub.add_parser("checkpoint")
    q.add_argument("--plan", required=True)
    q.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    q = sub.add_parser("run")
    q.add_argument("--plan", required=True)
    q.add_argument("--resume", action="store_true")
    q.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    q = sub.add_parser("cache-status")
    q.add_argument("--cache", required=True)
    q = sub.add_parser("cache-reconcile")
    q.add_argument("--cache", required=True)
    q.add_argument("--attempt", required=True)
    q.add_argument(
        "--disposition",
        choices=("retain", "verify-and-admit", "discard-owned-incomplete"),
        default="retain",
    )
    q = sub.add_parser("stage-date")
    q.add_argument("--plan", required=True)
    q.add_argument("--date", required=True)
    q = sub.add_parser("trace")
    q.add_argument("--run", required=True)
    q.add_argument("--window-id", required=True)
    q = sub.add_parser("render")
    q.add_argument("--run", required=True)
    return p


def main():
    a = parser().parse_args()
    if a.action == "plan":
        output = Path(a.output)
        output.mkdir(parents=True, exist_ok=True)
        path = output / "run_plan.json"
        value = plan_query(
            a.release,
            a.calculations,
            json.loads(Path(a.query).read_text()),
            a.date_from,
            a.date_to,
            a.cache,
            output,
            output_path=path,
        )
        print(
            json.dumps(
                {
                    "plan": str(path),
                    "query_hash": value["query_hash"],
                    "pilot_date": value["pilot_date"],
                    "routed_members": len(value["routed_members"]),
                },
                indent=2,
            )
        )
    elif a.action == "checkpoint":
        print(
            json.dumps(
                run_checkpoint(a.plan) if a.worker else monitored_checkpoint(a.plan)
            )
        )
    elif a.action == "run":
        if not a.worker:
            from tape_data_product.query.tape_cohort_reliability import supervise

            result = supervise(a.plan, resume=a.resume)
            print(json.dumps(result))
            raise SystemExit(result["exit_code"])
        if os.environ.get("COHORT_SUPERVISOR_PID"):
            import threading

            expected_parent = int(os.environ["COHORT_SUPERVISOR_PID"])

            def watch_parent():
                while True:
                    if os.getppid() != expected_parent:
                        os._exit(75)
                    time.sleep(0.5)

            threading.Thread(target=watch_parent, daemon=True).start()
        plan = _load_plan(a.plan)
        result = run_query(
            plan["source_release"],
            plan["accepted_calculations"],
            plan["query"],
            plan["date_from"],
            plan["date_to"],
            plan["cache_root"],
            plan["result_root"],
            "range",
            a.plan,
            resume=a.resume,
        )
        print(json.dumps(result))
        raise SystemExit(0 if result.get("state") == "complete" else 2)
    elif a.action == "cache-status":
        from tape_data_product.query.tape_cohort_cache import FeatureCache

        with FeatureCache(a.cache) as cache:
            print(json.dumps(cache.status(), indent=2))
    elif a.action == "cache-reconcile":
        from tape_data_product.query.tape_cohort_cache import FeatureCache

        with FeatureCache(a.cache) as cache:
            print(json.dumps(cache.reconcile(a.attempt, a.disposition), indent=2))
    elif a.action == "stage-date":
        with open_date(a.plan, a.date) as connection:
            count = connection.execute(
                "SELECT count(*) FROM tape_endpoints"
            ).fetchone()[0]
            print(
                json.dumps(
                    {
                        "date": a.date,
                        "rows": count,
                        "state": "staged-and-pinned-for-session",
                    }
                )
            )
    elif a.action == "trace":
        print(json.dumps(extract_trace(a.run, a.window_id), indent=2))
    elif a.action == "render":
        from tape_data_product.query.tape_cohort_render import render

        print(json.dumps(render(a.run), indent=2))


if __name__ == "__main__":
    main()
