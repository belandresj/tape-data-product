"""Reusable validated local retrieval, preserving the historical state machine."""

from pathlib import Path
from contextlib import ExitStack
from collections import Counter
import json
import sqlite3
from tape_data_product.features import compact_product
from tape_data_product.query.release import verify_release, iter_members
from tape_data_product.query.tape_cohort_config import normalize_config, query_hash
from tape_data_product.query.tape_cohort_state import CohortMachine
from tape_data_product.query.tape_cohort_reader import iter_query_batches
from tape_data_product.stages import write_stage


def run_query(
    release, config, output, *, expected_release_hash=None, pilot=False, dates=None
):
    """O(N K) processing, fixed machine state; all endpoints reach the machine.

    Supply and concentration are accumulated in an on-disk index. A prefix closes
    at a censored selection boundary; complete sessions retain terminal closure.
    """
    accepted = verify_release(release, expected_release_hash)
    config = normalize_config(config)
    root = Path(output)
    root.mkdir(parents=True, exist_ok=False)
    totals = Counter()
    database = root / "concentration.sqlite"
    db = sqlite3.connect(database)
    db.execute("PRAGMA cache_size=-4096")
    db.execute("PRAGMA temp_store=FILE")
    db.execute(
        "CREATE TABLE supply (dimension TEXT, key TEXT, members INTEGER, active_seconds INTEGER, PRIMARY KEY(dimension,key))"
    )
    outputs = [
        "windows.jsonl",
        "strict_runs.jsonl",
        "window_features.jsonl",
        "members.jsonl",
        "concentration.jsonl",
        "summary.json",
        "config.json",
    ]
    selected_date = None
    requested_dates = set(dates) if dates is not None else None
    observed_dates = set() if requested_dates is not None else None
    if requested_dates is not None and len(requested_dates) > 366:
        raise ValueError("Explicit date selection is capped at 366 days")
    if pilot:
        db.execute("CREATE TABLE dates (day TEXT PRIMARY KEY, members INTEGER)")
        for member in iter_members(release):
            db.execute(
                "INSERT INTO dates VALUES (?,1) ON CONFLICT(day) DO UPDATE SET members=members+1",
                (member["session_date"],),
            )
        n = db.execute("SELECT COUNT(*) FROM dates").fetchone()[0]
        values = [
            x[0]
            for x in db.execute(
                "SELECT members FROM dates ORDER BY members LIMIT 2 OFFSET ?",
                ((n - 1) // 2,),
            )
        ]
        median = values[0] if n % 2 else sum(values) / 2
        selected_date = db.execute(
            "SELECT day FROM dates ORDER BY abs(members-?),day LIMIT 1", (median,)
        ).fetchone()[0]
    try:
        with ExitStack() as stack:
            streams = {
                name: stack.enter_context((root / (name + ".jsonl")).open("x"))
                for name in ("windows", "strict_runs", "window_features", "members")
            }

            def sink(name):
                return lambda row: streams[name].write(
                    json.dumps(row, sort_keys=True, allow_nan=False) + "\n"
                )

            sinks = {
                name: sink(name)
                for name in ("windows", "strict_runs", "window_features")
            }
            for member in iter_members(release):
                if (
                    selected_date is not None
                    and member["session_date"] != selected_date
                ):
                    continue
                if (
                    requested_dates is not None
                    and member["session_date"] not in requested_dates
                ):
                    continue
                if observed_dates is not None:
                    observed_dates.add(member["session_date"])
                partition = compact_product.verify_complete(member["path"])
                machine = CohortMachine(
                    config,
                    member["partition_identity"],
                    session_date=member["session_date"],
                    symbol=member["symbol"],
                    checkpoint=member["expected_rows"] != 57600,
                    emit_trace=False,
                )
                for batch in iter_query_batches(
                    Path(member["path"]) / "features.parquet",
                    member,
                    partition["metadata"],
                    config,
                ):
                    for _ in machine.consume(batch, sinks):
                        pass
                counts = machine.finish(
                    (
                        "session_close"
                        if member["expected_rows"] == 57600
                        else "selection_boundary"
                    ),
                    sinks,
                )
                sink("members")(
                    {k: v for k, v in member.items() if k != "path"} | counts
                )
                totals.update(
                    {
                        key: value
                        for key, value in counts.items()
                        if isinstance(value, int)
                    }
                )
                totals["members"] += 1
                totals["zero_match_members"] += int(counts["observed_strict"] == 0)
                for dimension, key in [
                    ("symbol", member["symbol"]),
                    ("date", member["session_date"]),
                ]:
                    db.execute(
                        "INSERT INTO supply VALUES (?,?,1,?) ON CONFLICT(dimension,key) DO UPDATE SET members=members+1,active_seconds=active_seconds+excluded.active_seconds",
                        (dimension, key, counts["active_seconds"]),
                    )
        with (root / "concentration.jsonl").open("x") as stream:
            for row in db.execute(
                "SELECT dimension,key,members,active_seconds FROM supply ORDER BY dimension,active_seconds DESC,key"
            ):
                stream.write(
                    json.dumps(
                        dict(
                            zip(("dimension", "key", "members", "active_seconds"), row)
                        )
                    )
                    + "\n"
                )
    finally:
        db.close()
    database.unlink()
    if requested_dates is not None and observed_dates != requested_dates:
        raise ValueError(
            "Requested dates missing from release: "
            + str(sorted(requested_dates - observed_dates))
        )
    summary = dict(
        totals,
        release_identity=accepted["release_identity"],
        query_hash=query_hash(config),
        missing_members=0,
        scope="median_member_count_date_pilot" if pilot else "entire_release",
        selected_date=selected_date,
        expected_members=(
            totals["members"]
            if pilot or dates is not None
            else accepted["member_count"]
        ),
        selected_dates=sorted(requested_dates) if requested_dates else None,
        partial_release=accepted["allow_partial"],
        synthetic=accepted["synthetic"],
    )
    if summary["members"] != summary["expected_members"]:
        raise ValueError("Query member accounting incomplete")
    compact_product.atomic_json(root / "summary.json", summary)
    compact_product.atomic_json(root / "config.json", config)
    write_stage(
        root,
        "threshold_pilot" if pilot else "query",
        {"release_identity": accepted["release_identity"]},
        config,
        outputs,
        summary,
        synthetic=accepted["synthetic"],
    )
    return summary
