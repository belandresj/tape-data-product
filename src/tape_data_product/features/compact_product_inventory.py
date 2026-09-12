"""Streaming inventory bridge; preserve missing selected members before execution.

Consumes existing selection.sqlite files read-only. Six month/date membership is
an explicit caller input; historical calculation receipts alone never authorize
reuse. No new database and no raw data access are needed to freeze membership.
"""

from contextlib import ExitStack
import heapq
import json
from pathlib import Path
import sqlite3
from datetime import date, timedelta
from tape_data_product.features import compact_product_runtime as R


def selected(path):
    identity = dict(file=str(Path(path).resolve()), sha256=R.hash_file(path))
    db = sqlite3.connect(f"file:{Path(path).resolve()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        for row in db.execute("SELECT * FROM members ORDER BY day,symbol"):
            entry = json.loads(row["entry"]) if row["entry"] else {}
            source = entry.get("source", {})
            official = entry.get("official_reference")
            receipt = json.loads(row["receipt"]) if row["receipt"] else {}
            yield dict(
                session_date=row["day"],
                symbol=row["symbol"],
                inputs={k: source[k] for k in ("quotes", "trades") if source.get(k)},
                canonical_coverage_accepted=source.get("status")
                == "accepted_canonical_metadata",
                overlay_accepted=bool(official and official.get("manifest_sha256")),
                overlay=dict(halts=entry.get("halts", []), official_reference=official),
                discovery=json.loads(row["discovery"]) if row["discovery"] else {},
                inventory_reason=row["inventory_reason"],
                selection_identity=identity,
                candidate_base_manifest=(
                    receipt.get("feature_prefix") + "/manifest.json"
                    if receipt.get("feature_prefix")
                    else None
                ),
            )
    finally:
        db.close()


def freeze(selections, output, *, months):
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    if len(selections) > 12:
        raise ValueError("bounded selection input count exceeded")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".partial")
    previous = None
    counts = {}
    n = 0
    with temporary.open("x") as f:
        for m in heapq.merge(
            *(selected(p) for p in selections),
            key=lambda m: (m["session_date"], m["symbol"]),
        ):
            if m["session_date"][:7] not in months:
                continue
            key = m["session_date"], m["symbol"]
            if key == previous:
                raise ValueError("duplicate selected membership across inventories")
            previous = key
            f.write(json.dumps(m, sort_keys=True) + "\n")
            n += 1
            month = key[0][:7]
            counts[month] = counts.get(month, 0) + 1
        f.flush()
        __import__("os").fsync(f.fileno())
    temporary.replace(output)
    result = dict(
        members=n,
        months=counts,
        requested_months=list(months),
        missing_months=sorted(set(months) - set(counts)),
        sha256=R.hash_file(output),
        membership_authority="selected acquisition inventories; missing T/Q retained",
    )
    R.save(output.with_suffix(".manifest.json"), result)
    return result


def select_week(inventory, session_dates, output):
    """Earliest five consecutive supplied July trading sessions, coverage only.

    session_dates must come from the frozen acquisition/calendar authority;
    missing dates or missing members never become a shorter accepted week.
    """
    dates = sorted(session_dates)
    if len(set(dates)) != len(dates) or any(
        not d.startswith("2026-07-") for d in dates
    ):
        raise ValueError("invalid July trading session list")
    totals = {d: 0 for d in dates}
    blocked = {d: 0 for d in dates}
    for m in R.members(inventory):
        d = m["session_date"]
        if d not in totals:
            continue
        totals[d] += 1
        blocked[d] += not (
            m.get("canonical_coverage_accepted")
            and m.get("overlay_accepted")
            and all(m.get("inputs", {}).get(k) for k in ("quotes", "trades"))
        )
    chosen = None
    for i in range(len(dates) - 4):
        block = dates[i : i + 5]
        if all(totals[d] > 0 and blocked[d] == 0 for d in block):
            chosen = block
            break
    if chosen is None:
        return dict(
            state="blocked_input",
            dates={d: dict(selected=totals[d], blocked=blocked[d]) for d in dates},
        )
    output = Path(output)
    with output.open("x") as f:
        for m in R.members(inventory):
            if m["session_date"] in chosen:
                f.write(json.dumps(m, sort_keys=True) + "\n")
    return dict(
        state="frozen_one_week",
        dates=chosen,
        members=sum(totals[d] for d in chosen),
        inventory_sha256=R.hash_file(output),
        source_inventory_sha256=R.hash_file(inventory),
        scope="one_week_only",
        automatic_expansion=False,
    )
