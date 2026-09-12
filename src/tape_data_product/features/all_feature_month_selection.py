"""Immutable execution membership, separate from input-verification membership."""

import json
from pathlib import Path
from tape_data_product.features.all_feature_month_inventory import (
    connect,
    digest,
    read,
    sha,
)
from tape_data_product.features.all_feature_month_runtime import save


def create(parent, output, members, rule, seed=None):
    from tape_data_product.features.run_all_feature_month import verified_inventory

    inventory = verified_inventory(parent)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    db = connect(output / "selection.sqlite")
    source = connect(Path(parent) / "inventory.sqlite", True)
    db.execute(
        "CREATE TABLE members(day TEXT,symbol TEXT,seconds INTEGER,PRIMARY KEY(day,symbol))"
    )
    count = rows = 0
    try:
        with Path(members).open() as stream:
            for line in stream:
                item = json.loads(line)
                day = item["day"]
                symbol = item["symbol"]
                seconds = item.get("seconds", 57600)
                if type(seconds) is not int or not 1 <= seconds <= 57600:
                    raise ValueError("selection grid must be 1..57600 seconds")
                found = source.execute(
                    "SELECT status FROM members WHERE day=? AND symbol=?", (day, symbol)
                ).fetchone()
                if found is None or found["status"] != "admitted":
                    raise ValueError("selection member is not admitted in parent")
                db.execute("INSERT INTO members VALUES (?,?,?)", (day, symbol, seconds))
                count += 1
                rows += seconds
        if not count:
            raise ValueError("empty execution selection")
        db.commit()
    finally:
        db.close()
        source.close()
    meta = dict(
        version="tape_eda_execution_selection_v2",
        parent_inventory_hash=inventory["inventory_hash"],
        parent_inventory_root=str(Path(parent).resolve()),
        selection_rule=rule,
        seed=seed,
        members=count,
        rows=rows,
        requested_grid="per-member canonical prefix; 57600 means full",
        membership_sha256=sha(output / "selection.sqlite"),
    )
    meta["selection_hash"] = digest(meta)
    save(output / "selection_manifest.json", meta)
    return meta


def verify(root, parent_hash):
    root = Path(root)
    meta = read(root / "selection_manifest.json")
    expected = meta.pop("selection_hash")
    if (
        digest(meta) != expected
        or sha(root / "selection.sqlite") != meta["membership_sha256"]
    ):
        raise ValueError("execution selection identity changed")
    if meta["parent_inventory_hash"] != parent_hash:
        raise ValueError("execution selection parent changed")
    return meta | {"selection_hash": expected}


def members(db, selection=None):
    if selection:
        db.execute(
            "ATTACH DATABASE ? AS chosen",
            (str((Path(selection) / "selection.sqlite").resolve()),),
        )
        return db.execute(
            "SELECT p.*,s.seconds FROM members p JOIN chosen.members s USING(day,symbol) WHERE p.status='admitted' ORDER BY day,symbol"
        )
    return db.execute(
        "SELECT *,57600 AS seconds FROM members WHERE status='admitted' ORDER BY day,symbol"
    )
