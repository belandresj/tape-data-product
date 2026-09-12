"""Verified local-release adapters for the report's historical numerical engines.

One B-row projection is retained (B <= 4,096), with fixed histograms and an 8 MiB
SQLite contributor cache. Exact distributions sort one field at a time using
256 MB DuckDB and at most 1 GB spill. No remote reads or feature recomputation.
"""

from __future__ import annotations

import json
from itertools import islice
from pathlib import Path

import numpy as np

from tape_data_product import stages
from . import aggregates, rendering, selection


def _json(path, value):
    with Path(path).open("x") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")


def _root(path):
    path = Path(path)
    return path if path.is_dir() else path.parent


def projected_batches(partition, metadata, batch_size=4096):
    """Use the verified public reconstruction reader, retaining only report fields."""
    from tape_data_product.features.compact_product import joined_rows

    if not 1 <= batch_size <= 4096:
        raise ValueError("report batch size must be 1..4096")
    fields = aggregates.FEATURES
    rows = joined_rows(partition, metadata)
    while True:
        # Project before retaining a batch: support/primitives do not accumulate.
        selected = list(
            islice(
                (
                    (
                        [row[f] if row[f + "_eda_eligible"] else None for f in fields],
                        [row[f + "_reason_mask"] for f in fields],
                        row["post_discovery_eligible"],
                        selection.SESSIONS.index(row["session_segment"]),
                        row["midpoint_age_observation_status"],
                    )
                    for row in rows
                ),
                batch_size,
            )
        )
        if not selected:
            break
        values = {
            f: np.array([r[0][i] for r in selected], dtype=np.float64)
            for i, f in enumerate(fields)
        }
        reasons = {
            f: np.array([r[1][i] for r in selected], dtype=np.uint32)
            for i, f in enumerate(fields)
        }
        yield (
            values,
            np.array([r[2] for r in selected], dtype=bool),
            np.array([r[3] for r in selected], dtype=np.int8),
            reasons,
            np.array([r[4] for r in selected]),
        )


def _screen_roots(screen):
    source = Path(screen)
    if source.is_dir():
        screens = [source]
    else:
        if source.stat().st_size > 2 * 1024**2:
            raise ValueError("screen inventory exceeds 2 MiB")
        inventory = json.loads(source.read_text())
        screens = [
            Path(item) if Path(item).is_absolute() else source.parent / item
            for item in inventory["screens"]
        ]
    if not 1 <= len(screens) <= 10000:
        raise ValueError("population inventory needs 1..10,000 screening stages")
    return screens


def reconcile_screen_membership(screen, release, database):
    """Require exact selected symbol/date membership, including zero-match days."""
    import sqlite3
    from tape_data_product.query.release import iter_members

    connection = sqlite3.connect(database)
    connection.execute("PRAGMA cache_size=-8192")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute(
        "CREATE TABLE expected(day TEXT, symbol TEXT, found INTEGER DEFAULT 0, PRIMARY KEY(day,symbol)) WITHOUT ROWID"
    )
    connection.execute(
        "CREATE TABLE reference_union(symbol TEXT PRIMARY KEY) WITHOUT ROWID"
    )
    count = 0
    try:
        for root in _screen_roots(screen):
            stages.verify_stage(root)
            denominator = json.loads((root / "denominators.json").read_text())
            reference = sqlite3.connect(
                (root / "screen.sqlite").resolve().as_uri() + "?mode=ro", uri=True
            )
            reference_count = 0
            try:
                for (symbol,) in reference.execute("SELECT symbol FROM reference"):
                    connection.execute(
                        "INSERT OR IGNORE INTO reference_union VALUES (?)", (symbol,)
                    )
                    reference_count += 1
            finally:
                reference.close()
            if reference_count != denominator["reference_symbols"]:
                raise ValueError(
                    "reference membership does not match screen denominator"
                )
            selected = 0
            with (root / "selection.jsonl").open() as stream:
                for line in stream:
                    if len(line) > 1024 * 1024:
                        raise ValueError("selection record exceeds 1 MiB")
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    if row["session_date"] != denominator["session_date"]:
                        raise ValueError("screen selection day mismatch")
                    connection.execute(
                        "INSERT INTO expected(day,symbol) VALUES (?,?)",
                        (row["session_date"], row["symbol"]),
                    )
                    selected += 1
            if selected != denominator["candidates"]:
                raise ValueError(
                    "screen selection does not reconcile to denominator count"
                )
            count += selected
        for member in iter_members(_root(release)):
            changed = connection.execute(
                "UPDATE expected SET found=1 WHERE day=? AND symbol=? AND found=0",
                (member["session_date"], member["symbol"]),
            ).rowcount
            if changed != 1:
                raise ValueError(
                    "release contains unexpected or duplicate screen member"
                )
        missing = connection.execute(
            "SELECT COUNT(*) FROM expected WHERE found=0"
        ).fetchone()[0]
        if missing:
            raise ValueError(f"release is missing {missing} selected screening members")
        connection.commit()
        return {
            "selected_members": count,
            "missing_members": 0,
            "unexpected_members": 0,
            "distinct_selected_symbols": connection.execute(
                "SELECT COUNT(DISTINCT symbol) FROM expected"
            ).fetchone()[0],
            "distinct_reference_symbols": connection.execute(
                "SELECT COUNT(*) FROM reference_union"
            ).fetchone()[0],
        }
    finally:
        connection.close()


def population_from_screen(screen):
    """Consume a verified screening stage; keep reference members without bars."""
    if screen is None:
        return {
            "daily": [],
            "status": "unavailable: screening denominator input not supplied",
        }
    screens = _screen_roots(screen)
    daily = []
    identities = []
    complete = True
    missing_minute_sources = 0
    synthetic = False
    for root in screens:
        receipt = stages.verify_stage(root)
        synthetic = synthetic or receipt["synthetic"]
        row = json.loads((root / "denominators.json").read_text())
        covered = row.get("population_coverage_complete", False)
        complete = complete and covered
        missing_minute_sources += row.get(
            "missing_minute_sources", row["reference_symbols"]
        )
        if not receipt["synthetic"] and not covered:
            raise ValueError(
                "historical population requires complete verified minute-source coverage"
            )
        day = row["session_date"]
        eligible, selected = row["reference_symbols"], row["candidates"]
        if (
            not isinstance(eligible, int)
            or not isinstance(selected, int)
            or not 0 <= selected <= eligible
            or eligible == 0
        ):
            raise ValueError("invalid reference-denominator daily counts")
        daily.append({"date": day, "eligible": eligible, "selected": selected})
        identities.append(receipt["identity"])
    daily.sort(key=lambda row: row["date"])
    if not daily or len({r["date"] for r in daily}) != len(daily):
        raise ValueError("empty or duplicate screening dates")
    denominator = sum(r["eligible"] for r in daily)
    numerator = sum(r["selected"] for r in daily)
    return {
        "daily": daily,
        "population_coverage_complete": complete,
        "missing_minute_sources": missing_minute_sources,
        "synthetic": synthetic,
        "screen_identities": identities,
        "selected_symbol_days": numerator,
        "reference_symbol_days": denominator,
        "period_share_pct": 100 * numerator / denominator,
        "status": (
            "regenerated from verified screen stage"
            if complete
            else "synthetic partial screening coverage"
        ),
    }


def aggregate_report(
    release, output, *, screen=None, config=None, synthetic=False, tape_config=None
):
    """Aggregate accepted local members into exact, offline-renderable artifacts.

    The immutable output owns retained work intermediates. Synthetic releases
    stay explicitly synthetic. Historical release hash guards are configurable.
    """
    from tape_data_product.query.release import verify_release, iter_members
    from tape_data_product.features.compact_product import verify_complete
    from tape_data_product.features.compact_product_schema import field_metadata

    output = Path(output)
    if output.exists():
        raise ValueError("report output exists; use a new immutable output directory")
    effective = dict(config or {})
    unknown = set(effective) - {"batch_size", "axes", "expected_release_hash"}
    if unknown:
        raise ValueError(f"unknown report configuration: {sorted(unknown)}")
    batch_size = effective.get("batch_size", 4096)
    if type(batch_size) is not int or not 1 <= batch_size <= 4096:
        raise ValueError("report batch size must be 1..4096")
    source = verify_release(
        _root(release), expected_release_hash=effective.get("expected_release_hash")
    )
    source_root = _root(release)
    release_receipt = stages.verify_stage(source_root)
    synthetic = bool(synthetic or release_receipt["synthetic"])
    population = population_from_screen(screen)
    synthetic = synthetic or population.get("synthetic", False)
    axes = effective.get("axes") or json.loads(
        Path(__file__).with_name("report_axes.json").read_text()
    )
    metadata = {f: field_metadata()[f] for f in aggregates.FEATURES}
    input_identity = stages.file_identity(source_root / "manifest.json")
    binding = selection.artifact_binding(
        release_identity=release_receipt["identity"],
        inventory_identity=input_identity["sha256"],
        source_identities={"release_manifest": input_identity, "synthetic": synthetic},
        feature_definitions=metadata,
        bin_configuration=axes,
        units={f: m["unit"] for f, m in metadata.items()},
        code_identities={},
    )
    output.mkdir(parents=True)
    reconciliation = (
        reconcile_screen_membership(
            screen, source_root, output / "screen_members.sqlite"
        )
        if screen
        else {"status": "screen membership unavailable"}
    )
    accumulator = aggregates.ReportPlotAggregates(
        output / "work", axes, batch_size=batch_size, feature_metadata=metadata
    )
    rows = members = 0
    try:
        for entry in iter_members(source_root):
            partition = Path(entry["path"])
            manifest = verify_complete(partition)
            member = {
                "date": entry["session_date"],
                "symbol": entry["symbol"],
                "partition_identity": entry["partition_identity"],
                "source_identity": {"manifest_sha256": entry["manifest_sha256"]},
            }
            members += 1
            member_rows = 0
            for values, post, sessions, reasons, status in projected_batches(
                partition, manifest["metadata"], batch_size
            ):
                accumulator.add_batch(
                    member,
                    values,
                    post,
                    sessions,
                    reason_masks=reasons,
                    midpoint_status=status,
                )
                member_rows += len(post)
            if member_rows != entry["expected_rows"]:
                raise ValueError("release member row count mismatch")
            rows += member_rows
        manifest = accumulator.finalize(output / "numerical", binding)
        if rows != manifest["rows"] or members != manifest["members"]:
            raise ValueError("report membership accounting mismatch")
        from .verification import verify_numerical

        verify_numerical(output / "numerical")
    finally:
        accumulator.close()
        accumulator.database.close()
    figures = rendering.default_figure_config(
        population,
        scope=(
            "SYNTHETIC OFFLINE DEMONSTRATION"
            if synthetic
            else "Verified local release; horizon-specific report activity filter"
        ),
    )
    _json(output / "figure_config.json", figures)
    _json(output / "population.json", population)
    gate = json.loads((output / "numerical/coverage/gate.json").read_text())
    _json(
        output / "headline_tables.json",
        {
            "population": population,
            "screen_release_reconciliation": reconciliation,
            "feature_members": members,
            "represented_endpoints": rows,
            "post_discovery_stock_hours": gate["60s"]["pooled"]["post_discovery"]
            / 3600,
            "activity_selection_by_horizon_session": gate,
            "feature_pair_eligibility": "numerical/coverage/populations.json",
            "midpoint_status": "numerical/coverage/midpoint_status.json",
            "contributor_concentration": "numerical/coverage/concentration.json",
            "synthetic": synthetic,
        },
    )
    tape_status = {
        "state": "unavailable",
        "reason": "Canonical T/Q and two explicit tape slice inputs not supplied",
    }
    if tape_config is not None:
        from .tape import aggregate_tape

        tape_status = aggregate_tape(tape_config, output / "tape", synthetic=synthetic)
    _json(output / "tape_status.json", tape_status)
    # Include every numerical intermediate and denominator in the immutable receipt.
    outputs = [
        p.relative_to(output)
        for p in output.rglob("*")
        if p.is_file() and "work" not in p.relative_to(output).parts
    ]
    stages.write_stage(
        output,
        "report.aggregate",
        inputs={
            "release": release_receipt["identity"],
            "screen": population.get("screen_identities"),
        },
        parameters={"batch_size": batch_size, "axes": axes},
        outputs=outputs,
        validation={
            "rows": rows,
            "members": members,
            "numerical_accounting": "passed",
            "screen_reconciliation": reconciliation,
            "tape": tape_status["state"],
        },
        synthetic=synthetic,
    )
    return output / "stage.json"


def render_report(aggregate, output, *, dpi=120):
    """Render only saved aggregates; exact ECDF scans stay bounded by 4,096 rows."""
    root, output = _root(aggregate), Path(output)
    receipt = stages.verify_stage(root)
    if receipt["stage"] != "report.aggregate":
        raise ValueError("expected report.aggregate stage")
    if type(dpi) is not int or not 72 <= dpi <= 300:
        raise ValueError("dpi must be 72..300")
    result = rendering.render_bundle(
        root / "numerical", output, dpi, root / "figure_config.json"
    )
    if (root / "tape" / "tape.json").exists():
        from .tape import render_tape

        render_tape(root / "tape", output, dpi=dpi)
    stages.write_stage(
        output,
        "report.render",
        inputs={"aggregate": receipt["identity"]},
        parameters={"dpi": dpi},
        outputs=[p.name for p in output.iterdir() if p.is_file()],
        validation={
            "export": "passed",
            "pages": len(result["pages"]) + int((root / "tape" / "tape.json").exists()),
        },
        synthetic=receipt["synthetic"],
    )
    return output / "stage.json"


def verify_report(path):
    """Verify saved output identities and numerical/export accounting evidence."""
    root = _root(path)
    receipt = stages.verify_stage(root)
    if receipt["stage"] == "report.aggregate":
        manifest = json.loads((root / "numerical" / "manifest.json").read_text())
        from .verification import verify_numerical

        verify_numerical(root / "numerical")
    elif receipt["stage"] == "report.render":
        from .export_verify import verify_bundle

        manifest = json.loads((root / "render_manifest.json").read_text())
        verify_bundle(root, manifest["render_config_identity"])
    else:
        raise ValueError("expected report aggregate or render stage")
    return receipt
