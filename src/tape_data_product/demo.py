"""A small complete invented provider-to-report workflow.

Two 720-second prefixes are generated through paginated fake vendor responses.
No feature rows are hand authored and no market data or credentials are used.
"""

from importlib.resources import files
import json
import math
from pathlib import Path

from tape_data_product.acquisition.common import bounds
from tape_data_product.acquisition.vendor import (
    FixtureClient,
    acquire_reference,
    acquire_minutes,
    acquire_tq,
    BASE,
)
from tape_data_product.acquisition.screen import screen
from tape_data_product.stages import write_stage, verify_stage

DAY = "2026-07-01"
SYMBOLS = ("SYNTHA", "SYNTHB")
SECONDS = 720
NS = 1_000_000_000


def _midpoint(second, symbol):
    amplitude = 0.045 if symbol == SYMBOLS[0] else 0.042
    return 100 * math.exp(amplitude * math.sin(second / 8))


def _events(start, symbol, stream):
    if stream == "quotes":
        for second in range(-1, SECONDS):
            timestamp = start - 1 if second == -1 else start + second * NS
            midpoint = _midpoint(max(0, second), symbol)
            half_spread = 0.0001 if symbol == SYMBOLS[0] else 0.002
            yield dict(
                sip_timestamp=timestamp,
                sequence_number=second + 1,
                participant_timestamp=timestamp,
                bid_price=midpoint * (1 - half_spread),
                ask_price=midpoint * (1 + half_spread),
                bid_size=100.0,
                ask_size=101.0,
                bid_exchange=1,
                ask_exchange=1,
                conditions=[],
                indicators=[],
                tape=1,
            )
    else:
        for second in range(SECONDS):
            for trade in range(15):
                timestamp = start + second * NS + trade * 60_000_000
                yield dict(
                    sip_timestamp=timestamp,
                    sequence_number=second * 15 + trade,
                    participant_timestamp=timestamp,
                    price=_midpoint(second, symbol),
                    size=100.0,
                    conditions=[],
                    correction=0,
                )


def _pages(rows, url):
    """One 512-record page plus a one-record lookahead, never a full event list."""
    iterator = iter(rows)
    following = next(iterator, None)
    page_number = 0
    while following is not None:
        page = []
        while following is not None and len(page) < 512:
            page.append(following)
            following = next(iterator, None)
        payload = {"status": "OK", "results": page}
        page_number += 1
        if following is not None:
            payload["next_url"] = url + f"?cursor={page_number}"
        yield payload


def _minute_rows(start, symbol):
    for minute in range(SECONDS // 60):
        # Invented bars summarize the same invented event prices.
        mids = [
            _midpoint(second, symbol)
            for second in range(minute * 60, (minute + 1) * 60)
        ]
        yield dict(
            t=(start + minute * 60 * NS) // 1_000_000,
            n=900,
            v=90000,
            o=mids[0],
            c=mids[-1],
            h=max(mids),
            l=min(mids),
        )


def run_demo(output):
    import pyarrow as pa
    from tape_data_product.features.api import build_inventory
    from tape_data_product.query.release import build_release, verify_release
    from tape_data_product.query.api import run_query
    from tape_data_product.analysis.pipeline import (
        aggregate_report,
        render_report,
        verify_report,
    )

    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    start, _ = bounds(DAY)
    reference = acquire_reference(
        FixtureClient(
            [
                {
                    "status": "OK",
                    "results": [
                        {"ticker": symbol, "type": "CS"}
                        for symbol in (*SYMBOLS, "QUIET")
                    ],
                }
            ]
        ),
        DAY,
        root / "reference",
        synthetic=True,
    )
    minute_files = []
    for symbol in SYMBOLS:
        minute_files.append(
            acquire_minutes(
                FixtureClient(
                    [{"status": "OK", "results": list(_minute_rows(start, symbol))}]
                ),
                DAY,
                symbol,
                root / f"minutes-{symbol}",
                synthetic=True,
            )
        )
    minute_files.append(
        acquire_minutes(
            FixtureClient([{"status": "OK", "results": []}]),
            DAY,
            "QUIET",
            root / "minutes-QUIET",
            synthetic=True,
        )
    )
    minutes = root / "minutes.jsonl"
    with minutes.open("x") as sink:
        for path in minute_files:
            with path.open() as source:
                for line in source:
                    sink.write(line)
    selection = screen(
        reference,
        minutes,
        DAY,
        root / "screen",
        synthetic=True,
        minute_receipts=[path.parent for path in minute_files],
    )

    def vendor_pages():
        for symbol in SYMBOLS:
            for stream in ("trades", "quotes"):
                yield from _pages(
                    _events(start, symbol, stream), BASE + f"/v3/{stream}/{symbol}"
                )

    inventory = acquire_tq(
        FixtureClient(vendor_pages()),
        selection,
        root / "acquired",
        synthetic=True,
        coverage={
            "trades": (start, start + SECONDS * NS),
            "quotes": (start - 1, start + SECONDS * NS),
        },
    )
    verify_stage(root / "acquired")
    partitions = build_inventory(
        inventory, root / "features", seconds=SECONDS, synthetic=True, batch_size=127
    )
    build_release(selection, partitions, root / "release", allow_partial=True)
    accepted = verify_release(root / "release")
    query_config = json.loads(
        files("tape_data_product")
        .joinpath("resources/tape_cohort_300s_ms3_p050_selected_v1.json")
        .read_text()
    )
    query_result = run_query(root / "release", query_config, root / "query")
    tape_config = {
        "synthetic": True,
        "slices": [
            {
                "symbol": symbol,
                "date": DAY,
                "start": "04:06:40",
                "end": "04:11:40",
                "trades": str(
                    root
                    / f"acquired/tq/session_date={DAY}/symbol={symbol}/trades.parquet"
                ),
                "quotes": str(
                    root
                    / f"acquired/tq/session_date={DAY}/symbol={symbol}/quotes.parquet"
                ),
                "partition": str(
                    root / "features" / f"session_date={DAY}" / f"symbol={symbol}"
                ),
            }
            for symbol in SYMBOLS
        ],
    }
    aggregate_report(
        root / "release",
        root / "analysis",
        screen=root / "screen",
        tape_config=tape_config,
        synthetic=True,
    )
    render_report(root / "analysis", root / "figures")
    figure_receipt = verify_report(root / "figures")
    if figure_receipt["validation"]["pages"] != 7:
        raise ValueError(
            "The complete demo requires seven rendered and verified figures"
        )
    summary = dict(
        kind="synthetic_only",
        session_date=DAY,
        members=2,
        seconds_per_member=SECONDS,
        feature_rows=2 * SECONDS,
        quote_rows=2 * (SECONDS + 1),
        trade_rows=2 * SECONDS * 15,
        reference_members=3,
        selected_members=2,
        release_identity=accepted["release_identity"],
        query=query_result,
        note="Invented session-start prefixes; no historical reproduction or predictive/executable results.",
    )
    with (root / "summary.json").open("x") as stream:
        json.dump(summary, stream, indent=2)
        stream.write("\n")
    write_stage(
        root,
        "demo",
        {},
        {"seconds": SECONDS, "symbols": list(SYMBOLS)},
        [
            "summary.json",
            "minutes.jsonl",
            "features/partitions.jsonl",
            "features/stage.json",
            "figures/stage.json",
        ],
        {"complete": True, "feature_rows": 2 * SECONDS},
        synthetic=True,
    )
    return summary
