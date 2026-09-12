"""Bounded Massive REST transport and normalized streaming producers.

HTTP response bytes are capped before JSON decoding. Cursor identity lives in
SQLite, retries apply only to transient transport/status failures, and a terminal
successful page is necessary before any completion receipt is written.
"""

from pathlib import Path
import hashlib
import json
import sqlite3
import time
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import pyarrow as pa
import pyarrow.parquet as pq
from .common import (
    BATCH_ROWS,
    MAX_PAGE_BYTES,
    bounds,
    dump,
    digest,
    records,
    validate_symbol,
)

BASE = "https://api.massive.com"
TRADE_SCHEMA = pa.schema(
    [
        (k, t)
        for k, t in [
            ("conditions", pa.list_(pa.int64())),
            ("correction", pa.int64()),
            ("decimal_size", pa.string()),
            ("exchange", pa.int64()),
            ("id", pa.string()),
            ("participant_timestamp", pa.int64()),
            ("price", pa.float64()),
            ("sequence_number", pa.int64()),
            ("sip_timestamp", pa.int64()),
            ("size", pa.float64()),
            ("tape", pa.int64()),
            ("trf_id", pa.int64()),
            ("trf_timestamp", pa.int64()),
        ]
    ]
)
QUOTE_SCHEMA = pa.schema(
    [
        (k, t)
        for k, t in [
            ("ask_exchange", pa.int64()),
            ("ask_price", pa.float64()),
            ("ask_size", pa.float64()),
            ("bid_exchange", pa.int64()),
            ("bid_price", pa.float64()),
            ("bid_size", pa.float64()),
            ("conditions", pa.list_(pa.int64())),
            ("indicators", pa.list_(pa.int64())),
            ("participant_timestamp", pa.int64()),
            ("sequence_number", pa.int64()),
            ("sip_timestamp", pa.int64()),
            ("tape", pa.int64()),
            ("trf_timestamp", pa.int64()),
        ]
    ]
)
SCHEMAS = {"trades": TRADE_SCHEMA, "quotes": QUOTE_SCHEMA}


class MassiveHTTPClient:
    """Explicit credential, same-origin HTTPS only; no credentials in artifacts."""

    def __init__(self, api_key, *, retries=3, max_bytes=MAX_PAGE_BYTES):
        if not api_key or not 0 <= retries <= 6 or not 1 <= max_bytes <= MAX_PAGE_BYTES:
            raise ValueError("Invalid HTTP credential/resource configuration")
        self.api_key, self.retries, self.max_bytes = api_key, retries, max_bytes

    def fetch(self, url, params=None):
        parts = urlsplit(url)
        if parts.scheme != "https" or parts.netloc != "api.massive.com":
            raise ValueError("Unexpected provider destination")
        query = dict(parse_qsl(parts.query))
        query.pop("apiKey", None)
        query.update(params or {})
        target = urlunsplit(
            (parts.scheme, parts.netloc, parts.path, urlencode(query), "")
        )
        for attempt in range(self.retries + 1):
            try:
                request = Request(
                    target, headers={"Authorization": "Bearer " + self.api_key}
                )
                with urlopen(request, timeout=60) as response:
                    raw = response.read(self.max_bytes + 1)
                if len(raw) > self.max_bytes:
                    raise ValueError(
                        "Provider response exceeds byte ceiling; lower page size"
                    )
                return json.loads(raw)
            except HTTPError as exc:
                if exc.code not in {429, 500, 502, 503, 504} or attempt == self.retries:
                    raise RuntimeError(f"Provider HTTP failure ({exc.code})") from None
            except (URLError, TimeoutError):
                if attempt == self.retries:
                    raise RuntimeError(
                        "Provider transport failure after bounded retries"
                    ) from None
            time.sleep(min(2**attempt, 8))


class FixtureClient:
    """Offline client of explicit paginated JSON response files, read one at a time."""

    def __init__(self, pages):
        self.pages = iter(pages)

    def fetch(self, url, params=None):
        try:
            page = next(self.pages)
        except StopIteration:
            raise ValueError("Fixture exhausted before terminal page") from None
        if isinstance(page, (str, Path)):
            with Path(page).open("rb") as source:
                raw = source.read(MAX_PAGE_BYTES + 1)
            if len(raw) > MAX_PAGE_BYTES:
                raise ValueError("Fixture page exceeds byte ceiling")
            return json.loads(raw)
        return page


def pages(client, url, params, cursor_path):
    """O(page) memory; repeated pagination URLs rejected using an on-disk set."""
    con = sqlite3.connect(cursor_path)
    con.execute("PRAGMA cache_size=-1024")
    con.execute("CREATE TABLE cursors(hash TEXT PRIMARY KEY) WITHOUT ROWID")
    expected_path = urlsplit(url).path
    try:
        while url:
            parsed = urlsplit(url)
            if (
                parsed.scheme != "https"
                or parsed.netloc != "api.massive.com"
                or parsed.path != expected_path
            ):
                raise ValueError("Unexpected pagination destination")
            try:
                con.execute(
                    "INSERT INTO cursors VALUES (?)",
                    (hashlib.sha256(url.encode()).hexdigest(),),
                )
            except sqlite3.IntegrityError:
                raise ValueError("Repeated pagination cursor") from None
            payload = client.fetch(url, params)
            if (
                len(json.dumps(payload, separators=(",", ":")).encode())
                > MAX_PAGE_BYTES
            ):
                raise ValueError("Decoded page exceeds byte ceiling")
            result = payload.get("results", [])
            if (
                payload.get("status") != "OK"
                or not isinstance(result, list)
                or len(result) > BATCH_ROWS
            ):
                raise ValueError("Non-OK or oversized provider page")
            if payload.get("results_count", len(result)) != len(result):
                raise ValueError("Provider results_count mismatch")
            next_url = payload.get("next_url")
            if next_url is not None and (
                not isinstance(next_url, str) or not next_url or not result
            ):
                raise ValueError("Invalid or empty nonterminal page")
            yield result
            url, params = next_url, None
    finally:
        con.close()


def acquire_reference(client, session_date, output, *, synthetic=False):
    """Historical active US stocks; screen retains CS/ADRC denominator members."""
    from tape_data_product.stages import write_stage

    bounds(session_date)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    count = 0
    with (output / "reference.jsonl").open("x") as sink:
        for page in pages(
            client,
            BASE + "/v3/reference/tickers",
            {
                "date": session_date,
                "market": "stocks",
                "locale": "us",
                "active": "true",
                "limit": BATCH_ROWS,
            },
            output / "cursors.sqlite",
        ):
            for row in page:
                symbol = validate_symbol(row["ticker"])
                if (
                    row.get("market", "stocks") != "stocks"
                    or row.get("locale", "us") != "us"
                    or row.get("active", True) is not True
                ):
                    raise ValueError(
                        "Provider returned incompatible reference membership"
                    )
                sink.write(
                    json.dumps(
                        dict(
                            session_date=session_date,
                            symbol=symbol,
                            type=row.get("type"),
                        )
                    )
                    + "\n"
                )
                count += 1
    write_stage(
        output,
        "acquire.reference",
        inputs={},
        parameters={
            "session_date": session_date,
            "market": "stocks",
            "locale": "us",
            "active": True,
        },
        outputs=["reference.jsonl"],
        validation={"complete": True, "rows": count},
        synthetic=synthetic,
    )
    return output / "reference.jsonl"


def acquire_minutes(client, session_date, symbol, output, *, synthetic=False):
    from tape_data_product.stages import write_stage

    validate_symbol(symbol)
    start, end = bounds(session_date)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    count = 0
    url = (
        BASE
        + f"/v2/aggs/ticker/{symbol}/range/1/minute/{start // 1_000_000}/{(end - 1) // 1_000_000}"
    )
    with (output / "minutes.jsonl").open("x") as sink:
        for page in pages(
            client,
            url,
            {"adjusted": "false", "sort": "asc", "limit": BATCH_ROWS},
            output / "cursors.sqlite",
        ):
            for row in page:
                value = dict(
                    symbol=symbol,
                    window_start=row["t"] * 1_000_000,
                    transactions=row.get("n"),
                    volume=row.get("v"),
                    open=row.get("o"),
                    close=row.get("c"),
                    high=row.get("h"),
                    low=row.get("l"),
                )
                if (
                    not isinstance(row["t"], int)
                    or not start <= value["window_start"] < end
                ):
                    raise ValueError(
                        "Minute outside requested session or noninteger timestamp"
                    )
                sink.write(json.dumps(value, allow_nan=False) + "\n")
                count += 1
    write_stage(
        output,
        "acquire.minutes",
        inputs={},
        parameters={
            "session_date": session_date,
            "symbol": symbol,
            "adjusted": False,
            "start_ns": start,
            "end_ns": end,
        },
        outputs=["minutes.jsonl"],
        validation={"complete": True, "rows": count},
        synthetic=synthetic,
    )
    return output / "minutes.jsonl"


def normalize_stream(
    client, session_date, symbol, stream, output, *, coverage=None, synthetic=False
):
    """Stable SIP order is validated, never silently reordered or deduplicated."""
    validate_symbol(symbol)
    schema = SCHEMAS[stream]
    start, end = bounds(session_date, stream)
    if coverage is not None:
        if not synthetic:
            raise ValueError(
                "Narrow coverage is supported only for labeled synthetic fixtures"
            )
        start, end = coverage
        if (
            not bounds(session_date, stream)[0]
            <= start
            < end
            <= bounds(session_date, stream)[1]
        ):
            raise ValueError("Invalid synthetic coverage")
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    partial = output.with_suffix(".partial")
    if partial.exists():
        raise ValueError("Interrupted stream requires inspection")
    rows = page_count = 0
    previous = minimum = None
    with (
        partial.open("xb") as sink,
        pq.ParquetWriter(sink, schema, compression="zstd") as writer,
    ):
        for page in pages(
            client,
            BASE + f"/v3/{stream}/{symbol}",
            {
                "timestamp.gte": start,
                "timestamp.lt": end,
                "order": "asc",
                "sort": "timestamp",
                "limit": BATCH_ROWS,
            },
            output.with_suffix(".cursors.sqlite"),
        ):
            page_count += 1
            for row in page:
                if set(row) - set(schema.names):
                    raise ValueError(
                        "Unknown vendor fields require explicit schema update"
                    )
                ts = row.get("sip_timestamp")
                if (
                    type(ts) is not int
                    or not start <= ts < end
                    or (previous is not None and ts < previous)
                ):
                    raise ValueError("Invalid or out-of-order SIP timestamp")
                required = (
                    ("price", "size")
                    if stream == "trades"
                    else ("bid_price", "ask_price", "bid_size", "ask_size")
                )
                if any(row.get(k) is None for k in required):
                    raise ValueError("Missing required vendor values")
                previous = ts
                if minimum is None:
                    minimum = ts
            if page:
                writer.write_table(
                    pa.Table.from_pylist(page, schema=schema), row_group_size=BATCH_ROWS
                )
                rows += len(page)
    partial.rename(output)
    receipt = dict(
        stream=stream,
        session_date=session_date,
        symbol=symbol,
        rows=rows,
        pages=page_count,
        start_ns=start,
        end_ns=end,
        minimum_sip_ns=minimum,
        maximum_sip_ns=previous,
        pagination_complete=True,
        source_provider="massive",
        source_method="rest",
        sha256=digest(output),
        bytes=output.stat().st_size,
        synthetic=synthetic,
    )
    dump(output.with_suffix(".receipt.json"), receipt)
    return receipt


def acquire_tq(client, selection, output, *, synthetic=False, coverage=None):
    """Complete each local pair sequentially; no implicit remote publication."""
    from tape_data_product.stages import verify_stage, write_stage
    from tape_data_product.storage.catalog import build_inventory

    selection = Path(selection)
    selection_stage = verify_stage(selection.parent)
    if (
        selection_stage["stage"] != "screen"
        or selection_stage["synthetic"] != synthetic
        or not selection_stage["validation"].get("complete")
        or selection_stage["outputs"].get("selection.jsonl", {}).get("sha256")
        != digest(selection)
    ):
        raise ValueError("Expected exact completed screen selection artifact")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    count = 0
    for row in records(selection):
        if row.get("synthetic", False) != synthetic:
            raise ValueError("Selection synthetic status mismatch")
        if not synthetic and not (
            row.get("discovery_verified") is True
            and row.get("verified") is True
            and row.get("minute_source_stage_identity")
        ):
            raise ValueError(
                "Production T/Q requires verified first-discovery evidence"
            )
        day, symbol = row["session_date"], validate_symbol(row["symbol"])
        pair = output / f"tq/session_date={day}/symbol={symbol}"
        pair.mkdir(parents=True, exist_ok=False)
        streams = {}
        for stream in SCHEMAS:
            streams[stream] = normalize_stream(
                client,
                day,
                symbol,
                stream,
                pair / f"{stream}.parquet",
                synthetic=synthetic,
                coverage=(coverage or {}).get(stream),
            )
        dump(
            pair / "pair.json",
            dict(
                version="canonical_tq_pair_v1",
                session_date=day,
                symbol=symbol,
                streams=streams,
                selection_sha256=digest(selection),
                selection_record=row,
                selection_record_sha256=hashlib.sha256(
                    json.dumps(
                        row, sort_keys=True, separators=(",", ":"), allow_nan=False
                    ).encode()
                ).hexdigest(),
                selection_stage_identity=selection_stage["identity"],
                synthetic=synthetic,
            ),
        )
        count += 1
    inventory = build_inventory(selection, output, output / "inventory.jsonl")
    write_stage(
        output,
        "acquire.tq",
        inputs={"selection_sha256": digest(selection)},
        parameters={"synthetic_coverage": coverage},
        outputs=["inventory.jsonl"],
        validation={"complete": True, "pairs": count},
        synthetic=synthetic,
    )
    return inventory
