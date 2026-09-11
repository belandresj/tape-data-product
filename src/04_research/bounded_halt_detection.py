"""Bounded-memory implementation of the existing V1 halt detector.

Trade events reside in a private SQLite intermediate, never a full-day RAM
array. Cache is 8 MiB; temp sorting spills to disk. Fixed 57,600-second activity
arrays preserve the original predicate. Candidate traces are separate Parquet
files written in <=25,000-row batches. Quote evidence is batch-reduced.

Time O(N log N + C log N + selected trace rows); disk O(N); memory
O(25000 * columns + 57600 * fixed arrays + C), C <= floor(57600/240).
The candidate equations/populations are V1, including its original RTH activity
eligibility population in the extended clock. This does not promote a new
inference acceptance policy. Floating-point totals may differ at roundoff scale.
"""
from __future__ import annotations
from pathlib import Path
from typing import Any
import math
import sqlite3
import tempfile
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import historical_halt_registry_v1 as OLD
from historical_halt_registry_v1 import (DetectionConfig, MARKET, TRADE_SCAN_COLUMNS,
    QUOTE_EVIDENCE_COLUMNS, SESSION_SECONDS, NS, DETECTION_METHOD_VERSION,
    REGISTRY_VERSION, session_bounds_ns, activity_predicate, parquet_identity,
    ceil_second_strictly_after, canonical_json, sha256_file)
from historical_halt_registry_v1 import (_duplicate_payload, _pre_gap_qualified,
    _candidate_hash, _utc, _range_bps, _abs_return_bps)


class TradeStore:
    def __init__(self, root):
        root = Path(root); root.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix='halt-trades-', dir=root)
        self.path = Path(self.temporary.name) / 'events.sqlite'
        self.db = sqlite3.connect(self.path)
        self.db.execute('PRAGMA cache_size=-8192')
        self.db.execute('PRAGMA temp_store=FILE')
        self.db.execute('PRAGMA mmap_size=0')
        self.db.execute('PRAGMA journal_mode=OFF')
        self.db.execute('CREATE TABLE trades(sip INTEGER, price REAL, eligible INTEGER, prefix REAL)')
        self.prefix = 0.0

    def append(self, sip, price, eligible, dollars):
        # Seed each bounded cumsum with the preceding endpoint, preserving the
        # original sequential eligible-dollar prefix arithmetic across batches.
        amounts = dollars[eligible]
        prefix = np.cumsum(np.r_[self.prefix, amounts])[1:]
        positions = np.cumsum(eligible.astype(np.int64))
        all_prefix = np.r_[self.prefix, prefix][positions]
        if prefix.size: self.prefix = float(prefix[-1])
        self.db.executemany('INSERT INTO trades VALUES(?,?,?,?)',
            ((int(t), float(p) if math.isfinite(p) and p > 0 else None, int(e), float(v))
             for t, p, e, v in zip(sip, price, eligible, all_prefix)))
        self.db.commit()

    def index(self):
        self.db.execute('CREATE INDEX raw_time ON trades(sip)')
        self.db.execute('CREATE INDEX eligible_time ON trades(sip) WHERE eligible=1')
        self.db.commit()

    def gaps(self, floor):
        previous = None
        for t, p in self.db.execute('SELECT sip,price FROM trades WHERE eligible=1 ORDER BY sip,rowid'):
            if previous is not None and t - previous[0] >= floor:
                yield previous[0], t, previous[1], p
            previous = (t, p)

    def count(self, a, b, eligible=False):
        clause = ' AND eligible=1' if eligible else ''
        return self.db.execute('SELECT count(*) FROM trades WHERE sip>=? AND sip<?'+clause,(a,b)).fetchone()[0]

    def dollars(self, a, b):
        def before(t):
            r = self.db.execute('SELECT prefix FROM trades WHERE eligible=1 AND sip<? ORDER BY sip DESC,rowid DESC LIMIT 1',(t,)).fetchone()
            return r[0] if r else 0.0
        return before(b)-before(a)

    def window(self, a, b, eligible=False):
        clause = ' AND eligible=1' if eligible else ''
        base = ' FROM trades WHERE sip>=? AND sip<? AND price IS NOT NULL'+clause
        n, lo, hi = self.db.execute('SELECT count(*),min(price),max(price)'+base,(a,b)).fetchone()
        if not n: return np.array([])
        first = self.db.execute('SELECT price'+base+' ORDER BY sip,rowid LIMIT 1',(a,b)).fetchone()[0]
        if n == 1: return np.array([first])
        last = self.db.execute('SELECT price'+base+' ORDER BY sip DESC,rowid DESC LIMIT 1',(a,b)).fetchone()[0]
        return np.array([first,lo,hi,last])

    def presence(self, start):
        return self.db.execute('SELECT (sip-?)/1000000000,min(sip),max(sip) FROM trades WHERE eligible=1 GROUP BY 1 ORDER BY 1',(start,))

    def trace(self, a, b, path):
        schema = pa.schema([('sip_timestamp',pa.int64()),('price',pa.float64()),('eligible',pa.bool_())])
        cursor = self.db.execute('SELECT sip,price,eligible FROM trades WHERE sip>=? AND sip<? AND price IS NOT NULL ORDER BY sip,rowid',(a,b))
        with pq.ParquetWriter(path,schema,compression='zstd') as writer:
            while rows := cursor.fetchmany(25000):
                writer.write_table(pa.Table.from_pylist([dict(sip_timestamp=t,price=p,eligible=bool(e)) for t,p,e in rows],schema=schema))

    def close(self):
        self.db.close(); self.temporary.cleanup()

    def __del__(self):
        if hasattr(self,'db'): self.db.close()
        if hasattr(self,'temporary'): self.temporary.cleanup()


def scan_trade_symbol_day(
    *,
    session_date: str,
    symbol: str,
    instrument_type: str,
    universe_version: str,
    trade_path: Path,
    quote_path: Path,
    config: DetectionConfig,
    scratch_root: Path,
    review_root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Stream one trade Parquet and construct exact eligible-trade gaps."""

    trade_path = trade_path.expanduser().resolve()
    quote_path = quote_path.expanduser().resolve()
    if not trade_path.is_file():
        raise FileNotFoundError(trade_path)
    if not quote_path.is_file():
        raise FileNotFoundError(quote_path)
    parquet = pq.ParquetFile(trade_path, memory_map=True)
    quote_parquet = pq.ParquetFile(quote_path, memory_map=True)
    missing_quotes = sorted(set(QUOTE_EVIDENCE_COLUMNS) - set(quote_parquet.schema_arrow.names))
    if missing_quotes:
        raise ValueError(f"{quote_path}: missing canonical quote columns: {missing_quotes}")
    available = set(parquet.schema_arrow.names)
    mandatory = set(TRADE_SCAN_COLUMNS) - {"decimal_size"}
    missing = sorted(mandatory - available)
    if missing:
        raise ValueError(f"{trade_path}: missing trade columns: {missing}")
    columns = [name for name in TRADE_SCAN_COLUMNS if name in available]

    start_ns, end_ns = session_bounds_ns(session_date)
    raw_counts = np.zeros(SESSION_SECONDS, dtype=np.int64)
    eligible_counts = np.zeros(SESSION_SECONDS, dtype=np.int64)
    eligible_dollars = np.zeros(SESSION_SECONDS, dtype=np.float64)
    store = TradeStore(scratch_root)
    review_root.mkdir(parents=True, exist_ok=True)
    raw_rows = 0
    in_session_raw_rows = 0
    previous_key: tuple[int, int] | None = None
    previous_payload: tuple[Any, ...] | None = None
    duplicate_key_count = 0
    exact_duplicate_count = 0

    for batch in parquet.iter_batches(
        batch_size=config.batch_size,
        columns=columns,
        use_threads=False,
    ):
        table = pa.Table.from_batches([batch])
        if "decimal_size" not in table.column_names:
            table = table.append_column("decimal_size", pa.nulls(table.num_rows, pa.string()))
        table = table.select(list(TRADE_SCAN_COLUMNS))
        if table.num_rows == 0:
            continue
        for required in ("sip_timestamp", "participant_timestamp", "sequence_number"):
            if table[required].null_count:
                raise ValueError(f"{trade_path}: null {required}")
        sip = np.asarray(table["sip_timestamp"].to_numpy(), dtype=np.int64)
        seq = np.asarray(table["sequence_number"].to_numpy(), dtype=np.int64)
        if np.any(sip[1:] < sip[:-1]) or np.any((sip[1:] == sip[:-1]) & (seq[1:] < seq[:-1])):
            raise ValueError(f"{trade_path}: rows are not ordered by (sip_timestamp, sequence_number, raw_row_index)")
        first_key = (int(sip[0]), int(seq[0]))
        if previous_key is not None and first_key < previous_key:
            raise ValueError(f"{trade_path}: ordering decreases across Parquet batches")
        if previous_key is not None and first_key == previous_key:
            duplicate_key_count += 1
            payload = _duplicate_payload(table, 0)
            if payload == previous_payload:
                exact_duplicate_count += 1
                raise ValueError(f"{trade_path}: exact duplicate trade row for key {first_key}")
            raise ValueError(f"{trade_path}: conflicting duplicate trade key {first_key}")
        duplicate_positions = np.flatnonzero(
            (sip[1:] == sip[:-1]) & (seq[1:] == seq[:-1])
        ) + 1
        if duplicate_positions.size:
            position = int(duplicate_positions[0])
            duplicate_key_count += int(duplicate_positions.size)
            key = (int(sip[position]), int(seq[position]))
            if _duplicate_payload(table, position) == _duplicate_payload(table, position - 1):
                exact_duplicate_count += 1
                raise ValueError(f"{trade_path}: exact duplicate trade row for key {key}")
            raise ValueError(f"{trade_path}: conflicting duplicate trade key {key}")
        previous_key = (int(sip[-1]), int(seq[-1]))
        previous_payload = _duplicate_payload(table, table.num_rows - 1)

        semantics = MARKET.activity_trade_semantics(
            table, max_reporting_latency_ns=config.max_reporting_latency_ns
        )
        price = np.asarray(table["price"].to_numpy(), dtype=np.float64)
        in_session = (sip >= start_ns) & (sip < end_ns)
        session_sip = sip[in_session]
        indices = ((session_sip - start_ns) // NS).astype(np.int64)
        if indices.size:
            raw_counts += np.bincount(indices, minlength=SESSION_SECONDS).astype(np.int64)
        eligible = in_session & semantics.eligible_activity_trade
        eligible_sip = sip[eligible]
        if eligible_sip.size:
            eligible_indices = ((eligible_sip - start_ns) // NS).astype(np.int64)
            eligible_counts += np.bincount(eligible_indices, minlength=SESSION_SECONDS).astype(np.int64)
            dollars = price[eligible] * semantics.analytic_size[eligible]
            eligible_dollars += np.bincount(
                eligible_indices, weights=dollars, minlength=SESSION_SECONDS
            ).astype(np.float64)
        store.append(sip[in_session], price[in_session], eligible[in_session],
                     (price * semantics.analytic_size)[in_session])
        raw_rows += table.num_rows
        in_session_raw_rows += int(np.count_nonzero(in_session))

    if raw_rows != parquet.metadata.num_rows:
        raise AssertionError(f"{trade_path}: Parquet row count changed while scanning")

    store.index()
    predicate = activity_predicate(eligible_counts, eligible_dollars, config)
    candidates: list[dict[str, Any]] = []
    config_hash = config.digest()
    trade_identity = parquet_identity(trade_path)
    quote_identity = parquet_identity(quote_path)

    for last_pre, first_post, pre_price, post_price in store.gaps(config.gap_floor_seconds * NS):
        if not (start_ns <= last_pre < first_post < end_ns):
            continue
        halt_start = ceil_second_strictly_after(last_pre)
        start_index = int((halt_start - start_ns) // NS)
        if not _pre_gap_qualified(predicate, start_index, config):
            continue
        gap_seconds = (first_post - last_pre) / NS
        post_counts: list[int | None] = []
        post_dollars: list[float | None] = []
        for minute in range(5):
            lo = first_post + minute * 60 * NS
            hi = lo + 60 * NS
            if hi > end_ns:
                post_counts.append(None)
                post_dollars.append(None)
                continue
            post_counts.append(store.count(lo, hi, eligible=True))
            post_dollars.append(store.dollars(lo, hi))
        post_qualified = any(
            count is not None
            and count >= config.activity_minute_trades
            and dollars is not None
            and dollars >= config.activity_minute_dollars
            for count, dollars in zip(post_counts, post_dollars)
        )
        raw_gap_count = store.count(last_pre + 1, first_post)
        pre60_count = store.count(halt_start - 60 * NS, halt_start, eligible=True)
        pre300_count = store.count(halt_start - 300 * NS, halt_start, eligible=True)
        pre60_dollars = store.dollars(halt_start - 60 * NS, halt_start)
        pre300_dollars = store.dollars(halt_start - 300 * NS, halt_start)
        distances = [abs(gap_seconds - 300 * k) for k in (1, 2, 3, 4)]
        nearest_position = int(np.argmin(distances))

        raw_pre60 = store.window(halt_start - 60 * NS, halt_start, eligible=False)
        raw_pre300 = store.window(halt_start - 300 * NS, halt_start, eligible=False)
        raw_post60 = store.window(first_post, min(first_post + 60 * NS, end_ns), eligible=False)
        raw_post300 = store.window(first_post, min(first_post + 300 * NS, end_ns), eligible=False)
        eligible_pre60 = store.window(halt_start - 60 * NS, halt_start, eligible=True)
        eligible_pre300 = store.window(halt_start - 300 * NS, halt_start, eligible=True)
        eligible_post60 = store.window(first_post, min(first_post + 60 * NS, end_ns), eligible=True)
        eligible_post300 = store.window(first_post, min(first_post + 300 * NS, end_ns), eligible=True)
        review_start = max(start_ns, halt_start - 300 * NS)
        review_end = min(end_ns, first_post + 300 * NS)
        review_id = _candidate_hash(session_date, symbol, last_pre, first_post, config_hash)
        trace_path = review_root / f"{review_id}_trades.parquet"
        store.trace(review_start, review_end, trace_path)

        row: dict[str, Any] = {
            "candidate_id": _candidate_hash(session_date, symbol, last_pre, first_post, config_hash),
            "universe_version": universe_version,
            "session_date": session_date,
            "symbol": symbol,
            "instrument_type": instrument_type,
            "detection_method_version": DETECTION_METHOD_VERSION,
            "registry_version": REGISTRY_VERSION,
            "candidate_config_hash": config_hash,
            "last_pre_gap_trade_sip": _utc(last_pre),
            "first_post_gap_trade_sip": _utc(first_post),
            "inferred_halt_interval_start": _utc(halt_start),
            "inferred_trade_resume_time": _utc(first_post),
            "halt_interval_start": _utc(halt_start),
            "trade_resume_time": _utc(first_post),
            "eligible_gap_seconds": float(gap_seconds),
            "nearest_300s_multiple": nearest_position + 1,
            "distance_to_nearest_300s_multiple": float(distances[nearest_position]),
            "near_300s_multiple_90s": bool(distances[nearest_position] <= config.duration_multiple_tolerance_seconds),
            "raw_trade_count_in_gap": raw_gap_count,
            "eligible_trade_count_in_gap": 0,
            "pre_gap_trade_count_60s": pre60_count,
            "pre_gap_trade_count_300s": pre300_count,
            "pre_gap_dollar_volume_60s": pre60_dollars,
            "pre_gap_dollar_volume_300s": pre300_dollars,
            "post_gap_trade_counts_60s": post_counts,
            "post_gap_dollar_volumes_60s": post_dollars,
            "pre_gap_activity_qualified": True,
            "post_gap_activity_qualified": bool(post_qualified),
            "pre_gap_range_bps_60s": _range_bps(eligible_pre60),
            "pre_gap_range_bps_300s": _range_bps(eligible_pre300),
            "pre_gap_abs_return_bps_60s": _abs_return_bps(eligible_pre60),
            "pre_gap_abs_return_bps_300s": _abs_return_bps(eligible_pre300),
            "post_gap_range_bps_60s": _range_bps(eligible_post60),
            "post_gap_range_bps_300s": _range_bps(eligible_post300),
            "raw_pre_gap_range_bps_60s": _range_bps(raw_pre60),
            "raw_pre_gap_range_bps_300s": _range_bps(raw_pre300),
            "raw_pre_gap_abs_return_bps_60s": _abs_return_bps(raw_pre60),
            "raw_pre_gap_abs_return_bps_300s": _abs_return_bps(raw_pre300),
            "raw_post_gap_range_bps_60s": _range_bps(raw_post60),
            "raw_post_gap_range_bps_300s": _range_bps(raw_post300),
            "reopening_jump_bps": float(10_000.0 * math.log(post_price / pre_price)),
            "review_raw_trade_sip": [],
            "review_raw_trade_prices": [],
            "review_eligible_trade_sip": [],
            "review_eligible_trade_prices": [],
            "review_trade_trace_path": str(trace_path.resolve()),
            "review_trade_trace_sha256": sha256_file(trace_path),
            "review_storage_version": "bounded_parquet_traces_v1",
            "trade_source_path": str(trade_path),
            "trade_source_sha256": trade_identity["sha256"],
            "trade_source_parquet_metadata_sha256": trade_identity["parquet_metadata_sha256"],
            "quote_source_path": str(quote_path),
            "quote_source_sha256": quote_identity["sha256"],
            "quote_source_parquet_metadata_sha256": quote_identity["parquet_metadata_sha256"],
            "source_provenance": canonical_json({"trade": trade_identity, "quote": quote_identity}),
        }
        candidates.append(row)

    presence = [dict(session_date=session_date, symbol=symbol, second_index=second,
                     first_eligible_sip=first, last_eligible_sip=last)
                for second, first, last in store.presence(start_ns)]
    qualified_end_indices = np.flatnonzero(predicate)
    summary = {
        "universe_version": universe_version,
        "session_date": session_date,
        "symbol": symbol,
        "instrument_type": instrument_type,
        "status": "processed",
        "failure_reason": None,
        "raw_trade_rows": raw_rows,
        "in_session_raw_trade_rows": in_session_raw_rows,
        "raw_trade_active_seconds": int(np.count_nonzero(raw_counts)),
        "eligible_activity_trade_rows": int(eligible_counts.sum()),
        "eligible_activity_dollars": float(store.prefix),
        "candidate_count": len(candidates),
        "activity_qualified_end_ns": [int(start_ns + index * NS) for index in qualified_end_indices],
        "duplicate_key_count": duplicate_key_count,
        "exact_duplicate_count": exact_duplicate_count,
        "trade_source_path": str(trade_path),
        "trade_source_sha256": trade_identity["sha256"],
        "trade_source_parquet_metadata_sha256": trade_identity["parquet_metadata_sha256"],
        "quote_source_path": str(quote_path),
        "quote_source_sha256": quote_identity["sha256"],
        "quote_source_parquet_metadata_sha256": quote_identity["parquet_metadata_sha256"],
        "candidate_config_hash": config_hash,
    }
    summary["bounded_store_peak_bytes"] = store.path.stat().st_size
    store.close()
    return summary, candidates, presence


def quote_evidence(quote_path, candidates, review_root, config):
    """Reduce the exact V1 quote evidence without constructing QuoteState.

    All candidates are fixed by the trade-only pass before this function runs.
    Review traces retain exact events on disk; they are not classification data.
    """
    review_root = Path(review_root)
    review_root.mkdir(parents=True, exist_ok=True)
    rows, states = [], []
    schema = pa.schema([('sip_timestamp', pa.int64()), ('midpoint', pa.float64()), ('valid', pa.bool_())])
    for c in candidates:
        row = {k: c[k] for k in ('candidate_id', 'session_date', 'symbol')}
        for k in ('quote_message_count_in_gap', 'quote_active_minute_count_in_gap',
                  'semantic_quote_state_change_count_in_gap', 'valid_quote_message_count_in_gap',
                  'invalid_or_one_sided_transition_count', 'locked_transition_count', 'crossed_transition_count'):
            row[k] = 0
        for k in ('last_pre_gap_valid_midpoint', 'first_post_gap_valid_midpoint', 'last_pre_gap_spread_bps',
                  'first_post_gap_spread_bps', 'quote_resume_time'):
            row[k] = None
        row.update(review_quote_sip=[], review_quote_midpoint=[], review_quote_valid=[])
        path = review_root / f"{c['candidate_id']}_quotes.parquet"
        rows.append(row)
        states.append(dict(pre=OLD._ns(c['last_pre_gap_trade_sip']), post=OLD._ns(c['first_post_gap_trade_sip']),
                           start=OLD._ns(c['inferred_halt_interval_start']),
                           session=OLD.session_bounds_ns(str(c['session_date']))[0],
                           minutes=set(), writer=None, path=path, closed=False))
    previous_key = None
    signature = None
    consumed = 0
    try:
        for batch in pq.ParquetFile(quote_path).iter_batches(batch_size=config.batch_size,
                       columns=list(MARKET.QUOTE_COLUMNS), use_threads=False):
            table = pa.Table.from_batches([batch])
            if not table.num_rows:
                continue
            if table['sip_timestamp'].null_count or table['sequence_number'].null_count:
                raise ValueError('null quote event key')
            sip = table['sip_timestamp'].to_numpy()
            seq = table['sequence_number'].to_numpy()
            first_key = (int(sip[0]), int(seq[0]))
            if (previous_key is not None and first_key < previous_key) or np.any(sip[1:] < sip[:-1]) or np.any((sip[1:] == sip[:-1]) & (seq[1:] < seq[:-1])):
                raise ValueError('quote order decreases')
            # Identical keys are rejected by the feature pipeline even where the
            # legacy quote audit only counted them. Fail closed before publication.
            if previous_key == first_key or np.any((sip[1:] == sip[:-1]) & (seq[1:] == seq[:-1])):
                raise ValueError('duplicate quote key')
            previous_key = (int(sip[-1]), int(seq[-1]))
            sem = MARKET._quote_batch_semantics(table)
            change, signature = MARKET._state_change_mask(sem, signature)
            consumed += batch.num_rows
            for row, st in zip(rows, states):
                gap = (sip > st['pre']) & (sip < st['post'])
                transition = change & gap
                row['quote_message_count_in_gap'] += int(gap.sum())
                st['minutes'].update(((sip[gap]-st['session'])//(60*NS)).tolist())
                row['semantic_quote_state_change_count_in_gap'] += int(transition.sum())
                row['valid_quote_message_count_in_gap'] += int((gap & sem['price_state_valid']).sum())
                row['invalid_or_one_sided_transition_count'] += int((transition & (sem['one_sided'] | sem['condition_invalid'] | ~sem['price_state_valid'])).sum())
                row['locked_transition_count'] += int((transition & sem['locked']).sum())
                row['crossed_transition_count'] += int((transition & sem['crossed']).sum())
                pre = np.flatnonzero((sip >= st['start']-60*NS) & (sip < st['start']) & sem['price_state_valid'])
                post = np.flatnonzero((sip >= st['post']) & (sip < st['post']+60*NS) & sem['price_state_valid'])
                for positions, prefix, take in ((pre, 'last_pre_gap', -1), (post, 'first_post_gap', 0)):
                    if positions.size and (take == -1 or row[prefix+'_valid_midpoint'] is None):
                        i = positions[take]
                        row[prefix+'_valid_midpoint'] = float(sem['midpoint'][i])
                        spread = float(sem['spread_bps'][i])
                        row[prefix+'_spread_bps'] = spread if math.isfinite(spread) else None
                resume = int(np.searchsorted(sip, st['post'], side='left'))
                if row['quote_resume_time'] is None and resume < len(sip):
                    row['quote_resume_time'] = _utc(int(sip[resume]))
                review = (sip >= st['start']-300*NS) & (sip < st['post']+300*NS)
                if review.any():
                    if st['writer'] is None:
                        st['writer'] = pq.ParquetWriter(st['path'], schema, compression='zstd')
                    mid = sem['midpoint'][review]
                    st['writer'].write_table(pa.Table.from_arrays([
                        pa.array(sip[review]), pa.array(mid, mask=~np.isfinite(mid)),
                        pa.array(sem['price_state_valid'][review])], schema=schema))
                if sip[-1] >= st['post']+300*NS and st['writer'] is not None and not st['closed']:
                    st['writer'].close(); st['closed'] = True
        for row, st in zip(rows, states):
            if st['writer'] is None:
                st['writer'] = pq.ParquetWriter(st['path'], schema, compression='zstd')
            if not st['closed']:
                st['writer'].close(); st['closed'] = True
            row.update(quote_active_minute_count_in_gap=len(st['minutes']),
                       same_symbol_quote_stream_observed_in_gap=row['quote_message_count_in_gap'] > 0,
                       review_quote_trace_path=str(st['path'].resolve()),
                       review_quote_trace_sha256=sha256_file(st['path']))
    finally:
        for st in states:
            if st['writer'] is not None and not st['closed']:
                st['writer'].close()
    return rows, consumed
