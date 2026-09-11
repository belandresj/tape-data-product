"""Exact September-1 scaling and nearest neighbors with disk-backed populations."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("neighbors_v3_features", ROOT / "src/03_features/economic_tape_state_v3.py")
V3 = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = V3
spec.loader.exec_module(V3)
REFERENCE_DATE = "2026-09-01"
QUERY_DATE = "2026-09-02"
REFERENCE_BLOCK = 4096
QUERY_BLOCK = 16


def session_segment(day, end):
    elapsed = end-V3.session_bounds(day)[0]
    return "premarket" if elapsed <= 19_800*V3.NS else "rth" if elapsed <= 43_200*V3.NS else "after_hours"


def connect(path):
    db = sqlite3.connect(path)
    db.execute("PRAGMA cache_size=-32768")
    db.execute("PRAGMA temp_store=FILE")
    db.execute("PRAGMA mmap_size=0")
    return db


def feature_rows(paths, expected_day, expected_contract=None):
    """Stream eligible raw values, rejecting incompatible or mixed-date panels."""
    for path in sorted(map(Path, paths)):
        pf = pq.ParquetFile(path)
        meta = json.loads((pf.schema_arrow.metadata or {}).get(b"v3_identity", b"{}"))
        if meta.get("version") != V3.VERSION or meta.get("spread_version") != V3.SPREAD_VERSION:
            raise ValueError("incompatible V3 feature artifact")
        if expected_contract and meta.get("contract_hash") != expected_contract:
            raise ValueError("V3 contract hash differs between inputs")
        cols = ["session_date", "symbol", "interval_end_ns", "pilot_base_comparison_eligible", *V3.FEATURES]
        previous = None
        for batch in pf.iter_batches(batch_size=4096, columns=cols, use_threads=False):
            for row in batch.to_pylist():
                if row["session_date"] != expected_day:
                    raise ValueError("wrong date in reference/query population")
                key = (row["symbol"], row["interval_end_ns"])
                if previous is not None and key <= previous:
                    raise ValueError("duplicate/unordered feature key")
                previous = key
                if row["pilot_base_comparison_eligible"] is not True:
                    continue
                values = [row[x] for x in V3.FEATURES]
                if not all(v is not None and math.isfinite(v) and v >= 0 for v in values):
                    raise ValueError("eligible feature vector contains invalid values")
                yield row["symbol"], row["interval_end_ns"], values


def load_population(db, table, paths, day, contract):
    if table not in ("reference", "queries"):
        raise ValueError("invalid population")
    columns = ", ".join(f"x{i} REAL NOT NULL" for i in range(18))
    db.execute(f"CREATE TABLE {table}(symbol TEXT, endpoint INTEGER, priority TEXT, {columns}, PRIMARY KEY(symbol, endpoint))")
    insert = f"INSERT INTO {table} VALUES ({','.join('?' for _ in range(21))})"
    buffer = []
    for symbol, end, values in feature_rows(paths, day, contract):
        if table == "queries" and (end-V3.session_bounds(day)[0]) % (300*V3.NS):
            continue
        priority = hashlib.sha256(f"v3-pilot-anchor-1|{day}|{symbol}|{end}".encode()).hexdigest()
        buffer.append((symbol, end, priority, *values))
        if len(buffer) == 4096:
            db.executemany(insert, buffer)
            db.commit()
            buffer.clear()
    if buffer:
        db.executemany(insert, buffer)
    db.commit()
    return db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def fit_scaler(db, count, contract):
    if count < 2:
        raise ValueError("insufficient eligible September 1 reference rows")
    medians, iqrs = [], []
    for j in range(18):
        # log1p is monotonic: sort raw values externally, then interpolate in log space.
        db.execute(f"CREATE INDEX scaler_order ON reference(x{j})")
        quantiles = []
        for probability in (.25, .5, .75):
            pos = (count-1)*probability
            lower, upper = math.floor(pos), math.ceil(pos)
            values = [db.execute(f"SELECT x{j} FROM reference ORDER BY x{j} LIMIT 1 OFFSET ?", (offset,)).fetchone()[0]
                      for offset in (lower, upper)]
            weight = pos-lower
            quantiles.append(math.log1p(values[0])*(1-weight)+math.log1p(values[1])*weight)
        db.execute("DROP INDEX scaler_order")
        medians.append(quantiles[1])
        iqr = quantiles[2]-quantiles[0]
        if not math.isfinite(iqr) or iqr <= 0:
            raise ValueError(f"zero/nonfinite September 1 IQR: {V3.FEATURES[j]}")
        iqrs.append(iqr)
    return dict(version=V3.VERSION, reference_date=REFERENCE_DATE, features=list(V3.FEATURES),
                units=["1 ms" if "age_p90" in x else "1 bps" if "bps" in x else
                       "1 USD/s" if "dollar_rate" in x else "1 USD" if "notional" in x else "1 event/s" for x in V3.FEATURES],
                unit_values=[1.]*18, median=medians, iqr=iqrs, eligible_reference_rows=count,
                quantile_method="linear", contract_hash=contract, clipping=False)


def transform(x, scaler):
    return (np.log1p(x)-np.asarray(scaler["median"]))/np.asarray(scaler["iqr"])


def decompose(a, b, scaler):
    delta = transform(np.asarray(a), scaler)-transform(np.asarray(b), scaler)
    squared = delta*delta
    contributions = squared*V3.WEIGHTS
    families, offset = {}, 0
    for name, fields in V3.FAMILIES.items():
        families[name] = float(np.sqrt(squared[offset:offset+len(fields)].mean()))
        offset += len(fields)
    return float(np.sqrt(contributions.sum())), families, contributions.tolist()


def nearest(db, queries, scaler, k=5):
    """Exact blocked search; tie order is (distance squared, symbol, endpoint)."""
    if not 1 <= k <= 100 or len(queries) > QUERY_BLOCK:
        raise ValueError("neighbor bounds exceeded")
    qx = np.asarray([row[3:] for row in queries], dtype=np.float64)
    qz = transform(qx, scaler)
    best = [[] for _ in queries]
    cursor = db.execute("SELECT * FROM reference ORDER BY symbol, endpoint")
    while rows := cursor.fetchmany(REFERENCE_BLOCK):
        rx = np.asarray([row[3:] for row in rows], dtype=np.float64)
        rz = transform(rx, scaler)
        # Accumulate dimensions separately; no Q x R x 18 tensor or full pairwise matrix.
        distance = np.zeros((len(queries), len(rows)), dtype=np.float64)
        for j in range(18):
            delta = qz[:, j, None]-rz[None, :, j]
            distance += delta*delta*V3.WEIGHTS[j]
        if not np.isfinite(distance).all():
            raise ValueError("nonfinite transformed distance")
        for i in range(len(queries)):
            # Rows already have stable key order; stable sort resolves exact ties.
            candidates = np.argsort(distance[i], kind="stable")[:k]
            best[i].extend((float(distance[i, n]), rows[n][0], rows[n][1], rows[n][3:]) for n in candidates)
            best[i].sort(key=lambda item: item[:3])
            del best[i][k:]
    for query, matches in zip(queries, best):
        yield query, matches


def run(reference_paths, query_paths, output_dir, *, max_queries=128, k=5):
    """Build immutable review artifacts; temporary population storage stays private."""
    import tempfile
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise FileExistsError(output_dir)
    if not 1 <= max_queries <= 10_000 or not 1 <= k <= 100:
        raise ValueError("invalid query/k bounds")
    if not reference_paths or not query_paths:
        raise ValueError("both reference and query artifacts are required")
    contract = json.loads(pq.ParquetFile(reference_paths[0]).schema_arrow.metadata[b"v3_identity"])["contract_hash"]
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".v3-neighbors-", dir=output_dir.parent) as scratch:
        scratch = Path(scratch)
        db = connect(scratch / "populations.sqlite")
        try:
            references = load_population(db, "reference", reference_paths, REFERENCE_DATE, contract)
            candidates = load_population(db, "queries", query_paths, QUERY_DATE, contract)
            if candidates == 0:
                raise ValueError("no eligible fixed-grid September 2 query anchors")
            scaler = fit_scaler(db, references, contract)
            source_identity = {str(Path(p).resolve()): V3.sha256(p) for p in [*reference_paths, *query_paths]}
            scaler["reference_artifact_sha256"] = {str(Path(p).resolve()): source_identity[str(Path(p).resolve())] for p in reference_paths}
            (scratch / "scaler.json").write_text(json.dumps(scaler, indent=2)+"\n")
            cursor = db.execute("SELECT * FROM queries ORDER BY priority, symbol, endpoint LIMIT ?", (max_queries,))
            match_count = query_count = overlap_count = 0
            symbol_counts = {}
            segment_counts = {}
            db.execute("CREATE TABLE matched_blocks(symbol TEXT, block INTEGER, PRIMARY KEY(symbol,block))")
            with (scratch / "matches.jsonl").open("w") as handle:
                while queries := cursor.fetchmany(QUERY_BLOCK):
                    for query, matches in nearest(db, queries, scaler, k):
                        query_count += 1
                        prior_matches = []
                        for rank, (_, symbol, end, values) in enumerate(matches, 1):
                            overall, families, contributions = decompose(query[3:], values, scaler)
                            overlap = any(s == symbol and abs(t-end) <= 300*V3.NS for s, t in prior_matches)
                            prior_matches.append((symbol, end))
                            overlap_count += overlap
                            symbol_counts[symbol] = symbol_counts.get(symbol, 0)+1
                            segment = session_segment(REFERENCE_DATE,end)
                            segment_counts[segment] = segment_counts.get(segment,0)+1
                            block = (end-V3.session_bounds(REFERENCE_DATE)[0])//(300*V3.NS)
                            db.execute("INSERT OR IGNORE INTO matched_blocks VALUES (?,?)",(symbol,block))
                            row = dict(query_date=QUERY_DATE, query_symbol=query[0], query_interval_end_ns=query[1],
                                       reference_date=REFERENCE_DATE, reference_symbol=symbol, reference_interval_end_ns=end,
                                       rank=rank, D_base=overall, family_distances=families,
                                       weighted_squared_contributions=dict(zip(V3.FEATURES, contributions)),
                                       query_raw=dict(zip(V3.FEATURES, query[3:])), reference_raw=dict(zip(V3.FEATURES, values)),
                                       overlaps_higher_ranked_reference=overlap,
                                       reference_block_5m=block, reference_session_segment=segment,
                                       query_session_segment=session_segment(QUERY_DATE,query[1]))
                            handle.write(json.dumps(row, allow_nan=False)+"\n")
                            match_count += 1
            manifest = dict(version=V3.VERSION, contract_hash=contract, reference_rows=references,
                            eligible_grid_query_candidates=candidates, query_count=query_count, matches=match_count,
                            k=k, max_queries=max_queries, query_grid_seconds=300,
                            query_selection="smallest SHA256(v3-pilot-anchor-1|date|symbol|endpoint)",
                            same_symbol_allowed=True, session_segment_restriction=False,
                            reference_population="all eligible September 1 seconds",
                            tie_order=["distance_squared", "symbol", "interval_end_ns"],
                            overlapping_neighbor_rows=overlap_count, reference_symbol_counts=symbol_counts,
                            reference_session_segment_counts=segment_counts,
                            unique_matched_reference_symbols=len(symbol_counts),
                            unique_matched_reference_5m_blocks=db.execute("SELECT count(*) FROM matched_blocks").fetchone()[0],
                            source_artifact_sha256=source_identity, historical_ex_post_overlay=True,
                            independent_sample_count=None,
                            implementation_sha256=V3.sha256(__file__))
            for filename in ("scaler.json", "matches.jsonl"):
                manifest[f"{filename}_sha256"] = V3.sha256(scratch/filename)
            (scratch/"manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")
        finally:
            db.close()
        publish = scratch/"publish"
        publish.mkdir()
        for name in ("scaler.json", "matches.jsonl", "manifest.json"):
            (scratch/name).rename(publish/name)
        publish.rename(output_dir)
    return manifest
