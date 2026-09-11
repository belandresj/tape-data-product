"""Streaming V3 pilot measurements. No raw-day or output-day arrays.

The V2 bounded horizon containers and canonical market-state batch semantics
are reused; neither V2's raw replay nor its full-panel builder is invoked.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import sys
from bisect import bisect_left, insort
from collections import Counter, deque
from datetime import datetime, time, date
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
NS = 1_000_000_000
VERSION = "economic_tape_state_v3_pilot_2"
SPREAD_VERSION = "joint_valid_unlocked_duration_v3_1"
HORIZONS = (60, 300)


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


MARKET = load_module("economic_v3_market", ROOT / "src/02_preprocessing/build_market_state.py")
V2 = load_module("economic_v3_window", ROOT / "src/03_features/build_rolling_tape_state_v2.py")
FAMILY_BASES = {
    "movement": ("midpoint_movement_bps_per_30s",),
    "displayed_friction": ("quoted_spread_bps",),
    "activity": ("trade_rate", "nbbo_state_change_rate"),
    "event_continuity": ("trade_age_p90", "quote_age_p90"),
    "dollar_capacity": ("dollar_rate",),
    "displayed_capacity": ("mean_displayed_bid_notional", "mean_displayed_ask_notional"),
}
FAMILIES = {f: tuple(f"{b}_{h}s" for b in bases for h in HORIZONS)
            for f, bases in FAMILY_BASES.items()}
FEATURES = tuple(x for cols in FAMILIES.values() for x in cols)
WEIGHTS = np.array([1 / (6 * len(cols)) for cols in FAMILIES.values() for _ in cols])
SIGNATURE = ("bid", "ask", "bid_size", "ask_size", "bid_exchange", "ask_exchange",
             "one_sided", "nonfirm", "closed", "condition_invalid", "locked", "crossed",
             "price_state_valid", "depth_state_valid")
TRADE_COLUMNS = ("sip_timestamp", "sequence_number", "participant_timestamp", "conditions",
                 "correction", "price", "size", "decimal_size")


def session_bounds(day):
    d = date.fromisoformat(day)
    return tuple(int(datetime.combine(d, time(h), ZoneInfo("America/New_York")).timestamp()) * NS
                 for h in (4, 20))


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def contract_identity():
    paths = [Path(__file__), ROOT / "src/02_preprocessing/build_market_state.py",
             ROOT / "src/03_features/build_rolling_tape_state_v2.py",
             ROOT / "docs/rolling_tape/rolling_tape_state_v2_feature_spec.md",
             ROOT / "docs/tape_characterization_v3/tape_characterization_v3_model.md",
             ROOT / "docs/tape_characterization_v3/pilot_implementation_spec.md"]
    hashes = {str(p.relative_to(ROOT)): sha256(p) for p in paths}
    return {"version": VERSION, "spread_version": SPREAD_VERSION, "source_hashes": hashes,
            "contract_hash": hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()}


def events(path, stream, day, batch_size=25_000, stats=None, *, _include_changes=False):
    """Bounded column conversion and signature masks; retain per-event order checks.

    Signature masks compare adjacent raw quotes. The primitive reducer suppresses
    them whenever its own signature resets (session initialization, halt, break).
    """
    if not 1 <= batch_size <= 25_000:
        raise ValueError("raw batch size must be 1..25000")
    pf = pq.ParquetFile(path)
    columns = MARKET.QUOTE_COLUMNS if stream == "quote" else TRADE_COLUMNS
    previous = previous_signature = None
    start, _ = session_bounds(day)
    rth_start, rth_end = start + 19_800 * NS, start + 43_200 * NS
    for batch in pf.iter_batches(batch_size=batch_size, columns=list(columns), use_threads=False):
        table = pa.Table.from_batches([batch])
        for key in ("sip_timestamp", "sequence_number"):
            if table[key].null_count:
                raise ValueError(f"null {stream} event key")
        sip = table["sip_timestamp"].to_numpy()
        seq = table["sequence_number"].to_numpy()
        if stream == "quote":
            sem = MARKET._quote_batch_semantics(table)
            if np.any(sem["unknown_condition"]) or np.any(sem["unknown_indicator"]):
                raise ValueError("unaccepted unknown quote condition/indicator")
            masks = [np.zeros(batch.num_rows, dtype=bool) for _ in range(3)]
            for name in SIGNATURE:
                arr = sem[name]
                changed = np.zeros(batch.num_rows, dtype=bool)
                if batch.num_rows:
                    if previous_signature is not None:
                        changed[0] = MARKET._scalar_changed(arr[0], previous_signature[name])
                    if np.issubdtype(arr.dtype, np.floating):
                        changed[1:] = (arr[1:] != arr[:-1]) & ~(np.isnan(arr[1:]) & np.isnan(arr[:-1]))
                    else:
                        changed[1:] = arr[1:] != arr[:-1]
                masks[0] |= changed
                if name in ("bid", "ask"): masks[1] |= changed
                if name in ("bid_size", "ask_size"): masks[2] |= changed
            if batch.num_rows:
                previous_signature = {name: sem[name][-1].item() for name in SIGNATURE}
            names = (*SIGNATURE, "midpoint", "spread_bps", "_changed", "_price_changed", "_size_changed")
            values = zip(*(sem[name].tolist() for name in names[:-3]), *(mask.tolist() for mask in masks))
        else:
            sem = MARKET.activity_trade_semantics(table)
            if np.any(sem.unknown_trade_condition):
                raise ValueError("unaccepted unknown trade condition")
            if np.any(~np.isin(sem.correction_code, list(MARKET.KNOWN_CORRECTION_CODES))):
                raise ValueError("unaccepted unknown trade correction")
            eligible = np.where((sip >= rth_start) & (sip < rth_end),
                sem.eligible_activity_trade, sem.eligible_extended_hours_activity_trade)
            names = ("eligible", "price", "shares")
            values = zip(eligible.tolist(), table["price"].to_numpy().tolist(), sem.analytic_size.tolist())
        for ts, sequence, value in zip(sip.tolist(), seq.tolist(), values):
            key = (ts, sequence)
            if previous is not None and key <= previous:
                raise ValueError(f"duplicate or unordered {stream} event key")
            previous = key
            if stats is not None:
                stats[f"{stream}_rows_consumed"] += 1
            row = dict(zip(names, value))
            if stream == "quote" and not _include_changes:
                for key in ("_changed", "_price_changed", "_size_changed"): del row[key]
            yield ts, row


def _primitive_events(path, stream, day, batch_size=25_000, stats=None):
    return events(path, stream, day, batch_size, stats, _include_changes=True)


class Cursor:
    def __init__(self, iterator):
        self.iterator = iter(iterator)
        self.current = next(self.iterator, None)

    def pop(self):
        row = self.current
        self.current = next(self.iterator, None)
        return row


class MidpointIntegral:
    """Exact binary64-price x integer-nanosecond integration for one second.

    Sum in units of 2**-1074 dollars-ns; round only the final mean. Splitting
    any price interval into same-price refreshes leaves the result identical.
    O(events) time, O(1) state: <=2128 numerator bits for <=1e9 ns of finite
    binary64 prices. Cache conversion for repeated prices; retain no events.
    """
    def __init__(self):
        self.total = self.duration = 0
        self.price = None
        self.units = 0

    def add(self, price, duration_ns):
        if not isinstance(duration_ns, int) or not 0 <= duration_ns <= NS-self.duration:
            raise ValueError("midpoint integral exceeds one-second duration")
        if not math.isfinite(price) or price <= 0:
            raise ValueError("invalid supported midpoint")
        if price != self.price:
            numerator, denominator = float(price).as_integer_ratio()
            self.units = numerator << (1074-(denominator.bit_length()-1))
            self.price = price
        self.total += self.units * duration_ns
        self.duration += duration_ns

    def mean(self):
        return self.total / (self.duration << 1074) if self.duration else math.nan

    def mass(self):
        return self.total / (NS << 1074)


def primitives(quotes, trades, day, symbol, *, halts=(), seconds=57_600, batch_size=25_000,
               continuity_breaks_ns=(), stats=None, event_reader=None):
    """Yield canonical interval-end primitives; halt-overlapping seconds are closed.

    A partial resume second is closed in full, matching the historical overlay.
    Events in closed seconds never seed the next observable generation.
    """
    if not 1 <= seconds <= 57_600:
        raise ValueError("seconds must be 1..57600")
    start, _ = session_bounds(day)
    breaks = set(continuity_breaks_ns)
    if any((b - start) % NS for b in breaks):
        raise ValueError("source continuity breaks must align to seconds")
    intervals = sorted(halts)
    if any(a >= b for a, b, _ in intervals) or any(intervals[i][0] < intervals[i-1][1] for i in range(1, len(intervals))):
        raise ValueError("invalid or overlapping halt intervals")
    reader = _primitive_events if event_reader is None else event_reader
    q = Cursor(reader(quotes, "quote", day, batch_size, stats))
    tr = Cursor(reader(trades, "trade", day, batch_size, stats))
    current = signature = None
    latest_q = latest_t = None
    while q.current and q.current[0] < start:
        ts, value = q.pop()
        if ts >= start - 300 * NS:
            current, signature, latest_q = value, value, ts
    while tr.current and tr.current[0] < start:
        tr.pop()
    previous_halt = False
    epoch = generation = halt_index = 0
    for position in range(seconds):
        left, right = start + position * NS, start + (position + 1) * NS
        while halt_index < len(intervals) and intervals[halt_index][1] <= left:
            halt_index += 1
        interval = intervals[halt_index] if halt_index < len(intervals) else None
        active = bool(interval and interval[0] < right and interval[1] > left)
        reset = left in breaks
        resume = previous_halt and not active
        if reset and not active and not previous_halt:
            epoch += 1
        if resume:
            generation += 1
        if active or reset or resume:
            current = signature = None
            latest_q = latest_t = None
        masses = dict(midpoint_mass=0., midpoint_duration=0, depth_duration=0,
                      bid_mass=0., ask_mass=0., unlocked_duration=0, spread_mass=0.)
        midpoint_integral = MidpointIntegral()

        def integrate(a, b):
            if current is None or active or b <= a:
                return
            duration = (b - a) / NS
            if current["price_state_valid"]:
                masses["midpoint_duration"] += b-a
                midpoint_integral.add(current["midpoint"], b-a)
                if not current["locked"]:
                    masses["unlocked_duration"] += b-a
                    masses["spread_mass"] += duration * current["spread_bps"]
            if current["depth_state_valid"]:
                masses["depth_duration"] += b-a
                masses["bid_mass"] += duration * current["bid"] * current["bid_size"]
                masses["ask_mass"] += duration * current["ask"] * current["ask_size"]

        changes = prices = sizes = raw = 0
        last = left
        while q.current and q.current[0] < right:
            ts, value = q.pop()
            integrate(last, ts)
            last = ts
            if active:
                continue
            raw += 1
            if signature is not None:
                if "_changed" in value:
                    changes += value["_changed"]
                    prices += value["_price_changed"]
                    sizes += value["_size_changed"]
                else:  # Custom/reference readers retain the original interface.
                    changed = {name: MARKET._scalar_changed(value[name], signature[name]) for name in SIGNATURE}
                    changes += any(changed.values())
                    prices += changed["bid"] or changed["ask"]
                    sizes += changed["bid_size"] or changed["ask_size"]
            current = signature = value
            latest_q = ts
        integrate(last, right)
        masses["midpoint_mass"] = midpoint_integral.mass()
        count, dollars = 0, 0.
        while tr.current and tr.current[0] < right:
            ts, value = tr.pop()
            if not active and value["eligible"]:
                count += 1
                dollars += value["price"] * value["shares"]
                latest_t = ts
        if not all(math.isfinite(v) and v >= 0 for v in masses.values()) or not math.isfinite(dollars):
            raise ValueError("nonfinite/negative primitive accumulation")
        duration_ns = {f"{name}_ns": masses[name] for name in ("midpoint_duration", "depth_duration", "unlocked_duration")}
        for name in ("midpoint_duration", "depth_duration", "unlocked_duration"):
            masses[name] /= NS
        row = dict(session_date=day, symbol=symbol, interval_end_ns=right,
                   continuity_segment_id=epoch, generation=generation,
                   halt_interval_active=active, halt_interval_id=interval[2] if active else None,
                   halt_resume_boundary=resume, historical_ex_post_overlay=True,
                   quote_source_file_accepted=True, trade_source_file_accepted=True,
                   midpoint=midpoint_integral.mean(),
                   quote_age=(right-latest_q)/1e6 if latest_q is not None and current and current["price_state_valid"] else math.nan,
                   trade_age=(right-latest_t)/1e6 if latest_t is not None else math.nan,
                   trade_count=count, dollars=dollars, changes=changes,
                   price_changes=prices, size_changes=sizes, raw_messages=raw, **masses, **duration_ns)
        yield row
        previous_halt = active
    # Full builds validate the entire files, including any out-of-session tail.
    if seconds == 57_600:
        while q.current:
            q.pop()
        while tr.current:
            tr.pop()


class RollingValues:
    """Fixed bounded exact order statistics and compensated nonnegative sums."""
    def __init__(self, capacity):
        self.capacity = capacity
        self.clear()

    def clear(self):
        self.queue = deque()
        self.sorted = []
        self.accumulator = self.compensation = 0.
        self.nonzero = 0

    def add(self, x):
        total = self.accumulator + x
        self.compensation += ((self.accumulator-total)+x if abs(self.accumulator) >= abs(x)
                              else (x-total)+self.accumulator)
        self.accumulator = total

    def append(self, value):
        if len(self.queue) == self.capacity:
            old = self.queue.popleft()
            if math.isfinite(old):
                self.sorted.pop(bisect_left(self.sorted, old))
                self.add(-old)
                self.nonzero -= old != 0
        value = float(value)
        self.queue.append(value)
        if math.isfinite(value):
            if value < 0:
                raise ValueError("rolling values must be nonnegative")
            insort(self.sorted, value)
            self.add(value)
            self.nonzero += value != 0
        if self.nonzero == 0:
            self.accumulator = self.compensation = 0.

    @property
    def total(self):
        return max(0., self.accumulator+self.compensation)

    @property
    def count(self):
        return len(self.sorted)

    def mean(self):
        return self.total/self.count if self.count else math.nan

    def quantile(self, probability):
        if not self.count:
            return math.nan
        pos = (self.count-1)*probability
        lo, hi = math.floor(pos), math.ceil(pos)
        return self.sorted[lo]*(hi-pos)+self.sorted[hi]*(pos-lo) if hi != lo else self.sorted[lo]


class SumValues(RollingValues):
    """Same compensated update order as RollingValues, without unused sorting."""
    def clear(self):
        self.queue = deque()
        self.accumulator = self.compensation = 0.
        self.nonzero = self.finite_count = 0

    def append(self, value):
        if len(self.queue) == self.capacity:
            old = self.queue.popleft()
            if math.isfinite(old):
                self.add(-old)
                self.finite_count -= 1
                self.nonzero -= old != 0
        value = float(value)
        self.queue.append(value)
        if math.isfinite(value):
            if value < 0:
                raise ValueError("rolling values must be nonnegative")
            self.add(value)
            self.finite_count += 1
            self.nonzero += value != 0
        if self.nonzero == 0:
            self.accumulator = self.compensation = 0.

    @property
    def count(self):
        return self.finite_count

    def quantile(self, probability):
        raise TypeError("SumValues does not maintain order statistics")


class HorizonWindow(V2._HorizonWindow):
    def __init__(self, horizon):
        super().__init__(horizon)
        for name in ("move5", "move1", "trade_count", "dollars", "quote_age", "trade_age",
                     "midpoint_duration", "depth_duration", "bid_weight", "ask_weight"):
            setattr(self, name, (RollingValues if name in ("quote_age", "trade_age") else SumValues)(horizon-4 if name == "move5" else horizon))


class FeatureStream:
    """Only fixed-size horizon containers and six endpoint records are retained."""
    def __init__(self):
        self.reset()

    def reset(self):
        self.windows = {h: HorizonWindow(h) for h in HORIZONS}
        self.churn = {h: {k: SumValues(h) for k in ("changes", "price_changes", "size_changes", "raw_messages")}
                      for h in HORIZONS}
        self.cost = {h: {k: SumValues(h) for k in ("spread_mass", "unlocked_duration", "accepted")}
                     for h in HORIZONS}
        self.quality_duration = {h: {k: SumValues(h) for k in ("depth", "midpoint")} for h in HORIZONS}
        self.endpoints = deque(maxlen=6)
        self.epoch = None
        self.previous_end = None
        self.previous_halt = False
        self.post_count = None
        self.last = {}

    def push(self, p):
        if self.previous_end is not None and p["interval_end_ns"] <= self.previous_end:
            raise ValueError("feature rows must be strictly ordered")
        if self.epoch is not None and (p["continuity_segment_id"] != self.epoch or p["interval_end_ns"] != self.previous_end + NS):
            self.reset()
        self.epoch, self.previous_end = p["continuity_segment_id"], p["interval_end_ns"]
        active = p["halt_interval_active"]
        if active or p["halt_resume_boundary"] or self.previous_halt:
            self.endpoints.clear()
            for fields in self.cost.values():
                for field in fields.values():
                    field.clear()
        if p["halt_resume_boundary"] or (self.previous_halt and not active):
            self.post_count = 0
        out = dict(self.last) if active else {}
        out.update({k: p[k] for k in ("session_date", "symbol", "interval_end_ns", "continuity_segment_id",
                                    "halt_interval_active", "halt_interval_id", "historical_ex_post_overlay")})
        out["midpoint"] = p["midpoint"] if not active else math.nan
        if not active:
            if self.post_count is not None:
                self.post_count += 1
            self.endpoints.append((p["midpoint"], p["quote_source_file_accepted"], p["generation"]))
            ends = list(self.endpoints)

            def movement(lag):
                if len(ends) <= lag:
                    return math.nan
                a, b = ends[-lag-1], ends[-1]
                if a[2] != b[2] or not all(x[1] for x in ends[-lag-1:]):
                    return math.nan
                return abs(10_000 * math.log(b[0]/a[0])) if all(math.isfinite(x) and x > 0 for x in (a[0], b[0])) else math.nan

            move5, move1 = movement(5), movement(1)
            for h in HORIZONS:
                w = self.windows[h]
                depth = p["depth_duration"] if p["quote_source_file_accepted"] else 0.
                for kind in ("depth", "midpoint"):
                    value = p.get(f"{kind}_duration_ns", round(p[f"{kind}_duration"]*NS))
                    self.quality_duration[h][kind].append(value if p["quote_source_file_accepted"] else 0)
                depth_total_ns = self.quality_duration[h]["depth"].total
                depth_total = depth_total_ns/NS
                w.append(generation=p["generation"], move5=move5, move1=move1,
                         activity_valid=p["trade_source_file_accepted"], trade_count=p["trade_count"], dollars=p["dollars"],
                         quote_age=p["quote_age"] if p["quote_source_file_accepted"] else math.nan,
                         trade_age=p["trade_age"] if p["trade_source_file_accepted"] else math.nan,
                         midpoint_duration=p["midpoint_duration"] if p["quote_source_file_accepted"] else 0.,
                         depth_duration=depth, bid=p["bid_mass"]/depth if depth else math.nan,
                         ask=p["ask_mass"]/depth if depth else math.nan)
                def put(name, value):
                    out[f"{name}_{h}s"] = value
                count = len(w.generations)
                put("state_observed_second_count", count)
                put("state_mature", count == h)
                put("movement_valid_5s_count", w.move5.count)
                put("movement_valid_1s_count", w.move1.count)
                put("movement_support_valid", count >= math.ceil(.8*h) and w.move5.count >= math.ceil(.8*(h-4)) and w.move1.count >= math.ceil(.8*h))
                put("activity_valid_second_count", w.trade_count.count)
                put("activity_support_valid", w.trade_count.count >= math.ceil(.8*h))
                put("midpoint_movement_bps_per_30s", 6*w.move5.mean())
                put("trade_rate", w.trade_count.mean())
                put("dollar_rate", w.dollars.mean())
                put("displayed_notional_valid_fraction", depth_total_ns/(h*NS))
                put("displayed_notional_support_valid", count >= math.ceil(.8*h) and depth_total_ns*5 >= 4*h*NS)
                put("mean_displayed_bid_notional", w.bid_weight.total/depth_total if depth_total > 0 else math.nan)
                put("mean_displayed_ask_notional", w.ask_weight.total/depth_total if depth_total > 0 else math.nan)
                put("usable_nbbo_fraction", self.quality_duration[h]["midpoint"].total/(h*NS))
                for kind in ("quote", "trade"):
                    age = getattr(w, f"{kind}_age")
                    put(f"{kind}_age_p90", age.quantile(.9))
                    put(f"{kind}_age_observation_count", age.count)
                pre = count-w.generation_counts[p["generation"]] if self.post_count is not None else 0
                put("state_fully_post_halt", self.post_count is None or (count == h and self.post_count >= h+1))
                put("state_pre_halt_observation_fraction", pre/count)
                put("state_contains_pre_halt_history", pre > 0)
                put("state_post_halt_observed_seconds", min(h, self.post_count or 0))
                put("state_carried_forward_during_halt", False)
                for key, name in (("changes", "nbbo_state_change"), ("price_changes", "nbbo_price_change"),
                                  ("size_changes", "nbbo_size_change"), ("raw_messages", "raw_quote_message")):
                    field = self.churn[h][key]
                    field.append(p[key] if p["quote_source_file_accepted"] else math.nan)
                    put(f"{name}_rate", field.mean())
                n = self.churn[h]["changes"].count
                put("nbbo_state_change_observation_count", n)
                put("nbbo_state_change_support_valid", n >= math.ceil(.8*h))
                c = self.cost[h]
                c["spread_mass"].append(p["spread_mass"] if p["quote_source_file_accepted"] else 0.)
                duration_ns = p.get("unlocked_duration_ns", round(p["unlocked_duration"]*NS))
                c["unlocked_duration"].append(duration_ns if p["quote_source_file_accepted"] else 0)
                c["accepted"].append(float(p["quote_source_file_accepted"]))
                structural = len(c["accepted"].queue) == h and c["accepted"].total == h
                duration_ns = c["unlocked_duration"].total
                duration = duration_ns/NS
                put("quoted_spread_valid_fraction", duration_ns/(h*NS) if structural else math.nan)
                put("quoted_spread_valid", structural and duration_ns*10 >= 9*h*NS)
                put("quoted_spread_bps", c["spread_mass"].total/duration if structural and duration > 0 else math.nan)
        else:
            for h in HORIZONS:
                count = len(self.windows[h].generations)
                if count == 0:
                    for name in FEATURES:
                        if name.endswith(f"_{h}s"):
                            out[name] = math.nan
                    for name in ("state_observed_second_count", "movement_valid_5s_count", "movement_valid_1s_count",
                                 "activity_valid_second_count", "quote_age_observation_count", "trade_age_observation_count",
                                 "nbbo_state_change_observation_count"):
                        out[f"{name}_{h}s"] = 0
                    for name in ("state_mature", "movement_support_valid", "activity_support_valid",
                                 "displayed_notional_support_valid", "nbbo_state_change_support_valid"):
                        out[f"{name}_{h}s"] = False
                    for name in ("displayed_notional_valid_fraction", "usable_nbbo_fraction"):
                        out[f"{name}_{h}s"] = 0.
                    for name in ("nbbo_price_change_rate", "nbbo_size_change_rate", "raw_quote_message_rate"):
                        out[f"{name}_{h}s"] = math.nan
                for name in ("quoted_spread_bps", "quoted_spread_valid_fraction"):
                    out[f"{name}_{h}s"] = math.nan
                out[f"quoted_spread_valid_{h}s"] = False
                out[f"state_fully_post_halt_{h}s"] = False
                out[f"state_carried_forward_during_halt_{h}s"] = count > 0
                out[f"state_pre_halt_observation_fraction_{h}s"] = 1. if count else 0.
                out[f"state_contains_pre_halt_history_{h}s"] = count > 0
                out[f"state_post_halt_observed_seconds_{h}s"] = 0
        out["seconds_since_halt_resume"] = self.post_count if not active else None
        gates = ("state_mature", "movement_support_valid", "activity_support_valid", "displayed_notional_support_valid",
                 "nbbo_state_change_support_valid", "quoted_spread_valid", "state_fully_post_halt")
        eligible = not active and all(out.get(f"{name}_{h}s", False) for h in HORIZONS for name in gates)
        eligible &= all(out.get(f"{kind}_age_observation_count_{h}s", 0) >= math.ceil(.8*h) for h in HORIZONS for kind in ("quote", "trade"))
        eligible &= all(math.isfinite(out.get(f, math.nan)) and out[f] >= 0 for f in FEATURES)
        out["pilot_base_comparison_eligible"] = bool(eligible)
        self.previous_halt, self.last = active, dict(out)
        return out


def output_schema():
    # Derive field inventory from one empty accepted primitive, fixing null types.
    p = dict(session_date="2000-01-01", symbol="TEST", interval_end_ns=1, continuity_segment_id=0,
             generation=0, halt_interval_active=False, halt_interval_id=None, historical_ex_post_overlay=True,
             halt_resume_boundary=False, quote_source_file_accepted=True, trade_source_file_accepted=True,
             midpoint=math.nan, quote_age=math.nan, trade_age=math.nan)
    p.update({k: 0. for k in ("trade_count", "dollars", "midpoint_duration", "depth_duration", "bid_mass", "ask_mass",
                            "spread_mass", "unlocked_duration", "changes", "price_changes", "size_changes", "raw_messages")})
    row = FeatureStream().push(p)
    fields = []
    for k, v in row.items():
        t = pa.string() if k in ("session_date", "symbol", "halt_interval_id") else (
            pa.bool_() if isinstance(v, bool) else pa.int64() if isinstance(v, int) or k == "seconds_since_halt_resume" else pa.float64())
        fields.append(pa.field(k, t))
    return pa.schema(fields)


def write_features(quotes, trades, day, symbol, output, *, identity=None, **kwargs):
    """Atomic feature publication; output buffers never exceed 1024 records."""
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".partial")
    identity = identity or contract_identity()
    schema = output_schema().with_metadata({b"v3_identity": json.dumps(identity, sort_keys=True).encode()})
    model, buffer, counts = FeatureStream(), [], Counter()
    stats = Counter()
    try:
        with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
            for p in primitives(quotes, trades, day, symbol, stats=stats, **kwargs):
                row = model.push(p)
                row = {k: None if isinstance(v, float) and not math.isfinite(v) else v for k, v in row.items()}
                counts["rows"] += 1
                counts["eligible_rows"] += row["pilot_base_comparison_eligible"]
                for field in schema.names:
                    if row.get(field) is None:
                        counts[f"null:{field}"] += 1
                buffer.append(row)
                if len(buffer) == 1024:
                    writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
                    buffer.clear()
            if buffer:
                writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
        temporary.rename(output)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return dict(counts) | dict(stats) | {"sha256": sha256(output), "bytes": output.stat().st_size}
