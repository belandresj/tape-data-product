"""Bounded causal cohort/hysteresis state machine; no I/O or network."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from zoneinfo import ZoneInfo

from tape_data_product.features import compact_product_schema as schema
from tape_data_product.query.tape_cohort_config import (
    FEATURE_INDEX,
    normalize_config,
    query_hash,
)

NS = 1_000_000_000
NY = ZoneInfo("America/New_York")
KNOWN_MASK = sum(schema.REASONS.values())
PRIMARY_BITS = (
    (2, "source_unaccepted"),
    (4, "immature"),
    (8, "insufficient_support"),
    (32, "carried_history"),
    (64, "nonpositive_spread"),
    (1, "undefined"),
    (128, "zero_total_movement"),
)


def _emit(sinks, name, row):
    if sinks is None:
        return
    sink = sinks.get(name) if isinstance(sinks, dict) else getattr(sinks, name, None)
    if sink is None:
        return
    sink(row) if callable(sink) else sink.append(row)


def _passes(value, bounds):
    low, high = bounds["lower"], bounds["upper"]
    if low is not None and (
        value < low or (value == low and not bounds["lower_inclusive"])
    ):
        return False
    if high is not None and (
        value > high or (value == high and not bounds["upper_inclusive"])
    ):
        return False
    return True


def _local_parts(t):
    dt = datetime.fromtimestamp(t / NS, timezone.utc).astimezone(NY)
    return dt.hour * 3600 + dt.minute * 60 + dt.second


def source_stratum(t):
    second = (_local_parts(t) - 1) % 86400
    return "premarket" if second < 34200 else "rth" if second < 57600 else "after_hours"


def decision_stratum(t):
    second = _local_parts(t)
    return "premarket" if second < 34200 else "rth" if second < 57600 else "after_hours"


class Compensated:
    __slots__ = ("total", "correction")

    def __init__(self):
        self.total = self.correction = 0.0

    def add(self, value):
        adjusted = value - self.correction
        updated = self.total + adjusted
        self.correction = (updated - self.total) - adjusted
        self.total = updated


@dataclass
class FeatureAccumulator:
    count: int = 0
    first: float | None = None
    last: float | None = None
    minimum: float = math.inf
    maximum: float = -math.inf

    def __post_init__(self):
        self.sum = Compensated()

    def add(self, value):
        if self.count == 0:
            self.first = value
        self.count += 1
        self.last = value
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)
        self.sum.add(value)


class StrictRunReducer:
    def __init__(self, machine):
        self.machine = machine
        self.current = None
        self.ordinal = 0
        self.preceding = "session_start"

    def consume(self, row, strict, break_cause, sinks):
        t = row["interval_end_ns"]
        if break_cause is not None:
            self.close(break_cause, sinks)
            self.preceding = break_cause
        if strict:
            if self.current is None:
                self.current = dict(
                    first=t,
                    last=t,
                    count=1,
                    continuity_segment_id=row["continuity_segment_id"],
                    preceding=self.preceding,
                )
            else:
                self.current["last"] = t
                self.current["count"] += 1
        else:
            cause = break_cause or "strict_nonpass"
            self.close(cause, sinks)
            self.preceding = cause

    def close(self, cause, sinks, right_censored=False):
        if self.current is None:
            return
        m, current = self.machine, self.current
        identity = hashlib.sha256(
            f"{m.qhash}:{m.partition_identity}:{current['first']}:{self.ordinal}".encode()
        ).hexdigest()
        _emit(
            sinks,
            "strict_runs",
            dict(
                strict_run_id=identity,
                partition_identity=m.partition_identity,
                session_date=m.session_date,
                symbol=m.symbol,
                continuity_segment_id=current["continuity_segment_id"],
                first_endpoint_ns=current["first"],
                last_endpoint_ns=current["last"],
                endpoint_count=current["count"],
                preceding_cause=current["preceding"],
                following_cause=cause,
                left_censored=current["preceding"] == "selection_boundary",
                right_censored=right_censored,
            ),
        )
        m.counters["strict_run_count"] += 1
        self.ordinal += 1
        self.current = None


class CohortMachine:
    """Consumes validated row dictionaries and retains O(K) partition state."""

    def __init__(
        self,
        config,
        partition_identity,
        *,
        session_date="fixture",
        symbol="fixture",
        checkpoint=False,
        emit_trace=True,
    ):
        self.config = normalize_config(config)
        self.qhash = query_hash(self.config)
        self.conditions = self.config["conditions"]
        self.features = [c["feature"] for c in self.conditions]
        self.entry_n = max(1, self.config["entry_confirm_seconds"])
        self.exit_n = max(1, self.config["exit_confirm_seconds"])
        self.partition_identity = partition_identity
        self.session_date = session_date
        self.symbol = symbol
        self.checkpoint = checkpoint
        self.state = "OUT"
        self.previous_t = None
        self.previous_continuity = None
        self.emit_trace = emit_trace
        self.candidate_start = None
        self.entry_count = 0
        self.exit_count = 0
        self.exit_trigger = None
        self.trigger_failed = 0
        self.ordinal = 0
        self.window = None
        self.strict_reducer = StrictRunReducer(self)
        self.counters = {
            k: 0
            for k in (
                "observed_endpoints",
                "observed_available",
                "observed_unavailable",
                "observed_strict",
                "decision_slot_available",
                "window_count",
                "strict_run_count",
                "active_seconds",
                "premarket_active_seconds",
                "rth_active_seconds",
                "after_hours_active_seconds",
                "active_strict_seconds",
                "active_continuation_seconds",
                "active_pending_exit_seconds",
                "entry_candidates_started",
                "entry_candidates_confirmed",
                "entry_candidates_cancelled_economic",
                "entry_candidates_cancelled_boundary",
            )
        }
        self.boundary_cancellations = {}

    def _classify(self, row):
        unavailable = 0
        union = 0
        primary = None
        if not row["post_discovery_eligible"]:
            primary = "pre_discovery"
        for feature in self.features:
            mask = row[feature + "_reason_mask"]
            if type(mask) is not int or mask < 0 or mask & ~KNOWN_MASK:
                raise ValueError("null, noninteger or unknown reason mask")
            union |= mask
            value = row[feature]
            if value is not None and (
                not isinstance(value, (int, float)) or not math.isfinite(value)
            ):
                raise ValueError("stored nonfinite feature")
            if mask == 0:
                if value is None or value < 0:
                    raise ValueError("zero mask requires finite in-domain value")
                if feature.startswith("movement_participation_") and value > 1:
                    raise ValueError("participation outside [0,1]")
            if mask != 0 or not row["post_discovery_eligible"]:
                unavailable |= 1 << FEATURE_INDEX[feature]
                if primary is None:
                    for bit, name in PRIMARY_BITS:
                        if mask & bit:
                            primary = name
                            break
        available = unavailable == 0
        strict_failed = continuation_failed = 0
        if available:
            for condition in self.conditions:
                bit = 1 << FEATURE_INDEX[condition["feature"]]
                value = row[condition["feature"]]
                if not _passes(value, condition["entry"]):
                    strict_failed |= bit
                if not _passes(value, condition["continuation"]):
                    continuation_failed |= bit
            if strict_failed == 0 and continuation_failed != 0:
                raise AssertionError("strict must imply continuation")
        return (
            available,
            strict_failed == 0 and available,
            continuation_failed == 0 and available,
            unavailable,
            union,
            primary,
            strict_failed,
            continuation_failed,
        )

    def _cancel_candidate(self, cause, boundary=False):
        if self.state != "PENDING_ENTRY":
            return
        key = (
            "entry_candidates_cancelled_boundary"
            if boundary
            else "entry_candidates_cancelled_economic"
        )
        self.counters[key] += 1
        if boundary:
            self.boundary_cancellations[cause] = (
                self.boundary_cancellations.get(cause, 0) + 1
            )
        self.candidate_start = None
        self.entry_count = 0
        self.state = "OUT"

    def _open(self, row):
        t = row["interval_end_ns"]
        wid = hashlib.sha256(
            f"{self.qhash}:{self.partition_identity}:{t}:{self.ordinal}".encode()
        ).hexdigest()
        self.window = dict(
            candidate_start_endpoint_ns=self.candidate_start or t,
            entry_confirmed_at_ns=t,
            continuity_segment_id=row["continuity_segment_id"],
            window_id=wid,
            last_member_endpoint_ns=None,
            strict_pass_count=0,
            continuation_pass_count=0,
            pending_exit_count=0,
            strata={"premarket": 0, "rth": 0, "after_hours": 0},
            features={f: FeatureAccumulator() for f in self.features},
        )
        self.counters["entry_candidates_confirmed"] += 1
        self.state = "ACTIVE"

    def _member(self, row, strict, continuation):
        w = self.window
        t = row["interval_end_ns"]
        w["last_member_endpoint_ns"] = t
        if strict:
            w["strict_pass_count"] += 1
            self.counters["active_strict_seconds"] += 1
        if continuation:
            w["continuation_pass_count"] += 1
            self.counters["active_continuation_seconds"] += 1
        else:
            w["pending_exit_count"] += 1
            self.counters["active_pending_exit_seconds"] += 1
        stratum = decision_stratum(t)
        w["strata"][stratum] += 1
        self.counters[stratum + "_active_seconds"] += 1
        for f in self.features:
            w["features"][f].add(float(row[f]))

    def _close(
        self,
        t,
        reason,
        sinks,
        *,
        causes=(),
        confirmed=None,
        exit_failed=0,
        unavailable=0,
        union=0,
        masks=None,
        right_censored=False,
    ):
        if self.state == "PENDING_ENTRY":
            self._cancel_candidate(reason, boundary=True)
        if self.window is None:
            self.state = "OUT"
            self.exit_count = 0
            self.exit_trigger = None
            self.trigger_failed = 0
            return
        w = self.window
        count = (t - w["entry_confirmed_at_ns"]) // NS
        if count <= 0 or count != sum(a.count for a in w["features"].values()) // len(
            self.features
        ):
            raise AssertionError("window duration/member invariant")
        wid = w["window_id"]
        row = dict(
            window_id=wid,
            partition_identity=self.partition_identity,
            session_date=self.session_date,
            symbol=self.symbol,
            window_ordinal=self.ordinal,
            continuity_segment_id=w["continuity_segment_id"],
            candidate_start_endpoint_ns=w["candidate_start_endpoint_ns"],
            entry_confirmed_at_ns=w["entry_confirmed_at_ns"],
            last_member_endpoint_ns=w["last_member_endpoint_ns"],
            exit_effective_at_ns=t,
            exit_trigger_endpoint_ns=self.exit_trigger,
            exit_confirmed_at_ns=confirmed,
            entry_confirmation_count=self.entry_n,
            exit_confirmation_count=self.exit_count,
            member_endpoint_count=count,
            active_seconds=count,
            strict_pass_count=w["strict_pass_count"],
            continuation_pass_count=w["continuation_pass_count"],
            pending_exit_count=w["pending_exit_count"],
            entry_reason="strict_confirmed",
            exit_reason=reason,
            boundary_causes_json=json.dumps(list(causes), separators=(",", ":")),
            trigger_failed_features=self.trigger_failed,
            exit_failed_features=exit_failed,
            exit_unavailable_features=unavailable,
            exit_reason_union_mask=union,
            exit_field_reason_masks_json=json.dumps(
                masks or {}, sort_keys=True, separators=(",", ":")
            ),
            left_censored=False,
            right_censored=right_censored,
            censor_causes_json=json.dumps(
                [reason] if right_censored else [], separators=(",", ":")
            ),
            premarket_active_seconds=w["strata"]["premarket"],
            rth_active_seconds=w["strata"]["rth"],
            after_hours_active_seconds=w["strata"]["after_hours"],
            crosses_session_boundary=sum(v > 0 for v in w["strata"].values()) > 1,
        )
        if (
            w["last_member_endpoint_ns"] != t - NS
            or count != w["continuation_pass_count"] + w["pending_exit_count"]
        ):
            raise AssertionError("window endpoint/count invariant")
        _emit(sinks, "windows", row)
        for feature, acc in w["features"].items():
            _emit(
                sinks,
                "window_features",
                dict(
                    window_id=wid,
                    feature=feature,
                    count=acc.count,
                    first=acc.first,
                    last=acc.last,
                    minimum=acc.minimum,
                    maximum=acc.maximum,
                    mean=acc.sum.total / acc.count,
                ),
            )
        self.counters["window_count"] += 1
        self.counters["active_seconds"] += count
        self.ordinal += 1
        self.window = None
        self.state = "OUT"
        self.exit_count = 0
        self.exit_trigger = None
        self.trigger_failed = 0
        self.candidate_start = None
        self.entry_count = 0

    def consume_row(self, row, sinks=None):
        required = {
            "interval_end_ns",
            "continuity_segment_id",
            "halt_interval_active",
            "post_discovery_eligible",
            *self.features,
            *(f + "_reason_mask" for f in self.features),
        }
        if not required <= set(row):
            raise ValueError("row is missing query fields")
        t = row["interval_end_ns"]
        if type(t) is not int:
            raise ValueError("endpoint must be int64 nanoseconds")
        before = self.state
        available, strict, continuation, unavailable, union, primary, sfail, cfail = (
            self._classify(row)
        )
        self.counters["observed_endpoints"] += 1
        self.counters[
            "observed_available" if available else "observed_unavailable"
        ] += 1
        if strict:
            self.counters["observed_strict"] += 1
        boundary = []
        second = _local_parts(t)
        if second == 72000:
            if self.previous_t is not None and t <= self.previous_t:
                raise ValueError("non-increasing/duplicate endpoint")
            if self.previous_t is not None and t != self.previous_t + NS:
                raise ValueError("clock gap at session close")
            self.previous_t = t
            self.previous_continuity = row["continuity_segment_id"]
            boundary.append("session_close")
            self._close(t, "session_close", sinks, causes=boundary)
            self.strict_reducer.consume(
                row, strict, None if strict else "strict_nonpass", sinks
            )
            return self._trace(
                row,
                before,
                available,
                strict,
                continuation,
                unavailable,
                union,
                sfail,
                cfail,
                boundary,
                False,
            )
        if self.previous_t is not None:
            if t <= self.previous_t:
                raise ValueError("non-increasing/duplicate endpoint")
            if t != self.previous_t + NS:
                boundary.append("clock_gap")
                self._close(
                    self.previous_t + NS,
                    "clock_gap",
                    sinks,
                    causes=boundary,
                    right_censored=True,
                )
            if row["continuity_segment_id"] != self.previous_continuity:
                boundary.append("continuity_change")
        self.previous_t = t
        self.previous_continuity = row["continuity_segment_id"]
        if row["halt_interval_active"]:
            boundary.append("active_halt")
        elif not available:
            reason = "unavailable:" + (primary or "unknown")
            boundary.append(reason)
        current_causes = [cause for cause in boundary if cause != "clock_gap"]
        if current_causes:
            self._close(
                t,
                current_causes[0],
                sinks,
                causes=current_causes,
                unavailable=unavailable,
                union=union,
                masks={f: row[f + "_reason_mask"] for f in self.features},
            )
        if not row["halt_interval_active"] and available:
            self.counters["decision_slot_available"] += 1
            if self.state == "OUT":
                if strict:
                    self.counters["entry_candidates_started"] += 1
                    self.candidate_start = t
                    self.entry_count = 1
                    if self.entry_n == 1:
                        self._open(row)
                    else:
                        self.state = "PENDING_ENTRY"
            elif self.state == "PENDING_ENTRY":
                if not strict:
                    self._cancel_candidate("strict_nonpass", boundary=False)
                else:
                    self.entry_count += 1
                    if self.entry_count == self.entry_n:
                        self._open(row)
            elif self.state == "ACTIVE":
                if not continuation:
                    self.exit_trigger = t
                    self.trigger_failed = cfail
                    self.exit_count = 1
                    if self.exit_n == 1:
                        self._close(
                            t,
                            "economic_exit",
                            sinks,
                            confirmed=t,
                            exit_failed=cfail,
                            union=union,
                            masks={f: row[f + "_reason_mask"] for f in self.features},
                        )
                    else:
                        self.state = "PENDING_EXIT"
            elif self.state == "PENDING_EXIT":
                if continuation:
                    self.state = "ACTIVE"
                    self.exit_count = 0
                    self.exit_trigger = None
                    self.trigger_failed = 0
                else:
                    self.exit_count += 1
                    if self.exit_count == self.exit_n:
                        self._close(
                            t,
                            "economic_exit",
                            sinks,
                            confirmed=t,
                            exit_failed=cfail,
                            union=union,
                            masks={f: row[f + "_reason_mask"] for f in self.features},
                        )
            if self.state in ("ACTIVE", "PENDING_EXIT"):
                self._member(row, strict, continuation)
        strict_break = (
            boundary[0] if boundary else (None if strict else "strict_nonpass")
        )
        self.strict_reducer.consume(row, strict, strict_break, sinks)
        return self._trace(
            row,
            before,
            available,
            strict,
            continuation,
            unavailable,
            union,
            sfail,
            cfail,
            boundary,
            self.state in ("ACTIVE", "PENDING_EXIT"),
        )

    def _trace(
        self,
        row,
        before,
        available,
        strict,
        continuation,
        unavailable,
        union,
        sfail,
        cfail,
        causes,
        member,
    ):
        # Bulk retrieval consumes only the sinks/counters; diagnostic traces are opt-in there.
        if not self.emit_trace:
            return None
        return dict(
            **row,
            available=available,
            strict=strict,
            continuation=continuation,
            source_stratum=source_stratum(row["interval_end_ns"]),
            decision_stratum=decision_stratum(row["interval_end_ns"]),
            state_before=before,
            state_after=self.state,
            entry_count=self.entry_count,
            exit_count=self.exit_count,
            candidate_start_endpoint_ns=self.candidate_start,
            pending_exit_trigger_endpoint_ns=self.exit_trigger,
            window_id=(
                self.window["window_id"] if member and self.window is not None else None
            ),
            strict_failed_features=sfail,
            continuation_failed_features=cfail,
            unavailable_features=unavailable,
            reason_union_mask=union,
            boundary_causes_json=json.dumps(causes, separators=(",", ":")),
            is_member=member,
        )

    def consume(self, batch, sinks=None):
        rows = batch if isinstance(batch, list) else batch.to_pylist()
        for row in rows:
            yield self.consume_row(row, sinks)

    def finish(self, boundary="session_close", sinks=None):
        if boundary == "selection_boundary" and self.previous_t is not None:
            t = min(self.previous_t + NS, self.previous_t + NS)
            self._close(t, boundary, sinks, causes=[boundary], right_censored=True)
            self.strict_reducer.close(boundary, sinks, right_censored=True)
        else:
            if self.window is not None or self.state == "PENDING_ENTRY":
                if self.previous_t is None:
                    raise ValueError("cannot finish empty active partition")
                self._close(self.previous_t + NS, boundary, sinks, causes=[boundary])
            self.strict_reducer.close(boundary, sinks)
        return dict(
            self.counters,
            entry_boundary_cancellations_json=json.dumps(
                self.boundary_cancellations, sort_keys=True
            ),
            completion_state="checkpoint" if self.checkpoint else "complete",
        )
