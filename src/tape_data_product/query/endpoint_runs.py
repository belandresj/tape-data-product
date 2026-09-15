"""O(1)-state strict-run reduction over selected endpoint observations."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Mapping


NS = 1_000_000_000


@dataclass(frozen=True)
class RunBoundary:
    reason: str
    censored: bool = False


@dataclass(frozen=True)
class StrictRun:
    run_id: str
    session_date: str
    symbol: str
    selection_segment_id: str
    first_endpoint_ns: int
    last_endpoint_ns: int
    match_count: int
    represented_start_ns: int
    represented_end_ns: int
    represented_duration_seconds: int
    endpoint_elapsed_seconds: int
    preceding_reason: str
    closure_reason: str
    left_censored: bool
    right_censored: bool
    source_continuity_json: str

    def to_dict(self) -> dict:
        return self.__dict__.copy()


class StrictRunReducer:
    """Consumes every selected row; false and unavailable rows close runs."""

    def __init__(self, query_identity: str, required_sources: tuple[str, ...]):
        if not isinstance(query_identity, str) or not query_identity:
            raise ValueError("query identity is required")
        if any(source not in ("quote", "trade") for source in required_sources):
            raise ValueError("unknown required source")
        self.query_identity = query_identity
        self.required_sources = tuple(sorted(set(required_sources)))
        self._current = None
        self._previous = None
        self._preceding = RunBoundary("member_boundary", False)
        self._ordinal = 0
        self.runs: list[StrictRun] = []

    def _continuity(self, row: Mapping[str, object]) -> dict[str, int]:
        result = {}
        for source in self.required_sources:
            value = row.get(f"{source}_continuity_id")
            break_value = row.get(f"{source}_continuity_break_in_second")
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"invalid {source} continuity id")
            if type(break_value) is not bool:
                raise ValueError(f"invalid {source} continuity break flag")
            result[source] = value
        return result

    def _close(self, boundary: RunBoundary) -> StrictRun | None:
        if self._current is None:
            self._preceding = boundary
            return None
        current = self._current
        payload = (
            f"{self.query_identity}|{current['session_date']}|{current['symbol']}|"
            f"{current['segment']}|{current['first']}|{self._ordinal}"
        ).encode()
        run = StrictRun(
            run_id=hashlib.sha256(payload).hexdigest(),
            session_date=current["session_date"],
            symbol=current["symbol"],
            selection_segment_id=current["segment"],
            first_endpoint_ns=current["first"],
            last_endpoint_ns=current["last"],
            match_count=current["count"],
            represented_start_ns=current["first"] - NS,
            represented_end_ns=current["last"],
            represented_duration_seconds=current["count"],
            endpoint_elapsed_seconds=(current["last"] - current["first"]) // NS,
            preceding_reason=current["preceding"].reason,
            closure_reason=boundary.reason,
            left_censored=current["preceding"].censored,
            right_censored=boundary.censored,
            source_continuity_json=json.dumps(
                current["continuity"], sort_keys=True, separators=(",", ":")
            ),
        )
        self.runs.append(run)
        self._ordinal += 1
        self._current = None
        self._preceding = boundary
        return run

    def consume(
        self,
        row: Mapping[str, object],
        *,
        status: str,
        boundary_before: RunBoundary | None = None,
        boundary_after: RunBoundary | None = None,
    ) -> tuple[StrictRun, ...]:
        if status not in ("matching", "nonmatching", "unavailable"):
            raise ValueError("unknown predicate status")
        try:
            day, symbol, segment, endpoint = (
                row["session_date"],
                row["symbol"],
                row["selection_segment_id"],
                row["interval_end_ns"],
            )
        except KeyError as error:
            raise ValueError("row lacks run key") from error
        if (
            not isinstance(day, str)
            or not isinstance(symbol, str)
            or not isinstance(segment, str)
            or isinstance(endpoint, bool)
            or not isinstance(endpoint, int)
        ):
            raise ValueError("malformed run key")
        emitted = []
        continuity = self._continuity(row)
        previous = self._previous
        if previous is not None:
            changed_member = (day, symbol) != previous["member"]
            changed_segment = segment != previous["segment"]
            if changed_member:
                emitted_run = self._close(RunBoundary("member_boundary", False))
                if emitted_run:
                    emitted.append(emitted_run)
                self._preceding = boundary_before or RunBoundary("member_boundary", False)
            elif changed_segment:
                emitted_run = self._close(RunBoundary("selection_boundary", True))
                if emitted_run:
                    emitted.append(emitted_run)
                self._preceding = boundary_before or RunBoundary("selection_boundary", True)
            elif endpoint != previous["endpoint"] + NS:
                raise ValueError("nonconsecutive endpoint inside selection segment")
            else:
                source_break = any(
                    continuity[source] != previous["continuity"][source]
                    or row[f"{source}_continuity_break_in_second"]
                    for source in self.required_sources
                )
                if source_break:
                    emitted_run = self._close(RunBoundary("source_continuity", False))
                    if emitted_run:
                        emitted.append(emitted_run)
        if boundary_before is not None:
            emitted_run = self._close(boundary_before)
            if emitted_run:
                emitted.append(emitted_run)
        halt = row.get("halt_active")
        if type(halt) is not bool:
            raise ValueError("halt_active must be boolean")
        if halt:
            emitted_run = self._close(RunBoundary("halt", False))
            if emitted_run:
                emitted.append(emitted_run)
        elif status == "matching":
            if self._current is None:
                self._current = {
                    "session_date": day,
                    "symbol": symbol,
                    "segment": segment,
                    "first": endpoint,
                    "last": endpoint,
                    "count": 1,
                    "preceding": self._preceding,
                    "continuity": continuity,
                }
            else:
                self._current["last"] = endpoint
                self._current["count"] += 1
        else:
            reason = "unavailable" if status == "unavailable" else "nonmatching"
            emitted_run = self._close(RunBoundary(reason, False))
            if emitted_run:
                emitted.append(emitted_run)
        if boundary_after is not None:
            emitted_run = self._close(boundary_after)
            if emitted_run:
                emitted.append(emitted_run)
        self._previous = {
            "member": (day, symbol),
            "segment": segment,
            "endpoint": endpoint,
            "continuity": continuity,
        }
        return tuple(emitted)

    def finish(self, boundary: RunBoundary | None = None) -> tuple[StrictRun, ...]:
        before = len(self.runs)
        self._close(boundary or RunBoundary("selection_boundary", True))
        return tuple(self.runs[before:])
