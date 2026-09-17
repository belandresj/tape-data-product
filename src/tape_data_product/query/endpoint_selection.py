"""Explicit endpoint/session/population selection for endpoint/EW references."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

from ..contracts.config import ContractError

NS = 1_000_000_000
_ET = ZoneInfo("America/New_York")
SESSIONS = ("premarket", "rth", "after_hours")
POPULATION_TIMING_MODES = (
    "historical_membership",
    "nominal_post_discovery",
    "receipt_post_discovery",
)


@dataclass(frozen=True)
class EndpointSelection:
    """Saved selection semantics; every population timing choice is explicit."""

    population_timing_mode: str
    sessions: tuple[str, ...] = SESSIONS
    members: tuple[str, ...] | None = None
    dates: tuple[str, ...] | None = None
    endpoint_start_ns: int | None = None
    endpoint_stop_ns: int | None = None

    def __post_init__(self):
        if self.population_timing_mode not in POPULATION_TIMING_MODES:
            raise ContractError("unknown population timing mode")
        if not isinstance(self.sessions, tuple) or not self.sessions:
            raise ContractError("sessions must be a nonempty tuple")
        if len(set(self.sessions)) != len(self.sessions) or any(
            value not in SESSIONS for value in self.sessions
        ):
            raise ContractError("unknown or duplicate session")
        for name, values in (("members", self.members), ("dates", self.dates)):
            if values is not None and (
                not isinstance(values, tuple)
                or not values
                or len(set(values)) != len(values)
                or any(not isinstance(value, str) or not value for value in values)
            ):
                raise ContractError(f"{name} must be a nonempty unique tuple")
        for name in ("endpoint_start_ns", "endpoint_stop_ns"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise ContractError(f"{name} must be a nonnegative integer")
        if (
            self.endpoint_start_ns is not None
            and self.endpoint_stop_ns is not None
            and self.endpoint_start_ns >= self.endpoint_stop_ns
        ):
            raise ContractError("endpoint bounds are reversed or empty")

    def to_dict(self):
        return {
            "version": "endpoint_selection_v1",
            "population_timing_mode": self.population_timing_mode,
            "sessions": list(self.sessions),
            "members": None if self.members is None else list(self.members),
            "dates": None if self.dates is None else list(self.dates),
            "endpoint_start_ns": self.endpoint_start_ns,
            "endpoint_stop_ns": self.endpoint_stop_ns,
        }

    @classmethod
    def from_dict(cls, value):
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "version",
                "population_timing_mode",
                "sessions",
                "members",
                "dates",
                "endpoint_start_ns",
                "endpoint_stop_ns",
            }
            or value["version"] != "endpoint_selection_v1"
        ):
            raise ContractError("invalid endpoint selection")
        return cls(
            population_timing_mode=value["population_timing_mode"],
            sessions=tuple(value["sessions"]),
            members=None if value["members"] is None else tuple(value["members"]),
            dates=None if value["dates"] is None else tuple(value["dates"]),
            endpoint_start_ns=value["endpoint_start_ns"],
            endpoint_stop_ns=value["endpoint_stop_ns"],
        )


def _clock_ns(day: str, hour: int, minute: int = 0) -> int:
    parsed = date.fromisoformat(day)
    local = datetime.combine(parsed, time(hour, minute), _ET)
    return int(local.astimezone(timezone.utc).timestamp()) * NS


def session_ranges(day: str, sessions: tuple[str, ...]):
    """Return inclusive endpoint ranges for the stored reporting strata."""
    boundaries = {
        "premarket": (_clock_ns(day, 4) + NS, _clock_ns(day, 9, 30)),
        "rth": (_clock_ns(day, 9, 30) + NS, _clock_ns(day, 16)),
        "after_hours": (_clock_ns(day, 16) + NS, _clock_ns(day, 20)),
    }
    return tuple(boundaries[name] for name in SESSIONS if name in sessions)


def selected_ranges(day: str, selection: EndpointSelection, discovery: dict):
    ranges = list(session_ranges(day, selection.sessions))
    if selection.population_timing_mode != "historical_membership":
        nominal = discovery.get("nominal_discovery_endpoint_ns")
        if type(nominal) is not int:
            raise ContractError("nominal discovery endpoint unavailable")
        if not discovery.get("nominal_provenance"):
            raise ContractError("nominal discovery provenance unavailable")
        threshold = nominal
        if selection.population_timing_mode == "receipt_post_discovery":
            receipt = discovery.get("receipt_known_at_ns")
            if type(receipt) is not int:
                raise ContractError("discovery receipt clock unavailable")
            if not discovery.get("receipt_provenance"):
                raise ContractError("receipt discovery provenance unavailable")
            threshold = max(threshold, receipt)
        ranges = [(max(start, threshold + 1), stop) for start, stop in ranges]
    if selection.endpoint_start_ns is not None:
        ranges = [
            (max(start, selection.endpoint_start_ns), stop) for start, stop in ranges
        ]
    if selection.endpoint_stop_ns is not None:
        ranges = [
            (start, min(stop, selection.endpoint_stop_ns - 1))
            for start, stop in ranges
        ]
    aligned = []
    for start, stop in ranges:
        start = ((start + NS - 1) // NS) * NS
        stop = (stop // NS) * NS
        if start <= stop:
            aligned.append((start, stop))
    return tuple(aligned)
