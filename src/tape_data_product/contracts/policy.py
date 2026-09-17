"""Small executable timing/transition rules; no replay or EW calculator."""
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
import math
from zoneinfo import ZoneInfo
from .config import ContractError, DEFAULT_CONFIG, integer

NS = 1_000_000_000
RTH_TRADE_CONDITIONS = (0, 3, 14, 36, 37, 41, 60)
EXTENDED_TRADE_CONDITIONS = (0, 3, 12, 14, 36, 37, 41, 60)
CAUSAL_CORRECTIONS = (0, 7, 8)
KNOWN_CORRECTIONS = (0, 1, 7, 8, 10, 11, 12)


class Event(str, Enum):
    SESSION_START = "session_start"
    SESSION_END = "session_end"
    HALT_ENTER = "halt_enter"
    HALT_RESUME = "halt_resume"
    GAP_ENTER = "gap_enter"
    GAP_RECOVER = "gap_recover"
    INVALID_QUOTE = "invalid_quote"
    VALID_QUOTE_RECOVER = "valid_quote_recover"
    BAD_SIZE = "bad_size"
    LATE_TRADE = "late_trade"
    STRATUM = "reporting_stratum"
    DISCOVERY = "discovery"
    BATCH = "batch"
    COVERAGE_FAILURE = "coverage_failure"


@dataclass(frozen=True)
class Transition:
    ew: str = "retain"
    lag: str = "retain"
    event_origins: str = "retain"
    age_windows: str = "retain"
    source_scope: str = "affected"


TRANSITIONS = {
    Event.SESSION_START: Transition("reset", "clear", "clear", "reset", "both"),
    Event.SESSION_END: Transition("discard", "clear", "clear", "reset", "both"),
    Event.HALT_ENTER: Transition("reset", "clear", "clear", "reset", "both"),
    Event.HALT_RESUME: Transition("start", "clear", "seed_from_new_events", "start", "both"),
    Event.GAP_ENTER: Transition("decay", "clear", "clear", "reset"),
    Event.GAP_RECOVER: Transition("retain", "seed_from_new_endpoints", "seed_from_new_events", "start"),
    Event.INVALID_QUOTE: Transition("retain", "retain", "clear_midpoint_only", "retain", "quote"),
    Event.VALID_QUOTE_RECOVER: Transition("retain", "retain", "seed_midpoint_only", "retain", "quote"),
    **{e: Transition() for e in (Event.BAD_SIZE, Event.LATE_TRADE, Event.STRATUM,
                               Event.DISCOVERY, Event.BATCH, Event.COVERAGE_FAILURE)},
}


def transition(event):
    try:
        return TRANSITIONS[Event(event)]
    except (ValueError, KeyError) as error:
        raise ContractError("unknown transition") from error


def session_bounds(day):
    try:
        d = date.fromisoformat(day)
        if d.isoformat() != day:
            raise ValueError()
    except (ValueError, TypeError) as error:
        raise ContractError("noncanonical session date") from error
    zone = ZoneInfo("America/New_York")
    return tuple(int(datetime.combine(d, time(h), zone).timestamp()) * NS for h in (4, 20))


def activity_endpoint(sip_ns):
    integer(sip_ns, "SIP timestamp", 0, 2**63 - NS)
    return (sip_ns // NS + 1) * NS


def timely_trade(sip_ns, participant_ns, config=DEFAULT_CONFIG):
    """Known late rows are ineligible, not missing feed exposure."""
    for v in (sip_ns, participant_ns):
        integer(v, "trade timestamp", 0, 2**63 - 1)
    return 0 <= sip_ns - participant_ns <= config.max_trade_reporting_age_ns


def endpoint_return(start_mid, end_mid, *, continuity_same, crosses_halt):
    """Intermediate invalid quote values are deliberately not inputs."""
    if type(continuity_same) is not bool or type(crosses_halt) is not bool:
        raise ContractError("continuity flags must be boolean")
    if not continuity_same or crosses_halt or start_mid is None or end_mid is None:
        return None
    if any(type(v) not in (float, int) or not math.isfinite(v) or v <= 0 for v in (start_mid, end_mid)):
        raise ContractError("invalid supplied midpoint")
    # Avoid overflow/underflow of a direct price ratio; log1p for nearby prices.
    if .5 <= end_mid / start_mid <= 2:
        return 10000 * math.log1p((end_mid - start_mid) / start_mid)
    return 10000 * (math.log(end_mid) - math.log(start_mid))

# Frozen accepted vendor-code vocabulary; unknown values have local uncertainty semantics.
KNOWN_TRADE_CONDITIONS = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 59, 60)
KNOWN_QUOTE_CONDITIONS = (-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 71, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 94)
KNOWN_QUOTE_INDICATORS = (1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 304, 501, 502, 503, 504, 505, 506, 507, 508, 509, 601, 602, 603, 604, 605, 901, 902, 903, 904, 905, 906, 907, 908)
QUOTE_ONE_SIDED_CODES = (2, 34)
QUOTE_NONFIRM_CODES = (20,)
QUOTE_CLOSED_OR_NO_QUOTE_CODES = (15, 19, 32)
QUOTE_INVALID_CODES = (-1, 80, 83)
QUOTE_EXPLICIT_CROSSED_CODES = (84,)
QUOTE_EXPLICIT_LOCKED_CODES = (85,)

SEMANTIC_CODES = {name: globals()[name] for name in ('KNOWN_TRADE_CONDITIONS', 'KNOWN_QUOTE_CONDITIONS', 'KNOWN_QUOTE_INDICATORS', 'QUOTE_ONE_SIDED_CODES', 'QUOTE_NONFIRM_CODES', 'QUOTE_CLOSED_OR_NO_QUOTE_CODES', 'QUOTE_INVALID_CODES', 'QUOTE_EXPLICIT_CROSSED_CODES', 'QUOTE_EXPLICIT_LOCKED_CODES')}
