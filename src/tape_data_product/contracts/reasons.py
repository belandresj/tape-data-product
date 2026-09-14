"""Publication decisions, not a record of every observed market-data defect."""
from enum import IntEnum, IntFlag
import math
from .config import ContractError, integer


class SourceStatus(IntEnum):
    UNVERIFIED = 0
    ACCEPTED = 1
    UNAVAILABLE = 2


class MidpointAgeStatus(IntEnum):
    UNOBSERVABLE = 0
    NO_CHANGE_OBSERVED = 1
    KNOWN = 2


class Reason(IntFlag):
    SOURCE_UNAVAILABLE = 1
    SOURCE_UNVERIFIED = 2
    HALT = 4
    STARTUP = 8
    LOW_COVERAGE = 16
    NO_SUPPORTED_DATA = 32
    INVALID_CURRENT_VALUE = 64
    NO_OBSERVED_EVENT = 128
    MIDPOINT_AGE_LOWER_BOUND_ONLY = 256
    CONTINUITY_BREAK = 512
    ZERO_RETURN_VARIATION = 1024
    ZERO_SPREAD = 2048


ALL_REASONS = sum(int(r) for r in Reason)
HISTORY_REASONS = int(Reason.SOURCE_UNAVAILABLE | Reason.SOURCE_UNVERIFIED | Reason.HALT |
                      Reason.STARTUP | Reason.LOW_COVERAGE | Reason.NO_SUPPORTED_DATA)


def decode_reasons(mask):
    integer(mask, "reason mask", 0, ALL_REASONS)
    return tuple(r.name for r in Reason if mask & r)


def validate_value(value, mask, *, allowed=ALL_REASONS, maximum=None, positive=False):
    decode_reasons(mask)
    if mask & ~allowed:
        raise ContractError("reason does not apply to this measurement")
    if (value is None) != (mask != 0):
        raise ContractError("value/null and reason mask disagree")
    if value is not None:
        if isinstance(value, bool) or not math.isfinite(value) or value < 0:
            raise ContractError("measurement must be finite and nonnegative")
        if positive and value <= 0 or maximum is not None and value > maximum:
            raise ContractError("measurement outside domain")


def history_reasons(*, source, halted, elapsed, startup, usable, possible, minimum):
    """No current-value gate. A gap's accumulated coverage does not reset history."""
    integer(source, "source status", 0, 2)
    if type(halted) is not bool:
        raise ContractError("halted must be boolean")
    integer(elapsed, "elapsed", 0, 57600)
    integer(startup, "startup", 1, 57600)
    if any(type(v) not in (int, float) or not math.isfinite(v) for v in (usable, possible, minimum)):
        raise ContractError("invalid coverage inputs")
    if not 0 <= usable <= possible or not 0 < minimum <= 1:
        raise ContractError("invalid coverage range")
    reason = Reason(0)
    if source == SourceStatus.UNVERIFIED:
        reason |= Reason.SOURCE_UNVERIFIED
    elif source == SourceStatus.UNAVAILABLE:
        reason |= Reason.SOURCE_UNAVAILABLE
    if halted:
        reason |= Reason.HALT
    if elapsed < startup:
        reason |= Reason.STARTUP
    if usable == 0:
        reason |= Reason.NO_SUPPORTED_DATA
    elif elapsed >= startup and usable < minimum * possible:
        reason |= Reason.LOW_COVERAGE
    return int(reason)
