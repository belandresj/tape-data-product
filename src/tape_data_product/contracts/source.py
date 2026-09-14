"""Lossless decimal capacity and explicit source-unit declarations."""
from dataclasses import dataclass
from decimal import Decimal
import re
from .config import ContractError

SCALE = 9
MAX_UNITS = 10**38 - 1


def share_units(value):
    """Return scaled integer shares, without decimal-context rounding.

    Floats are accepted only if their exact binary value fits scale 9. In
    particular float .1 is not evidence of the original exact decimal string.
    """
    if type(value) is str:
        if len(value) > 128 or not re.fullmatch(r"[+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", value):
            raise ContractError("invalid/beyond-bound decimal quantity")
        d = Decimal(value)
    elif type(value) is int:
        if value < 0 or value > MAX_UNITS // 10**SCALE:
            raise ContractError("quantity outside decimal capacity")
        d = Decimal(value)
    elif type(value) is float:
        d = Decimal.from_float(value)
    elif type(value) is Decimal:
        d = value
    else:
        raise ContractError("quantity requires decimal text, Decimal or exact numeric input")
    if not d.is_finite() or d < 0:
        raise ContractError("invalid quantity")
    sign, digits, exponent = d.as_tuple()
    if not any(digits):
        return 0
    if len(digits) > 128:
        raise ContractError("quantity representation exceeds bound")
    coefficient = int(''.join(map(str, digits)))
    power = exponent + SCALE
    if power >= 0:
        if len(digits) + power > 38:
            raise ContractError("quantity outside decimal capacity")
        units = coefficient * 10**power
    else:
        if -power > len(digits):
            raise ContractError("quantity needs more than nine decimal places")
        units, remainder = divmod(coefficient, 10**(-power))
        if remainder:
            raise ContractError("quantity needs more than nine decimal places")
    if units > MAX_UNITS:
        raise ContractError("quantity outside decimal capacity")
    return units


def shares_from_units(units):
    if type(units) is not int or not 0 <= units <= MAX_UNITS:
        raise ContractError("share total overflow")
    return Decimal((0, tuple(map(int, str(units))), -SCALE))


def add_share_units(total, value):
    shares_from_units(total)
    result = total + share_units(value)
    shares_from_units(result)
    return result


@dataclass(frozen=True)
class SourceUnits:
    quote_size_unit: str
    quote_size_evidence_sha256: str
    trade_quantity_evidence_sha256: str
    round_lot_shares: int | None = None

    def __post_init__(self):
        if self.quote_size_unit not in ("shares", "round_lots"):
            raise ContractError("quote-size units must be verified, not guessed")
        for evidence in (self.quote_size_evidence_sha256, self.trade_quantity_evidence_sha256):
            if type(evidence) is not str or not re.fullmatch(r"[0-9a-f]{64}", evidence):
                raise ContractError("source evidence requires SHA-256 identity")
        if self.quote_size_unit == "shares":
            if self.round_lot_shares is not None:
                raise ContractError("share-denominated source must not be multiplied by a lot size")
        elif type(self.round_lot_shares) is not int or not 1 <= self.round_lot_shares <= 1_000_000:
            raise ContractError("lot source requires verified positive member-specific lot size")

    @property
    def multiplier(self):
        return 1 if self.quote_size_unit == "shares" else self.round_lot_shares
