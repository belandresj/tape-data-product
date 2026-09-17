"""Versioned, bounded configuration for the endpoint/EW contract."""
from dataclasses import asdict, dataclass
import hashlib
import json
import math

CONTRACT_VERSION = "tape_endpoint_ew_contract_v1"


class ContractError(ValueError):
    """A configuration, source declaration or persisted contract is invalid."""


def integer(value, name, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ContractError(f"{name} must be an integer in {low}..{high}")
    return value


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


@dataclass(frozen=True)
class EWView:
    half_life_seconds: int
    startup_seconds: int

    def __post_init__(self):
        integer(self.half_life_seconds, "half life", 1, 86400)
        integer(self.startup_seconds, "startup", 6, 57600)


@dataclass(frozen=True)
class FeatureConfig:
    views: tuple[EWView, ...] = (EWView(30, 60), EWView(120, 300))
    age_windows_seconds: tuple[int, ...] = (60, 300)
    spread_min_coverage: float = .9
    other_min_coverage: float = .8
    age_min_coverage: float = .8
    max_trade_reporting_age_ns: int = 1_000_000_000

    def __post_init__(self):
        if type(self.views) is not tuple or not 1 <= len(self.views) <= 8:
            raise ContractError("views must be a tuple of 1..8 EWView values")
        if any(type(v) is not EWView for v in self.views):
            raise ContractError("invalid view")
        hs = [v.half_life_seconds for v in self.views]
        if hs != sorted(set(hs)):
            raise ContractError("views must have unique ascending half lives")
        ws = self.age_windows_seconds
        if type(ws) is not tuple or not 1 <= len(ws) <= 8:
            raise ContractError("age windows must be a tuple of 1..8 lengths")
        for w in ws:
            integer(w, "age window", 1, 300)
        if list(ws) != sorted(set(ws)):
            raise ContractError("age windows must be unique and ascending")
        for name in ("spread_min_coverage", "other_min_coverage", "age_min_coverage"):
            x = getattr(self, name)
            if type(x) not in (int, float) or not math.isfinite(x) or not 0 < x <= 1:
                raise ContractError(f"invalid {name}")
            object.__setattr__(self, name, float(x))
        integer(self.max_trade_reporting_age_ns, "reporting age", 0, 86400_000_000_000)

    def to_dict(self):
        return {"contract_version": CONTRACT_VERSION, **asdict(self)}

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict:
            raise ContractError("configuration must be an object")
        expected = set(cls().to_dict())
        if set(value) != expected or value["contract_version"] != CONTRACT_VERSION:
            raise ContractError("unknown/missing configuration fields or version")
        try:
            return cls(
                views=tuple(EWView(**v) for v in value["views"]),
                age_windows_seconds=tuple(value["age_windows_seconds"]),
                **{k: value[k] for k in expected - {"contract_version", "views", "age_windows_seconds"}},
            )
        except (TypeError, KeyError) as error:
            raise ContractError("malformed configuration") from error


DEFAULT_CONFIG = FeatureConfig()
