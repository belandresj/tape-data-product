"""Validated conjunctions for endpoint/EW queries, independent of row I/O."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Mapping, Sequence


PREDICATE_VERSION = "endpoint_predicates_v1"
QUERY_SEMANTICS = "endpoint_strict_runs_v1"
_OPERATORS = frozenset((">", ">=", "<", "<=", "=="))
_PREDICATE_KEYS = frozenset(("field", "operator", "value", "range"))
_RANGE_KEYS = frozenset(
    ("lower", "lower_inclusive", "upper", "upper_inclusive")
)


def _finite_number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite numeric value")
    try:
        result = float(value)
    except (OverflowError, ValueError) as error:
        raise ValueError(f"{label} must be finite") from error
    if not math.isfinite(result):
        raise ValueError(f"{label} must be finite")
    return 0.0 if result == 0.0 else result


def _empty_bounds() -> dict[str, float | bool | None]:
    return {
        "lower": None,
        "lower_inclusive": True,
        "upper": None,
        "upper_inclusive": True,
    }


def _intersect_lower(bounds: dict, value: float, inclusive: bool) -> None:
    old = bounds["lower"]
    if old is None or value > old:
        bounds["lower"] = value
        bounds["lower_inclusive"] = inclusive
    elif value == old:
        bounds["lower_inclusive"] = bounds["lower_inclusive"] and inclusive


def _intersect_upper(bounds: dict, value: float, inclusive: bool) -> None:
    old = bounds["upper"]
    if old is None or value < old:
        bounds["upper"] = value
        bounds["upper_inclusive"] = inclusive
    elif value == old:
        bounds["upper_inclusive"] = bounds["upper_inclusive"] and inclusive


def _validate_nonempty(bounds: Mapping[str, object], field: str) -> None:
    lower, upper = bounds["lower"], bounds["upper"]
    if lower is None or upper is None:
        return
    if lower > upper or (
        lower == upper
        and not (bounds["lower_inclusive"] and bounds["upper_inclusive"])
    ):
        raise ValueError(f"contradictory predicates for {field}")


def _descriptor_map(field_descriptors: Sequence[Mapping[str, object]]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for descriptor in field_descriptors:
        if not isinstance(descriptor, Mapping):
            raise ValueError("field descriptor must be a mapping")
        try:
            name = descriptor["name"]
            reason_mask = descriptor["reason_mask"]
            sources = descriptor["sources"]
        except KeyError as error:
            raise ValueError("incomplete field descriptor") from error
        if (
            not isinstance(name, str)
            or not name
            or name in result
            or not isinstance(reason_mask, str)
            or not reason_mask
            or not isinstance(sources, (list, tuple))
            or any(source not in ("quote", "trade") for source in sources)
        ):
            raise ValueError("malformed or duplicate field descriptor")
        result[name] = dict(descriptor)
    return result


@dataclass(frozen=True)
class PredicateField:
    name: str
    reason_mask: str
    sources: tuple[str, ...]
    lower: float | None
    lower_inclusive: bool
    upper: float | None
    upper_inclusive: bool

    def passes(self, value: float) -> bool:
        if self.lower is not None and (
            value < self.lower or (value == self.lower and not self.lower_inclusive)
        ):
            return False
        if self.upper is not None and (
            value > self.upper or (value == self.upper and not self.upper_inclusive)
        ):
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "field": self.name,
            "lower": self.lower,
            "lower_inclusive": self.lower_inclusive,
            "upper": self.upper,
            "upper_inclusive": self.upper_inclusive,
        }


@dataclass(frozen=True)
class PredicateResult:
    eligible: bool
    matching: bool
    unavailable_fields: tuple[str, ...]
    nonmatching_fields: tuple[str, ...]

    @property
    def status(self) -> str:
        if not self.eligible:
            return "unavailable"
        return "matching" if self.matching else "nonmatching"


class PredicateSet:
    """A canonical conjunction whose dependencies are supplied by A's mapping."""

    def __init__(self, fields: Sequence[PredicateField]):
        if not fields:
            raise ValueError("at least one predicate is required")
        self.fields = tuple(fields)
        self.names = tuple(field.name for field in self.fields)
        self.reason_masks = tuple(field.reason_mask for field in self.fields)
        self.sources = tuple(sorted({s for field in fields for s in field.sources}))

    def to_dict(self) -> dict:
        return {
            "version": PREDICATE_VERSION,
            "semantics": QUERY_SEMANTICS,
            "conjunction": [field.to_dict() for field in self.fields],
        }

    @property
    def identity(self) -> str:
        payload = json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        return hashlib.sha256(payload).hexdigest()

    def evaluate(self, row: Mapping[str, object]) -> PredicateResult:
        unavailable: list[str] = []
        nonmatching: list[str] = []
        for predicate in self.fields:
            if predicate.name not in row or predicate.reason_mask not in row:
                raise ValueError(f"row lacks predicate dependency {predicate.name}")
            mask = row[predicate.reason_mask]
            value = row[predicate.name]
            if isinstance(mask, bool) or not isinstance(mask, int) or mask < 0:
                raise ValueError(f"invalid reason mask for {predicate.name}")
            if mask != 0 or value is None:
                unavailable.append(predicate.name)
                continue
            numeric = _finite_number(value, predicate.name)
            if not predicate.passes(numeric):
                nonmatching.append(predicate.name)
        eligible = not unavailable
        return PredicateResult(
            eligible=eligible,
            matching=eligible and not nonmatching,
            unavailable_fields=tuple(unavailable),
            nonmatching_fields=tuple(nonmatching),
        )


def compile_predicates(
    predicates: Sequence[Mapping[str, object]],
    field_descriptors: Sequence[Mapping[str, object]],
) -> PredicateSet:
    """Validate, intersect and canonicalize a finite predicate conjunction."""
    if not isinstance(predicates, (list, tuple)) or not predicates:
        raise ValueError("predicates must be a nonempty sequence")
    descriptors = _descriptor_map(field_descriptors)
    bounds_by_field: dict[str, dict] = {}
    descriptor_order = {name: index for index, name in enumerate(descriptors)}
    for index, raw in enumerate(predicates):
        if not isinstance(raw, Mapping) or set(raw) - _PREDICATE_KEYS:
            raise ValueError("malformed predicate")
        field = raw.get("field")
        if field not in descriptors:
            raise ValueError(f"unknown predicate field {field!r}")
        bounds = bounds_by_field.setdefault(field, _empty_bounds())
        has_comparison = "operator" in raw or "value" in raw
        has_range = "range" in raw
        if has_comparison == has_range:
            raise ValueError("predicate must contain exactly one comparison or range")
        if has_comparison:
            if set(raw) != {"field", "operator", "value"}:
                raise ValueError("comparison predicate has missing or unknown fields")
            operator = raw["operator"]
            if operator not in _OPERATORS:
                raise ValueError(f"unsupported predicate operator {operator!r}")
            value = _finite_number(raw["value"], f"{field} threshold")
            if operator in (">", ">="):
                _intersect_lower(bounds, value, operator == ">=")
            elif operator in ("<", "<="):
                _intersect_upper(bounds, value, operator == "<=")
            else:
                _intersect_lower(bounds, value, True)
                _intersect_upper(bounds, value, True)
        else:
            if set(raw) != {"field", "range"}:
                raise ValueError("range predicate has missing or unknown fields")
            range_value = raw["range"]
            if not isinstance(range_value, Mapping) or set(range_value) != _RANGE_KEYS:
                raise ValueError("range must specify both endpoints and inclusivity")
            for label, flag in (
                ("lower", "lower_inclusive"),
                ("upper", "upper_inclusive"),
            ):
                if type(range_value[flag]) is not bool:
                    raise ValueError(f"{field} {flag} must be boolean")
                endpoint = range_value[label]
                if endpoint is not None:
                    endpoint = _finite_number(endpoint, f"{field} {label}")
                    if label == "lower":
                        _intersect_lower(bounds, endpoint, range_value[flag])
                    else:
                        _intersect_upper(bounds, endpoint, range_value[flag])
            if range_value["lower"] is None and range_value["upper"] is None:
                raise ValueError(f"{field} range must have an endpoint")
        _validate_nonempty(bounds, field)
    fields = []
    for name in sorted(bounds_by_field, key=descriptor_order.get):
        descriptor = descriptors[name]
        bounds = bounds_by_field[name]
        fields.append(
            PredicateField(
                name=name,
                reason_mask=descriptor["reason_mask"],
                sources=tuple(descriptor["sources"]),
                **bounds,
            )
        )
    return PredicateSet(fields)
