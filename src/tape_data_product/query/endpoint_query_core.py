"""Streaming endpoint predicate, accounting, and strict-run orchestration."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence

from .endpoint_predicates import PredicateSet
from .endpoint_runs import RunBoundary, StrictRunReducer


QUERY_CONFIG_VERSION = "endpoint_query_config_v1"
_IMPLEMENTATION_MODULES = (
    "endpoint_predicates.py",
    "endpoint_runs.py",
    "endpoint_query_core.py",
    "endpoint_query_outputs.py",
)


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def query_descriptor(
    *,
    reference_identity: str,
    selection: Mapping[str, object],
    predicates: PredicateSet,
    display_fields: Sequence[str],
    field_descriptors: Sequence[Mapping[str, object]],
    contract_identity: str,
    implementation_identity: str,
    observation_schema_sha256: str,
) -> dict:
    descriptors = {row["name"]: row for row in field_descriptors}
    if not isinstance(reference_identity, str) or not reference_identity:
        raise ValueError("reference identity is required")
    if not isinstance(selection, Mapping):
        raise ValueError("selection must be a mapping")
    if not isinstance(contract_identity, str) or not contract_identity:
        raise ValueError("contract identity is required")
    if not isinstance(implementation_identity, str) or not implementation_identity:
        raise ValueError("implementation identity is required")
    if (
        not isinstance(observation_schema_sha256, str)
        or len(observation_schema_sha256) != 64
        or any(character not in "0123456789abcdef" for character in observation_schema_sha256)
    ):
        raise ValueError("observation schema identity must be lowercase SHA-256")
    if not isinstance(display_fields, (list, tuple)) or len(set(display_fields)) != len(
        display_fields
    ):
        raise ValueError("display fields must be a unique sequence")
    unknown = [name for name in display_fields if name not in descriptors]
    if unknown:
        raise ValueError(f"unknown display fields: {unknown}")
    projection_names = tuple(dict.fromkeys((*predicates.names, *display_fields)))
    return {
        "version": QUERY_CONFIG_VERSION,
        "reference_identity": reference_identity,
        "selection": dict(selection),
        "predicates": predicates.to_dict(),
        "display_fields": list(display_fields),
        "field_projection": [
            {
                "name": name,
                "reason_mask": descriptors[name]["reason_mask"],
            }
            for name in projection_names
        ],
        "predicate_dependencies": list(predicates.names),
        "required_sources": list(predicates.sources),
        "contract_identity": contract_identity,
        "implementation_identity": implementation_identity,
        "observation_schema_sha256": observation_schema_sha256,
    }


def query_identity(descriptor: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_json(descriptor).encode()).hexdigest()


def endpoint_query_implementation_identity() -> dict:
    root = Path(__file__).parent
    files = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in _IMPLEMENTATION_MODULES
    }
    return {
        "files": files,
        "sha256": hashlib.sha256(canonical_json(files).encode()).hexdigest(),
    }


@dataclass
class MemberAccounting:
    session_date: str
    symbol: str
    selected: int = 0
    eligible: int = 0
    matching: int = 0
    nonmatching: int = 0
    unavailable: int = 0

    def add(self, status: str) -> None:
        self.selected += 1
        if status == "matching":
            self.eligible += 1
            self.matching += 1
        elif status == "nonmatching":
            self.eligible += 1
            self.nonmatching += 1
        elif status == "unavailable":
            self.unavailable += 1
        else:
            raise ValueError("unknown accounting status")

    @property
    def match_fraction(self) -> float | None:
        return None if self.eligible == 0 else self.matching / self.eligible

    @property
    def eligibility_class(self) -> str:
        if self.eligible == 0:
            return "no_eligible_rows"
        if self.matching == 0:
            return "valid_zero_match"
        return "positive_match"

    def validate(self) -> None:
        if self.selected != self.eligible + self.unavailable:
            raise AssertionError("selected accounting does not reconcile")
        if self.eligible != self.matching + self.nonmatching:
            raise AssertionError("eligible accounting does not reconcile")

    def to_dict(self) -> dict:
        self.validate()
        return {
            "session_date": self.session_date,
            "symbol": self.symbol,
            "selected": self.selected,
            "eligible": self.eligible,
            "matching": self.matching,
            "nonmatching": self.nonmatching,
            "unavailable": self.unavailable,
            "match_fraction": self.match_fraction,
            "eligibility_class": self.eligibility_class,
        }


class EndpointQueryReducer:
    """Consumes row mappings while retaining only run and member aggregate state."""

    def __init__(
        self,
        predicates: PredicateSet,
        identity: str,
        *,
        display_fields: Sequence[str] = (),
        display_reason_masks: Mapping[str, str] | None = None,
        observation_sink: Callable[[dict], None] | None = None,
        run_sink: Callable[[dict], None] | None = None,
        planned_members: Iterable[tuple[str, str]] = (),
    ):
        self.predicates = predicates
        self.identity = identity
        self.display_fields = tuple(display_fields)
        self.display_reason_masks = dict(display_reason_masks or {})
        if set(self.display_reason_masks) != set(self.display_fields) or any(
            not isinstance(mask, str) or not mask
            for mask in self.display_reason_masks.values()
        ):
            raise ValueError("display reason masks must map every display field")
        self.observation_sink = observation_sink
        self.run_sink = run_sink
        self.runs = StrictRunReducer(identity, predicates.sources)
        self.members = {
            (day, symbol): MemberAccounting(day, symbol)
            for day, symbol in planned_members
        }
        self.total_rows = 0

    @staticmethod
    def _boundary(row: Mapping[str, object], side: str) -> RunBoundary | None:
        reason = row.get(f"boundary_{side}_reason")
        if reason is None:
            return None
        censored = row.get(f"boundary_{side}_censored", False)
        if not isinstance(reason, str) or not reason or type(censored) is not bool:
            raise ValueError("malformed explicit run boundary")
        return RunBoundary(reason, censored)

    def consume(self, row: Mapping[str, object]) -> None:
        result = self.predicates.evaluate(row)
        try:
            key = (row["session_date"], row["symbol"])
        except KeyError as error:
            raise ValueError("row lacks member key") from error
        if not all(isinstance(value, str) and value for value in key):
            raise ValueError("invalid member key")
        accounting = self.members.setdefault(key, MemberAccounting(*key))
        accounting.add(result.status)
        self.total_rows += 1
        if result.matching and self.observation_sink is not None:
            names = (
                "session_date",
                "symbol",
                "interval_end_ns",
                *self.predicates.names,
                *self.predicates.reason_masks,
                *self.display_fields,
                *(self.display_reason_masks[name] for name in self.display_fields),
            )
            observation = {name: row[name] for name in dict.fromkeys(names)}
            self.observation_sink(observation)
        emitted = self.runs.consume(
            row,
            status=result.status,
            boundary_before=self._boundary(row, "before"),
            boundary_after=self._boundary(row, "after"),
        )
        if self.run_sink is not None:
            for run in emitted:
                self.run_sink(run.to_dict())

    def consume_rows(self, rows: Iterable[Mapping[str, object]]) -> None:
        for row in rows:
            self.consume(row)

    def finish(self, boundary: RunBoundary | None = None) -> dict:
        emitted = self.runs.finish(boundary)
        if self.run_sink is not None:
            for run in emitted:
                self.run_sink(run.to_dict())
        member_rows = [
            self.members[key].to_dict()
            for key in sorted(self.members)
        ]
        totals = {
            name: sum(row[name] for row in member_rows)
            for name in (
                "selected",
                "eligible",
                "matching",
                "nonmatching",
                "unavailable",
            )
        }
        if totals["selected"] != self.total_rows:
            raise AssertionError("member accounting omits selected rows")
        contributions = [
            {
                "session_date": row["session_date"],
                "symbol": row["symbol"],
                "matching": row["matching"],
                "matching_share": (
                    None
                    if totals["matching"] == 0
                    else row["matching"] / totals["matching"]
                ),
            }
            for row in member_rows
        ]
        return {
            "query_identity": self.identity,
            "totals": totals,
            "match_fraction": (
                None
                if totals["eligible"] == 0
                else totals["matching"] / totals["eligible"]
            ),
            "members": member_rows,
            "contributions": contributions,
            "strict_run_count": self.runs.run_count,
        }
