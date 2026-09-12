"""Horizon-local transaction gate and immutable report selection identity.

The functions operate on projected NumPy batches.  They never alter or rebuild
rolling histories.  Time is O(B) and temporary memory O(B) per horizon, where
B defaults to 4,096 and may not exceed 25,000 rows.
"""

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path

import numpy as np

HORIZONS = (60, 300)
SESSIONS = ("premarket", "rth", "after_hours")
DEFAULT_BATCH_SIZE = 4096
MAX_BATCH_SIZE = 25000
RATE_MINIMUM = 1.0
TRADE_AGE_MAXIMUM_SECONDS = 2.0
RATE_BAND_EDGES = (1.0, 10.0, 30.0, 100.0)
RATE_BAND_LABELS = ("[1,10)", "[10,30)", "[30,100)", "[100,+inf)")
GATE_POLICY = {
    "version": "report_transaction_gate_v1",
    "definition": (
        "post_discovery AND primary_eligible(trade_rate_Hs) AND "
        "primary_eligible(trade_age_p90_seconds_Hs) AND trade_rate_Hs >= 1 "
        "AND trade_age_p90_seconds_Hs <= 2"
    ),
    "rate_minimum_inclusive": RATE_MINIMUM,
    "trade_age_maximum_seconds_inclusive": TRADE_AGE_MAXIMUM_SECONDS,
    "quote_age_gate": False,
    "horizon_local": True,
}


def canonical_digest(value):
    """Content identity for JSON-compatible policy/configuration values."""
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def file_sha256(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024**2), b""):
            result.update(block)
    return result.hexdigest()


def feature_name(stem, horizon):
    if horizon not in HORIZONS:
        raise ValueError("report horizon must be 60 or 300 seconds")
    return f"{stem}_{horizon}s"


def _bool_array(value, name, length=None):
    result = np.asarray(value)
    if result.ndim != 1 or (length is not None and len(result) != length):
        raise ValueError(f"{name} must be a one-dimensional batch array")
    if result.dtype.kind != "b":
        raise ValueError(f"{name} must be boolean")
    return result


def _numeric_array(value, name, length):
    result = np.asarray(value, dtype=np.float64)
    if result.ndim != 1 or len(result) != length:
        raise ValueError(f"{name} must match the batch length")
    if np.isinf(result).any():
        raise ValueError(
            f"{name} contains infinity; nulls must not be encoded as infinity"
        )
    return result


def primary_eligible(values, name, post_discovery, eligibility=None):
    """Return the stored primary-analysis mask for one projected feature.

    Compact report extracts encode ineligible values as null/NaN.  A caller
    with separate stored eligibility may supply it; an eligible numerical value
    must still be finite and an eligible pre-discovery value is rejected.
    """
    post = _bool_array(post_discovery, "post_discovery")
    value = _numeric_array(values[name], name, len(post))
    finite = np.isfinite(value)
    if eligibility is None:
        result = finite
    else:
        explicit = _bool_array(eligibility[name], name + " eligibility", len(post))
        if np.any(explicit & ~finite):
            raise ValueError(f"{name} is marked eligible without a finite value")
        result = explicit & finite
    if np.any(result & ~post):
        raise ValueError(f"{name} is primary eligible before discovery")
    return result


@dataclass(frozen=True)
class GateMasks:
    horizon: int
    represented: np.ndarray
    post_discovery: np.ndarray
    input_eligible: np.ndarray
    passing: np.ndarray
    failing: np.ndarray
    unavailable: np.ndarray

    def validate(self):
        n = len(self.post_discovery)
        for name in (
            "represented",
            "input_eligible",
            "passing",
            "failing",
            "unavailable",
        ):
            _bool_array(getattr(self, name), name, n)
        if np.any(self.input_eligible & ~self.post_discovery):
            raise ValueError("gate eligibility must be post-discovery")
        if np.any(self.passing & self.failing) or np.any(
            self.unavailable & self.input_eligible
        ):
            raise ValueError("gate populations overlap")
        if not np.array_equal(self.input_eligible, self.passing | self.failing):
            raise ValueError("eligible gate inputs do not partition into pass/fail")
        if not np.array_equal(
            self.post_discovery, self.input_eligible | self.unavailable
        ):
            raise ValueError(
                "post-discovery rows do not partition into eligible/unavailable"
            )
        return self


def transaction_gate(values, post_discovery, horizon, eligibility=None):
    """Evaluate exactly one horizon's gate; equality at both bounds passes."""
    post = _bool_array(post_discovery, "post_discovery")
    if len(post) > MAX_BATCH_SIZE:
        raise ValueError(f"batch exceeds {MAX_BATCH_SIZE} rows")
    rate_name = feature_name("trade_rate", horizon)
    age_name = feature_name("trade_age_p90_seconds", horizon)
    rate = _numeric_array(values[rate_name], rate_name, len(post))
    age = _numeric_array(values[age_name], age_name, len(post))
    rate_ok = primary_eligible(values, rate_name, post, eligibility)
    age_ok = primary_eligible(values, age_name, post, eligibility)
    inputs = post & rate_ok & age_ok
    passing = inputs & (rate >= RATE_MINIMUM) & (age <= TRADE_AGE_MAXIMUM_SECONDS)
    result = GateMasks(
        horizon=horizon,
        represented=np.ones(len(post), dtype=bool),
        post_discovery=post,
        input_eligible=inputs,
        passing=passing,
        failing=inputs & ~passing,
        unavailable=post & ~inputs,
    )
    return result.validate()


def feature_population(gate, values, feature, eligibility=None):
    """Gate intersected with one feature's own eligibility only."""
    good = primary_eligible(values, feature, gate.post_discovery, eligibility)
    return gate.passing & good


def pair_population(gate, values, first, second, eligibility=None):
    """Gate intersected with exactly the two requested features."""
    return (
        gate.passing
        & primary_eligible(values, first, gate.post_discovery, eligibility)
        & primary_eligible(values, second, gate.post_discovery, eligibility)
    )


def rate_band_ids(gate, values):
    """Return 0..3 for passing rows and -1 elsewhere.

    Boundaries are left-inclusive: 10 belongs to band 1, 30 to band 2, and
    100 to band 3.  The passing gate guarantees no assigned rate below 1.
    """
    name = feature_name("trade_rate", gate.horizon)
    rate = _numeric_array(values[name], name, len(gate.passing))
    result = np.full(len(rate), -1, dtype=np.int8)
    result[gate.passing] = np.searchsorted(
        (10.0, 30.0, 100.0), rate[gate.passing], side="right"
    ).astype(np.int8)
    if np.any(result[gate.passing] < 0) or np.any(rate[gate.passing] < 1):
        raise ValueError("passing gate contains rate below one")
    return result


def artifact_binding(
    *,
    release_identity,
    inventory_identity,
    source_identities,
    feature_definitions,
    bin_configuration,
    units,
    code_identities,
    horizons=HORIZONS,
):
    """Build the cache/artifact identity shared by both report horizons."""
    horizons = tuple(horizons)
    if horizons != HORIZONS:
        raise ValueError("V1 report artifacts require shared 60s and 300s horizons")
    if (
        not release_identity
        or not inventory_identity
        or not source_identities
        or not feature_definitions
        or not bin_configuration
        or not units
    ):
        raise ValueError(
            "release, inventory, source, feature, bin, and unit identities are required"
        )
    from tape_data_product.stages import implementation_identity

    code_identities = {
        **code_identities,
        "installed_runtime": implementation_identity(),
    }
    binding = dict(
        version="tape_report_numerical_artifacts_v1",
        release_identity=release_identity,
        inventory_identity=inventory_identity,
        source_identities=source_identities,
        gate_policy=GATE_POLICY,
        horizons=list(horizons),
        feature_definitions=feature_definitions,
        bin_configuration=bin_configuration,
        units=units,
        code_identities=code_identities,
        matched_endpoints=False,
        batch_policy=dict(default=DEFAULT_BATCH_SIZE, maximum=MAX_BATCH_SIZE),
    )
    binding["artifact_identity"] = canonical_digest(binding)
    return binding


def validate_artifact_binding(binding):
    candidate = dict(binding)
    saved = candidate.pop("artifact_identity", None)
    if candidate.get("version") != "tape_report_numerical_artifacts_v1":
        raise ValueError("unsupported report artifact version")
    if tuple(candidate.get("horizons", ())) != HORIZONS:
        raise ValueError("report artifact horizons are incomplete")
    if (
        candidate.get("gate_policy") != GATE_POLICY
        or candidate.get("matched_endpoints") is not False
    ):
        raise ValueError("report selection policy mismatch")
    if canonical_digest(candidate) != saved:
        raise ValueError("report artifact identity mismatch")
    return binding
