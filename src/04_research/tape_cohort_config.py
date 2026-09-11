"""Strict normalization and identity for tape_cohort_hysteresis_v1."""
from __future__ import annotations

import hashlib
import json
import math

import compact_product_schema as S

SCHEMA = "tape_cohort_config_v1"
SEMANTICS = "tape_cohort_hysteresis_v1"
ELIGIBILITY = "post_discovery_and_requested_zero_masks_v1"
SESSION = "extended_0400_2000_ET"
_TOP = {"schema", "semantics_version", "eligibility", "decision_session",
        "entry_confirm_seconds", "exit_confirm_seconds", "conditions"}
_COND = {"feature", "unit", "entry", "continuation"}
_BOUNDS = {"lower", "lower_inclusive", "upper", "upper_inclusive"}
UNITS = {name: meta["unit"] for name, meta in S.field_metadata().items()}
FEATURE_INDEX = {name: i for i, name in enumerate(S.FEATURES)}
REASON_BITS = dict(S.REASONS)


def _number(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite number or null")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{label} must be finite")
    if value == 0.0:
        if math.copysign(1.0, value) < 0:
            raise ValueError(f"{label} cannot be negative zero")
        value = 0.0
    return value


def _bounds(raw, inherited, label):
    if raw is None:
        raw = {}
    if not isinstance(raw, dict) or set(raw) - _BOUNDS:
        raise ValueError(f"unknown or malformed {label} fields")
    out = {}
    for side in ("lower", "upper"):
        value = raw.get(side, inherited.get(side) if inherited else None)
        out[side] = None if value is None else _number(value, f"{label}.{side}")
        flag = side + "_inclusive"
        inclusive = raw.get(flag, inherited.get(flag, True) if inherited else True)
        if type(inclusive) is not bool:
            raise ValueError(f"{label}.{flag} must be boolean")
        out[flag] = inclusive if out[side] is not None else True
    if out["lower"] is not None and out["upper"] is not None:
        if out["lower"] > out["upper"] or (out["lower"] == out["upper"] and
                not (out["lower_inclusive"] and out["upper_inclusive"])):
            raise ValueError(f"empty {label} interval")
    return out


def _inside(entry, continuation):
    el, eu, cl, cu = entry["lower"], entry["upper"], continuation["lower"], continuation["upper"]
    if cl is not None and (el is None or el < cl or
            (el == cl and entry["lower_inclusive"] and not continuation["lower_inclusive"])):
        return False
    if cu is not None and (eu is None or eu > cu or
            (eu == cu and entry["upper_inclusive"] and not continuation["upper_inclusive"])):
        return False
    return True


def normalize_config(mapping):
    if not isinstance(mapping, dict) or set(mapping) != _TOP:
        raise ValueError("config has missing or unknown fields")
    expected = {"schema": SCHEMA, "semantics_version": SEMANTICS,
                "eligibility": ELIGIBILITY, "decision_session": SESSION}
    for key, value in expected.items():
        if mapping.get(key) != value:
            raise ValueError(f"unsupported {key}")
    confirms = {}
    for key in ("entry_confirm_seconds", "exit_confirm_seconds"):
        value = mapping[key]
        if type(value) is not int or not 0 <= value <= 300:
            raise ValueError(f"{key} must be an integer in [0,300]")
        confirms[key] = 0 if value == 1 else value
    raw_conditions = mapping["conditions"]
    if not isinstance(raw_conditions, list) or not raw_conditions:
        raise ValueError("at least one condition is required")
    normalized = []
    seen = set()
    for raw in raw_conditions:
        if not isinstance(raw, dict) or set(raw) - _COND or set(raw) != _COND:
            raise ValueError("condition has missing or unknown fields")
        feature = raw["feature"]
        if feature not in UNITS or feature in seen:
            raise ValueError("unknown or duplicate feature")
        seen.add(feature)
        if raw["unit"] != UNITS[feature]:
            raise ValueError(f"unit mismatch for {feature}")
        entry = _bounds(raw["entry"], None, f"{feature}.entry")
        if entry["lower"] is None and entry["upper"] is None:
            raise ValueError("condition requires at least one entry bound")
        continuation = _bounds(raw["continuation"], entry, f"{feature}.continuation")
        if not _inside(entry, continuation):
            raise ValueError("entry interval must be a subset of continuation")
        if (entry["lower"] is not None and entry["lower"] < 0) or \
                (continuation["lower"] is not None and continuation["lower"] < 0):
            raise ValueError("feature domain is nonnegative")
        if feature.startswith("movement_participation_"):
            for bounds in (entry, continuation):
                if bounds["upper"] is not None and bounds["upper"] > 1:
                    raise ValueError("participation bounds must be in [0,1]")
        normalized.append({"feature": feature, "unit": raw["unit"],
                           "entry": entry, "continuation": continuation})
    normalized.sort(key=lambda c: FEATURE_INDEX[c["feature"]])
    return {**expected, **confirms, "conditions": normalized}


def canonical_bytes(config):
    return json.dumps(normalize_config(config), sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode()


def query_hash(config):
    return hashlib.sha256(canonical_bytes(config)).hexdigest()


INITIAL_CONFIG = normalize_config({
    "schema": SCHEMA, "semantics_version": SEMANTICS, "eligibility": ELIGIBILITY,
    "decision_session": SESSION, "entry_confirm_seconds": 5, "exit_confirm_seconds": 5,
    "conditions": [
        {"feature": "movement_mean_5s_bps_60s", "unit": "bps", "entry": {"lower": 10.0, "lower_inclusive": True, "upper": None, "upper_inclusive": True}, "continuation": {"lower": 8.0, "lower_inclusive": True, "upper": None, "upper_inclusive": True}},
        {"feature": "quoted_spread_mean_bps_60s", "unit": "bps", "entry": {"lower": 0.0, "lower_inclusive": False, "upper": 100.0, "upper_inclusive": True}, "continuation": {"lower": 0.0, "lower_inclusive": False, "upper": 125.0, "upper_inclusive": True}},
        {"feature": "trade_rate_60s", "unit": "trades/second", "entry": {"lower": 10.0, "lower_inclusive": True, "upper": None, "upper_inclusive": True}, "continuation": {"lower": 8.0, "lower_inclusive": True, "upper": None, "upper_inclusive": True}},
        {"feature": "dollar_rate_60s", "unit": "USD/second", "entry": {"lower": 5000.0, "lower_inclusive": True, "upper": None, "upper_inclusive": True}, "continuation": {"lower": 4000.0, "lower_inclusive": True, "upper": None, "upper_inclusive": True}},
        {"feature": "movement_mean_to_spread_60s", "unit": "dimensionless", "entry": {"lower": 1.5, "lower_inclusive": True, "upper": None, "upper_inclusive": True}, "continuation": {"lower": 1.2, "lower_inclusive": True, "upper": None, "upper_inclusive": True}},
    ]})
