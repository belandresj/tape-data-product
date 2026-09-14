"""Explicit Arrow schemas; no raw-data sample or legacy imports needed."""
import pyarrow as pa
from .config import DEFAULT_CONFIG, digest
from .registry import AGES, feature_registry

BASE_VERSION = "tape_base_1s_v1"
FEATURE_VERSION = "tape_features_endpoint_ew_v1"
SHARE_TYPE = pa.decimal128(38, 9)


def field(name, kind=pa.float64(), nullable=True, unit="1"):
    return pa.field(name, kind, nullable=nullable, metadata={b"unit": unit.encode()})


KEYS = (field("session_date", pa.string(), False, "YYYY-MM-DD"),
        field("symbol", pa.string(), False, "symbol"),
        field("interval_end_ns", pa.int64(), False, "UTC Unix ns"))


def mask(name):
    return field(name + "_reason_mask", pa.uint16(), False)


def duration(name):
    return field(name, pa.int64(), False, "ns")


def base_schema():
    f = list(KEYS)
    f += [field(n + "_twap_usd", unit="USD/share") for n in ("bid", "ask", "midpoint")]
    f += [duration("price_valid_duration_ns"), mask("price_twap")]
    f += [field(n + "_end_usd", unit="USD/share") for n in ("bid", "ask")]
    f += [mask("price_end"), field("spread_integral_bps_seconds", unit="bps*s"),
          duration("spread_valid_duration_ns"), mask("spread_integral")]
    for side in ("bid", "ask"):
        f += [field(side + "_size_integral_shares_seconds", unit="shares*s"),
              duration(side + "_size_valid_duration_ns"), mask(side + "_size_integral")]
    f += [field(n + "_size_end_shares", unit="shares") for n in ("bid", "ask")]
    f += [mask(n + "_size_end") for n in ("bid", "ask")]
    f += [field("trade_count_1s", pa.int64(), unit="trades"),
          field("share_volume_1s", SHARE_TYPE, unit="shares"),
          field("dollar_volume_1s_usd", unit="USD"), duration("activity_valid_duration_ns"),
          mask("activity")]
    f += [field(a + "_age_seconds", unit="s") for a in AGES]
    f += [mask(a + "_age") for a in AGES]
    f += [field("midpoint_age_status", pa.uint8(), False),
          field("midpoint_observation_start_ns", pa.int64(), unit="UTC Unix ns"),
          field("midpoint_age_lower_bound_seconds", unit="s")]
    for source in ("quote", "trade"):
        f += [field(source + "_source_status", pa.uint8(), False)]
    f += [duration(s + "_observed_duration_ns") for s in ("quote", "trade")]
    f += [field(s + "_continuity_id", pa.int64(), False) for s in ("quote", "trade")]
    f += [field(s + "_continuity_break_in_second", pa.bool_(), False) for s in ("quote", "trade")]
    f += [field("halt_active", pa.bool_(), False), field("halt_id", pa.string())]
    return pa.schema(f, metadata={b"schema_identity": BASE_VERSION.encode()})


def feature_schema(config=DEFAULT_CONFIG):
    registry = feature_registry(config)
    return pa.schema(list(KEYS) + [field(f.name, unit=f.unit) for f in registry] +
                     [mask(f.name) for f in registry],
                     metadata={b"schema_identity": FEATURE_VERSION.encode()})


def support_schema(config=DEFAULT_CONFIG):
    f = list(KEYS)
    for v in config.views:
        suffix = f"_hl{v.half_life_seconds}s"
        for kind in ("usable", "possible"):
            f.append(field("return_" + kind + "_weight" + suffix, nullable=False))
        for family in ("spread", "activity", "bid_size", "ask_size"):
            for kind in ("usable", "possible"):
                f.append(field(f"{family}_{kind}_exposure_seconds{suffix}", nullable=False, unit="s"))
    f += [field(s + "_ew_startup_elapsed_seconds", pa.int64(), False, "s") for s in ("quote", "trade")]
    for age in AGES:
        for w in config.age_windows_seconds:
            f += [field(f"{age}_age_{kind}_window{w}s", pa.int32(), False, "samples")
                  for kind in ("sample_count", "elapsed_slots")]
    return pa.schema(f, metadata={b"schema_identity": b"tape_feature_support_ew_v1"})


def schema_descriptor(schema):
    def metadata(m):
        return {k.decode(): v.decode() for k, v in sorted((m or {}).items())}
    return {"fields": [{"name": f.name, "type": str(f.type), "nullable": f.nullable,
                        "metadata": metadata(f.metadata)} for f in schema],
            "metadata": metadata(schema.metadata)}


def schema_hash(schema):
    return digest(schema_descriptor(schema))


BASE_SCHEMA = base_schema()
FEATURE_SCHEMA = feature_schema()
SUPPORT_SCHEMA = support_schema()
