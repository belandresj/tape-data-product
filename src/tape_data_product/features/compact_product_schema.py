"""Frozen compact V1 physical schemas; independent of the legacy namespace."""

import hashlib
import pyarrow as pa
from tape_data_product.features.all_feature_month_schema import (
    FEATURES,
    REASONS,
    PRIMITIVES,
    field_metadata,
)

LAYOUT_VERSION = "tape_product_compact_v1"
FEATURE_CONTRACT = "tape_data_product_v1"
KEYS = ("session_date", "symbol", "interval_end_ns")
DISCOVERY_FIELDS = (
    "first_discovery_endpoint_ns",
    "discovery_received_at_ns",
    "discovery_timing_basis",
    "discovery_provenance_hash",
)
FEATURE_SCHEMA = pa.schema(
    [
        pa.field("session_date", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("interval_end_ns", pa.int64(), nullable=False),
        pa.field("movement_mean_5s_bps_60s", pa.float64(), nullable=True),
        pa.field("movement_participation_60s", pa.float64(), nullable=True),
        pa.field("quoted_spread_mean_bps_60s", pa.float64(), nullable=True),
        pa.field("trade_rate_60s", pa.float64(), nullable=True),
        pa.field("dollar_rate_60s", pa.float64(), nullable=True),
        pa.field("trade_age_p90_seconds_60s", pa.float64(), nullable=True),
        pa.field("quote_age_p90_seconds_60s", pa.float64(), nullable=True),
        pa.field("midpoint_change_age_p90_seconds_60s", pa.float64(), nullable=True),
        pa.field("movement_mean_to_spread_60s", pa.float64(), nullable=True),
        pa.field("movement_mean_5s_bps_300s", pa.float64(), nullable=True),
        pa.field("movement_participation_300s", pa.float64(), nullable=True),
        pa.field("quoted_spread_mean_bps_300s", pa.float64(), nullable=True),
        pa.field("trade_rate_300s", pa.float64(), nullable=True),
        pa.field("dollar_rate_300s", pa.float64(), nullable=True),
        pa.field("trade_age_p90_seconds_300s", pa.float64(), nullable=True),
        pa.field("quote_age_p90_seconds_300s", pa.float64(), nullable=True),
        pa.field("midpoint_change_age_p90_seconds_300s", pa.float64(), nullable=True),
        pa.field("movement_mean_to_spread_300s", pa.float64(), nullable=True),
        pa.field("movement_mean_5s_bps_60s_reason_mask", pa.int64(), nullable=False),
        pa.field("movement_participation_60s_reason_mask", pa.int64(), nullable=False),
        pa.field("quoted_spread_mean_bps_60s_reason_mask", pa.int64(), nullable=False),
        pa.field("trade_rate_60s_reason_mask", pa.int64(), nullable=False),
        pa.field("dollar_rate_60s_reason_mask", pa.int64(), nullable=False),
        pa.field("trade_age_p90_seconds_60s_reason_mask", pa.int64(), nullable=False),
        pa.field("quote_age_p90_seconds_60s_reason_mask", pa.int64(), nullable=False),
        pa.field(
            "midpoint_change_age_p90_seconds_60s_reason_mask",
            pa.int64(),
            nullable=False,
        ),
        pa.field("movement_mean_to_spread_60s_reason_mask", pa.int64(), nullable=False),
        pa.field("movement_mean_5s_bps_300s_reason_mask", pa.int64(), nullable=False),
        pa.field("movement_participation_300s_reason_mask", pa.int64(), nullable=False),
        pa.field("quoted_spread_mean_bps_300s_reason_mask", pa.int64(), nullable=False),
        pa.field("trade_rate_300s_reason_mask", pa.int64(), nullable=False),
        pa.field("dollar_rate_300s_reason_mask", pa.int64(), nullable=False),
        pa.field("trade_age_p90_seconds_300s_reason_mask", pa.int64(), nullable=False),
        pa.field("quote_age_p90_seconds_300s_reason_mask", pa.int64(), nullable=False),
        pa.field(
            "midpoint_change_age_p90_seconds_300s_reason_mask",
            pa.int64(),
            nullable=False,
        ),
        pa.field(
            "movement_mean_to_spread_300s_reason_mask", pa.int64(), nullable=False
        ),
        pa.field("midpoint", pa.float64(), nullable=True),
        pa.field("continuity_segment_id", pa.int64(), nullable=False),
        pa.field("halt_interval_active", pa.bool_(), nullable=False),
        pa.field("primitive_quote_source_file_accepted", pa.bool_(), nullable=False),
        pa.field("primitive_trade_source_file_accepted", pa.bool_(), nullable=False),
        pa.field("post_discovery_eligible", pa.bool_(), nullable=False),
    ]
)
FEATURE_COLUMNS = tuple(FEATURE_SCHEMA.names)
FEATURE_SCHEMA_HASH = "0ee68c7b58a53262af4eaa9252ac54ba6f1a80a919ca78543d8a1943298ee3e0"
SUPPORT_SCHEMA = pa.schema(
    [
        pa.field("session_date", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("interval_end_ns", pa.int64(), nullable=False),
        pa.field("halt_interval_id", pa.string(), nullable=True),
        pa.field("historical_ex_post_overlay", pa.bool_(), nullable=True),
        pa.field("live_reproducible", pa.bool_(), nullable=True),
        pa.field("seconds_since_halt_resume", pa.int64(), nullable=True),
        pa.field("state_observed_second_count_60s", pa.int64(), nullable=True),
        pa.field("state_mature_60s", pa.bool_(), nullable=True),
        pa.field("movement_valid_5s_count_60s", pa.int64(), nullable=True),
        pa.field("movement_valid_1s_count_60s", pa.int64(), nullable=True),
        pa.field("movement_support_valid_60s", pa.bool_(), nullable=True),
        pa.field("activity_valid_second_count_60s", pa.int64(), nullable=True),
        pa.field("activity_support_valid_60s", pa.bool_(), nullable=True),
        pa.field("quote_age_observation_count_60s", pa.int64(), nullable=True),
        pa.field("trade_age_observation_count_60s", pa.int64(), nullable=True),
        pa.field("quoted_spread_valid_60s", pa.bool_(), nullable=True),
        pa.field("quoted_spread_valid_fraction_60s", pa.float64(), nullable=True),
        pa.field("state_fully_post_halt_60s", pa.bool_(), nullable=True),
        pa.field(
            "state_pre_halt_observation_fraction_60s", pa.float64(), nullable=True
        ),
        pa.field("state_contains_pre_halt_history_60s", pa.bool_(), nullable=True),
        pa.field("state_post_halt_observed_seconds_60s", pa.int64(), nullable=True),
        pa.field("state_carried_forward_during_halt_60s", pa.bool_(), nullable=True),
        pa.field("state_observed_second_count_300s", pa.int64(), nullable=True),
        pa.field("state_mature_300s", pa.bool_(), nullable=True),
        pa.field("movement_valid_5s_count_300s", pa.int64(), nullable=True),
        pa.field("movement_valid_1s_count_300s", pa.int64(), nullable=True),
        pa.field("movement_support_valid_300s", pa.bool_(), nullable=True),
        pa.field("activity_valid_second_count_300s", pa.int64(), nullable=True),
        pa.field("activity_support_valid_300s", pa.bool_(), nullable=True),
        pa.field("quote_age_observation_count_300s", pa.int64(), nullable=True),
        pa.field("trade_age_observation_count_300s", pa.int64(), nullable=True),
        pa.field("quoted_spread_valid_300s", pa.bool_(), nullable=True),
        pa.field("quoted_spread_valid_fraction_300s", pa.float64(), nullable=True),
        pa.field("state_fully_post_halt_300s", pa.bool_(), nullable=True),
        pa.field(
            "state_pre_halt_observation_fraction_300s", pa.float64(), nullable=True
        ),
        pa.field("state_contains_pre_halt_history_300s", pa.bool_(), nullable=True),
        pa.field("state_post_halt_observed_seconds_300s", pa.int64(), nullable=True),
        pa.field("state_carried_forward_during_halt_300s", pa.bool_(), nullable=True),
        pa.field("movement_5s_bps", pa.float64(), nullable=True),
        pa.field("movement_5s_valid", pa.bool_(), nullable=True),
        pa.field("movement_1s_valid", pa.bool_(), nullable=True),
        pa.field("halt_resume_boundary", pa.bool_(), nullable=True),
        pa.field("halt_generation", pa.int64(), nullable=True),
        pa.field("movement_zero_total_60s", pa.bool_(), nullable=True),
        pa.field("movement_zero_total_300s", pa.bool_(), nullable=True),
        pa.field("midpoint_change_age_end_seconds", pa.float64(), nullable=True),
        pa.field("midpoint_age_observation_status", pa.string(), nullable=True),
        pa.field("midpoint_observation_start_ns", pa.int64(), nullable=True),
        pa.field("midpoint_no_change_observed_seconds", pa.float64(), nullable=True),
        pa.field("midpoint_valid_duration_ns", pa.int64(), nullable=True),
        pa.field("quoted_spread_integral_bps_seconds", pa.float64(), nullable=True),
        pa.field("quoted_spread_valid_duration_ns", pa.int64(), nullable=True),
        pa.field(
            "midpoint_change_age_observation_count_60s", pa.int64(), nullable=True
        ),
        pa.field("midpoint_change_age_mature_60s", pa.bool_(), nullable=True),
        pa.field("midpoint_change_age_support_valid_60s", pa.bool_(), nullable=True),
        pa.field(
            "midpoint_change_age_observation_count_300s", pa.int64(), nullable=True
        ),
        pa.field("midpoint_change_age_mature_300s", pa.bool_(), nullable=True),
        pa.field("midpoint_change_age_support_valid_300s", pa.bool_(), nullable=True),
        pa.field("trade_count_1s", pa.int64(), nullable=True),
        pa.field("dollar_volume_1s", pa.float64(), nullable=True),
        pa.field("trade_age_end_seconds", pa.float64(), nullable=True),
        pa.field("quote_age_end_seconds", pa.float64(), nullable=True),
    ]
)
SUPPORT_COLUMNS = tuple(SUPPORT_SCHEMA.names)
SUPPORT_SCHEMA_HASH = "fe9b4307fef42d58932432201c4e0702e54af8c50bf9fa8e4b1e27645bd68f34"


def schema_hash(schema):
    return hashlib.sha256(schema.remove_metadata().serialize().to_pybytes()).hexdigest()


def eligibility(mask, post_discovery):
    if type(mask) is not int or mask < 0 or mask & ~255:
        raise ValueError("null, noninteger or unknown reason mask")
    return (mask & 223) == 0, mask == 0 and post_discovery


def dictionary():
    return dict(
        layout_version=LAYOUT_VERSION,
        feature_contract=FEATURE_CONTRACT,
        feature_schema_hash=FEATURE_SCHEMA_HASH,
        support_schema_hash=SUPPORT_SCHEMA_HASH,
        features=field_metadata(),
        primitives=PRIMITIVES,
        reason_bits=REASONS,
        analysis_valid="(reason_mask & 223) == 0",
        eda_eligible="reason_mask == 0 and post_discovery_eligible",
        session_segment="interval start in America/New_York: premarket before 09:30, rth before 16:00, after_hours otherwise",
        discovery_fields=DISCOVERY_FIELDS,
        discovery="post-discovery eligibility does not imply live received-at availability",
    )
