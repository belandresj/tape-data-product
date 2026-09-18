SELECT
    session_date,
    symbol,
    session,
    interval_end_ns,
    midpoint_rms_5s_bps_hl30s,
    quoted_spread_bps_hl30s,
    midpoint_rms_5s_to_spread_hl30s,
    movement_participation_hl30s,
    trade_rate_per_second_hl30s,
    midpoint_rms_5s_bps_hl120s,
    quoted_spread_bps_hl120s,
    midpoint_rms_5s_to_spread_hl120s,
    movement_participation_hl120s,
    trade_rate_per_second_hl120s,
    quote_age_p90_seconds_window60s,
    trade_age_p90_seconds_window60s
FROM features
