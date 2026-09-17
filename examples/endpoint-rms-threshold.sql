SELECT symbol,
       session_date,
       session,
       endpoint_time,
       midpoint_rms_5s_bps_hl30s
FROM features
WHERE midpoint_rms_5s_bps_hl30s > 10
ORDER BY session_date, symbol, endpoint_time;
