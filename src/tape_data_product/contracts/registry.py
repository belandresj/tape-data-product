"""Published numerical fields and dependencies, independent of legacy code."""
from dataclasses import dataclass
from .config import DEFAULT_CONFIG


@dataclass(frozen=True)
class Feature:
    name: str
    unit: str
    family: str
    sources: tuple[str, ...]
    half_life_seconds: int | None = None
    window_seconds: int | None = None


EW_FIELDS = (
    ("midpoint_rms_5s_bps", "bps", "return"),
    ("movement_participation", "1", "participation"),
    ("quoted_spread_bps", "bps", "spread"),
    ("trade_rate_per_second", "trades/s", "activity"),
    ("share_rate_per_second", "shares/s", "activity"),
    ("dollar_rate_usd_per_second", "USD/s", "activity"),
    ("bid_size_mean_shares", "shares", "bid_size"),
    ("ask_size_mean_shares", "shares", "ask_size"),
    ("midpoint_rms_5s_to_spread", "1", "ratio"),
)
AGES = ("trade", "quote", "midpoint_change")


def feature_registry(config=DEFAULT_CONFIG):
    result = []
    for view in config.views:
        for stem, unit, family in EW_FIELDS:
            result.append(Feature(f"{stem}_hl{view.half_life_seconds}s", unit, family,
                                  ("trade",) if family == "activity" else ("quote",),
                                  half_life_seconds=view.half_life_seconds))
    for w in config.age_windows_seconds:
        for age in AGES:
            result.append(Feature(f"{age}_age_p90_seconds_window{w}s", "s", age + "_age",
                                  ("trade",) if age == "trade" else ("quote",), window_seconds=w))
    return tuple(result)


def query_registry(config=DEFAULT_CONFIG):
    return feature_registry(config) + tuple(
        Feature(f"{a}_age_seconds", "s", a + "_current_age",
                ("trade",) if a == "trade" else ("quote",)) for a in AGES)
