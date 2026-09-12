"""Lightweight supervisor identity; never imports Arrow/NumPy or feature reducers."""

from datetime import datetime
from zoneinfo import ZoneInfo
from tape_data_product.features import run_all_feature_month as OLD
from tape_data_product.features.all_feature_month_inventory import sha


def code_identity():
    from tape_data_product.stages import implementation_identity

    return implementation_identity()


def session_start(day):
    return (
        int(
            datetime.fromisoformat(day)
            .replace(hour=4, tzinfo=ZoneInfo("America/New_York"))
            .timestamp()
        )
        * 10**9
    )
