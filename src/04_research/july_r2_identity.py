"""Lightweight supervisor identity; never imports Arrow/NumPy or feature reducers."""
from datetime import datetime
from zoneinfo import ZoneInfo
import run_all_feature_month as OLD
from all_feature_month_inventory import sha


def code_identity():
    return OLD.code_identity() | {str(p.relative_to(OLD.ROOT)):sha(p) for p in sorted(OLD.HERE.parent.glob('july_r2_*.py'))} | {'docs/tape_data_product/README.md':sha(OLD.ROOT/'docs/tape_data_product/README.md')}


def session_start(day):
    return int(datetime.fromisoformat(day).replace(hour=4,tzinfo=ZoneInfo('America/New_York')).timestamp())*10**9
