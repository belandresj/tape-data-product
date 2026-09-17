"""Publication regressions for the endpoint/EW-only installed product."""

import hashlib
import importlib.util
from pathlib import Path

from tape_data_product.cli import parser


ROOT = Path(__file__).resolve().parents[1]


def _choices(command_parser):
    action = next(
        action
        for action in command_parser._actions
        if getattr(action, "choices", None)
    )
    return action.choices


def test_installed_command_surface_is_endpoint_ew_only():
    top_level = _choices(parser())
    assert set(top_level) == {
        "acquire",
        "screen",
        "storage",
        "base",
        "calculate",
        "features",
        "endpoint-data",
        "report",
    }
    assert set(_choices(top_level["features"])) == {
        "build-from-base",
        "verify-from-base",
    }
    assert {"population", "activity-ecdf-calculate", "feature-ecdf-calculate", "joint", "gpus-cast"} <= set(
        _choices(top_level["report"])
    )


def test_retired_packages_are_not_installed():
    retired = (
        "tape_data_product.demo",
        "tape_data_product.experiments",
        "tape_data_product.features.compact_product",
        "tape_data_product.features.all_feature_month_core",
        "tape_data_product.features.july_r2_product",
        "tape_data_product.query.tape_cohort_pipeline",
        "tape_data_product.query.release",
    )
    assert all(importlib.util.find_spec(name) is None for name in retired)


def test_protected_report_identity_is_unchanged():
    path = ROOT / "reports/report_v2/report_v2_final.md"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == (
        "17336cd8deb0bb871ac6fa48ce36433f5c5c14c97263a34e9ba19f1d547ce170"
    )
