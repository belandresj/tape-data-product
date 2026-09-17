"""One bounded synthetic raw-to-query smoke path for the published V2 product."""

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from test_endpoint_pipeline import fixture  # noqa: E402
from test_endpoint_reader import _open  # noqa: E402

from tape_data_product.features.endpoint_ew import build_from_base
from tape_data_product.integrity import sha256_file
from tape_data_product.query import build_endpoint_query_catalog, open_tape_database
from tape_data_product.replay.builder import build_base_partition


def _manifest_record(path):
    manifest = json.loads((path / "manifest.json").read_text())
    sha256, size = sha256_file(path / "manifest.json")
    return {
        "sha256": sha256,
        "bytes": size,
        "outputs": {row["path"]: row for row in manifest["outputs"]},
        "implementation_identity": manifest["implementation_identity"]["sha256"],
        "consumed_base_manifest_sha256": manifest.get("inputs", {}).get(
            "base_manifest_sha256"
        ),
    }, manifest


def test_synthetic_raw_base_features_reference_and_query(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    pair, context_path = fixture(source, seconds=70)
    day, symbol = "2026-09-02", "SYN"
    relative = Path(f"session_date={day}") / f"symbol={symbol}"
    base_root, feature_root = tmp_path / "base", tmp_path / "features"
    base_path, feature_path = base_root / relative, feature_root / relative

    build_base_partition(pair, context_path, base_path, batch_size=7)
    build_from_base(base_path, feature_path, batch_size=7)
    base_record, base_manifest = _manifest_record(base_path)
    feature_record, _ = _manifest_record(feature_path)
    context = json.loads((base_path / "context.json").read_text())
    partition = {
        "base_root": base_root,
        "feature_root": feature_root,
        "relative": str(relative),
        "day": day,
        "symbol": symbol,
        "coverage": base_manifest["coverage"],
        "context": context,
        "base_manifest": base_record,
        "feature_manifest": feature_record,
    }

    handle, _, identity = _open(tmp_path, partition)
    assert handle.manifest["reference_identity"] == identity
    assert handle.manifest["members"] == {
        **handle.manifest["members"],
        "count": 1,
        "rows_per_table": 70,
    }
    catalog = tmp_path / "catalog"
    catalog_result = build_endpoint_query_catalog(handle, catalog)
    with open_tape_database(
        catalog,
        expected_identity=catalog_result["catalog_identity"],
        data_roots={"base": base_root, "features": feature_root},
        start_date=day,
        end_date=day,
        members=(f"{day}/{symbol}",),
    ) as database:
        assert database.sql("SELECT count(*) FROM features").fetchone()[0] == 70
        assert database.catalog_identity == catalog_result["catalog_identity"]
