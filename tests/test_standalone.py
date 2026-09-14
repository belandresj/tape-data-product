"""Portability and identity regressions introduced by curation."""
import importlib
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]


def test_query_identity_covers_transitive_local_imports_and_files():
    from tape_data_product.stages import implementation_identity
    from tape_data_product.query import tape_cohort_pipeline as pipeline
    value = implementation_identity()
    assert pipeline.implementation_identity()['files'] == value
    package = ROOT/'src/tape_data_product'
    expected = {str(p.relative_to(package)) for p in package.rglob('*') if p.suffix in {'.py','.json'}}
    assert set(value['files']) == expected
    assert 'resources/feature_semantics.json' in value['files']


def test_selected_config_hash_and_schema_identity_are_preserved():
    import json
    from tape_data_product.query.tape_cohort_config import query_hash
    from tape_data_product.features import compact_product_schema as schema
    config=json.loads((ROOT/'config/tape_cohort_300s_ms3_p050_selected_v1.json').read_text())
    assert query_hash(config)=='edf8ce6e063a68184ff223efe810e3d670ef775c09df33c2e72fe679bf481054'
    assert schema.schema_hash(schema.FEATURE_SCHEMA)==schema.FEATURE_SCHEMA_HASH
    assert schema.schema_hash(schema.SUPPORT_SCHEMA)==schema.SUPPORT_SCHEMA_HASH


def test_imported_product_modules_resolve_inside_installed_package():
    import tape_data_product

    package = Path(tape_data_product.__file__).resolve().parent
    for name in ('run_tape_cohort_query','run_direct_frozen_product','verify_tape_cohort_query'):
        importlib.import_module('tape_data_product.' + ('features.' if name == 'run_direct_frozen_product' else 'query.') + name)
    for name,module in list(sys.modules.items()):
        if (name == 'tape_data_product' or name.startswith('tape_data_product.')) and getattr(module,'__file__',None):
            assert Path(module.__file__).resolve().is_relative_to(package), name
