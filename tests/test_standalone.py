"""Portability and identity regressions introduced by curation."""
import ast
import importlib
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src/04_research'))


def test_query_identity_covers_transitive_local_imports_and_files():
    import tape_cohort_pipeline as pipeline
    modules={p.stem:p for p in (ROOT/'src').rglob('*.py')}
    declared={(ROOT/'src/04_research'/name).resolve() for name in pipeline.QUERY_FILES}
    todo=['run_tape_cohort_query','verify_tape_cohort_query'];seen=set()
    while todo:
        name=todo.pop()
        if name in seen:continue
        seen.add(name);path=modules[name]
        assert path in declared, path.relative_to(ROOT)
        for node in ast.walk(ast.parse(path.read_text())):
            names=[]
            if isinstance(node,ast.Import):names=[x.name.split('.')[0] for x in node.names]
            if isinstance(node,ast.ImportFrom) and node.module:names=[node.module.split('.')[0]]
            if isinstance(node,ast.Constant) and isinstance(node.value,str) and node.value.endswith('.py'):
                names.append(Path(node.value).stem)
            todo.extend(x for x in names if x in modules and x not in seen)
    assert all(p.is_file() and ROOT in p.parents for p in declared)
    assert len(pipeline.implementation_identity()['files'])==len(declared)


def test_selected_config_hash_and_schema_identity_are_preserved():
    import json
    from tape_cohort_config import query_hash
    import compact_product_schema as schema
    config=json.loads((ROOT/'config/tape_cohort_300s_ms3_p050_selected_v1.json').read_text())
    assert query_hash(config)=='edf8ce6e063a68184ff223efe810e3d670ef775c09df33c2e72fe679bf481054'
    assert schema.schema_hash(schema.FEATURE_SCHEMA)==schema.FEATURE_SCHEMA_HASH
    assert schema.schema_hash(schema.SUPPORT_SCHEMA)==schema.SUPPORT_SCHEMA_HASH


def test_imported_product_modules_resolve_inside_checkout():
    for name in ('run_tape_cohort_query','run_direct_frozen_product','verify_tape_cohort_query'):
        importlib.import_module(name)
    local_names={p.stem for p in (ROOT/'src').rglob('*.py')}
    for name,module in list(sys.modules.items()):
        if name in local_names and getattr(module,'__file__',None):
            assert ROOT in Path(module.__file__).resolve().parents, name
