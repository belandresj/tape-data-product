from pathlib import Path
import pytest
from tape_data_product import stages


def test_receipt_detects_output_corruption_and_refuses_overwrite(tmp_path):
    (tmp_path / 'result.txt').write_text('complete')
    stages.write_stage(tmp_path, 'test', {}, {}, ['result.txt'], {'checked': True})
    stages.verify_stage(tmp_path, require_current_implementation=True)
    with pytest.raises(FileExistsError):
        stages.write_stage(tmp_path, 'test', {}, {}, ['result.txt'], {})
    (tmp_path / 'result.txt').write_text('corrupt')
    with pytest.raises(ValueError, match='output identity'):
        stages.verify_stage(tmp_path)


def test_receipt_rejects_escape_and_changed_implementation(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match='contained'):
        stages.write_stage(tmp_path, 'test', {}, {}, ['../outside'], {})
    (tmp_path / 'result.txt').write_text('complete')
    stages.write_stage(tmp_path, 'test', {}, {}, ['result.txt'], {})
    monkeypatch.setattr(stages, 'implementation_identity', lambda: {'sha256': 'changed'})
    with pytest.raises(ValueError, match='Implementation/runtime'):
        stages.verify_stage(tmp_path, require_current_implementation=True)


def test_material_source_and_semantic_resource_invalidate_reuse(tmp_path, monkeypatch):
    package = tmp_path / 'package'
    package.mkdir()
    source = package / 'calculator.py'
    resource = package / 'semantics.json'
    source.write_text('HORIZON = 300\n')
    resource.write_text('{"interval": "[t-1s,t)"}')
    monkeypatch.setattr(stages, '__file__', str(package / 'stages.py'))
    output = tmp_path / 'output'
    output.mkdir()
    (output / 'result.txt').write_text('result')
    stages.write_stage(output, 'test', {}, {}, ['result.txt'], {})
    original = stages.implementation_identity()['sha256']
    source.write_text('HORIZON = 60\n')
    assert stages.implementation_identity()['sha256'] != original
    with pytest.raises(ValueError, match='Implementation/runtime'):
        stages.verify_stage(output, require_current_implementation=True)
    source.write_text('HORIZON = 300\n')
    resource.write_text('{"interval": "(t-1s,t]"}')
    assert stages.implementation_identity()['sha256'] != original
    with pytest.raises(ValueError, match='Implementation/runtime'):
        stages.verify_stage(output, require_current_implementation=True)


def test_runtime_identity_includes_transitive_data_and_render_dependencies():
    dependencies = stages.implementation_identity()['dependencies']
    assert {'numpy', 'pyarrow', 'duckdb', 'botocore', 'pillow', 'packaging'} <= set(dependencies)
    assert all(dependencies[name] != 'not-installed' for name in ('botocore', 'pillow'))
