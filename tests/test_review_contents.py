"""Publication checks must bind exceptions and links to the staged bytes."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def make_repo(tmp_path):
    subprocess.run(['git', 'init', '-q', str(tmp_path)], check=True)
    (tmp_path / 'scripts').mkdir()
    shutil.copy2(ROOT / 'scripts/review_contents.py', tmp_path / 'scripts/review_contents.py')
    return tmp_path


def stage(root):
    subprocess.run(['git', 'add', '.'], cwd=root, check=True)


def review(root):
    result = subprocess.run([sys.executable, str(root / 'scripts/review_contents.py')],
                            cwd=root, capture_output=True, text=True)
    return result.returncode, json.loads(result.stdout)['findings']


def test_publication_image_requires_matching_staged_identity(tmp_path):
    root = make_repo(tmp_path)
    assets = root / 'reports/report_assets'
    assets.mkdir(parents=True)
    data = b'\x89PNG\r\n\x1a\n' + b'0' * (600 * 1024)
    picture = assets / 'example.png'
    picture.write_bytes(data)
    (assets / 'publication_manifest.json').write_text(json.dumps({'files': {
        'example.png': {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}}}))
    stage(root)
    assert review(root) == (0, [])
    # Local changes cannot replace the indexed publication identity.
    picture.write_bytes(data + b'changed')
    assert review(root) == (0, [])
    stage(root)
    code, findings = review(root)
    assert code == 1
    assert any(reason == 'publication asset missing or identity mismatch' for _, reason in findings)


def test_unlisted_image_and_untracked_link_target_are_rejected(tmp_path):
    root = make_repo(tmp_path)
    (root / 'README.md').write_text('[Required file](local-only.md)')
    (root / 'unapproved.png').write_bytes(b'\x89PNG\r\n\x1a\n')
    stage(root)
    (root / 'local-only.md').write_text('Exists only in this checkout.')
    code, findings = review(root)
    assert code == 1
    assert ['unapproved.png', 'excluded file type'] in findings
    assert ['README.md', 'Markdown target absent from Git index: local-only.md'] in findings
