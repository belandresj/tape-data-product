"""Execute a script with the same no-network/no-sibling-read guard as tests."""
from pathlib import Path
import runpy
import sys
ROOT=Path(__file__).resolve().parents[1]
runpy.run_path(str(ROOT/'tests/conftest.py'))
script=Path(sys.argv[1]).resolve()
sys.argv=sys.argv[1:]
runpy.run_path(str(script),run_name='__main__')
