"""Compatibility entry point for the installed complete offline demonstration."""
import argparse
import json
from pathlib import Path
from tape_data_product.demo import run_demo

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run_demo(args.output), indent=2))
