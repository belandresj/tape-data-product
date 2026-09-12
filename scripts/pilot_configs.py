"""Export the four fixed historical participation pilot configurations."""
import argparse
import json
from pathlib import Path
from tape_data_product.experiments.threshold_pilot import variants
from tape_data_product.query.tape_cohort_config import query_hash

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=False)
for name, config in variants():
    (args.output/(name+'.json')).write_text(json.dumps(config, indent=2)+'\n')
    print(name, query_hash(config))
