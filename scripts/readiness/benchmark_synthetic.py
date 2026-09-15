"""Bounded synthetic baseline; never a market-data readiness measurement.

Runs public installed builders on independent invented fixtures. Does not add
multi-worker support to the production calculation runner.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ProcessPoolExecutor
import json
import multiprocessing
from pathlib import Path
import runpy
import time


def build_one(job):
    fixture_path, root, seconds = job
    from tape_data_product.replay.builder import build_base_partition
    from tape_data_product.features.endpoint_ew import build_from_base
    import tape_data_product
    fixture = runpy.run_path(fixture_path)["fixture"]
    root = Path(root); root.mkdir()
    pair, context = fixture(root, seconds=seconds)
    started = time.perf_counter()
    build_base_partition(pair, context, root / "base")
    base_end = time.perf_counter()
    build_from_base(root / "base", root / "features")
    end = time.perf_counter()
    return {"rows":seconds, "base_seconds":base_end-started,
            "feature_seconds":end-base_end,"total_seconds":end-started,
            "base_bytes":(root/'base'/'base.parquet').stat().st_size,
            "feature_bytes":sum((root/'features'/f).stat().st_size for f in ['features.parquet','support.parquet']),
            "installed_package":str(Path(tape_data_product.__file__).resolve())}


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--fixture',required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--workers',type=int,choices=[1,2,4],required=True)
    p.add_argument('--seconds',type=int,default=3600)
    p.add_argument('--tasks',type=int,default=4)
    a=p.parse_args()
    if not 1<=a.seconds<=7200 or not 1<=a.tasks<=4:raise ValueError('synthetic scope exceeded')
    a.output.mkdir(parents=True,exist_ok=False)
    jobs=[(a.fixture,str(a.output/f'member-{i}'),a.seconds) for i in range(a.tasks)]
    start=time.perf_counter()
    with ProcessPoolExecutor(max_workers=a.workers,mp_context=multiprocessing.get_context('spawn')) as pool:
        results=list(pool.map(build_one,jobs))
    elapsed=time.perf_counter()-start
    out={'kind':'synthetic_only_not_external_acceptance','workers':a.workers,
         'tasks':a.tasks,'rows':a.seconds*a.tasks,'wall_seconds':elapsed,
         'aggregate_rows_per_second':a.seconds*a.tasks/elapsed,'members':results}
    (a.output/'measurement.json').write_text(json.dumps(out,indent=2)+'\n')
    print(json.dumps(out))

if __name__=='__main__':main()
