"""Profile public installed builders on one tiny synthetic fixture."""
import cProfile,json,pstats,runpy
from pathlib import Path
from tape_data_product.replay.builder import build_base_partition
from tape_data_product.features.endpoint_ew import build_from_base
root=Path('/srv/tape-data-product/scratch/feature-readiness-20260915/profile');root.mkdir(parents=True,exist_ok=False)
fixture=runpy.run_path('/opt/tape-data-product/worktrees/feature-readiness/tests/test_endpoint_pipeline.py')['fixture']
pair,context=fixture(root,seconds=1200)
prof=cProfile.Profile();prof.enable()
build_base_partition(pair,context,root/'base')
build_from_base(root/'base',root/'features')
prof.disable();prof.dump_stats(str(root/'profile.pstats'))
st=pstats.Stats(prof)
rows=[]
for (file,line,fn),(cc,nc,tt,ct,callers) in st.stats.items():
 rows.append({'file':file,'line':line,'function':fn,'calls':nc,'self_seconds':tt,'cumulative_seconds':ct})
print(json.dumps({'top_self':sorted(rows,key=lambda x:x['self_seconds'],reverse=True)[:18], 'top_cumulative':sorted(rows,key=lambda x:x['cumulative_seconds'],reverse=True)[:18]},indent=2))
