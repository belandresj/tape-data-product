"""Generate the four historical descriptive pilot variants without private data."""
import argparse
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src/04_research'))
from tape_cohort_config import normalize_config,query_hash
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,default=ROOT/'output/pilot-configs');a=p.parse_args()
a.output.mkdir(parents=True,exist_ok=False)
base=json.loads((ROOT/'config/tape_cohort_300s_ms3_p050_selected_v1.json').read_text())
for name,bounds in [('A',None),('B',(.4,.32)),('C',(.5,.4)),('D',(.6,.48))]:
    cfg=json.loads(json.dumps(base))
    if bounds is None:cfg['conditions']=[x for x in cfg['conditions'] if x['feature']!='movement_participation_300s']
    else:
        condition=next(x for x in cfg['conditions'] if x['feature']=='movement_participation_300s')
        condition['entry']['lower'],condition['continuation']['lower']=bounds
    cfg=normalize_config(cfg);(a.output/(name+'.json')).write_text(json.dumps(cfg,indent=2)+'\n')
    print(name,query_hash(cfg))
