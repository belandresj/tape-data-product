from pathlib import Path
from ..contracts import DEFAULT_CONFIG
from ..contracts.config import FeatureConfig
from ..integrity import read_json

def _config(path):return DEFAULT_CONFIG if path is None else FeatureConfig.from_dict(read_json(path))

def register_commands(commands):
    build=commands.add_parser("build-from-base",help="Build endpoint/EW features from completed base only");build.add_argument("--base",required=True);build.add_argument("--output",required=True);build.add_argument("--config");build.add_argument("--batch-size",type=int,default=4096);build.set_defaults(func=_build)
    verify=commands.add_parser("verify-from-base",help="Verify feature/support companions");verify.add_argument("--input",required=True);verify.add_argument("--base",required=True);verify.add_argument("--config");verify.add_argument("--reconstruction",action="store_true");verify.set_defaults(func=_verify)

def _build(a):
    from .endpoint_ew import build_from_base
    r=build_from_base(a.base,a.output,config=_config(a.config),batch_size=a.batch_size)
    return {"member_identity":r.member_identity,"contract_identity":r.contract_identity,"manifest_path":str(r.manifest_path),"rows":r.rows}

def _verify(a):
    from .endpoint_ew import verify_feature_partition
    result=verify_feature_partition(a.input,a.base,config=_config(a.config))
    return {"input":str(Path(a.input).resolve()),"rows":result["coverage"]["expected_rows"],"integrity":"passed","reconstruction":"not_run" if not a.reconstruction else "unsupported_without_reference_oracle"}
