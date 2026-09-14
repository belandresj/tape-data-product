from pathlib import Path

from ..contracts import DEFAULT_CONFIG
from ..contracts.config import FeatureConfig
from ..integrity import read_json


def _config(path): return DEFAULT_CONFIG if path is None else FeatureConfig.from_dict(read_json(path))


def register_commands(commands):
    base=commands.add_parser("base",help="Admit, build, and verify one-second base partitions").add_subparsers(dest="base_command",required=True)
    admit=base.add_parser("admit");admit.add_argument("--inventory",required=True);admit.add_argument("--evidence",required=True);admit.add_argument("--output",required=True);admit.set_defaults(func=_admit)
    build=base.add_parser("build");build.add_argument("--source-pair",required=True);build.add_argument("--context",required=True);build.add_argument("--output",required=True);build.add_argument("--config");build.add_argument("--batch-size",type=int,default=4096);build.set_defaults(func=_build)
    verify=base.add_parser("verify");verify.add_argument("--input",required=True);verify.add_argument("--reconstruction",action="store_true");verify.add_argument("--source-pair");verify.set_defaults(func=_verify)


def _admit(a):
    from .admission import admit_inventory
    return admit_inventory(a.inventory,a.evidence,a.output)


def _build(a):
    from .builder import build_base_partition
    r=build_base_partition(a.source_pair,a.context,a.output,config=_config(a.config),batch_size=a.batch_size)
    return {"member_identity":r.member_identity,"contract_identity":r.contract_identity,"manifest_path":str(r.manifest_path),"rows":r.rows}


def _verify(a):
    from .builder import verify_base_partition
    if a.reconstruction and not a.source_pair:raise ValueError("--reconstruction requires --source-pair")
    result=verify_base_partition(a.input)
    # Reconstruction is intentionally a separate verifier obligation; never relabel integrity as reconstruction.
    return {"input":str(Path(a.input).resolve()),"rows":result["coverage"]["expected_rows"],"integrity":"passed","reconstruction":"not_run" if not a.reconstruction else "unsupported_without_reference_oracle"}
