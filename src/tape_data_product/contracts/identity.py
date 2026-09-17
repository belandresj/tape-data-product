"""Semantic, schema and local implementation identities are separate claims."""
from dataclasses import asdict
import hashlib
from pathlib import Path
from .config import CONTRACT_VERSION, DEFAULT_CONFIG, digest
from .schemas import BASE_SCHEMA, feature_schema, support_schema, schema_hash
from .registry import query_registry
from .policy import TRANSITIONS, RTH_TRADE_CONDITIONS, EXTENDED_TRADE_CONDITIONS, CAUSAL_CORRECTIONS, SEMANTIC_CODES
from .reasons import Reason


def contract_descriptor(config=DEFAULT_CONFIG):
    return {"version": CONTRACT_VERSION, "config": config.to_dict(),
            "schemas": {"base": schema_hash(BASE_SCHEMA), "features": schema_hash(feature_schema(config)),
                        "support": schema_hash(support_schema(config))},
            "registry": [asdict(f) for f in query_registry(config)],
            "reasons": {r.name: int(r) for r in Reason},
            "transitions": {e.value: asdict(t) for e, t in TRANSITIONS.items()},
            "timing": {"interval": "[t-1s,t)", "endpoint": "strictly prior",
                       "return_lag_seconds": 5, "lag_validity": "two endpoints, no source break/halt",
                       "return_estimator": "EW uncentered RMS, one-second updates",
                       "quantile": "linear p90 of known wall-clock endpoint ages",
                       "quote_age": "structurally accepted message, price-independent",
                       "spread": "positive numeric locks included as zero; contradictory locks unsupported",
                       "rth_conditions": RTH_TRADE_CONDITIONS, "extended_conditions": EXTENDED_TRADE_CONDITIONS,
                       "causal_corrections": CAUSAL_CORRECTIONS, "vendor_codes": SEMANTIC_CODES},
            "restart": "verify completed member or reconstruct from session-start; no partial-state resume"}


def contract_identity(config=DEFAULT_CONFIG):
    return digest(contract_descriptor(config))


def implementation_identity():
    root = Path(__file__).parent
    files = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.glob('*.py'))}
    return {"files": files, "sha256": digest(files)}
