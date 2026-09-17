"""Executable endpoint/EW definitions; not a production feature builder."""
from .config import ContractError, EWView, FeatureConfig, DEFAULT_CONFIG
from .identity import contract_identity, contract_descriptor, implementation_identity
from .registry import feature_registry, query_registry
from .schemas import BASE_SCHEMA, FEATURE_SCHEMA, SUPPORT_SCHEMA, base_schema, feature_schema, support_schema
from .reasons import Reason, SourceStatus, MidpointAgeStatus, decode_reasons

__all__ = ["ContractError", "EWView", "FeatureConfig", "DEFAULT_CONFIG", "contract_identity",
           "contract_descriptor", "implementation_identity", "feature_registry", "query_registry",
           "BASE_SCHEMA", "FEATURE_SCHEMA", "SUPPORT_SCHEMA", "base_schema", "feature_schema",
           "support_schema", "Reason", "SourceStatus", "MidpointAgeStatus", "decode_reasons"]
