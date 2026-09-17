"""Structural interfaces only. Production builders are implemented in phases 2/3."""
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Protocol
from .config import DEFAULT_CONFIG, FeatureConfig, ContractError


@dataclass(frozen=True)
class BuildResult:
    member_identity: str
    contract_identity: str
    manifest_path: Path
    rows: int


@dataclass(frozen=True)
class CompletedMember:
    """Only completed members are reusable in the first production protocol."""
    member_identity: str
    input_identity: str
    contract_identity: str
    implementation_identity: str
    manifest_sha256: str
    rows: int

    def __post_init__(self):
        for name in ('member_identity', 'input_identity', 'contract_identity', 'implementation_identity', 'manifest_sha256'):
            value = getattr(self, name)
            if type(value) is not str or not re.fullmatch(r'[0-9a-f]{64}', value):
                raise ContractError("completion identities must be SHA-256 values")
        if type(self.rows) is not int or not 1 <= self.rows <= 57600:
            raise ContractError("invalid completion row count")

    def require_match(self, expected):
        if self != expected:
            raise ContractError("completed-member identities differ; explicit rebuild required")


class BaseBuilder(Protocol):
    def __call__(self, source_pair: Path, member_context: Path, output: Path, *,
                 config: FeatureConfig = DEFAULT_CONFIG, batch_size: int = 4096) -> BuildResult: ...


class FeatureBuilder(Protocol):
    def __call__(self, base_partition: Path, output: Path, *,
                 config: FeatureConfig = DEFAULT_CONFIG, batch_size: int = 4096) -> BuildResult: ...
