"""Endpoint/EW and retained legacy query interfaces."""

from .endpoint_reader import describe_endpoint_fields, iter_endpoint_batches
from .endpoint_release import (
    build_endpoint_full_reference,
    build_endpoint_reference,
    build_endpoint_scaling_reference,
    open_endpoint_reference,
)
from .endpoint_selection import EndpointSelection

__all__ = (
    "EndpointSelection",
    "build_endpoint_full_reference",
    "build_endpoint_reference",
    "build_endpoint_scaling_reference",
    "open_endpoint_reference",
    "describe_endpoint_fields",
    "iter_endpoint_batches",
)
