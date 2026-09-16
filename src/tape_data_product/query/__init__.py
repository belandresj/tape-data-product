"""Endpoint/EW and retained legacy query interfaces."""

from .endpoint_reader import describe_endpoint_fields, iter_endpoint_batches
from .endpoint_release import build_endpoint_reference, open_endpoint_reference
from .endpoint_selection import EndpointSelection
from .endpoint_catalog import build_endpoint_query_catalog
from .endpoint_database import TapeDatabase, open_tape_database

__all__ = (
    "EndpointSelection",
    "build_endpoint_reference",
    "build_endpoint_query_catalog",
    "open_endpoint_reference",
    "open_tape_database",
    "TapeDatabase",
    "describe_endpoint_fields",
    "iter_endpoint_batches",
)
