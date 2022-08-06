"""Built-in generators recommended for use."""
from ._cache import attrs_schema_cache
from ._meta import meta
from .nonempty import nonempty_validator_xf
from .schemas import SchemaGenerator

neo = SchemaGenerator(meta(), [nonempty_validator_xf], cache=attrs_schema_cache())
"""Non-Empty, Ordered - the preferred default API."""

ordered = SchemaGenerator(meta(), list(), cache=attrs_schema_cache())
"""Ordered, but allows empty values for required fields. Prefer neo for new usage."""
