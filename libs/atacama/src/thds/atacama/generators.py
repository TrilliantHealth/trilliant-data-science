from ._meta import meta
from .nonempty import nonempty_validator_xf
from .schemas import SchemaGenerator

neo = SchemaGenerator(meta(), [nonempty_validator_xf])
"""Non-Empty, Ordered - the preferred default API."""

ordered = SchemaGenerator(meta(), list())
"""Ordered, but allows empty values for required fields. Prefer neo for new usage."""
