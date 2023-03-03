"""Meta is a Marshmallow-specific concept and is a way of
configuring Schema load/dump behavior recursively.

We have an additional layer of configuration for Schema _generation_,
and that lives in _config.py.
"""
import typing as ty

SchemaMeta = ty.NewType("SchemaMeta", type)


_META_DEFAULTS = dict(ordered=True)
# We see no reason to ever throw away the order defined by the programmer.


def meta(**meta) -> SchemaMeta:
    return type("Meta", (), {**_META_DEFAULTS, **meta})  # type: ignore
