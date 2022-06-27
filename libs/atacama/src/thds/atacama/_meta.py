import typing as ty

SchemaMeta = ty.NewType("SchemaMeta", type)


_META_DEFAULTS = dict(ordered=True)
# We see no reason to ever throw away the order defined by the programmer.


def meta(**meta) -> SchemaMeta:
    return type("Meta", (), {**_META_DEFAULTS, **meta})  # type: ignore
