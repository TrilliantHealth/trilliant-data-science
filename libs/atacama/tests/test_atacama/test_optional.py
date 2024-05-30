import typing as ty

import attrs

from thds.atacama import ordered


@attrs.frozen
class _AttrsClass:
    # union_value: int | None
    optional_value: ty.Optional[int]


@attrs.frozen
class _OptionalClass:
    # UnionType only works for python > 3.10
    # union_int: int | None
    # union_list: list[int] | None
    # union_attrs: _AttrsClass | None
    optional_int: ty.Optional[int]
    optional_list: ty.Optional[ty.List[str]]
    optional_attrs: ty.Optional[_AttrsClass]


def test_optional():
    schema = ordered(_OptionalClass)
    schema().load(dict(optional_int=None, optional_list=["1"], optional_attrs=dict(optional_value=None)))
