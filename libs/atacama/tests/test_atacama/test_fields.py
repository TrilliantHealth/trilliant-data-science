"""Test various field types that we support. Much thanks to `desert` for this mostly-unchanged code."""
import typing as ty
from enum import Enum

import attrs
import marshmallow as ma

from core.atacama import neo


class MyEnum(Enum):
    one = 1
    two = 2


SStr = ty.NewType("SStr", str)


@attrs.define
class Crazy:
    two_tuple: ty.Tuple[float, str]
    vtuple: ty.Tuple[int, ...] = tuple()
    union: ty.Union[int, float, str] = ""
    enum: ty.Optional[MyEnum] = None
    sstr: SStr = SStr("")
    crazy: ty.Optional["Crazy"] = None
    ddict: ty.Dict[str, int] = attrs.Factory(dict)


def test_serde_no_recurse():
    cs = neo(Crazy)

    assert cs().load(
        dict(
            two_tuple=[1.2, "three"],
            vtuple=[1, 2, 3, 4],
            union="five",
            sstr="whoa",
            enum="one",
            crazy=None,
            ddict=dict(a=2, b=3),
        )
    ).vtuple == (1, 2, 3, 4)


def test_serde_recurse():
    cs = neo(Crazy)

    assert cs().load(
        dict(
            two_tuple=[1.2, "three"],
            vtuple=[1, 2, 3, 4],
            union="five",
            sstr="whoa",
            enum="one",
            crazy=dict(two_tuple=[4.4, "seven"]),
            ddict=dict(a=2, b=3),
        )
    ).crazy.two_tuple == (4.4, "seven")


def test_generate_dict_value_type_from_simple():
    @attrs.define
    class Foo:
        dd: ty.Dict[str, float]

    foos = neo(Foo)

    assert type(foos().fields["dd"].value_field) == ma.fields.Float  # type: ignore


def test_generate_number_field_from_float_int_union():
    @attrs.define
    class Foo:
        dd: ty.Dict[str, ty.Union[float, int]]

    foos = neo(Foo)

    assert type(foos().fields["dd"].value_field) == ma.fields.Number  # type: ignore


def test_union_types_are_hash_equal():
    assert ty.Union[float, int] == ty.Union[int, float]
    assert ty.Union[int, float] in {ty.Union[float, int]: "foo"}


def test_raw_allows_none_by_default():
    """This used to require raw: ty.Optional[ty.Any] because Raw does not accept None by default."""

    @attrs.define
    class Bar:
        raw: ty.Any

    neo(Bar)().load(dict(raw=None))
