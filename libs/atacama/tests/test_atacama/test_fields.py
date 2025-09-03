"""Test various field types that we support. Much thanks to `desert` for this mostly-unchanged code."""

import typing as ty
from enum import Enum

import attrs
import marshmallow as ma
import pytest
from typing_extensions import Literal

from thds.atacama import neo


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


def test_literals_are_supported_out_of_the_box():
    @attrs.define
    class Boo:
        a: int
        lit: Literal["s", "p"] = "s"

    BooS = neo(Boo)
    BooS().load(dict(a=3, lit="p"))
    BooS().load(dict(a=3, lit="s"))
    with pytest.raises(ma.exceptions.ValidationError) as e_info:
        BooS().load(dict(a=4, lit="blah"))
    assert str(e_info.value) == "{'lit': ['Must be one of: s, p.']}"

    @attrs.define
    class Goo:
        lit: Literal["L", 2, True]

    GooS = neo(Goo)
    assert GooS().load(dict(lit=True)).lit is True
    assert GooS().load(dict(lit=2)).lit == 2
    assert GooS().load(dict(lit="L")).lit == "L"


def test_unions_of_literals_inside_optionals_are_collapsed():
    L1 = Literal["a", "b"]
    L2 = Literal["c", "d"]

    @attrs.define
    class Roo:
        lit: ty.Optional[ty.Union[L1, L2]] = None

    RooS = neo(Roo)

    roo = RooS().load(dict(lit="d"))
    assert roo.lit == "d"

    assert RooS().load(dict(lit=None)).lit is None
    assert RooS().load(dict()).lit is None

    with pytest.raises(ma.exceptions.ValidationError):
        RooS().load(dict(lit="z"))


def test_new_types_are_unnested():
    MyInt = ty.NewType("MyInt", int)

    @attrs.define
    class Moo:
        my_int: MyInt

    MooS = neo(Moo)

    moo = MooS().load(dict(my_int=4))
    assert moo.my_int == 4
    assert type(moo.my_int) is int


def test_new_types_are_recursively_handled():
    MyFloat = ty.NewType("MyFloat", float)
    MyFancyFloat = ty.NewType("MyFancyFloat", MyFloat)

    @attrs.define
    class Goo:
        my_fancy_float: MyFancyFloat

    GooS = neo(Goo)

    goo = GooS().load(dict(my_fancy_float=42.42))
    assert goo.my_fancy_float == 42.42
    assert type(goo.my_fancy_float) is float
