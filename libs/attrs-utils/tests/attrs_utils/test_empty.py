import typing as ty

import attr
import pytest

from thds.attrs_utils.empty import empty_gen

MyInt = ty.NewType("MyInt", int)


@attr.define
class AllDefaults:
    a: int = 1
    b: str = "default"
    c: list = attr.field(factory=list)


@attr.define
class Collections:
    s: ty.Set[str]
    d: ty.Dict[str, int]
    l: ty.List[int] = attr.field(factory=lambda: [1])
    t: ty.Tuple[int, ...] = (1, 2, 3)


@attr.define
class Nested:
    i: MyInt
    t: ty.Tuple[Collections, bool]
    d: ty.Annotated[AllDefaults, "annotation"] = attr.field(factory=AllDefaults)


@pytest.mark.parametrize(
    "cls, args, kwargs, expected",
    [
        (AllDefaults, (), {}, AllDefaults()),
        (AllDefaults, (123, "custom"), {}, AllDefaults(123, "custom")),
        (AllDefaults, (), {"a": 42}, AllDefaults(a=42)),
        (AllDefaults, (), {"c": [1, 1, 2, 3]}, AllDefaults(c=[1, 1, 2, 3])),
        (Collections, (), {}, Collections(set(), {})),
        (Collections, ({"a", "b"},), {}, Collections({"a", "b"}, {})),
        (Collections, (), {"s": {"x", "y"}, "l": []}, Collections(s={"x", "y"}, d={}, l=[])),
        (Collections, ({"x", "y"},), {"t": (123,)}, Collections({"x", "y"}, {}, t=(123,))),
        (Nested, (), {}, Nested(MyInt(0), (Collections(set(), {}), False), AllDefaults())),
        (
            Nested,
            (MyInt(7), (Collections({"a"}, {"b": 1}), True)),
            {},
            Nested(MyInt(7), (Collections({"a"}, {"b": 1}), True), AllDefaults()),
        ),
        (
            Nested,
            (),
            {"i": MyInt(99), "d": AllDefaults(a=10)},
            Nested(i=MyInt(99), t=(Collections(set(), {}), False), d=AllDefaults(a=10)),
        ),
        (
            Nested,
            (MyInt(123),),
            {"t": (Collections({"x"}, {"y": 2}), True)},
            Nested(MyInt(123), t=(Collections({"x"}, {"y": 2}), True)),
        ),
        (type(None), (), {}, None),
        *(
            (t, (), {}, t())
            for t in (
                int,
                str,
                float,
                bool,
                bytes,
                bytearray,
                list,
                dict,
                set,
                frozenset,
                tuple,
            )
        ),
    ],
)
def test_empty_attrs(cls: ty.Type, args: ty.Tuple, kwargs: ty.Dict, expected):
    factory = empty_gen(cls)
    instance = factory(*args, **kwargs)

    if isinstance(cls, type):
        assert isinstance(instance, cls)
        assert type(instance) is cls

    assert instance == expected
