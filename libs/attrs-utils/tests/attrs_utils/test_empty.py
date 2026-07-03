import dataclasses
import inspect
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


_T = ty.TypeVar("_T")


@attr.define
class _Generic(ty.Generic[_T]):
    required: int
    payload: _T
    defaulted: str = "d"


def test_empty_gen_defaults_required_fields_of_parameterized_generic():
    """A subscripted generic alias (Foo[Bar]) reports inspect.signature == (*args, **kwargs) in stock
    CPython, so empty_gen must introspect the origin to still fill required fields. Without that, this
    raises TypeError: missing required argument 'required'."""
    assert list(inspect.signature(_Generic[int]).parameters) == [
        "args",
        "kwargs",
    ]  # the trap we route around

    factory = empty_gen(_Generic[int])
    assert factory() == _Generic(required=0, payload=0)
    assert factory(required=7, payload=9, defaulted="x") == _Generic(7, 9, "x")

    # the generated signature resolves the type var against the concrete param (int), not the bare ~_T
    assert inspect.signature(factory).parameters["payload"].annotation is int


@dataclasses.dataclass
class _DcAllKinds:
    required: int
    valued: str = "d"
    factoried: list = dataclasses.field(default_factory=lambda: [1])


@pytest.mark.parametrize(
    "args, kwargs, expected",
    [
        ((), {}, _DcAllKinds(required=0)),  # required defaulted; value + factory defaults preserved
        ((5,), {}, _DcAllKinds(5)),
        ((), {"required": 7, "valued": "x"}, _DcAllKinds(7, "x")),
        ((), {"factoried": [9]}, _DcAllKinds(required=0, factoried=[9])),
    ],
)
def test_empty_dataclass(args: ty.Tuple, kwargs: ty.Dict, expected):
    """A dataclass field is required only when it has neither `default` nor `default_factory`."""
    instance = empty_gen(_DcAllKinds)(*args, **kwargs)

    assert type(instance) is _DcAllKinds
    assert instance == expected


_U = ty.TypeVar("_U")


@dataclasses.dataclass
class _DcGeneric(ty.Generic[_U]):
    required: int
    payload: _U
    factoried: list = dataclasses.field(default_factory=list)


def test_empty_gen_parameterized_dataclass():
    """A subscripted dataclass alias must still dispatch to the dataclass hook - dataclasses.is_dataclass is
    False for aliases, so dispatch tests the origin - then default its required fields, honor default_factory,
    and resolve the type var in the generated signature."""
    assert not dataclasses.is_dataclass(_DcGeneric[str])  # the trap the dispatch predicate routes around

    factory = empty_gen(_DcGeneric[str])
    assert factory() == _DcGeneric(required=0, payload="")
    assert factory(required=7, payload="p") == _DcGeneric(7, "p")
    assert inspect.signature(factory).parameters["payload"].annotation is str


class _Unhandled:
    """Not attrs / dataclass / namedtuple / collection / ... - hits the `unknown_type` fallback."""


def test_empty_gen_unknown_type_raises_helpful_typeerror():
    # the message template defers {!r} to str.format; regression guard against it carrying an unfilled
    # named field (which would surface as a confusing KeyError instead of this TypeError).
    with pytest.raises(
        TypeError, match=f"Don't know how to generate an 'empty' value for type {_Unhandled!r}"
    ):
        empty_gen(_Unhandled)
