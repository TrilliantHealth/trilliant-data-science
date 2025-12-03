import typing as ty

import attrs
import pytest

from thds.attrs_utils.params import attrs_fields_parameterized, dataclass_fields_parameterized

from .conftest import (
    Bar,
    Baz,
    DBar,
    DBaz,
    DFoo,
    DNonGeneric,
    DNotGenericAtAll,
    DPartiallyGeneric,
    DQux,
    Foo,
    NonGeneric,
    NotGenericAtAll,
    PartiallyGeneric,
    Qux,
    U,
    V,
    W,
    X,
)


@pytest.mark.parametrize(
    "type_, expected_field_types",
    [
        pytest.param(
            Foo[float],
            {"t": ty.Optional[float]},
            id="Foo[float]",
        ),
        pytest.param(
            Qux[int, str],
            {"t": str, "u": int, "v": int, "w": str, "x": int, "baz": Baz[int]},
            id="Qux[int,str]",
        ),
        pytest.param(
            Qux,
            {"t": W, "u": X, "v": X, "w": W, "x": X, "baz": Baz[X]},
            id="Qux",
        ),
        pytest.param(
            Qux[U, V],
            {"t": V, "u": U, "v": U, "w": V, "x": U, "baz": Baz[U]},
            id="Qux[U,V]",
        ),
        pytest.param(
            NonGeneric,
            {"t": str, "u": int, "v": int, "w": str, "x": int, "z": bool, "baz": Baz[int]},
            id="NonGeneric",
        ),
        pytest.param(
            NotGenericAtAll,
            {
                "t": str,
                "u": int,
                "v": int,
                "w": str,
                "x": int,
                "z": bool,
                "baz": Baz[int],
                "bar": Bar[bool],
            },
            id="NotGenericAtAll",
        ),
        pytest.param(
            PartiallyGeneric[float],
            {
                "t": str,
                "u": int,
                "v": int,
                "w": str,
                "x": int,
                "z": bool,
                "baz": Baz[int],
                "foo": Foo[float],
            },
            id="PartiallyGeneric[float]",
        ),
    ],
)
def test_attrs_fields_parameterized(
    type_: ty.Type[attrs.AttrsInstance], expected_field_types: ty.Dict[str, ty.Type]
):
    fields = attrs_fields_parameterized(type_)
    field_types = {field.name: field.type for field in fields}
    assert field_types == expected_field_types


@pytest.mark.parametrize(
    "type_, expected_field_types",
    [
        pytest.param(
            DFoo[float],
            {"t": ty.Optional[float]},
            id="DFoo[float]",
        ),
        pytest.param(
            DQux[int, str],
            {"t": str, "u": int, "v": int, "w": str, "x": int, "baz": DBaz[int]},
            id="DQux[int,str]",
        ),
        pytest.param(
            DQux[U, V],
            {"t": V, "u": U, "v": U, "w": V, "x": U, "baz": DBaz[U]},
            id="DQux[U,V]",
        ),
        pytest.param(
            DQux,
            {"t": W, "u": X, "v": X, "w": W, "x": X, "baz": DBaz[X]},
            id="DQux",
        ),
        pytest.param(
            DNonGeneric,
            {"t": str, "u": int, "v": int, "w": str, "x": int, "z": bool, "baz": DBaz[int]},
            id="DNonGeneric",
        ),
        pytest.param(
            DNotGenericAtAll,
            {
                "t": str,
                "u": int,
                "v": int,
                "w": str,
                "x": int,
                "z": bool,
                "baz": DBaz[int],
                "bar": DBar[bool],
            },
            id="DNotGenericAtAll",
        ),
        pytest.param(
            DPartiallyGeneric[float],
            {
                "t": str,
                "u": int,
                "v": int,
                "w": str,
                "x": int,
                "z": bool,
                "baz": DBaz[int],
                "foo": DFoo[float],
            },
            id="DPartiallyGeneric[float]",
        ),
    ],
)
def test_dataclass_fields_parameterized(type_: ty.Type, expected_field_types: ty.Dict[str, ty.Type]):
    fields = dataclass_fields_parameterized(type_)
    field_types = {field.name: field.type for field in fields}
    assert field_types == expected_field_types
