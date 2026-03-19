import dataclasses
import typing as ty
import warnings

import attrs
import cattrs
import pytest

from thds.attrs_utils.cattrs import DEFAULT_JSON_CONVERTER
from thds.attrs_utils.cattrs.custom_hooks import (
    Struct,
    T,
    register_structure_hook_with_defaults,
    structure_hook_with_defaults,
)


@attrs.define
class FooAttrs:
    a: int
    b: str
    c: bool
    d: float | None = None


@dataclasses.dataclass
class FooDataclass:
    a: int
    b: str
    c: bool
    d: float | None = None


@pytest.fixture
def converter() -> cattrs.Converter:
    return DEFAULT_JSON_CONVERTER.copy()


@pytest.mark.parametrize(
    "cls",
    [pytest.param(FooAttrs, id="attrs"), pytest.param(FooDataclass, id="dataclass")],
)
@pytest.mark.parametrize(
    "defaults, override, input_data, expected",
    [
        pytest.param(
            {"c": True},
            False,
            {"a": 1, "b": "x"},
            {"a": 1, "b": "x", "c": True, "d": None},
            id="fill-missing-field",
        ),
        pytest.param(
            {"c": True},
            False,
            {"a": 1, "b": "x", "c": False},
            {"a": 1, "b": "x", "c": False, "d": None},
            id="input-takes-precedence-over-default",
        ),
        pytest.param(
            {"b": "default_b", "c": False},
            False,
            {"a": 1},
            {"a": 1, "b": "default_b", "c": False, "d": None},
            id="multiple-defaults-fill-missing",
        ),
        pytest.param(
            {"d": 3.14},
            True,
            {"a": 1, "b": "x", "c": True},
            {"a": 1, "b": "x", "c": True, "d": 3.14},
            id="override-true-uses-supplied-default-over-field-default",
        ),
        pytest.param(
            {"c": True, "d": 2.0},
            True,
            {"a": 1, "b": "x", "c": False, "d": 9.0},
            {"a": 1, "b": "x", "c": False, "d": 9.0},
            id="input-still-wins-even-with-override-true",
        ),
    ],
)
def test_structure_hook_with_defaults(
    cls: ty.Type[T],
    defaults: dict[str, ty.Any],
    override: bool,
    input_data: dict,
    expected: dict,
    converter: cattrs.Converter,
) -> None:
    hook: Struct[T] = structure_hook_with_defaults(DEFAULT_JSON_CONVERTER, cls, defaults, override)
    expected_ = cls(**expected)
    assert hook(input_data, cls) == expected_

    register_structure_hook_with_defaults(converter, DEFAULT_JSON_CONVERTER, cls, defaults, override)
    assert converter.structure(input_data, cls) == expected_


@pytest.mark.parametrize(
    "cls",
    [pytest.param(FooAttrs, id="attrs"), pytest.param(FooDataclass, id="dataclass")],
)
def test_override_false_warns_and_ignores_field_with_existing_default(
    cls: ty.Type[FooAttrs], converter: cattrs.Converter
) -> None:
    """When override=False and a default is supplied for a field that already has a default
    in the type definition, the supplied default is ignored and a warning is emitted."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        hook = structure_hook_with_defaults(DEFAULT_JSON_CONVERTER, cls, {"d": 3.14}, override=False)

    assert len(caught) == 1
    assert "already have default values" in str(caught[0].message)
    assert "'d'" in str(caught[0].message)
    # d should use the field's own default (None), not 3.14
    input_ = {"a": 1, "b": "x", "c": True}
    expected = cls(a=1, b="x", c=True, d=None)
    assert hook(input_, cls) == expected

    register_structure_hook_with_defaults(
        converter, DEFAULT_JSON_CONVERTER, cls, {"d": 3.14}, override=False
    )
    assert converter.structure(input_, cls) == expected


@pytest.mark.parametrize(
    "cls",
    [pytest.param(FooAttrs, id="attrs"), pytest.param(FooDataclass, id="dataclass")],
)
def test_unknown_field_warns(cls: ty.Type[FooAttrs], converter: cattrs.Converter) -> None:
    """Supplying defaults for fields not present in the type emits a warning and ignores them."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        hook = structure_hook_with_defaults(
            DEFAULT_JSON_CONVERTER, cls, {"c": True, "z": 42}, override=False
        )

    assert len(caught) == 1
    assert "not present" in str(caught[0].message)
    assert "'z'" in str(caught[0].message)
    # hook should still work with the valid default
    input_ = {"a": 1, "b": "x"}
    expected = cls(a=1, b="x", c=True, d=None)
    assert hook(input_, cls) == expected

    register_structure_hook_with_defaults(
        converter, DEFAULT_JSON_CONVERTER, cls, {"c": True, "z": 42}, override=False
    )
    assert converter.structure(input_, cls) == expected


@pytest.mark.parametrize(
    "cls",
    [pytest.param(FooAttrs, id="attrs"), pytest.param(FooDataclass, id="dataclass")],
)
def test_all_defaults_unknown_returns_plain_structure(
    cls: ty.Type[FooAttrs], converter: cattrs.Converter
) -> None:
    """When all supplied defaults refer to unknown fields, the hook degrades to a plain structure call."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        hook = structure_hook_with_defaults(DEFAULT_JSON_CONVERTER, cls, {"z": 42}, override=False)

    assert len(caught) == 1
    input_ = {"a": 1, "b": "x", "c": True}
    expected = cls(a=1, b="x", c=True, d=None)
    result = hook(input_, cls)
    assert result == expected

    base_converter = DEFAULT_JSON_CONVERTER.copy()
    register_structure_hook_with_defaults(
        base_converter, DEFAULT_JSON_CONVERTER, cls, {"z": 42}, override=False
    )
    assert base_converter.structure(input_, cls) == expected


@pytest.mark.parametrize(
    "cls",
    [pytest.param(FooAttrs, id="attrs"), pytest.param(FooDataclass, id="dataclass")],
)
def test_badly_typed_default_raises(cls: ty.Type[FooAttrs]) -> None:
    """Supplying a default whose type doesn't match the field's annotation raises TypeError."""
    with pytest.raises(TypeError, match="not of the expected type"):
        structure_hook_with_defaults(DEFAULT_JSON_CONVERTER, cls, {"c": "not_a_bool"}, override=False)


def test_non_record_type_raises() -> None:
    """Passing a type that is neither attrs nor a dataclass raises TypeError."""

    class Plain:
        x: int

    with pytest.raises(TypeError, match="only supports attrs or dataclass"):
        structure_hook_with_defaults(DEFAULT_JSON_CONVERTER, Plain, {"x": 1})
