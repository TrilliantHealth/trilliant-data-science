import marshmallow.fields
import pytest
import typing_inspect
from marshmallow import ValidationError
from typing_extensions import Literal

from core.atacama.leaf import NATIVE_TO_MARSHMALLOW, DynamicLeafTypeMapping, handle_literals


def _assert_core_native_types(mapping):
    assert marshmallow.fields.Integer == mapping[int]
    assert marshmallow.fields.Float == mapping[float]
    assert marshmallow.fields.String == mapping[str]


def test_base_leaf_type_mapping():
    _assert_core_native_types(NATIVE_TO_MARSHMALLOW)


def test_handles_literals():
    dm = DynamicLeafTypeMapping(NATIVE_TO_MARSHMALLOW, [handle_literals])
    str_one_of_field = dm[Literal["s", "p"]]
    s_or_p = str_one_of_field()
    assert isinstance(s_or_p, marshmallow.fields.String)
    assert s_or_p.deserialize("s") == "s"
    assert s_or_p.deserialize("p") == "p"
    with pytest.raises(ValidationError):
        s_or_p.deserialize("g")

    assert isinstance(dm[Literal[1, 2]](), marshmallow.fields.Integer)
    assert isinstance(dm[Literal[1.0, 2.0]](), marshmallow.fields.Float)
    assert isinstance(dm[Literal[True, False]](), marshmallow.fields.Boolean)
    assert isinstance(dm[Literal[1, False]](), marshmallow.fields.Raw)


def test_dynamic_leaf_type_mapping():
    def fake_handle_literals(obj):
        if typing_inspect.is_literal_type(obj):
            return typing_inspect.get_args(obj)
        return None

    dm = DynamicLeafTypeMapping(NATIVE_TO_MARSHMALLOW, [fake_handle_literals])

    _assert_core_native_types(dm)
    assert ("s", "p") == dm[Literal["s", "p"]]
