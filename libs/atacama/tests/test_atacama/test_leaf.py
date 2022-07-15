import marshmallow.fields
import typing_inspect
from typing_extensions import Literal

from core.atacama.leaf import NATIVE_TO_MARSHMALLOW, DynamicLeafTypeMapping


def _assert_core_native_types(mapping):
    assert marshmallow.fields.Integer == mapping[int]
    assert marshmallow.fields.Float == mapping[float]
    assert marshmallow.fields.String == mapping[str]


def test_base_leaf_type_mapping():
    _assert_core_native_types(NATIVE_TO_MARSHMALLOW)


def test_dynamic_leaf_type_mapping():
    def handle_literals(obj):
        if typing_inspect.is_literal_type(obj):
            return typing_inspect.get_args(obj)
        return None

    dm = DynamicLeafTypeMapping(NATIVE_TO_MARSHMALLOW, [handle_literals])

    _assert_core_native_types(dm)
    assert ("s", "p") == dm[Literal["s", "p"]]
