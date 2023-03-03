from typing import Optional, Union

import marshmallow
import pytest
from attrs import define
from typing_extensions import Literal

from thds.atacama import ordered

LitA = Literal["1", "2"]
LitB = Literal["3", "4"]

LiteralUnionAlias = Union[LitA, LitB]


@define
class ThingWithUnion:
    name: str
    opt_union_lit: Optional[LiteralUnionAlias]


def test_union_of_literals_preserved():
    schema = ordered(ThingWithUnion)
    schema().load(dict(name="foo", opt_union_lit=None))
    schema().load(dict(name="foo", opt_union_lit="1"))
    with pytest.raises(marshmallow.exceptions.ValidationError):
        schema().load(dict(name="foo", opt_union_lit="5"))
