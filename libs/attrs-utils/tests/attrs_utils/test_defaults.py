import typing as ty
from datetime import datetime

import attrs

from thds.attrs_utils.defaults import attrs_value_defaults


@attrs.define
class NoDefaults:
    id: int
    name: str
    age: float


def test_no_defaults():
    assert not attrs_value_defaults(NoDefaults)


@attrs.define
class BasicDefaults:
    id: int
    type: str = "foo"
    when: ty.Optional[datetime] = None


def test_basic_defaults():
    assert dict(type="foo", when=None) == attrs_value_defaults(BasicDefaults)


@attrs.define
class FactoryDefaults:
    id: int
    tiles: ty.Dict[str, int] = attrs.Factory(dict)
    takes_self: ty.Any = attrs.Factory(lambda self: self.id, takes_self=True)


def test_factory_defaults():
    assert dict(tiles={}) == attrs_value_defaults(FactoryDefaults)
