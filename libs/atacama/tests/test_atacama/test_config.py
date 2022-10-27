import typing as ty

import pytest
from attrs import Factory, define
from marshmallow.exceptions import ValidationError

from core.atacama import config, neo


@define
class Foo:
    id: str  # always required
    count: int = 0
    bars: ty.List[float] = Factory(list)


def test_fields_required_based_on_defaults_normally():
    schema = neo(Foo)

    assert schema().load(dict(id="here")).count == 0

    with pytest.raises(ValidationError):
        # id is required
        schema().load(dict(count=4))


def test_all_fields_required_if_require_all():
    schema = neo(Foo, config(require_all=True))

    with pytest.raises(ValidationError):
        # bars is required
        schema().load(dict(id="id", count=3))
    with pytest.raises(ValidationError):
        # count is required
        schema().load(dict(id="id", bars=[1.2, 2.3]))

    schema().load(dict(id="id", count=4, bars=[0.0, 0.1]))


def test_name_suffix():
    assert "Input" not in neo(Foo).__name__
    assert "InputSchema" in neo(Foo, config(schema_name_suffix="Input")).__name__
