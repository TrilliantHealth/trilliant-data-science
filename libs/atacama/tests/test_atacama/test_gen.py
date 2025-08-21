import typing as ty
from datetime import datetime
from typing import Dict, List, Optional

import attrs
import marshmallow as ma
import pytest

from thds.atacama import neo


@attrs.define
class Quux:
    created_at: datetime
    data: str = ""
    updated_at: ty.Optional[datetime] = None


@attrs.define
class FooBar:
    id: str
    results_size: int = 10
    breakdown_difference_categories: bool = False
    filter_expr: str = ""
    max_geo_dist: Optional[float] = None
    additional_provider_data_fields: Optional[List[str]] = None
    difference_weights: Dict[str, float] = attrs.Factory(dict)
    quux: ty.Optional[Quux] = None


def test_basic():
    foobar_schema_cls = neo(FooBar)
    assert type(foobar_schema_cls().fields["id"]) == ma.fields.String

    fb = foobar_schema_cls().load(dict(id="1234", breakdown_difference_categories=True))
    assert fb.id == "1234"
    assert fb.breakdown_difference_categories
    assert fb.filter_expr == ""
    assert fb.results_size == 10
    assert fb.max_geo_dist is None
    assert fb.additional_provider_data_fields is None
    assert fb.difference_weights == dict()
    assert fb.quux is None


def test_nested():
    def updated_on_15th(dt: datetime):
        if dt.day != 15:
            raise ma.validate.ValidationError("day is not 15")
        return True

    foobar_schema_cls = neo(
        FooBar,
        results_size=neo.field(validate=ma.validate.Range(min=1)),
        quux=neo.nested(validate=ma.validate.Predicate("__str__"))(
            updated_at=neo.field(validate=updated_on_15th),
        ),
    )
    quux = foobar_schema_cls().fields["quux"]
    assert type(quux) == ma.fields.Nested
    quux_s = ty.cast(ma.Schema, quux.nested())  # type: ignore
    assert {"created_at", "data", "updated_at"} == set(quux_s.fields.keys())
    with pytest.raises(ma.validate.ValidationError):
        quux_s.fields["created_at"].validate("")  # type: ignore
    assert not quux_s.fields["data"].validate  # type: ignore
    with pytest.raises(ma.validate.ValidationError):
        quux_s.fields["updated_at"].validate(datetime(2020, 3, 14, 3, 3, 3))  # type: ignore
    assert quux_s.fields["updated_at"].validate(datetime(2020, 3, 15, 3, 3, 3))  # type: ignore

    with pytest.raises(ma.validate.ValidationError):
        foobar_schema_cls().fields["results_size"].validate(0)  # type: ignore

    now = datetime.utcnow()
    now_s = now.isoformat()
    fb = foobar_schema_cls().load(dict(id="nested", quux=dict(created_at=now_s, data="whatever")))
    assert fb.quux.created_at == now
    assert fb.quux.data == "whatever"


def test_ordered():
    fbs = neo(FooBar)
    assert list(fbs().fields.keys()) == [
        "id",
        "results_size",
        "breakdown_difference_categories",
        "filter_expr",
        "max_geo_dist",
        "additional_provider_data_fields",
        "difference_weights",
        "quux",
    ]


def test_misspelled_named_field():
    with pytest.raises(KeyError):
        neo(FooBar, bazzz=neo.field(load_only=True))
