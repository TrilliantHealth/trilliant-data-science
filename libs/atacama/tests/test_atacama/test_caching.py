"""Test schema reuse."""

from attrs import define
from marshmallow import Schema, fields

from thds.atacama import neo, ordered


@define
class Foo:
    id: int
    bar: str = ""


def test_reuse_generated_schema():
    assert neo(Foo) is neo(Foo)


def test_dont_reuse_schema_from_different_generator():
    assert neo(Foo) is not ordered(Foo)


def test_dont_reuse_if_schema_arguments_are_different():
    assert neo(Foo, id=neo.field(allow_none=True)) is not neo(Foo)


class FooSchema(Schema):
    id = fields.Int()
    bar = fields.String()


def test_dont_reuse_if_we_didnt_generate():
    assert neo(Foo) is not FooSchema
