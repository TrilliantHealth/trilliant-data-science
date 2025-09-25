from types import ModuleType
from typing import List, Literal, NewType, Set

import attr
import pytest

from thds.attrs_utils import jsonschema, type_cache

ListType = List[int]
OtherListType = List[int]
LiteralType = Literal[1, 2, 3]
OtherLiteralType = Literal[1, 2, 3]


@attr.define
class Foo:
    x = ListType

    @attr.define
    class Bar:
        y = ListType

        @attr.define
        class Baz:
            # mypy doesn't like when you wrap a Literal with a NewType - we abuse that here to prove the concept
            ReallyAnotherLiteralType = NewType("ReallyAnotherLiteralType", LiteralType)  # type: ignore
            z = ListType
            lit: ReallyAnotherLiteralType = ReallyAnotherLiteralType(2)

        baz: Baz
        lit: OtherLiteralType

    bar: Bar
    lit: LiteralType = 1


@pytest.fixture(scope="session")
def this_module() -> ModuleType:
    # this module essentially
    module = ModuleType("module")
    # some other module
    module.__dict__.update(globals())
    return module


@pytest.fixture(scope="session")
def other_module() -> ModuleType:
    # another module with some repeated/imported types that should have lower precedence for naming
    other_module = ModuleType("other_module")
    # add same type to other module to ensure precedence of the first
    other_module.OtherListType = ListType  # type: ignore
    other_module.OtherLiteralType = OtherLiteralType  # type: ignore
    # add another type with a duplicate name, ensuring it's named as it appears in its module,
    # and not as its __name__ would suggest
    OtherFoo = type("OtherFoo", (), {})
    other_module.Foo = OtherFoo  # type: ignore
    return other_module


@pytest.fixture(scope="session")
def schema(this_module, other_module) -> jsonschema.JSONSchema:
    return jsonschema.to_jsonschema(Foo, modules=dict(module=[this_module], other_module=[other_module]))


def test_object_names_recursive():
    obj_names = [
        (name, obj)
        for name, obj in type_cache.object_names_recursive(Foo, cache=set())
        if "__" not in name
    ]
    expected = [
        ("Foo.x", ListType),
        ("Foo.Bar", Foo.Bar),
        # ("Foo.bar", Foo.Bar), member descriptor
        # ("Foo.lit", 1), member descriptor
        # ("Foo.Bar.y", ListType), cache hit
        ("Foo.Bar.Baz", Foo.Bar.Baz),
        # ("Foo.Bar.baz", Foo.Bar.Baz), member descriptor
        ("Foo.Bar.Baz.ReallyAnotherLiteralType", Foo.Bar.Baz.ReallyAnotherLiteralType),
        # ("Foo.Bar.Baz.z", ListType), cache hit
        # ("Foo.Bar.baz.lit", 2), member descriptor
    ]
    assert obj_names == expected


def test_nested_type_names(this_module: ModuleType, other_module: ModuleType):
    types = jsonschema.JSONSchemaTypeCache(module=this_module, other_module=other_module)

    assert types.name_of(Foo) == "module.Foo"
    assert types.name_of(Foo.Bar) == "module.Foo.Bar"
    assert types.name_of(Foo.Bar.Baz) == "module.Foo.Bar.Baz"
    assert types.name_of(ListType) == "module.ListType"
    assert types.name_of(other_module.OtherListType) == "module.ListType"
    assert types.name_of(other_module.OtherLiteralType) == "module.LiteralType"
    assert types.name_of(other_module.Foo) == "other_module.Foo"


def walk(o):
    if isinstance(o, dict):
        for k, v in o.items():
            yield k, v
            yield from walk(v)
    elif isinstance(o, list):
        for p in o:
            yield from walk(p)


def test_all_refs_present(schema: jsonschema.JSONSchema):
    refnames: Set[str] = {v.split("/")[-1] for k, v in walk(schema) if k == jsonschema.REF}

    assert refnames == {
        "module.Foo.Bar",
        "module.Foo.Bar.Baz",
        "module.LiteralType",
        "module.Foo.Bar.Baz.ReallyAnotherLiteralType",
    }

    missing = set(schema[jsonschema.DEFS]).difference(refnames)
    assert not missing, f"Dangling references in jsonschema for {Foo}: {missing}"
    extra = refnames.difference(schema[jsonschema.DEFS])
    assert not extra, f"Dangling definitions in jsonschema for {Foo}: {extra}"


def test_to_jsonschema(schema: jsonschema.JSONSchema):
    assert schema == {
        "$schema": "http://json-schema.org/draft-07/schema",
        "title": "module.Foo",
        "type": "object",
        "properties": {
            "bar": {"$ref": "#/$defs/module.Foo.Bar"},
            "lit": {"$ref": "#/$defs/module.LiteralType", "default": 1},
        },
        "required": ["bar"],  # `lit` has a default
        "additionalProperties": False,
        "$defs": {
            "module.Foo.Bar.Baz": {
                "title": "module.Foo.Bar.Baz",
                "type": "object",
                "properties": {
                    "lit": {
                        "$ref": "#/$defs/module.Foo.Bar.Baz.ReallyAnotherLiteralType",
                        "default": 2,
                    }
                },
                "required": [],
                "additionalProperties": False,
            },
            "module.LiteralType": {"title": "module.LiteralType", "enum": [1, 2, 3]},
            "module.Foo.Bar.Baz.ReallyAnotherLiteralType": {
                "title": "module.Foo.Bar.Baz.ReallyAnotherLiteralType",
                "$ref": "#/$defs/module.LiteralType",
            },
            "module.Foo.Bar": {
                "title": "module.Foo.Bar",
                "type": "object",
                "properties": {
                    "baz": {"$ref": "#/$defs/module.Foo.Bar.Baz"},
                    "lit": {"$ref": "#/$defs/module.LiteralType"},
                },
                "required": ["baz", "lit"],
                "additionalProperties": False,
            },
        },
    }
