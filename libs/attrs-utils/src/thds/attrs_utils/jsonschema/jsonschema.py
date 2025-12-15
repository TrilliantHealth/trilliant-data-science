import datetime
import enum
import importlib
import uuid
from types import ModuleType
from typing import Any, Callable, Collection, Dict, List, Mapping, Optional, Type, Union
from warnings import warn

import attr
from typing_inspect import get_args, get_origin, is_optional_type

from thds.core import scope
from thds.core.stack_context import StackContext

from ..cattrs import DEFAULT_JSON_CONVERTER
from ..params import attrs_fields_parameterized
from ..recursion import RecF, value_error
from ..registry import Registry
from ..type_recursion import TypeRecursion
from ..type_utils import UNIQUE_COLLECTION_TYPES, newtype_base, typename
from .constructors import array, boolean, integer, null, number, object_, string
from .str_formats import DEFAULT_FORMAT_CHECKS, string_pattern_for
from .util import (
    DEFS,
    JSON,
    TITLE,
    JSONSchema,
    JSONSchemaTypeCache,
    ToJSON,
    _merge_schemas_anyof,
    check_cache,
)

ENUM = "enum"
DEFAULT = "default"
SCHEMA, BASE_URI = "$schema", "$id"
DESCRIPTION = "description"
DATE, DATETIME, TIME, UUID = "date", "date-time", "time", "uuid"
DRAFT_7_SCHEMA_URL = "http://json-schema.org/draft-07/schema"


### Recursively available context
OptionalNullDefaults = StackContext("OptionalNullDefaults", True)
# arguably this is the wrong default; consult with Hawthorn and Spencer about changing it
AllAttributesRequired = StackContext("AllAttributesRequired", False)
# for creating an output schema; when serializing attrs objects, all
# attributes are guaranteed to be present, and the only way to express
# this with JSONSchema is to call them 'required'.


#################
# Main function #
#################


@scope.bound
def to_jsonschema(
    type_: Type,
    to_json: ToJSON = DEFAULT_JSON_CONVERTER.unstructure,
    as_ref: bool = False,
    modules: Optional[
        Union[
            Collection[ModuleType],
            Mapping[str, Union[ModuleType, Collection[ModuleType]]],
        ]
    ] = None,
    base_uri: Optional[str] = None,
    optional_null_defaults: bool = True,
    all_attributes_required: bool = False,
) -> JSONSchema:
    """Generate a jsonschema for a python type. Handles most useful builtin types and `attrs` classes.

    :param type_: the type to generate a schema for. Can be an `attrs` class, builtin python type,
      annotation type from `typing` or `typing_extensions`.
    :param to_json: callable taking an arbitrary python object and returning a json-serializable python
      object. The default is a `cattrs` converter's 'unstructure' method. This is used for serializing
      literals in the form of default values and arguments to `typing_extensions.Literal`.
    :param as_ref: if True, put the explicit schema for `type_` in the "$defs" section of the schema, and
      reference it at the top level using a "$ref". If false, generated no "$def" section for `type_`,
      instead including all of its fields at the top level of the schema.
    :param modules: collection of module objects or mapping of `str` to module. These are used for
      looking up names of types that would otherwise be anonymous, such as `typing.NewType` and
      `typing_extensions.Literal` instances. Each such nameable type gets a section in the "$defs"
      section of the schema. If not supplied, the module of `type_` is dynamically imported and its name
      used to qualify any types found there. When a mapping is passed for `modules`, the keys of the
      mapping are used to qualify the names of types found in the associated module. When a collection of
      modules is passed, type names are qualified by the names of their respective modules.
    :param base_uri: sets the base URI of the schema (manifests as the `$id` keyword)
    :param optional_null_defaults: If an attrs attribute is Optional, then the JSON Schema default value is null.
    :param all_attributes_required: For output schemas; all data serialized from attrs classes
      is guaranteed to have all object keys present recursively.
    :return: a json-serializable python dict representing the generated jsonschema
    """
    scope.enter(AllAttributesRequired.set(all_attributes_required))
    scope.enter(OptionalNullDefaults.set(optional_null_defaults))
    named_modules: Mapping[str, Union[ModuleType, Collection[ModuleType]]]
    if modules is None:
        module_name = getattr(type_, "__module__", None)
        if module_name is not None:
            named_modules = {module_name: importlib.import_module(module_name)}
        else:
            named_modules = {}
    elif not isinstance(modules, Mapping):
        named_modules = {module.__name__: module for module in modules}
    else:
        named_modules = modules

    type_cache = JSONSchemaTypeCache(**named_modules)
    ref = gen_jsonschema(type_, type_cache, to_json)
    schema = {SCHEMA: DRAFT_7_SCHEMA_URL}
    if base_uri:
        schema[BASE_URI] = base_uri
    if as_ref:
        schema.update(ref)
    else:
        schema[TITLE] = type_cache.name_of(type_)
        schema.update(type_cache.pop(type_))
    if type_cache:
        schema[DEFS] = type_cache.to_defs()

    return schema


def jsonschema_validator(
    schema: JSONSchema,
    use_default: bool = True,
    formats: Optional[Mapping[str, Callable[[str], bool]]] = None,
) -> Callable[[JSON], JSON]:
    """Convenience method for constructing a validator for a given jsonschema.

    :param schema: the jsonschema to validate against
    :param use_default: if True (default), the validator returns instances with default values filled in.
      See the corresponding argument of `fastjsonschema.compile` for more information
    :param formats: mapping of format name to predicate indicating whether a given string satisfies the
      format specification. If not passed, a default, extensible set of format predicates is used,
      encompassing some common types. This set may be extended by using the `register_format_check`
      decorator.
    :return: a callable accepting json-serializable python objects and returning them optionally with
      defaults populated. The callable raises an exception if the input doesn't match the jsonschema.
      See `fastjsonschema.compile` for more information
    """
    try:
        import fastjsonschema
    except ImportError:
        raise ImportError(
            f"fastjsonschema is required to use {jsonschema_validator.__name__} but is not installed; "
            "include the 'jsonschema' extra of this library to install a compatible version"
        )

    if formats is None:
        formats = DEFAULT_FORMAT_CHECKS.copy()

    return fastjsonschema.compile(schema, formats=formats, use_default=use_default)


#################################
# Jsonschema generation by type #
#################################


@check_cache
def gen_jsonschema_newtype(
    gen_jsonschema, type_: Type, type_cache: JSONSchemaTypeCache, serializer: ToJSON
):
    # we don't use the default implementation from type_recursion because we want to allow the naming
    # cache to pick up the new name to make the intended semantics of the resulting schema clearer
    return gen_jsonschema(newtype_base(type_), type_cache, serializer)


@check_cache
def gen_jsonschema_attrs(
    gen_jsonschema,
    type_: Type,
    type_cache: JSONSchemaTypeCache,
    serializer: ToJSON,
):
    """Note that there are two recursively available input parameters that can only be set via StackContext.

    See above for notes on OptionalNullDefaults and AllAttributesRequired.
    """
    optional_null_defaults = OptionalNullDefaults()
    all_attributes_required = AllAttributesRequired()

    type_ = attr.resolve_types(type_)
    attrs = attrs_fields_parameterized(type_)
    properties: Dict[str, Any] = {}
    required: List[str] = []

    for at in attrs:
        # case where we should add a null default value
        null_default = (
            optional_null_defaults and is_optional_type(at.type) and at.default is attr.NOTHING
        )

        if at.type is None:
            warn(
                f"No type annotation for field {at.name} of attrs class {typename(type_)}; using empty schema"
            )
            attr_schema = {}
        else:
            attr_schema = gen_jsonschema(at.type, type_cache, serializer)
        if all_attributes_required or (at.default is attr.NOTHING and not null_default):
            required.append(at.name)
        else:
            if isinstance(at.default, attr.Factory):  # type: ignore
                if at.default.takes_self:  # type: ignore
                    warn(
                        "Can't define a default value for an attrs field with a factory where "
                        f"takes_self=True; occurred for field {at.name!r} on class {type_!r}"
                    )
                else:
                    attr_schema[DEFAULT] = serializer(at.default.factory())  # type: ignore
            else:
                default = None if null_default else at.default
                attr_schema[DEFAULT] = serializer(default)

        properties[at.name] = attr_schema

    return object_(properties=properties, required=required, additionalProperties=False)


@check_cache
def gen_jsonschema_literal(
    gen_jsonschema, type_: Type, type_cache: JSONSchemaTypeCache, serializer: ToJSON
):
    return _gen_jsonschema_enum(get_args(type_), type_, serializer, "literal")


@check_cache
def gen_jsonschema_enum(
    gen_jsonschema, type_: Type[enum.Enum], type_cache: JSONSchemaTypeCache, serializer: ToJSON
):
    return _gen_jsonschema_enum([v.value for v in type_], type_, serializer, "enum")


def _gen_jsonschema_enum(base_values, type_: Type[enum.Enum], serializer: ToJSON, kind: str):
    values = []
    for i, value in enumerate(base_values):
        try:
            values.append(serializer(value))
        except Exception:
            raise TypeError(f"Can't serialize value {value!r} at index {i:d} of {kind} type {type_!r}")
    return {ENUM: list(values)}


def gen_jsonschema_union(
    gen_jsonschema, type_: Type, type_cache: JSONSchemaTypeCache, serializer: ToJSON
):
    types = get_args(type_)
    return _merge_schemas_anyof([gen_jsonschema(t, type_cache, serializer) for t in types])


def gen_jsonschema_mapping(
    gen_jsonschema, type_: Type, type_cache: JSONSchemaTypeCache, serializer: ToJSON
):
    args = get_args(type_)
    if len(args) != 2:
        return unknown_type_for_jsonschema(gen_jsonschema, type_, type_cache, serializer)
    key_type, value_type = args
    value_schema = gen_jsonschema(value_type, type_cache, serializer)
    key_pattern = string_pattern_for(key_type, serializer)
    return object_(patternProperties={key_pattern: value_schema})


def gen_jsonschema_collection(
    gen_jsonschema, type_: Type, type_cache: JSONSchemaTypeCache, serializer: ToJSON
):
    origin = get_origin(type_)
    args = get_args(type_)
    if len(args) > 1:
        return unknown_type_for_jsonschema(gen_jsonschema, type_, type_cache, serializer)
    item_schema = gen_jsonschema(args[0], type_cache, serializer) if args else {}
    if origin in UNIQUE_COLLECTION_TYPES:
        return array(items=item_schema, uniqueItems=True)
    else:
        return array(items=item_schema)


def gen_jsonschema_tuple(
    gen_jsonschema, type_: Type, type_cache: JSONSchemaTypeCache, serializer: ToJSON
):
    args = get_args(type_)
    items = [gen_jsonschema(t, type_cache, serializer) for t in args]
    return array(prefixItems=items, items=False)


def gen_jsonschema_variadic_tuple(
    gen_jsonschema, type_: Type, type_cache: JSONSchemaTypeCache, serializer: ToJSON
):
    args = get_args(type_)
    list_type: Type = List[args[0]]  # type: ignore [valid-type]
    gen_jsonschema_collection(gen_jsonschema, list_type, type_cache, serializer)


GEN_JSONSCHEMA_REGISTRY: Registry[Type, JSONSchema] = Registry(
    {
        type(None): null(),
        str: string(),
        bool: boolean(),
        int: integer(),
        float: number(),
        datetime.date: string(format=DATETIME),
        datetime.datetime: string(format=DATETIME),
        datetime.time: string(format=TIME),
        uuid.UUID: string(format=UUID),
    }
)

unknown_type_for_jsonschema: "RecF[Type, [JSONSchemaTypeCache, ToJSON], JSONSchema]" = value_error(
    "Don't know how to interpret type {!r} as jsonschema",
    TypeError,
)

gen_jsonschema: "TypeRecursion[[JSONSchemaTypeCache, ToJSON], JSONSchema]" = TypeRecursion(
    GEN_JSONSCHEMA_REGISTRY,
    cached=False,  # no caching by type since behavior depends on to_json, type_cache args
    attrs=gen_jsonschema_attrs,
    literal=gen_jsonschema_literal,
    enum=gen_jsonschema_enum,
    newtype=gen_jsonschema_newtype,
    union=gen_jsonschema_union,
    mapping=gen_jsonschema_mapping,
    tuple=gen_jsonschema_tuple,
    variadic_tuple=gen_jsonschema_variadic_tuple,
    collection=gen_jsonschema_collection,
    otherwise=unknown_type_for_jsonschema,
)
