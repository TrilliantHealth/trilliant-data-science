import functools
import typing as ty
from collections import ChainMap, defaultdict

from ..type_cache import TypeCache
from .constructors import INTEGER, NUMBER, TYPE

ENUM, DEFAULT, ANYOF = (
    "enum",
    "default",
    "anyOf",
)
DEFS, REF = "$defs", "$ref"
TITLE = "title"

JSONSchema = ty.Dict[str, ty.Any]
JSON = ty.Union[int, float, bool, str, None, ty.List["JSON"], ty.Dict[str, "JSON"]]
ToJSON = ty.Callable[[ty.Any], JSON]

REF_TEMPLATE = f"#/{DEFS}/{{}}"


class JSONSchemaTypeCache(TypeCache[JSONSchema]):
    def to_defs(self):
        return {self.type_names[type_id]: schema for type_id, schema in self.schemas.items()}

    def to_ref(self, type_: ty.Type):
        return {REF: REF_TEMPLATE.format(self.name_of(type_))}


def check_cache(func):
    """Decorator to cause a jsonschema generator to cache full schema definitions and return jsonschema
    refs to the generated schemas. Useful for complex/custom types with names so as not to duplicate
    sub-schemas throughout the main schema"""

    @functools.wraps(func)
    def new_func(gen_jsonschema, type_: ty.Type, type_cache: JSONSchemaTypeCache, serializer: ToJSON):
        if type_ not in type_cache:
            type_name = type_cache.name_of(type_)
            schema = {TITLE: type_name, **func(gen_jsonschema, type_, type_cache, serializer)}
            type_cache[type_] = schema

        return type_cache.to_ref(type_)

    return new_func


def _flatten_anyof_schemas(schemas: ty.Iterable[JSONSchema]) -> ty.Iterator[JSONSchema]:
    for schema in schemas:
        if ANYOF in schema and len(schema) == 1:
            yield from _flatten_anyof_schemas(schema[ANYOF])
        else:
            yield schema


def _merge_schemas_anyof(schemas: ty.Iterable[JSONSchema]) -> JSONSchema:
    enum_values = []
    schemas_by_type = defaultdict(list)
    other_schemas = []
    number = "_"
    for schema in _flatten_anyof_schemas(schemas):
        if REF in schema:
            other_schemas.append(schema)
        elif ENUM in schema and len(schema) == 1:
            # enums can simply be combined
            enum_values.extend(schema[ENUM])
        elif TYPE in schema:
            types = [schema[TYPE]] if isinstance(schema[TYPE], str) else schema[TYPE]
            for t in types:
                # integer and number share keywords; combine into the same list
                if t in (INTEGER, NUMBER):
                    t = number
                schemas_by_type[t].append(schema)
        else:
            other_schemas.append(schema)

    # any type with only one schema can be keyword-merged with all other such schemas
    simple = []
    complex = []
    for _type, schemas in schemas_by_type.items():
        if len(schemas) == 1:
            simple.append(schemas[0].copy())
        else:
            # we'll transfer these unchanged
            complex.extend(schemas)

    simple_types = [schema.pop(TYPE) for schema in simple]
    simple_type = simple_types[0] if len(simple_types) == 1 else simple_types
    multi_type_schema = {TYPE: simple_type, **ChainMap(*simple)} if simple else None
    enum_schema = {ENUM: enum_values} if enum_values else None
    all_schemas = [schema for schema in (enum_schema, multi_type_schema) if schema is not None]
    all_schemas.extend(complex)
    all_schemas.extend(other_schemas)
    return all_schemas[0] if len(all_schemas) == 1 else {ANYOF: all_schemas}
