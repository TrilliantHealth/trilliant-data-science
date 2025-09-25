from functools import partial
from textwrap import indent
from typing import Dict, Union

import pyarrow as pa

from thds.tabularasa.schema import metaschema

from ._format import autoformat
from .util import AUTOGEN_DISCLAIMER

_pyarrow_type_to_name: Dict[pa.DataType, str] = {}
_pyarrow_type_to_name.update(
    (t(), f"int{t().bit_width}")
    for t in [
        pa.int8,
        pa.int16,
        pa.int32,
        pa.int64,
    ]
)
_pyarrow_type_to_name.update(
    (t(), f"uint{t().bit_width}")
    for t in [
        pa.uint8,
        pa.uint16,
        pa.uint32,
        pa.uint64,
    ]
)
_pyarrow_type_to_name.update(
    (t(), f"float{t().bit_width}") for t in [pa.float16, pa.float32, pa.float64]
)
_pyarrow_type_to_name.update((t(), f"date{t().bit_width}") for t in [pa.date32, pa.date64])
_pyarrow_type_to_name[pa.string()] = "string"
_pyarrow_type_to_name[pa.bool_()] = "bool_"
_pyarrow_type_to_name[pa.null()] = "null"


def render_pyarrow_schema(
    schema: metaschema.Schema,
) -> str:
    pyarrow_schemas = "\n\n".join(
        (
            f"{table.snake_case_name}_pyarrow_schema = {pyarrow_schema_literal(table.parquet_schema)}"
            for table in schema.package_tables
        )
    )
    return autoformat(f"import {pa.__name__}\n\n# {AUTOGEN_DISCLAIMER}\n\n{pyarrow_schemas}\n")


def pyarrow_schema_literal(schema: pa.Schema) -> str:
    return _pyarrow_schema_literal(schema, "schema")


def pyarrow_field_literal(field: pa.Field) -> str:
    t = field.type
    if not t.num_fields:
        return (
            f'{pa.__name__}.field("{field.name}", {pyarrow_type_literal(field.type)}, '
            f"nullable={field.nullable!r})"
        )
    else:
        return (
            f'{pa.__name__}.field(\n    "{field.name}",\n'
            f'{indent(pyarrow_type_literal(field.type), "    ")},\n    nullable={field.nullable!r},\n)'
        )


def pyarrow_type_literal(type_: pa.DataType) -> str:
    if isinstance(type_, pa.StructType):
        return _pyarrow_schema_literal(type_, "struct")
    elif isinstance(type_, pa.ListType):
        v = type_.value_type
        return f"{pa.__name__}.list_({pyarrow_type_literal(v)})"
    elif isinstance(type_, pa.FixedSizeListType):
        v = type_.value_type
        return f"{pa.__name__}.list_({pyarrow_type_literal(v)}, list_size={v.list_size})"
    elif isinstance(type_, pa.MapType):
        k, v = type_.key_type, type_.item_type
        return f"{pa.__name__}.map_({pyarrow_type_literal(k)}, {pyarrow_type_literal(v)})"
    elif isinstance(type_, pa.TimestampType):
        tz = "None" if type_.tz is None else f'"{type_.tz}"'
        return f'{pa.__name__}.timestamp("{type_.unit}", {tz})'
    else:
        return f"{pa.__name__}.{_pyarrow_type_to_name[type_]}()"


def _pyarrow_schema_literal(type_: Union[pa.Schema, pa.StructType], kind: str) -> str:
    indent_ = partial(indent, prefix="    ")
    fields = map(indent_, map(pyarrow_field_literal, type_))
    sep = ",\n"
    return f"{pa.__name__}.{kind}([\n{sep.join(fields)}\n])"
