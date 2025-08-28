"""Should be able to 'extract' a Schema object from an existing parquet file.

You might want to use this to convert a Parquet file directly to
SQLite without additional ceremony - use `tabularasa.to_sqlite` in
conjunction with this.
"""

import typing as ty
from functools import partial
from pathlib import Path

import pyarrow as pa
import pyarrow.lib
import pyarrow.parquet as pq

from . import metaschema as ms

_LEAF_MAPPINGS = {
    # DType doesn't currently support everything:
    # https://arrow.apache.org/docs/python/api/datatypes.html
    pa.string(): ms.DType.STR,
    pa.bool_(): ms.DType.BOOL,
    # ints
    pa.int8(): ms.DType.INT8,
    pa.int16(): ms.DType.INT16,
    pa.int32(): ms.DType.INT32,
    pa.int64(): ms.DType.INT64,
    pa.uint8(): ms.DType.UINT8,
    pa.uint16(): ms.DType.UINT16,
    pa.uint32(): ms.DType.UINT32,
    pa.uint64(): ms.DType.UINT64,
    # reals
    pa.float16(): ms.DType.FLOAT32,  # should we support float16 in DType?
    pa.float32(): ms.DType.FLOAT32,
    pa.float64(): ms.DType.FLOAT64,
    # dates/times
    pa.date32(): ms.DType.DATE,
    pa.date64(): ms.DType.DATETIME,
    pa.timestamp("s"): ms.DType.DATETIME,
    pa.timestamp("ms"): ms.DType.DATETIME,
    pa.timestamp("us"): ms.DType.DATETIME,
    pa.timestamp("ns"): ms.DType.DATETIME,
}
ColumnType = ty.Union[ms.DType, ms.AnonCustomType, ms.CustomType, ms.ArrayType, ms.MappingType]


def pyarrow_type_to_dtype(pyarrow_type: pyarrow.lib.DataType) -> ColumnType:
    if pyarrow_type in _LEAF_MAPPINGS:
        return _LEAF_MAPPINGS[pyarrow_type]
    if pa.types.is_map(pyarrow_type):
        key_type = pyarrow_type_to_dtype(pyarrow_type.key_type)
        assert not isinstance(key_type, (ms._RawArrayType, ms._RawMappingType))
        return ms.MappingType(
            keys=key_type,
            values=pyarrow_type_to_dtype(pyarrow_type.item_type),
        )
    if pa.types.is_list(pyarrow_type):
        return ms.ArrayType(
            values=pyarrow_type_to_dtype(pyarrow_type.value_type),
        )
    if pa.types.is_struct(pyarrow_type):
        # TODO support these as though they were mappings, possibly?
        raise ValueError("Structs are not yet supported by tabularasa.")
    raise ValueError(f"Unsupported pyarrow type: {pyarrow_type}")


def _decide_field_nullability(pyarrow_field: pyarrow.lib.Field, pq_file: pq.ParquetFile) -> bool:
    if not pyarrow_field.nullable:
        # if the incoming schema is certain about this, then
        # maintain their declaration without inspecting the actual file.
        return False
    # otherwise, infer it from the data
    for batch in pq_file.iter_batches(columns=[pyarrow_field.name]):
        if batch[pyarrow_field.name].null_count:
            return True
    # the impact of saying this is False if no nulls were found but it
    # was theoretically possible is low - we're trying to create a
    # schema based on the data we _already have_, rather than the
    # entire world of possible data.
    return False


def pyarrow_field_to_column(pq_file: pq.ParquetFile, pyarrow_field: pyarrow.lib.Field) -> ms.Column:
    """Convert a pyarrow field to a Column object."""
    return ms.Column(
        name=pyarrow_field.name,
        type=pyarrow_type_to_dtype(pyarrow_field.type),
        nullable=_decide_field_nullability(pyarrow_field, pq_file),
        doc=pyarrow_field.metadata and pyarrow_field.metadata.get("doc", "") or "autoextracted",
    )


def define_table_from_parquet(
    pq_file: Path,
    name: str,
    *,
    primary_key: ty.Optional[ms.IdTuple] = None,
    indexes: ty.Collection[ms.IdTuple] = tuple(),
) -> ms.Table:
    """Extract a table from parquet into a Schema object.

    The filename will be embedded in the doc field.
    """
    pq_schema = pq.read_schema(pq_file)

    columns = list(map(partial(pyarrow_field_to_column, pq.ParquetFile(pq_file)), pq_schema))
    valid_colnames = {column.name for column in columns}

    # validate that primary_key and indexes match
    def _validate(id_tuple: ms.IdTuple, descrip: str):
        for identifier in id_tuple:
            if identifier not in valid_colnames:
                raise ValueError(
                    f"Cannot specify name {identifier} as part of {descrip}"
                    " since it is not a valid column name."
                    f" Options are: {valid_colnames}"
                )

    for keys in indexes:
        _validate(keys, f"index {keys}")
    if primary_key:
        _validate(primary_key, f"primary key {primary_key}")

    return ms.Table(
        name=name,
        columns=columns,
        doc=str(pq_file),
        dependencies=None,
        transient=True,
        indexes=list(indexes),
        primary_key=primary_key,
    )
