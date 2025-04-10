import datetime
from enum import IntEnum
from functools import singledispatch
from logging import getLogger
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union

import numpy as np
import pandas as pd
import pyarrow

K1 = TypeVar("K1")
K2 = TypeVar("K2")
V1 = TypeVar("V1")
V2 = TypeVar("V2")

TuplesToDict = Callable[[Iterable[Tuple[K1, V1]]], Dict[K2, V2]]
IterableToList = Callable[[Iterable[V1]], List[V2]]
DictToList = Callable[[Dict[K1, V1]], List[Tuple[K2, V2]]]

PANDAS_NULL_VALUES = {None, np.nan, pd.NA}
NONHASHABLE_TYPES = {dict, list, np.ndarray}


def identity(x):
    return x


def pandas_maybe(f: Callable[[V1], V2]) -> Callable[[Optional[V1]], Optional[V2]]:
    """Wrap a function with this to allow passing it to `pandas.Series.apply` in case null values are
    present"""

    def f_(x):
        if (type(x) not in NONHASHABLE_TYPES) and (x in PANDAS_NULL_VALUES):
            return None
        return f(x)

    return f_


def is_primitive_type(t: pyarrow.DataType) -> bool:
    return pyarrow.types.is_primitive(t) or pyarrow.types.is_string(t)


# helpers for postprocessing dataframes read from parquet files possibly with complex types


@singledispatch
def to_pyiterable(a: Union[np.ndarray, Iterable]) -> Iterable:
    return a


@to_pyiterable.register(np.ndarray)
def to_pyiterable_array(a: np.ndarray) -> Iterable:
    if a.dtype.kind == "O":
        # can iterate over object type array and get python objects; no need to make a copy
        return a
    return a.tolist()


@singledispatch
def tolist(list_: Iterable[V1]) -> List[V1]:
    raise NotImplementedError(type(list_))


tolist.register(list)(identity)
tolist.register(np.ndarray)(np.ndarray.tolist)


def list_map(values: Callable[[V1], V2]) -> IterableToList:
    def mapped(it):
        return list(map(values, to_pyiterable(it)))

    return mapped


def dict_map_keys_values(keys: Callable[[K1], K2], values: Callable[[V1], V2]) -> TuplesToDict:
    def mapped(it):
        return dict((keys(k), values(v)) for k, v in it)

    return mapped


def dict_map_keys(keys: Callable[[K1], K2]) -> TuplesToDict[K1, V1, K2, V1]:
    def mapped(it):
        return dict((keys(k), v) for k, v in it)

    return mapped


def dict_map_values(values: Callable[[V1], V2]) -> TuplesToDict[K1, V1, K1, V2]:
    def mapped(it):
        return dict((k, values(v)) for k, v in it)

    return mapped


def todate(x: datetime.date) -> datetime.date:
    return x.date() if isinstance(x, datetime.datetime) else x


def postprocessor_for_pyarrow_value_type(value_type: pyarrow.DataType) -> Optional[Callable]:
    # Only for entries in arrays/maps; some newer versions of pyarrow load date types as datetime there.
    # Not for scalar columns, where we'll just allow the more efficient pandas datetime/timestamp dtypes
    if value_type in (pyarrow.date32(), pyarrow.date64()):
        return todate
    return postprocessor_for_pyarrow_type(value_type)


@singledispatch
def postprocessor_for_pyarrow_type(t: pyarrow.DataType) -> Optional[Callable]:
    return None


@postprocessor_for_pyarrow_type.register(pyarrow.ListType)
def postprocessor_for_pyarrow_array(t: pyarrow.Array) -> IterableToList:
    pproc = postprocessor_for_pyarrow_value_type(t.value_type)
    if pproc is None:
        return tolist
    return list_map(pproc)


@postprocessor_for_pyarrow_type.register(pyarrow.MapType)
def postprocessor_for_pyarrow_map(t: pyarrow.MapType) -> TuplesToDict:
    key_pproc = postprocessor_for_pyarrow_value_type(t.key_type)
    val_pproc = postprocessor_for_pyarrow_value_type(t.item_type)
    if key_pproc is None:
        if val_pproc is None:
            return dict
        return dict_map_values(val_pproc)
    elif val_pproc is None:
        return dict_map_keys(key_pproc)
    else:
        return dict_map_keys_values(key_pproc, val_pproc)


def postprocess_parquet_dataframe(df: pd.DataFrame, schema: pyarrow.Schema) -> pd.DataFrame:
    """Postprocess a dataframe read from an arrow table (casts collection types to dicts and lists)"""
    for name in schema.names:
        field = schema.field(name)
        pproc = postprocessor_for_pyarrow_type(field.type)
        if pproc is not None:
            if field.nullable:
                pproc = pandas_maybe(pproc)
            df[name] = df[name].apply(pproc)

    return df


# helpers for preprocessing dataframes for writing to parquet files possibly with complex types


def dict_to_list(d: Dict[K1, V1]) -> List[Tuple[K1, V1]]:
    return list(d.items())


def dict_to_list_keys_values(keys: Callable[[K1], K2], values: Callable[[V1], V2]) -> DictToList:
    def mapped(it: Dict[K1, V1]):
        return [(keys(k), values(v)) for k, v in it.items()]

    return mapped


def dict_to_list_keys(keys: Callable[[K1], K2]) -> DictToList[K1, V1, K2, V1]:
    def mapped(it: Dict[K1, V1]):
        return [(keys(k), v) for k, v in it.items()]

    return mapped


def dict_to_list_values(values: Callable[[V1], V2]) -> DictToList[K1, V1, K1, V2]:
    def mapped(it: Dict[K1, V1]):
        return [(k, values(v)) for k, v in it.items()]

    return mapped


@singledispatch
def preprocessor_for_pyarrow_type(t: pyarrow.DataType) -> Optional[Callable]:
    return None


@preprocessor_for_pyarrow_type.register(pyarrow.MapType)
def preprocessor_for_pyarrow_map(t: pyarrow.MapType) -> DictToList:
    key_pproc = preprocessor_for_pyarrow_type(t.key_type)
    val_pproc = preprocessor_for_pyarrow_type(t.item_type)
    if key_pproc is None:
        if val_pproc is None:
            return dict_to_list
        return dict_to_list_values(val_pproc)
    elif val_pproc is None:
        return dict_to_list_keys(key_pproc)
    else:
        return dict_to_list_keys_values(key_pproc, val_pproc)


# parquet type safety

_pyarrow_type_to_py_type: Dict[pyarrow.DataType, Type] = {}
_pyarrow_type_to_py_type.update(
    (t(), int) for t in [pyarrow.uint8, pyarrow.uint16, pyarrow.uint32, pyarrow.uint64]
)
_pyarrow_type_to_py_type.update(
    (t(), int) for t in [pyarrow.int8, pyarrow.int16, pyarrow.int32, pyarrow.int64]
)
_pyarrow_type_to_py_type.update(
    (t(), float) for t in [pyarrow.float16, pyarrow.float32, pyarrow.float64]
)
_pyarrow_type_to_py_type.update((t(), datetime.date) for t in [pyarrow.date32, pyarrow.date64])
_pyarrow_type_to_py_type[pyarrow.string()] = str
_pyarrow_type_to_py_type[pyarrow.bool_()] = bool
_pyarrow_type_to_py_type[pyarrow.null()] = type(None)


class TypeCheckLevel(IntEnum):
    """Enum specifying a level of type safety when checking arrow schemas at runtime
    same_names: only require that the expected field name set is a subset of the supplied field name set.
      This applies recursively to record types
    compatible: also require that all types are semantically compatible; e.g. if floats are expected but
      ints are given, that will pass, but not vice-versa. This also includes nullability constraints:
      if a nullable type is expected and a non-nullable version is given, that will pass, but not
      vice-versa
    same_kind: additionally require that types given have the same kind as those expected. E.g. an int32
      in place of an int8 will be fine, but not a float type
    exact: require exactly the same types and nullability constraints as expected
    """

    same_names = 0
    compatible = 1
    same_kind = 2
    exact = 3


def type_check_pyarrow_schemas(
    actual_schema: Union[pyarrow.Schema, pyarrow.StructType],
    expected_schema: Union[pyarrow.Schema, pyarrow.StructType],
    type_check_level: TypeCheckLevel,
    columns: Optional[List[str]] = None,
    raise_: bool = True,
    warn_inexact: bool = True,
) -> bool:
    actual_fields = {field.name: field for field in actual_schema}
    expected_fields = {field.name: field for field in expected_schema}
    if columns is None:
        columns = [field.name for field in expected_schema]

    missing = set(columns).difference(actual_fields)
    extra = set(actual_fields).difference(columns)
    errors = []
    logger = getLogger(__name__)
    if extra:
        error = f"Expected only columns {columns}, but {sorted(extra)} were also present"
        if type_check_level >= TypeCheckLevel.exact:
            logger.error(error)
            errors.append(error)
        else:
            logger.warning(error)
    if missing:
        error = f"Expected columns {columns}, but {sorted(missing)} were missing"
        logger.error(error)
        errors.append(error)

    for column in columns:
        if column not in missing:
            expected = expected_fields[column]
            actual = actual_fields[column]

            if (
                warn_inexact
                and (type_check_level < TypeCheckLevel.exact)
                and not pyarrow_field_compatible(actual, expected, TypeCheckLevel.exact)
            ):
                logger.warning(
                    f"Field {actual} didn't match expected {expected} "
                    f"according to type check rule {TypeCheckLevel.exact.name!r}"
                )
            if not pyarrow_field_compatible(actual, expected, type_check_level):
                error = (
                    f"Field {actual} didn't match expected {expected} "
                    f"according to type check rule {type_check_level.name!r}"
                )
                logger.error(error)
                errors.append(error)

    if raise_ and errors:
        raise TypeError("\n".join(errors))

    return not bool(errors)


def pyarrow_field_compatible(
    actual: pyarrow.Field, expected: pyarrow.Field, level: TypeCheckLevel
) -> bool:
    if level >= TypeCheckLevel.exact and actual.nullable != expected.nullable:
        return False
    elif level >= TypeCheckLevel.compatible and actual.nullable and not expected.nullable:
        return False
    elif level >= TypeCheckLevel.compatible and level < TypeCheckLevel.same_kind:
        return pyarrow_type_compatible(actual.type, expected.type, level) or (
            (actual.type == pyarrow.null()) and expected.nullable
        )
    else:
        return pyarrow_type_compatible(actual.type, expected.type, level)


@singledispatch
def pyarrow_type_compatible(
    actual: pyarrow.DataType, expected: pyarrow.DataType, level: TypeCheckLevel
) -> bool:
    if level >= TypeCheckLevel.exact:
        return actual == expected
    elif level >= TypeCheckLevel.same_kind:
        return _pyarrow_type_to_py_type[actual] == _pyarrow_type_to_py_type.get(expected)
    elif level >= TypeCheckLevel.compatible:
        actual_kind = _pyarrow_type_to_py_type[actual]
        expected_kind = _pyarrow_type_to_py_type.get(expected)
        if expected_kind is int:
            return actual_kind is int
        elif expected_kind is float:
            return actual_kind is int or actual_kind is float
        else:
            return actual_kind == expected_kind
    return True


@pyarrow_type_compatible.register(pyarrow.StructType)
def _pyarrow_type_compatible_struct(
    actual: pyarrow.StructType, expected: pyarrow.DataType, level: TypeCheckLevel
) -> bool:
    return isinstance(expected, pyarrow.StructType) and type_check_pyarrow_schemas(
        actual, expected, level, raise_=False
    )


@pyarrow_type_compatible.register(pyarrow.ListType)
def _pyarrow_type_compatible_list(
    actual: pyarrow.ListType, expected: pyarrow.DataType, level: TypeCheckLevel
) -> bool:
    return isinstance(expected, pyarrow.ListType) and pyarrow_field_compatible(
        actual.value_field, expected.value_field, level
    )


@pyarrow_type_compatible.register(pyarrow.FixedSizeListType)
def _pyarrow_type_compatible_fixed_size_list(
    actual: pyarrow.FixedSizeListType, expected: pyarrow.DataType, level: TypeCheckLevel
) -> bool:
    if level >= TypeCheckLevel.compatible:
        if isinstance(expected, pyarrow.FixedSizeListType) and actual.list_size != expected.list_size:
            return False
    return isinstance(
        expected, (pyarrow.FixedSizeListType, pyarrow.ListType)
    ) and pyarrow_field_compatible(actual.value_field, expected.value_field, level)


@pyarrow_type_compatible.register(pyarrow.MapType)
def _pyarrow_type_compatible_map(
    actual: pyarrow.MapType, expected: pyarrow.DataType, level: TypeCheckLevel
) -> bool:
    return (
        isinstance(expected, pyarrow.MapType)
        and pyarrow_field_compatible(actual.key_field, expected.key_field, level)
        and pyarrow_field_compatible(actual.item_field, expected.item_field, level)
    )


@pyarrow_type_compatible.register(pyarrow.TimestampType)
def _pyarrow_type_compatible_timestamp(
    actual: pyarrow.TimestampType, expected: pyarrow.DataType, level: TypeCheckLevel
):
    if level == TypeCheckLevel.compatible:
        expected_kind = _pyarrow_type_to_py_type.get(expected)
        if expected_kind is datetime.date:
            return actual.tz is None
    elif level < TypeCheckLevel.compatible:
        return True

    if not isinstance(expected, pyarrow.TimestampType):
        return False
    elif level >= TypeCheckLevel.exact:
        return actual.unit == expected.unit and actual.tz == expected.tz
    elif level >= TypeCheckLevel.compatible:
        units = ["s", "ms", "us", "ns"]
        if units.index(actual.unit) < units.index(expected.unit):
            return False
        return actual.tz == expected.tz
