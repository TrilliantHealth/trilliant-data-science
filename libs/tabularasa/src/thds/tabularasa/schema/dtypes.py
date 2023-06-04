import builtins
import datetime
from enum import Enum
from typing import Callable, Iterator, List, Optional, Type, Union

import numpy as np
import pandas as pd
import pyarrow
from pandas.core.dtypes import base as pd_dtypes

from .util import EnumList, Identifier

AnyDtype = Union[pd_dtypes.ExtensionDtype, np.dtype]
PyType = Union[int, float, bool, str, datetime.date, datetime.datetime]


_dtype_name_to_pd_dtype = dict(
    int8=pd.Int8Dtype(),
    int16=pd.Int16Dtype(),
    int32=pd.Int32Dtype(),
    int64=pd.Int64Dtype(),
    uint8=pd.UInt8Dtype(),
    uint16=pd.UInt16Dtype(),
    uint32=pd.UInt32Dtype(),
    uint64=pd.UInt64Dtype(),
    bool=pd.BooleanDtype(),
    str=pd.StringDtype(),
)


class DType(Enum):
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    UINT8 = "uint8"
    UINT16 = "uint16"
    UINT32 = "uint32"
    UINT64 = "uint64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    STR = "str"
    DATE = "date"
    DATETIME = "datetime"
    BOOL = "bool"

    @property
    def enum(self) -> None:
        return None

    @property
    def is_int_type(self):
        return self.value.startswith("int") or self.value.startswith("uint")

    @property
    def is_float_type(self):
        return self.value.startswith("float")

    def pandas(
        self,
        nullable: bool = False,
        index: bool = False,
        enum: Optional[EnumList] = None,
        ordered: bool = False,
    ) -> Union[np.dtype, pd_dtypes.ExtensionDtype]:
        if enum:
            return pd.CategoricalDtype(enum, ordered=ordered)
        elif self == DType.DATE or self == DType.DATETIME:
            # pandas supports *only* nanosecond datetimes
            return np.dtype("datetime64[ns]")
        elif self == DType.BOOL:
            return pd.BooleanDtype() if nullable else np.dtype("bool")
        elif self == DType.STR:
            # StringDtype is better than dtype('O') for explicitness and null handling,
            # but note that pandas<1.4 coerces to dtype('O') for indexes - we used to handle that case
            # but it is no longer necessary in pandas>=1.4
            return pd.StringDtype()
        else:
            # int and float types
            if nullable and self.is_int_type:
                # nullable int extension types
                return _dtype_name_to_pd_dtype[self.value]
            else:
                # non-nullable ints or floats
                if index:
                    # no low-resolution types on indexes
                    if self.is_float_type:
                        return np.dtype("float")
                    else:
                        return np.dtype("int") if self.value.startswith("int") else np.dtype("uint")
                else:
                    return np.dtype(self.value)

    @property
    def sqlite(self) -> str:
        if self == DType.BOOL:
            return "BOOLEAN"
        elif self == DType.STR:
            return "TEXT"
        else:
            # only one true float and int type in sqlite, but all aliases accepted; we keep them as-is
            # for explicitness
            return self.name

    @property
    def python(self) -> Type[PyType]:
        if self.is_int_type:
            return int
        elif self.is_float_type:
            return float
        elif self == DType.STR:
            return str
        elif self == DType.DATE:
            return datetime.date
        elif self == DType.DATETIME:
            return datetime.datetime
        elif self == DType.BOOL:
            return bool
        else:
            raise TypeError(f"No python type registered for {self}")

    def python_type_literal(self, builtin: bool = False) -> str:
        cls = self.python
        if cls.__module__ == builtins.__name__:
            # int, str, bool, etc
            return cls.__name__
        else:
            return f"{cls.__module__}.{cls.__name__}"

    @property
    def custom_type_refs(self) -> Iterator[Identifier]:
        yield from ()

    @property
    def parquet(self) -> pyarrow.DataType:
        if self == DType.STR:
            return pyarrow.string()
        elif self == DType.BOOL:
            return pyarrow.bool_()
        elif self is DType.DATETIME:
            return pyarrow.timestamp("ms")
        elif self is DType.DATE:
            return pyarrow.date32()
        else:
            dtype: Callable[[], pyarrow.DataType] = getattr(pyarrow, self.name.lower())
            return dtype()

    @property
    def attrs_required_imports(self) -> List[str]:
        if self in (DType.DATE, DType.DATETIME):
            return ["datetime"]
        return []
