from typing import AbstractSet, Any, Callable, Dict, List, Optional, TypeVar, cast

import pandas as pd
import pandera as pa

from thds.tabularasa.loaders.sqlite_util import sqlite_postprocessor_for_type
from thds.tabularasa.schema import metaschema
from thds.tabularasa.schema.dtypes import DType, PyType
from thds.tabularasa.schema.files import TabularFileSource

from .util import check_categorical_values

T = TypeVar("T")
K = TypeVar("K", bound=PyType)
V = TypeVar("V", bound=PyType)

BOOL_CONSTANTS = {
    "true": True,
    "false": False,
    "t": True,
    "f": False,
    "yes": True,
    "no": False,
    "y": True,
    "n": False,
    "1": True,
    "0": False,
}
JSON_NULL = "null"


class PandasCSVLoader:
    """Base interface for loading package data CSV files as pandas.DataFrames
    This is only for use at build time"""

    def __init__(self, table: metaschema.Table, schema: Optional[pa.DataFrameSchema] = None):
        if not isinstance(table.dependencies, TabularFileSource):
            raise ValueError(
                f"Table '{table.name}' has no single tablular text file source of truth; it depends on "
                f"{table.dependencies}"
            )

        self.schema = schema
        self.table = table
        self.header = [column.header_name for column in self.table.columns]
        self.rename = {column.header_name: column.name for column in self.table.columns}
        # set the primary key as the index
        self.index_cols: List[str] = list(self.table.primary_key) if self.table.primary_key else []
        self.cols = [c.name for c in self.table.columns if c.name not in self.index_cols]
        # pass these to the csv parser as parse_dates
        self.parse_date_cols = [
            column.header_name
            for column in self.table.columns
            if column.dtype in (DType.DATE, DType.DATETIME)
        ]
        self.na_values = table.csv_na_values
        self.dtypes = {}
        self.dtypes_for_csv_read = {}
        self.converters: Dict[str, Callable[[str], Any]] = {}
        for column in self.table.columns:
            dtype = column.pandas(index=column.name in self.index_cols)
            self.dtypes[column.name] = dtype
            na_values_for_col = self.na_values.get(column.header_name)
            if column.dtype == DType.BOOL:
                # we have a custom converter (parser) for boolean values.
                # converters override na_values in pandas.read_csv, so we have to specify them here.
                self.converters[column.header_name] = (
                    parse_optional(parse_bool, na_values_for_col) if na_values_for_col else parse_bool
                )
            elif isinstance(column.dtype, (metaschema.ArrayType, metaschema.MappingType)):
                # parse as json - again a custom converter which overrides na_values
                converter = sqlite_postprocessor_for_type(column.dtype.python)
                assert converter is not None  # converter for a structured type will not be None
                self.converters[column.header_name] = cast(
                    Callable[[str], Any],
                    parse_optional(converter, na_values_for_col) if na_values_for_col else converter,
                )
            elif column.header_name not in self.parse_date_cols:
                # read_csv requires passing `parse_dates` for determining date-typed columns
                # also do NOT tell pandas.read_csv you want an enum; it will mangle unknown values to null!
                self.dtypes_for_csv_read[column.header_name] = (
                    dtype
                    if column.type.enum is None
                    else column.dtype.pandas(
                        nullable=column.nullable, index=column.name in self.index_cols
                    )
                )

    def __call__(self, validate: bool = False) -> pd.DataFrame:
        if validate and self.schema is None:
            raise ValueError(f"Can't validate table {self.table.name} with no schema")

        df = self.read()
        df = self.postprocess(df)
        # schema not None only to make mypy happy - error thrown above in case it's required
        return self.schema.validate(df) if (validate and self.schema is not None) else df

    def read(self):
        # make mypy happy; this is checked in __init__
        deps = self.table.dependencies
        assert isinstance(deps, TabularFileSource)
        with deps.file_handle as f:
            df = pd.read_csv(  # type: ignore
                f,
                usecols=self.header,
                dtype=self.dtypes_for_csv_read,
                parse_dates=self.parse_date_cols,
                converters=self.converters,
                skiprows=deps.skiprows or 0,
                encoding=deps.encoding,
                dialect=deps.csv_dialect,
                na_values={k: sorted(v) for k, v in self.na_values.items()},
                # without this, pandas adds in its own extensive set of strings to interpret as null.
                # we force the user to be explicit about the values they want to parse as null.
                keep_default_na=False,
            )

        return df

    def postprocess(
        self,
        df: pd.DataFrame,
    ):
        """Ensure correct column names, column order, dtypes and index. Mutates `df` in-place"""
        df.rename(columns=self.rename, inplace=True)

        # pandas silently nullifies values not matching a categorical dtype!
        # so we have to do this ourselves before we coerce with .astype below
        for col in self.table.columns:
            name = col.name
            dtype = self.dtypes[name]
            if isinstance(dtype, pd.CategoricalDtype):
                check_categorical_values(df[name], dtype)

        df = df.astype(self.dtypes, copy=False)
        if self.index_cols:
            df.set_index(self.index_cols, inplace=True)

        if list(df.columns) != self.cols:
            df = df[self.cols]

        return df


# CSV parsing for complex types


def identity(x):
    return x


def parse_optional(
    func: Callable[[str], V], null_values: AbstractSet[str] = frozenset([""])
) -> Callable[[str], Optional[V]]:
    """Turn a csv parser for a type V into a parser for Optional[V] by treating the empty string as a
    null value"""

    def parse(s: str) -> Optional[V]:
        if s in null_values:
            return None
        return func(s)

    return parse


def parse_bool(s: str) -> bool:
    return BOOL_CONSTANTS[s.lower()]
