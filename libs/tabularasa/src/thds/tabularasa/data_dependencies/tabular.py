from typing import Any, Callable, Dict, FrozenSet, List, Optional, TypeVar

import pandas as pd
import pandera as pa

from thds.tabularasa.loaders.sqlite_util import sqlite_postprocessor_for_type
from thds.tabularasa.schema import metaschema
from thds.tabularasa.schema.dtypes import DType, PyType
from thds.tabularasa.schema.files import TabularFileSource

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
        self.skip_rows = table.dependencies.skiprows
        self.encoding = table.dependencies.encoding
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
        self.date_cols = [column.name for column in self.table.columns if column.dtype == DType.DATE]
        self.nullable_bool_cols = [
            column.name
            for column in self.table.columns
            if column.dtype == DType.BOOL and column.nullable
        ]
        self.dtypes = {}
        self.dtypes_for_csv_read = {}
        self.converters: Dict[str, Callable[[str], Any]] = {}
        for column in self.table.columns:
            index = column.name in self.index_cols
            dtype = column.pandas(index=index)
            self.dtypes[column.name] = dtype
            if column.dtype == DType.BOOL:
                self.converters[column.header_name] = (
                    parse_optional(parse_bool) if column.nullable else parse_bool
                )
            elif column.dtype == DType.STR and not column.nullable:
                # read_csv treats empty strings as null; we want to let them pass through unchanged
                # this also includes string enums - if they're not nullable and there are empty strings
                # that are invalid they'll fail downstream when we check
                self.converters[column.header_name] = identity
            elif isinstance(column.dtype, (metaschema.ArrayType, metaschema.MappingType)):
                converter = sqlite_postprocessor_for_type(column.dtype.python)
                assert converter is not None  # converter for a structured type will not be None
                if column.nullable:
                    converter = parse_optional(  # type: ignore
                        converter,
                        null_values=frozenset(["", JSON_NULL]),
                    )
                self.converters[column.header_name] = converter  # type: ignore
            elif (column.header_name not in self.parse_date_cols) and (column.type.enum is None):
                # read_csv requires passing `parse_dates` for this purpose
                # also do NOT tell pandas.read_csv you want an enum; it will mangle unknown values to null!
                self.dtypes_for_csv_read[column.header_name] = dtype

    def __call__(self, validate: bool = False) -> pd.DataFrame:
        if validate and self.schema is None:
            raise ValueError(f"Can't validate table {self.table.name} with no schema")

        df = self.read()
        df = self.postprocess(df)
        # schema not None only to make mypy happy - error thrown above in case it's required
        return self.schema.validate(df) if (validate and self.schema is not None) else df

    def read(self):
        # make mypy happy; this is checked in __init__
        assert isinstance(self.table.dependencies, TabularFileSource)
        with self.table.dependencies.file_handle as f:
            df = pd.read_csv(  # type: ignore
                f,
                usecols=self.header,
                dtype=self.dtypes_for_csv_read,
                parse_dates=self.parse_date_cols,
                converters=self.converters,
                skiprows=self.skip_rows,
                encoding=self.encoding,
                dialect=self.table.dependencies.csv_dialect,
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
        for name, dtype in self.dtypes.items():
            if isinstance(dtype, pd.CategoricalDtype):
                if not (df[name].isin(dtype.categories) | df[name].isna()).all():
                    categories = (
                        df.loc[~df[name].isin(dtype.categories), name].dropna().unique().tolist()
                    )
                    raise ValueError(
                        f"Column {name} of table {self.table.name} should have categorical type with values "
                        f"{dtype.categories.tolist()}; values {categories} are present in source file"
                    )

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
    func: Callable[[str], V], null_values: FrozenSet[str] = frozenset([""])
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
