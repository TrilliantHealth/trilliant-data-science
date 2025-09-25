import hashlib
import multiprocessing
import multiprocessing.connection
import os
import warnings
from functools import partial, wraps
from logging import getLogger
from pathlib import Path
from typing import IO, Callable, Dict, List, Optional, TypeVar, Union, cast

import pandas as pd
import pkg_resources
import pyarrow

from thds.tabularasa.data_dependencies.adls import ADLSDownloadResult
from thds.tabularasa.loaders.parquet_util import pandas_maybe, preprocessor_for_pyarrow_type
from thds.tabularasa.schema.files import LocalDataSpec
from thds.tabularasa.schema.metaschema import Table
from thds.tabularasa.schema.util import Identifier, import_func

PARQUET_FORMAT_VERSION = "2.4"
HASH_FILE_BUFFER_SIZE = 2**16
FILENAME_YEAR_REGEX = r"(?:[^\d])(20\d{2})(?:[^\d])"
FILENAME_QUARTER_REGEX = r"(?:[^\d])([Qq]\d{1})(?:[^\d])"

MaterializedPackageDataDeps = Dict[Identifier, pd.DataFrame]
SyncedADLSDeps = Dict[Identifier, List[ADLSDownloadResult]]
RawLocalDataDeps = Dict[Identifier, LocalDataSpec]
DataPreprocessor = Callable[
    [MaterializedPackageDataDeps, SyncedADLSDeps, RawLocalDataDeps], pd.DataFrame
]
F = TypeVar("F", bound=Callable)


def package_data_file_size(package: str, path: str) -> int:
    os_path = pkg_resources.resource_filename(package, path)
    return os.stat(os_path).st_size


def run_in_subprocess(func: F) -> F:
    """Decorator to cause a side-effect-producing routine to run in a subprocess. Isolates memory
    consumption of the function call to the subprocess to ensure that all memory resources are reclaimed
    at the end of the call. For example, `pyarrow` is known to be quite aggressive with memory allocation
    and reluctant to free consumed memory. For example, a routine that simply reads a parquet file using
    `pyarrow` and then writes the result somewhere else would benefit from use of this decorator.
    """

    @wraps(func)
    def subprocess_func(*args, _subprocess: bool = True, **kwargs):
        # extra _subprocess arg required to avoid trying to send the wrapped `func` to a subprocess,
        # which in python 3.8+ results in a pickling error since it isn't the same object as the
        # function importable at its own module/name (it's been replaced by `subprocess_func`)
        if _subprocess:
            recv_con, send_con = multiprocessing.Pipe(duplex=False)
            proc = multiprocessing.Process(
                target=SubprocessFunc(subprocess_func),
                args=(send_con, *args),
                kwargs=dict(_subprocess=False, **kwargs),
            )
            proc.start()
            try:
                result, exc = recv_con.recv()
                proc.join()
            except Exception as e:
                # communication error, e.g. unpicklable return value
                raise e
            else:
                if exc is not None:
                    raise exc
            finally:
                proc.close()
        else:
            result = func(*args, **kwargs)

        return result

    return cast(F, subprocess_func)


class SubprocessFunc:
    def __init__(self, func):
        self.func = func

    def __call__(self, con: multiprocessing.connection.Connection, *args, **kwargs):
        exc: Optional[Exception]
        try:
            result = self.func(*args, **kwargs)
        except Exception as e:
            exc = e
            result = None
        else:
            exc = None
        con.send((result, exc))


def hash_file(file: Union[Path, str, IO[bytes]]) -> str:
    """MD5 hash of the contents of a file (specified by path or passed directly as a handle)"""
    io: IO[bytes]
    if isinstance(file, (str, Path)):
        io = open(file, "rb")
        close = True
    else:
        io = file
        close = False

    hash_ = hashlib.md5()
    for bytes_ in iter(partial(io.read, HASH_FILE_BUFFER_SIZE), b""):
        hash_.update(bytes_)

    if close:
        io.close()

    return hash_.hexdigest()


def import_data_preprocessor(path: str) -> DataPreprocessor:
    return import_func(path)


def arrow_table_for_parquet_write(df: pd.DataFrame, table: Table) -> pyarrow.Table:
    """Preprocess a dataframe with possibly complex object types in preparation to write to a parquet
    file. Casts dicts to lists since pyarrow expects lists or arrays of key-value tuples as the
    represenation of mapping types. Also casts any types with different kinds - e.g. if a float column is
    expected but an int column is passed. Possibly mutates input as this should only be called on a table
    which is about to be saved as a parquet file and then garbage-collected."""
    logger = getLogger(__name__)

    if any(df.index.names):
        df.reset_index(inplace=True)

    table_columns = {c.name for c in table.columns}
    extra_columns = [c for c in df.columns if c not in table_columns]
    if extra_columns:
        logger.warning(
            f"Extra columns {extra_columns!r} in dataframe but not in schema of table {table.name!r} "
            "will be dropped on parquet write"
        )

    for column in table.columns:
        field = column.parquet_field
        name = column.name
        pproc = preprocessor_for_pyarrow_type(field.type)
        if pproc is not None:
            if field.nullable:
                pproc = pandas_maybe(pproc)
            df[name] = df[name].apply(pproc)

        if (enum_constraint := column.dtype.enum) is not None:
            try:
                check_categorical_values(df[name], pd.CategoricalDtype(enum_constraint.enum))
            except ValueError as e:
                # only warn on write since the data may in fact be correct while only the schema needs
                # updating, potentially saving the developer an expensive derivation
                warnings.warn(str(e))

    if table.primary_key:
        df.sort_values(list(table.primary_key), inplace=True)

    arrow = pyarrow.Table.from_pandas(df, table.parquet_schema, safe=True)
    # we remove the pandas-related metadata to ensure that insignificant changes e.g. to pandas/pyarrow
    # versions do not effect file hashes. We can safely do this since we don't rely on pandas to infer
    # data types on load, instead using the parquet/arrow schemas directly on load (pyarrow uses the
    # 'ARROW:schema' metadata key to document arrow schemas in a serialized binary format so we don't
    # lose that information by discarding the pandas information)
    meta = arrow.schema.metadata
    meta.pop(b"pandas")
    return arrow.replace_schema_metadata(meta)


def check_categorical_values(col: pd.Series, dtype: pd.CategoricalDtype):
    """Check that values in a column match an expected categorical dtype prior to a write or cast operation.
    This exists to preempt the unfortunate behavior of pandas wherein a cast silently nullifies any values
    which are not in the categories of the target `CategoricalDtype`, resulting in confusing errors (or
    worse - no errors in case null values are tolerated) downstream.

    :raises TypeError: when the underlying data type of the `series` has a different kind than the categories of the `dtype`
    :raises ValueError: when any values in the `series` are outside the expected set of categories of the `dtype`
    """
    current_dtype = col.dtype
    expected_dtype = dtype.categories.dtype

    if isinstance(current_dtype, pd.CategoricalDtype):
        current_dtype_kind = current_dtype.categories.dtype.kind
    else:
        current_dtype_kind = current_dtype.kind

    int_kinds = {"i", "u"}
    if current_dtype_kind != expected_dtype.kind and not (
        current_dtype_kind in int_kinds and expected_dtype.kind in int_kinds
    ):
        raise TypeError(
            f"Column {col.name} is expected to be categorical with underlying data type "
            f"{expected_dtype}, but has incompatible type {current_dtype}"
        )

    expected_values = dtype.categories
    actual_values = pd.Series(col.dropna().unique())
    bad_values = actual_values[~actual_values.isin(expected_values)]
    if len(bad_values):
        display_max_values = 20
        addendum = (
            f"(truncated to {display_max_values} unique values)"
            if len(bad_values) > display_max_values
            else ""
        )
        raise ValueError(
            f"Column {col.name} is expected to have values in the set {expected_values.tolist()}, "
            f"but also contains values {bad_values[:display_max_values].tolist()}{addendum}"
        )
