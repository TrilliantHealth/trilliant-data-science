import os
from logging import getLogger
from pathlib import Path
from typing import (
    IO,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import attr
import numpy as np
import pandas as pd
import pandas.core.dtypes.base as pd_dtypes
import pandera as pa
import pkg_resources
import pyarrow
import pyarrow.parquet as pq

from thds.tabularasa.data_dependencies.adls import sync_adls_data
from thds.tabularasa.data_dependencies.util import check_categorical_values, hash_file
from thds.tabularasa.schema.dtypes import PyType
from thds.tabularasa.schema.metaschema import RemoteBlobStoreSpec, Table
from thds.tabularasa.schema.util import snake_case

from .parquet_util import (
    TypeCheckLevel,
    list_map,
    postprocess_parquet_dataframe,
    postprocessor_for_pyarrow_type,
    type_check_pyarrow_schemas,
)

# sqlite_constructor_for_record_type and AttrsSQLiteDatabase are not
# used here but they're imported for backward compatibility with
# existing generated code, which expects it to be importable from
# here.  They were moved to sqlite_util to reduce the size of this file.
from .sqlite_util import AttrsSQLiteDatabase  # noqa: F401
from .sqlite_util import sqlite_constructor_for_record_type  # noqa: F401

T = TypeVar("T")
K = TypeVar("K", bound=PyType)
V = TypeVar("V", bound=PyType)
Record = TypeVar("Record", bound=attr.AttrsInstance)

PARQUET_EXT = ".parquet"
PQ_BATCH_SIZE_ATTRS = 100
PQ_BATCH_SIZE_PANDAS = 2**16


def identity(x):
    return x


def maybe(f: Callable[[T], V]) -> Callable[[Optional[T]], Optional[V]]:
    def f_(x):
        return None if x is None else f(x)

    return f_


def default_parquet_package_data_path(
    table_name: str, data_dir: str, as_package_data: bool = True
) -> str:
    """Standardized path to a parquet file resource for a packaged table inside a
    shared package subdirectory.
    When `as_package_data == True`, return a *package data* (Not OS) path to a resource; otherwise return
    a regular OS-compatible file path."""
    return package_data_path(
        f"{snake_case(table_name)}{PARQUET_EXT}", data_dir, as_package_data=as_package_data
    )


def package_data_path(filename: str, data_dir: str, as_package_data: bool = True) -> str:
    """Standardized path to a file resource for inside a shared package subdirectory.
    When `as_package_data == True`, return a *package data* (Not OS) path to a resource; otherwise return
    a regular OS-compatible file path.
    see https://setuptools.pypa.io/en/latest/pkg_resources.html#basic-resource-access"""
    return (
        f"{data_dir.rstrip('/')}/{filename}"
        if as_package_data
        else str(Path(data_dir.replace("/", os.sep)) / filename)
    )


def unique_across_columns(df: pd.DataFrame, colnames: Sequence[str]) -> pd.Series:
    index_cols = [c for c in colnames if c in df.index.names]
    cols = [c for c in colnames if c in df.columns]
    if not index_cols:
        check_cols = df[cols]
    elif not cols and set(index_cols) == set(df.index.names):
        # optimization - don't duplicate the index if we don't have to
        check_cols = df.index  # type: ignore
    else:
        check_cols = pd.DataFrame(
            {
                **{c: df.index.get_level_values(c).values for c in index_cols},
                **{c: df[c].values for c in cols},
            }
        )

    duped = check_cols.duplicated(keep=False)
    if isinstance(duped, pd.Series):
        # if check_cols was a frame vs an index
        duped = duped.values  # type: ignore

    return pd.Series(~duped, index=df.index)


def _register_unique_across_columns() -> None:
    # make sure the registration runs once
    # forced re-importing with `mops.testing.deferred_imports.assert_dev_deps_not_imported` raises an error
    if hasattr(pa.Check, "unique_across_columns"):
        return None

    pa.extensions.register_check_method(statistics=["colnames"], supported_types=pd.DataFrame)(
        unique_across_columns
    )


_register_unique_across_columns()


class _PackageDataOrFileInterface:
    package: Optional[str]
    data_path: str
    md5: Optional[str] = None
    blob_store: Optional[RemoteBlobStoreSpec] = None

    def set_path(self, table_name: str, data_dir: Union[str, Path], filename: Optional[str]):
        # package data or local file
        as_package_data = self.package is not None
        if filename is None:
            self.data_path = default_parquet_package_data_path(
                table_name, str(data_dir), as_package_data=as_package_data
            )
        else:
            self.data_path = package_data_path(filename, str(data_dir), as_package_data=as_package_data)
            assert os.path.exists(self.data_path)
        if self.blob_store is not None and self.md5 is None:
            raise ValueError(
                f"No md5 defined for remote file in blob store {self.blob_store} for table with local path "
                f"{self.data_path}"
            )

    def file_exists(self) -> bool:
        if self.package is None:
            return os.path.exists(self.data_path)
        else:
            return pkg_resources.resource_exists(self.package, self.data_path)

    def _resource_stream(self, sync: bool = True) -> IO[bytes]:
        if sync:
            maybe_path = self.sync_blob()
            if maybe_path:
                return open(maybe_path, "rb")

        if self.package is None:
            return open(self.data_path, "rb")
        else:
            return pkg_resources.resource_stream(self.package, self.data_path)

    def file_path(self, sync: bool = True) -> Path:
        """Path on the local filesystem to the file underlying this loader. If a blob store is specified
        and the local path doesn't exist, it will be synced, unless `sync=False` is passed."""
        if sync:
            maybe_path = self.sync_blob()
            if maybe_path:
                return maybe_path

        if self.package is None:
            return Path(self.data_path)
        else:
            return Path(pkg_resources.resource_filename(self.package, self.data_path))

    def sync_blob(self, link: bool = False) -> Optional[Path]:
        """Ensure that the local file underlying this loader is available.
        If the file does not exist, sync it from the blob store, or raise `FileNotFoundError` when no
        blob store is defined. Returns a local path to the cached download if a sync was performed,
        otherwise returns `None`.
        When `link` is True and a download is performed, the resulting file is linked to the local file
        associated with this resource.
        """
        if not self.file_exists():
            if self.blob_store is None:
                raise FileNotFoundError(
                    "Local or package data file doesn't exist and no remote blob is defined for table "
                    f"with local path {self.data_path}"
                )
            else:
                assert (
                    self.md5 is not None
                ), f"No md5 defined for {self.data_path}; can't safely sync blob"
                target_local_path = self.file_path(sync=False)
                getLogger(__name__).info(
                    f"Syncing blob with hash {self.md5}" f" to {target_local_path}" if link else ""
                )
                remote_data_spec = self.blob_store.data_spec(self.md5)
                local_files = sync_adls_data(remote_data_spec)
                local_path = local_files[0].local_path
                if link:
                    os.link(local_path, target_local_path)
                return local_path
        else:
            return None

    def file_hash(self) -> str:
        with self._resource_stream() as f:
            return hash_file(f)


class _ParquetPackageDataOrFileInterface(_PackageDataOrFileInterface):
    def metadata(self) -> pq.FileMetaData:
        with self._resource_stream() as f:
            return pq.ParquetFile(f).metadata

    def num_rows(self) -> int:
        return self.metadata().num_rows


class AttrsParquetLoader(Generic[Record], _ParquetPackageDataOrFileInterface):
    """Base interface for loading package resources as record iterators"""

    def __init__(
        self,
        table_name: str,
        type_: Type[Record],
        *,
        package: Optional[str],
        data_dir: Union[str, Path],
        filename: Optional[str] = None,
        pyarrow_schema: Optional[pyarrow.Schema] = None,
        md5: Optional[str] = None,
        blob_store: Optional[RemoteBlobStoreSpec] = None,
    ):
        self.type_ = type_
        self.table_name = table_name
        self.package = package
        self.pyarrow_schema = pyarrow_schema
        self.md5 = md5
        self.blob_store = blob_store
        self.set_path(table_name=table_name, data_dir=data_dir, filename=filename)

    def __call__(
        self, path: Optional[Path] = None, type_check: Optional[Union[int, TypeCheckLevel]] = None
    ) -> Iterator[Record]:
        """Load an iterator of instances of the attrs record type `self.type_` from a package data
        parqet file.

        :param path: Optional path to a local parquet file. Overrides the underlying package data file
          when passed.
        :param type_check: Optional `reference_data.loaders.parquet_util.TypeCheckLevel` indicating that
          a type check should be performed on the arrow schema of the parquet file _before_ reading any
          data, and at what level of strictness.
        :return: an iterator of instances of `self.type_`, an attrs class matching the schema of the
          parquet file being read.
        """
        if type_check is not None and self.pyarrow_schema is None:
            raise ValueError(f"Can't type check table {self.table_name} with no pyarrow schema")

        with self._resource_stream() if path is None else open(path, "rb") as f:
            col_order = [col.name for col in attr.fields(self.type_)]
            parquet_file = pq.ParquetFile(f)
            schema = parquet_file.schema.to_arrow_schema()
            if type_check is not None:
                assert self.pyarrow_schema is not None  # make mypy happy; this condition is checked
                type_check_pyarrow_schemas(
                    schema, self.pyarrow_schema, TypeCheckLevel(type_check), col_order
                )
            # this is to re-order columns *just in case* they're in a different order in the parquet file
            ixs_postprocessors: List[Tuple[int, Callable]] = []
            for name in col_order:
                i = schema.names.index(name)
                field = schema.field(i)
                pproc = postprocessor_for_pyarrow_type(field.type)
                if pproc is not None:
                    if field.nullable:
                        pproc = maybe(pproc)
                    pproc = list_map(pproc)
                else:
                    pproc = identity
                ixs_postprocessors.append((i, pproc))
            for batch in parquet_file.iter_batches(batch_size=PQ_BATCH_SIZE_ATTRS):
                columns = [pproc(batch.columns[i].to_pylist()) for i, pproc in ixs_postprocessors]
                parsed_rows = map(self.type_, *columns)
                yield from parsed_rows


class PandasParquetLoader(_ParquetPackageDataOrFileInterface):
    def __init__(
        self,
        table_name: str,
        *,
        package: Optional[str],
        data_dir: Union[str, Path],
        filename: Optional[str] = None,
        md5: Optional[str] = None,
        blob_store: Optional[RemoteBlobStoreSpec] = None,
        columns: Optional[List[str]] = None,
        schema: Optional[pa.DataFrameSchema] = None,
        pyarrow_schema: Optional[pyarrow.Schema] = None,
        index_columns: Optional[List[str]] = None,
        casts: Optional[Dict[str, Union[np.dtype, pd_dtypes.ExtensionDtype]]] = None,
    ):
        self.table_name = table_name
        self.schema = schema
        self.pyarrow_schema = pyarrow_schema
        self.columns = columns
        self.index_columns = index_columns
        self.casts = casts
        self.package = package
        self.md5 = md5
        self.blob_store = blob_store
        self.set_path(table_name=table_name, data_dir=data_dir, filename=filename)

    def __call__(
        self,
        path: Optional[Path] = None,
        validate: bool = False,
        type_check: Optional[Union[int, TypeCheckLevel]] = None,
        postprocess: bool = True,
        cast: bool = False,
    ) -> pd.DataFrame:
        """Load a `pandas.DataFrame` from a package data parqet file

        See the `load_batched` method for documentation of the parameters"""
        return next(
            self.load_batched(
                path,
                validate=validate,
                type_check=type_check,
                postprocess=postprocess,
                cast=cast,
                batch_size=None,
            )
        )

    def load_batched(
        self,
        path: Optional[Path] = None,
        batch_size: Optional[int] = PQ_BATCH_SIZE_PANDAS,
        validate: bool = False,
        type_check: Optional[Union[int, TypeCheckLevel]] = None,
        postprocess: bool = True,
        cast: bool = False,
    ) -> Iterator[pd.DataFrame]:
        """Load an iterator of `pandas.DataFrame`s from a package data parqet file in a memory-efficient way.

        :param path: Optional path to a local parquet file. Overrides the underlying package data file
          when passed.
        :param batch_size: Read the data in batches of this many rows. Every DataFrame yielded except
          possibly the last will have this many rows. Allows for control of memory usage. If `None`,
          the entire table will be read and yielded as a single DataFrame.
        :param validate: validate against the associated `pandera` schema?
        :param postprocess: apply postprocessors to complex types? E.g., `pyarrow` returns lists of
          tuples for mapping types; this will cast those to dicts
        :param type_check: Optional `reference_data.loaders.parquet_util.TypeCheckLevel` indicating that
          a type check should be performed on the arrow schema of the parquet file _before_ reading any
          data, and at what level of strictness.
        :param cast: Indicates whether to attempt a pyarrow table cast on read. When `False`, no cast
          will ever be performed. When `True`, the behavior depends on the value of `type_check`:
          When `type_check` is supplied, in case of a type check failure, attempt to cast the
          arrow table to the arrow schema for this table. When `type_check` is `None`, always cast the
          arrow table to the arrow schema for this table.
        :return: iterator of the the loaded and possibly postprocessed and validated DataFrames
        """
        if validate and self.schema is None:
            raise ValueError(f"Can't validate table {self.table_name} with no pandera schema")

        if type_check is not None and self.pyarrow_schema is None:
            raise ValueError(f"Can't type check table {self.table_name} with no pyarrow schema")

        if cast and self.pyarrow_schema is None:
            raise ValueError(f"Can't cast table {self.table_name} with no pyarrow schema")

        logger = getLogger(__name__)

        with self._resource_stream() if path is None else open(path, "rb") as f:
            pq_file = pyarrow.parquet.ParquetFile(f)
            schema = pq_file.schema.to_arrow_schema()
            if type_check is not None:
                assert self.pyarrow_schema is not None  # make mypy happy; this condition is checked
                try:
                    logger.info(f"Type-checking parquet file for table {self.table_name}")
                    type_check_pyarrow_schemas(
                        schema, self.pyarrow_schema, TypeCheckLevel(type_check), self.columns
                    )
                except TypeError as e:
                    if cast:
                        logger.warning(
                            f"Type-checking failed at level '{type_check.name}'; "  # type: ignore
                            "a type cast will be attempted on read"
                        )
                    else:
                        raise e
                else:
                    cast = False

            logger.info(f"Loading arrow data for table {self.table_name} from parquet")
            if batch_size is None:
                table = pq_file.read(self.columns)
                batches: Iterable[pyarrow.Table] = [table]
            else:
                batches = (
                    pyarrow.Table.from_batches([b])
                    for b in pq_file.iter_batches(batch_size, columns=self.columns)
                )

            categorical_dtypes = (
                [
                    (name, dtype)
                    for name, dtype in self.casts.items()
                    if isinstance(dtype, pd.CategoricalDtype)
                ]
                if self.casts
                else []
            )

            for table in batches:
                if cast:
                    assert self.pyarrow_schema is not None  # make mypy happy; this condition is checked
                    table = table.cast(self.pyarrow_schema)

                # ignore_metadata is only here because table.to_pandas has a bug wherein some dtypes get
                # changed for index columns (which are specified in the metadata under a 'pandas' key).
                # We handle setting the index ourselves below.
                # Likewise, we omit the `categories` arg here, as pyarrow cannot in general recover the
                # order of the original categories from the parquet file - those are handled using
                # `self.casts` below.
                df = table.to_pandas(date_as_object=False, ignore_metadata=True)

                if self.casts:
                    for name, dtype in categorical_dtypes:
                        check_categorical_values(df[name], dtype)

                    df = df.astype(self.casts, copy=False)

                if postprocess:
                    df = postprocess_parquet_dataframe(df, schema)

                if self.index_columns is not None:
                    df.set_index(self.index_columns, inplace=True)

                if validate:
                    assert self.schema is not None  # make mypy happy; this condition is checked above
                    df = self.schema.validate(df)

                yield df

    @classmethod
    def from_pandera_schema(
        cls,
        table_name: str,
        schema: pa.DataFrameSchema,
        package: str,
        data_dir: str,
        *,
        blob_store: Optional[RemoteBlobStoreSpec] = None,
        md5: Optional[str] = None,
        filename: Optional[str] = None,
        pyarrow_schema: Optional[pyarrow.Schema] = None,
    ) -> "PandasParquetLoader":
        casts: Dict[str, Union[np.dtype, pd_dtypes.ExtensionDtype]] = {}
        index_columns: Optional[List[str]]
        all_cols = list(schema.columns.items())
        if isinstance(schema.index, pa.MultiIndex):
            all_cols.extend(schema.index.columns.items())
            index_columns = list(schema.index.names)
        elif isinstance(schema.index, pa.Index):
            all_cols.append((schema.index.name, schema.index))
            index_columns = list(schema.index.names)
        else:
            index_columns = None

        for name, col in all_cols:
            assert isinstance(name, str)
            dtype = col.dtype.type if isinstance(col.dtype, pa.DataType) else col.dtype

            if isinstance(dtype, pd_dtypes.ExtensionDtype):
                casts[name] = dtype
            elif isinstance(dtype, pa.dtypes.Int) and (  # type: ignore
                (not dtype.signed) or dtype.bit_width not in (32, 64)
            ):
                typename = f"{'' if dtype.signed else 'u'}int{dtype.bit_width}"
                casts[name] = np.dtype(typename)
            elif np.issubdtype(dtype, np.datetime64):
                casts[name] = np.dtype("datetime64[ns]")

        return cls(
            table_name,
            schema=schema,
            pyarrow_schema=pyarrow_schema,
            columns=[name for name, _col in all_cols],
            index_columns=index_columns,
            casts=casts,
            package=package,
            data_dir=data_dir,
            filename=filename,
            blob_store=blob_store,
            md5=md5,
        )

    @classmethod
    def from_schema_table(
        cls,
        table: Table,
        package: Optional[str],
        data_dir: Union[str, Path],
        filename: Optional[str] = None,
        derive_schema: bool = False,
    ) -> "PandasParquetLoader":
        return cls(
            table.name,
            schema=table.pandera_schema if derive_schema else None,
            pyarrow_schema=table.parquet_schema,
            columns=[t.name for t in table.columns],
            index_columns=table.primary_key if table.primary_key is None else list(table.primary_key),
            casts=table.parquet_casts,
            package=package,
            data_dir=data_dir,
            filename=filename,
            md5=table.md5,
        )
