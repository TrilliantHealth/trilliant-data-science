import contextlib
import sqlite3
import tempfile
import typing as ty
import warnings
from pathlib import Path

import attrs
import pyarrow as pa
import pyarrow.parquet

from thds.core.types import StrOrPath
from thds.tabularasa.data_dependencies import sqlite, util
from thds.tabularasa.schema import load_schema


class _GeneratedSqliteLoader(ty.Protocol):
    def __init__(
        self,
        package: ty.Optional[str],
        db_path: str,
    ) -> None: ...


L = ty.TypeVar("L", bound=_GeneratedSqliteLoader)


def mock_sqlite_loader(
    loader_cls: ty.Type[L],
    data: ty.Mapping[str, ty.Collection[attrs.AttrsInstance]],
    package: ty.Optional[str],
    schema_path: str = "schema.yaml",
    tmp_db_path: ty.Optional[StrOrPath] = None,
    validate: bool = False,
) -> L:
    """Construct an instance of your custom generated sqlite loader from mocked data.

    :param loader_cls: The generated sqlite loader class to instantiate.
    :param data: A mapping from table names to collections of attrs records representing rows.
    :param package: The root package name containing the schema and generated loader(s).
    :param schema_path: The path to the schema file within the package.
    :param tmp_db_path: Optional path to a temporary sqlite database file. If None, an in-memory database is used.
    :param validate: Whether to validate data against the schema when inserting data into the database.
    :return: An instance of the specified sqlite loader class populated with the provided mocked data, with empty
      tables for any table names that were not included in the `data` mapping.
    """
    schema = load_schema(package, schema_path)
    if tmp_db_path is None:
        tmp_db_path = tempfile.NamedTemporaryFile(suffix=".sqlite").name
        warnings.warn(f"Created temp sqlite file at {tmp_db_path}; you'll have to clean it up yourself")

    data_ = dict(data)
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        contextlib.closing(sqlite3.connect(str(tmp_db_path))) as con,
    ):
        for name, table in schema.tables.items():
            rows = data_.pop(name, [])
            pa_table = pa.Table.from_pylist(
                [attrs.asdict(row, recurse=True) for row in rows], schema=table.parquet_schema
            )
            filename = name + ".parquet"
            pyarrow.parquet.write_table(
                pa_table, Path(tmpdir) / filename, version=util.PARQUET_FORMAT_VERSION
            )
            sqlite.insert_table(
                con,
                table,
                package=None,
                data_dir=tmpdir,
                filename=filename,
                validate=validate,
                cast=False if validate else True,
            )

    if data_:
        msg = f"Data provided for unknown tables: {list(data_.keys())}"
        if validate:
            raise ValueError(msg)
        else:
            warnings.warn(msg)

    return loader_cls(package=None, db_path=str(tmp_db_path))
