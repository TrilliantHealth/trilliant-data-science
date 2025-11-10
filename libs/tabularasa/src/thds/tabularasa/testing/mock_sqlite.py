import contextlib
import inspect
import sqlite3
import tempfile
import typing as ty
from pathlib import Path

import attrs
import pyarrow as pa
import pyarrow.parquet

from thds.core import scope
from thds.core.types import StrOrPath
from thds.tabularasa.data_dependencies import sqlite, util
from thds.tabularasa.schema import load_schema


class _GeneratedSqliteLoader(ty.Protocol):
    def __init__(
        self,
        package: ty.Optional[str],
        db_path: str,
    ) -> None: ...


_UNTIL_EXIT_SCOPE = scope.Scope("tabularasa.testing.mock_sqlite_loader")
# this scope is for creating temporary sqlite database files that persist until program exit, in case the caller of
# mock_sqlite_loader doesn't want to manage the database file themselves

L = ty.TypeVar("L", bound=_GeneratedSqliteLoader)


def mock_sqlite_loader(
    loader_cls: ty.Type[L],
    data: ty.Mapping[str, ty.Collection[attrs.AttrsInstance]],
    tmp_db_path: ty.Optional[StrOrPath] = None,
    *,
    package: ty.Optional[str] = None,
    schema_path: str = "schema.yaml",
    validate: bool = False,
) -> L:
    """Construct an instance of your custom generated sqlite loader from mocked data. Note that this is guaranteed
    typesafe because regardless of how you define your mock records, the resulting sqlite loader will be a true instance
    of your generated loader class, and will have all the same lookup methods and will use all the same deserialization
    logic for reading rows from the database and returning actual instances from your library's data model.

    :param loader_cls: The generated sqlite loader class to instantiate.
    :param data: A mapping from table names to collections of attrs records representing rows.
    :param package: The root package name containing the schema and generated loader(s). If omitted, it will be inferred
      from the loader class's `__module__` attribute by climbing up until a schema file is found.
    :param schema_path: The path to the schema file within the package.
    :param tmp_db_path: Optional path to a file to use for the sqlite database. If None, a temporary file is created.
      Note that in this case the temporary file will not be cleaned up until program exit.
    :param validate: Whether to validate data against the schema when inserting data into the database.
    :return: An instance of the specified sqlite loader class populated with the provided mocked data, with empty
      tables for any table names that were not included in the `data` mapping.
    """
    if package is None:
        if package_ := inspect.signature(loader_cls).parameters["package"].default:
            package_candidates = [package_]
        else:
            loader_module_path = loader_cls.__module__.split(".")
            package_candidates = [
                ".".join(loader_module_path[:i]) for i in range(len(loader_module_path), 0, -1)
            ]
    else:
        package_candidates = [package]

    for package_ in package_candidates:
        try:
            schema = load_schema(package_, schema_path)
        except (ModuleNotFoundError, FileNotFoundError):
            continue
        else:
            break
    else:
        raise ValueError(
            f"Could not infer package containing schema from loader class {loader_cls.__qualname__}; "
            "please specify the 'package' argument explicitly."
        )

    if tmp_db_path is None:
        tmp_db_path = _UNTIL_EXIT_SCOPE.enter(tempfile.NamedTemporaryFile(suffix=".sqlite")).name

    unknown_tables = set(data.keys()).difference(schema.tables.keys())
    if unknown_tables:
        raise ValueError(f"Data provided for unknown tables: {sorted(unknown_tables)}")

    with (
        tempfile.TemporaryDirectory() as tmpdir,
        contextlib.closing(sqlite3.connect(str(tmp_db_path))) as con,
    ):
        # this tmpdir is only for staging parquet files before loading into sqlite; it's fine that they get deleted
        # immediately after the database is populated
        for name, table in schema.tables.items():
            rows = data.get(name, [])
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

    return loader_cls(package=None, db_path=str(tmp_db_path))
