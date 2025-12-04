import datetime
import io
import warnings
from logging import getLogger
from pathlib import Path
from typing import Callable, Mapping, Optional, Union

import pandas as pd

from thds.tabularasa.loaders.parquet_util import TypeCheckLevel, pandas_maybe
from thds.tabularasa.loaders.sqlite_util import bulk_write_connection, sqlite_preprocessor_for_type
from thds.tabularasa.loaders.util import PandasParquetLoader
from thds.tabularasa.schema.compilation.sqlite import render_sql_index_schema, render_sql_table_schema
from thds.tabularasa.schema.metaschema import Schema, Table, is_build_time_package_table
from thds.tabularasa.sqlite3_compat import sqlite3

from .util import hash_file

TABLE_METADATA_TABLE_NAME = "_table_metadata"
TABLE_METADATA_DDL = f"""CREATE TABLE IF NOT EXISTS {TABLE_METADATA_TABLE_NAME}(
    table_name TEXT PRIMARY KEY,
    n_rows INTEGER NOT NULL,
    data_hash TEXT NOT NULL,
    table_ddl_hash TEXT NOT NULL ,
    index_ddl_hash TEXT
);"""


def insert_table(
    con: sqlite3.Connection,
    table: Table,
    package: Optional[str],
    data_dir: str,
    filename: Optional[str] = None,
    validate: bool = False,
    check_hash: bool = True,
    type_check: Optional[TypeCheckLevel] = None,
    cast: bool = False,
):
    """Insert data for a schema table but only if the table doesn't exist in the database OR it exists
    and the hashes of DDL scripts and data recorded there do not match those derived from auto-generated
    code and data from the schema
    """
    _LOGGER = getLogger(__name__)
    table_ddl = render_sql_table_schema(table)
    index_ddl = render_sql_index_schema(table)

    table_ddl_hash_expected = hash_file(io.BytesIO(table_ddl.strip().encode()))
    if index_ddl is None:
        index_ddl_hash_expected = None
    else:
        index_ddl_hash_expected = hash_file(io.BytesIO(index_ddl.strip().encode()))

    loader = PandasParquetLoader.from_schema_table(
        table,
        package=package or None,
        data_dir=data_dir,
        filename=filename,
        derive_schema=validate,
    )
    data_hash_expected = loader.file_hash()

    _ensure_metadata_table(con)

    if (not check_hash) or (not table_populated(con, table)):
        insert_data = True
    else:
        row = con.execute(
            f"SELECT data_hash, table_ddl_hash, index_ddl_hash FROM {TABLE_METADATA_TABLE_NAME} "
            "WHERE table_name = ?",
            (table.snake_case_name,),
        ).fetchone()
        if row is None:
            insert_data = True
        else:
            data_hash, table_ddl_hash, index_ddl_hash = row
            if table_ddl_hash != table_ddl_hash_expected:
                _LOGGER.info(
                    f"Hash of table DDL doesn't match that recorded in the database; reinserting data "
                    f"for {table.name}"
                )
                insert_data = True
            elif index_ddl_hash != index_ddl_hash_expected:
                _LOGGER.info(
                    f"Hash of index DDL doesn't match that recorded in the database; reinserting data "
                    f"for {table.name}"
                )
                insert_data = True
            elif data_hash != data_hash_expected:
                _LOGGER.info(
                    f"Hash of source data doesn't match that recorded in the database; reinserting data "
                    f"for {table.name}"
                )
                insert_data = True
            else:
                _LOGGER.info(
                    f"Hashes of table DDL, index DDL, and source data match those recorded in the "
                    f"database; skipping data insert for {table.name}"
                )
                insert_data = False

    if insert_data:
        _LOGGER.info(f"Inserting data for {table.name}")
        with con:
            _LOGGER.debug(f"Dropping existing table {table.name}")
            con.execute(f"DROP TABLE IF EXISTS {table.snake_case_name}")
            con.execute(
                f"DELETE FROM {TABLE_METADATA_TABLE_NAME} WHERE table_name = ?",
                (table.snake_case_name,),
            )

        with con:
            _LOGGER.debug(f"Executing table DDL for {table.name}:\n{table_ddl}")
            con.execute(table_ddl)
            # `postprocess=True` implies that all collection-valued columns will have values that are
            # instances of builtin python types (dicts, lists), and thus that they are handleable by
            # `cattrs` and thus ultimately JSON-serializable
            try:
                batches = loader.load_batched(
                    validate=validate, postprocess=True, type_check=type_check, cast=cast
                )
            except KeyError as key_error:
                _LOGGER.error(
                    "Column names in parquet file may not match "
                    "schema. Try deleting derived files and building again."
                )
                raise Exception(key_error)

            _LOGGER.debug(f"Inserting data for table {table.name}")
            for df in batches:
                df = _prepare_for_sqlite(df, table)

                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", "pandas only supports SQLAlchemy connectable")
                    df.to_sql(table.snake_case_name, con, if_exists="append", index=False)

        if index_ddl:
            with con:
                _LOGGER.debug(f"Executing index DDL for table {table.name}:\n{index_ddl}")
                con.executescript(index_ddl)

        with con:
            # only insert hashes once all table and index creation is complete
            _LOGGER.debug(f"Inserting metadata for {table.name}")
            num_rows = loader.num_rows()
            con.execute(
                f"INSERT INTO {TABLE_METADATA_TABLE_NAME}"
                "(table_name, n_rows, data_hash, table_ddl_hash, index_ddl_hash) VALUES (?, ?, ?, ?, ?)",
                (
                    table.snake_case_name,
                    num_rows,
                    data_hash_expected,
                    table_ddl_hash_expected,
                    index_ddl_hash_expected,
                ),
            )


def _prepare_for_sqlite(df: pd.DataFrame, table: Table) -> pd.DataFrame:
    """Only meant for use right before `df` is inserted into a sqlite table. Mutates `df` by converting
    collection-valued columns to JSON literals"""
    if table.primary_key:
        df.reset_index(inplace=True, drop=False)

    for column in table.columns:
        name = column.snake_case_name
        pytype = column.type.python
        if pytype is datetime.date:
            # pandas has only one type for dates/datetimes and uses a datetime ISO format for it, whereas
            # our sqlite adapters use ISO formats specific to dates and datetimes; just cast to regular
            # `datetime.date` here and the sqlite API will handle the conversion to ISO format since
            # we're using `detect_types=sqlite3.PARSE_DECLTYPES`
            df[name] = df[name].dt.date
        else:
            preproc = sqlite_preprocessor_for_type(pytype)  # type: ignore
            if preproc is not None:
                preproc_ = pandas_maybe(preproc) if column.nullable else preproc
                df[name] = df[name].apply(preproc_)

    return df


def _ensure_metadata_table(con: sqlite3.Connection):
    if not table_exists(con, TABLE_METADATA_TABLE_NAME):
        con.execute(TABLE_METADATA_DDL)


def table_populated(con: sqlite3.Connection, table: Table) -> bool:
    """Return True if a table named `table` exists in the database represented by `con` and has any rows,
    False otherwise"""
    try:
        cur = con.execute(
            f"SELECT {', '.join(c.snake_case_name for c in table.columns)} FROM {table.snake_case_name} limit 1"
        )
    except sqlite3.Error:
        return False
    else:
        results = list(cur)
        return bool(results)


def table_exists(con: sqlite3.Connection, table: Union[str, Table]) -> bool:
    table_name = table if isinstance(table, str) else table.snake_case_name
    try:
        con.execute(f"SELECT * FROM {table_name} limit 1")
    except sqlite3.Error:
        return False
    return True


def populate_sqlite_db(
    schema: Schema,
    db_package: Optional[str],
    db_path: str,
    data_package: Optional[str],
    data_dir: str,
    transient_data_dir: str,
    validate: bool = False,
    check_hash: bool = True,
    type_check: Optional[TypeCheckLevel] = None,
    cast: bool = False,
    table_predicate: Callable[[Table], bool] = is_build_time_package_table,
    data_path_overrides: Optional[Mapping[str, Path]] = None,
):
    """Populate a sqlite database with data for a set of tables from a `reference_data.schema.Schema`.
    Note that this can safely be called concurrently in multiple processes on the same database file; a file lock
    is acquired on the database file and only released when the data insertion is complete.

    :param schema: the `reference_data.schema.Schema` object defining the data to be inserted
    :param db_package: name of the package where the database file is stored, if any. In case `None` is
      passed, `db_path` refers to an ordinary file.
    :param db_path: path to the sqlite database archive file in which the data will be inserted
    :param data_package: optional package name, to be used if the data files are distributed as package
      data
    :param data_dir: path to the directory where the table parquet files are stored (relative to
      `data_package` if it is passed). Ignored for any tables specified in `data_path_overrides` - see
      below
    :param transient_data_dir: path to the directory where transient table parquet files are stored
      (relative to `data_package` if it is passed). Ignored for any tables specified in
      `data_path_overrides` - see below
    :param validate: if True, validate tables against their pandera schemas on load before inserting the
      data into the database
    :param check_hash: if True, skip tables whose data has already been inserted into the database, as
      indicated by a hash of file contents and DDL statements in the _table_metadata table in the
      database
    :param type_check: optional `reference_data.loaders.parquet_util.TypeCheckLevel`. If given, the
      arrow schemas of the files to be loaded will be checked against their expected arrow schemas as
      derived from `schema` before read. This is a very efficient check as it requires no data to be
      read. Useful for loading tables at run time as a quick validity check. This is passed to the same
      keyword argument of `reference_data.loaders.util.PandasParquetLoader.__call__`.
    :param cast: indicates that a safe cast of the parquet data should be performed on load using
      `pyarrow`, in case the file arrow schema doesn't match the expected one exactly. This is passed to
      the same keyword argument of `reference_data.loaders.util.PandasParquetLoader.__call__`.
    :param table_predicate: Optional predicate indicating which tables from `schema.tables` should be
      inserted into the database. If not given, all tables in the schema will be inserted.
    :param data_path_overrides: Optional mapping from table name to the file path where the parquet
      data for the table is to be loaded from. Any table whose name is a key in this mapping will be
      loaded from the associated file path as a normal file (`data_package` and `data_dir` will be
      ignored). This is useful for specifying dynamic run-time-installed tables.
    """
    # gather all tables before executing any I/O
    insert_tables = [table for table in schema.filter_tables(table_predicate) if table.has_indexes]

    if not insert_tables:
        return

    with bulk_write_connection(db_path, db_package, close=True) as con:
        for table in insert_tables:
            table_filename: Optional[str]
            table_package: Optional[str]
            if data_path_overrides and table.name in data_path_overrides:
                data_path = Path(data_path_overrides[table.name]).absolute()
                table_package = None
                table_data_dir = str(data_path.parent)
                table_filename = data_path.name
            else:
                table_package = data_package
                table_data_dir = transient_data_dir if table.transient else data_dir
                table_filename = None

            insert_table(
                con=con,
                table=table,
                package=table_package,
                data_dir=table_data_dir,
                filename=table_filename,
                validate=validate,
                check_hash=check_hash,
                type_check=type_check,
                cast=cast,
            )
