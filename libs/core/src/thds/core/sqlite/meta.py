"""Read-only utilities for inspecting a SQLite database.

Note that none of these are safe from SQL injection - you should probably
not be allowing users to specify tables in an ad-hoc fashion.
"""

import contextlib
import os
import sqlite3
import typing as ty
from pathlib import Path

from thds.core import log
from thds.core.source import from_file

from .connect import autoconn_scope, autoconnect
from .types import Connectable, TableSource

logger = log.getLogger(__name__)


def fullname(table_name: str, schema_name: str = "") -> str:
    if schema_name:
        return f"{schema_name}.[{table_name}]"
    return f"[{table_name}]"


@autoconn_scope.bound
def list_tables(connectable: Connectable, schema_name: str = "") -> ty.List[str]:
    conn = autoconnect(connectable)
    return [
        row[0]
        for row in conn.execute(
            f"SELECT name FROM {fullname('sqlite_master', schema_name)} WHERE type='table'"
        )
    ]


@autoconn_scope.bound
def get_tables(connectable: Connectable, *, schema_name: str = "main") -> ty.Dict[str, str]:
    """Keys of the returned dict are the names of tables in the database.

    Values of the returned dict are the raw SQL that can be used to recreate the table.
    """
    conn = autoconnect(connectable)
    return {
        row[0]: row[1]
        for row in conn.execute(
            f"""
            SELECT name, sql
            FROM {fullname('sqlite_master', schema_name)}
            WHERE type = 'table'
            AND sql is not null
            """
        )
    }


def pydd(path: os.PathLike):
    """Sometimes running this on a big sqlite file before starting a
    query will make a big difference to overall query performance.
    """
    with open(path, "rb") as f:
        while f.read(1024 * 1024):
            pass


def table_name_from_path(db_path: Path) -> str:
    tables = list_tables(db_path)
    assert len(tables) == 1, f"Expected exactly one table, got {tables}"
    return tables[0]


def table_source(db_path: Path, table_name: str = "") -> TableSource:
    if not table_name:
        table_name = table_name_from_path(db_path)
    return TableSource(from_file(db_path), table_name)


@autoconn_scope.bound
def primary_key_cols(table_name: str, connectable: Connectable) -> ty.Tuple[str, ...]:
    conn = autoconnect(connectable)
    return tuple(
        row[0]
        for row in conn.execute(
            f"""
            SELECT name
            FROM pragma_table_info('{table_name}')
            WHERE pk <> 0
            """
        )
    )


@autoconn_scope.bound
def column_names(table_name: str, connectable: Connectable) -> ty.Tuple[str, ...]:
    conn = autoconnect(connectable)
    return tuple([row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")])


def preload_sources(*table_srcs: ty.Optional[TableSource]) -> None:
    for table_src in table_srcs:
        if not table_src:
            continue
        assert isinstance(table_src, TableSource)
        logger.debug("Preloading %s from %s", table_src.table_name, table_src.db_src.uri)
        pydd(table_src.db_src)
    logger.debug("Preloading complete")


@autoconn_scope.bound
def get_indexes(
    table_name: str, connectable: Connectable, *, schema_name: str = "main"
) -> ty.Dict[str, str]:
    """Keys of the returned dict are the names of indexes belonging to the given table.

    Values of the returned dict are the raw SQL that can be used to recreate the index(es).
    """
    conn = autoconnect(connectable)
    return {
        row[0]: row[1]
        for row in conn.execute(
            f"""
            SELECT name, sql
            FROM {fullname('sqlite_master', schema_name)}
            WHERE type = 'index' AND tbl_name = '{table_name}'
            AND sql is not null
            """
        )
        if row[1]
        # sqlite has internal indexes, usually prefixed with "sqlite_autoindex_",
        # that do not have sql statements associated with them.
        # we exclude these because they are uninteresting to the average caller of this function.
    }


@contextlib.contextmanager
@autoconn_scope.bound
def debug_errors(connectable: Connectable) -> ty.Iterator:
    try:
        yield
    except Exception:
        try:
            conn = autoconnect(connectable)
            cur = conn.cursor()
            cur.execute("PRAGMA database_list")
            rows = cur.fetchall()
            databases = {row[1]: dict(num=row[0], file=row[2]) for row in rows}
            logger.error(f"Database info: {databases}")
            for db_name in databases:
                logger.error(
                    f"    In {db_name}, tables are: {get_tables(connectable, schema_name=db_name)}"
                )
        except Exception:
            logger.error(f"SQLite database: {connectable} is not introspectable")

        raise


@autoconn_scope.bound
def get_table_schema(
    conn: ty.Union[sqlite3.Connection, Connectable], table_name: str
) -> ty.Dict[str, str]:
    """
    Retrieve the schema of a given table.

    Args:
        conn: The database connection object or a Connectable.
        table_name: The name of the table.

    Returns: A dictionary with column names as keys and their types as values.
    """
    # Ensure we have a connection object
    connection = autoconnect(conn)

    # Fetch the table schema
    cursor = connection.cursor()
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    schema = {row[1]: row[2].lower() for row in cursor.fetchall()}
    return schema


@autoconn_scope.bound
def attach(connectable: Connectable, db_path: os.PathLike, schema_name: str) -> None:
    """ATTACH a database to the current connection, using your provided schema name.

    It must be an actual file.
    """
    conn = autoconnect(connectable)
    conn.execute(f"ATTACH DATABASE '{os.fspath(db_path)}' AS {schema_name}")
