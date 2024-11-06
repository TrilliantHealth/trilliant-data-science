"""Utility for copying a table from one connection to another."""

from ..log import getLogger
from .connect import autoconn_scope, autoconnect
from .types import Connectable

logger = getLogger(__name__)


@autoconn_scope.bound
def table(source: Connectable, table_name: str, dest: Connectable) -> None:
    """Copy a table from one connection to another, including its table definition.

    If you can do this using ATTACH instead, that will be faster because it involves
    no Python code running in the loop.
    """
    source_conn = autoconnect(source)
    dest_conn = autoconnect(dest)

    source_table_sql = source_conn.execute(
        f"SELECT sql FROM sqlite_master WHERE name = '{table_name}'"
    ).fetchone()[0]

    dest_conn.execute(source_table_sql)

    src_data = source_conn.execute(f"SELECT * FROM {table_name}")

    dest_conn.execute("BEGIN TRANSACTION;")
    while True:
        data = src_data.fetchmany(1000)
        if not data:
            break
        placeholders = ", ".join(["?"] * len(data[0]))
        dest_conn.executemany(f"INSERT INTO {table_name} VALUES ({placeholders});", data)
    dest_conn.execute("COMMIT;")
