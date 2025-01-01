#!/usr/bin/env python
import sqlite3

from thds.adls.defaults import env_root
from thds.mops import impure, pure


@pure.pipeline_id_mask("examples/impure")
@pure.use_runner(impure.KeyedLocalRunner(env_root, keyfunc=impure.nil_args("conn")))
def run_limit_query_with_database_client(
    conn: sqlite3.Connection, tbl_name: str, limit: int = 3
) -> list:
    # the connection has no bearing on our pure function, so we can
    # use ImpureLocalRunner to memoize these results without making
    # alternative arrangements for the connection.
    return conn.execute(f"select * from {tbl_name} limit ?", (limit,)).fetchall()


# everything that follows is just test code that you can ignore.


def _create_test_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # Create a table called 'test_data' with 3 columns: 'id', 'name', and 'value'
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS test_data (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value REAL
        )
        """
    )

    # Insert 4 rows of test data into the 'test_data' table
    data = [(1, "John", 10.5), (2, "Jane", 15.2), (3, "Bob", 7.8), (4, "Alice", 12.3)]

    cursor.executemany("INSERT INTO test_data (id, name, value) VALUES (?, ?, ?)", data)

    conn.commit()
    return conn


test_conn = _create_test_conn()

assert run_limit_query_with_database_client(
    test_conn, "test_data"
) == run_limit_query_with_database_client(test_conn, "test_data", limit=3)
