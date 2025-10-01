import typing as ty

from .connect import Connectable, autoconn_scope, autoconnect


@autoconn_scope.bound
def create(connectable: Connectable, table_name: str, columns: ty.Collection[str], unique: bool = False):
    """Is idempotent, but does not verify that your index DDL matches what you're asking for."""
    conn = autoconnect(connectable)
    """Create an index on a table in a SQLite database using only sqlite3 and SQL."""
    colnames = "_".join(colname for colname in columns).replace("-", "_")

    sql_create_index = (
        f"CREATE {'UNIQUE' if unique else ''} INDEX IF NOT EXISTS "
        f"[{table_name}_{colnames}_idx] ON [{table_name}] ({', '.join(columns)})"
    )
    try:
        conn.execute(sql_create_index)
        # do not commit - let the caller decide when to commit, or allow autoconnect to do its job
    except Exception:
        print(sql_create_index)
        raise
