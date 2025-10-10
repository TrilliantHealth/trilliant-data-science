import typing as ty

from .connect import Connectable, autoconn_scope, autoconnect


@autoconn_scope.bound
def create(
    connectable: Connectable, table_name: str, columns: ty.Collection[str], unique: bool = False
) -> str:
    """Create an index on a table in a SQLite database using only sqlite3 and SQL.

    Is idempotent, but does not verify that your index DDL matches what you're asking for."""
    colnames = "_".join(colname for colname in columns).replace("-", "_")
    idx_name = f"idx_{table_name}__{colnames}"

    sql_create_index = (
        f"CREATE {'UNIQUE' if unique else ''} INDEX IF NOT EXISTS "
        f"[{idx_name}] ON [{table_name}] ({', '.join(columns)})"
    )
    try:
        autoconnect(connectable).execute(sql_create_index)
        # do not commit - let the caller decide when to commit, or allow autoconnect to do its job
        return idx_name
    except Exception:
        print("FAILURE TO CREATE INDEX: " + sql_create_index)
        raise
