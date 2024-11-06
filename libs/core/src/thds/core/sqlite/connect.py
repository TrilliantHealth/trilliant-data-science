import contextlib
import os
import sqlite3
import typing as ty

from thds.core import scope

from .functions import register_functions_on_connection
from .types import Connectable


def row_connect(path: ty.Union[str, os.PathLike]) -> sqlite3.Connection:
    """Get a connection to a row database"""
    conn = sqlite3.connect(os.fspath(path), isolation_level=None)  # autocommit
    conn.row_factory = sqlite3.Row
    return register_functions_on_connection(conn)


autoconn_scope = scope.Scope("sqlite3.autoconn")


def autoconnect(connectable: Connectable) -> sqlite3.Connection:
    """Will automatically commit when it hits the autoconn_scope.bound, but only if
    the connectable was not already a connection.
    """
    if isinstance(connectable, sqlite3.Connection):
        return connectable

    return autoconn_scope.enter(
        contextlib.closing(  # close the connection when we exit the scope
            row_connect(os.fspath(connectable))
        )
    )
