import contextlib
import os
import sqlite3
import typing as ty

from thds.core import scope

from .functions import register_functions_on_connection
from .types import Connectable


def row_connect(path: ty.Union[str, os.PathLike]) -> sqlite3.Connection:
    """Get a connection to a row database"""
    conn = sqlite3.connect(os.fspath(path))
    conn.row_factory = sqlite3.Row
    register_functions_on_connection(conn)
    return conn


autoconn_scope = scope.Scope("sqlite3.autoconn")


def autoconnect(connectable: Connectable) -> sqlite3.Connection:
    """Will automatically commit when it hits the autoconn_scope.bound"""
    if isinstance(connectable, sqlite3.Connection):
        return connectable

    return autoconn_scope.enter(
        contextlib.closing(  # close the connection when we exit the scope
            register_functions_on_connection(
                sqlite3.connect(
                    os.fspath(connectable),
                    isolation_level=None,  # autocommit
                )
            )
        )
    )
