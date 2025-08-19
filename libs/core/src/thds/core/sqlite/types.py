import os
import sqlite3
import typing as ty
from pathlib import Path

from thds.core.source import Source


class DbAndTableP(ty.Protocol):
    @property  # read-only
    def db_path(self) -> os.PathLike:
        ...

    @property  # read-only
    def table_name(self) -> str:
        ...


class DbAndTable(ty.NamedTuple):
    db_path: Path
    table_name: str


class TableSource(ty.NamedTuple):
    db_src: Source
    table_name: str

    @property
    def db_path(self) -> os.PathLike:
        return self.db_src


AnyDbTableSrc = ty.Union[DbAndTableP, ty.Callable[[], DbAndTableP]]


def resolve_lazy_db_and_table(table_src: AnyDbTableSrc) -> DbAndTableP:
    if hasattr(table_src, "table_name"):
        src = ty.cast(DbAndTableP, table_src)
    else:
        src = table_src()  # type: ignore
    assert hasattr(src, "table_name"), "table_name must be provided"
    return src


class TableMaster(ty.NamedTuple):
    """Element/asset table and its corresponding metadata table"""

    table: TableSource
    metadata: TableSource


T = ty.TypeVar("T")


def maybe_t(
    to_t: ty.Callable[[ty.Mapping[str, ty.Any]], T],
    row: ty.Optional[ty.Mapping[str, ty.Any]],
) -> ty.Optional[T]:
    if row:
        return to_t(row)
    return None


Connectable = ty.Union[os.PathLike, sqlite3.Connection]
