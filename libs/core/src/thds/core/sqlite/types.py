import sqlite3
import typing as ty
from pathlib import Path

from thds.core.source import Source


class DbAndTableP(ty.Protocol):
    def get_path(self) -> Path:
        ...

    @property
    def table_name(self) -> str:
        ...


class DbAndTable(ty.NamedTuple):
    db_path: Path
    table_name: str

    def get_path(self) -> Path:
        return self.db_path


class TableSource(ty.NamedTuple):
    db_src: Source
    table_name: str

    def get_path(self) -> Path:
        return self.db_src.path()


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


Connectable = ty.Union[str, Path, sqlite3.Connection]
