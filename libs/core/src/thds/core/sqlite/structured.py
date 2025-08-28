import functools
import typing as ty
from dataclasses import dataclass
from sqlite3 import Connection, OperationalError

from thds.core import config
from thds.core.lazy import ThreadLocalLazy
from thds.core.log import getLogger
from thds.core.types import StrOrPath

from .connect import row_connect
from .meta import column_names, get_tables, primary_key_cols
from .read import matching
from .types import T, TableSource

SQLITE_CACHE_SIZE = config.item("cache_size", 100_000)
MMAP_BYTES = config.item("mmap_bytes", 8_589_934_592)
_logger = getLogger(__name__)


@dataclass
class TableMeta:
    """Things which can be derived and cached once we have established the first Connection."""

    conn: Connection
    name: str
    pk_cols: ty.Set[str]
    colnames: ty.Set[str]


DbPathAndTableName = ty.Tuple[StrOrPath, str]


class BadPrimaryKey(ValueError):
    pass


class UnknownColumns(ValueError):
    pass


class _Table(ty.Protocol):
    def __call__(self, ignore_mmap_size: bool = False) -> TableMeta:
        ...


class StructTable(ty.Generic[T]):
    def __init__(
        self,
        from_item: ty.Callable[[ty.Mapping[str, ty.Any]], T],
        table_meta: ty.Callable[[ty.Optional[int]], ThreadLocalLazy[TableMeta]],
        cache_size: ty.Optional[int] = None,
        mmap_size: int = -1,
    ):
        if cache_size is None:
            cache_size = SQLITE_CACHE_SIZE()
        if mmap_size < 0:
            mmap_size = MMAP_BYTES()

        self._tbl = table_meta(mmap_size)
        self.from_item = from_item
        if cache_size:
            # Caching is only applied to `.list` and `.get` as `.matching` returns a consumable iterator
            self.get = functools.lru_cache(cache_size)(self.get)  # type: ignore
            self.list = functools.lru_cache(cache_size)(self.list)  # type: ignore

    def matching(self, **where: ty.Any) -> ty.Iterator[T]:
        tbl = self._tbl()
        try:
            for item in matching(tbl.name, tbl.conn, where):
                yield self.from_item(item)
        except OperationalError as e:
            if unknown_cols := (set(where) - tbl.colnames):
                raise UnknownColumns(f"Can't match on columns that don't exist: {unknown_cols}")
            else:
                raise e

    def get(self, **primary_key: ty.Any) -> ty.Optional[T]:
        """A primary key lookup. Returns None if there is no match.

        Raises if there is more than one match.
        """
        tbl = self._tbl()
        if not set(primary_key) == tbl.pk_cols:
            raise BadPrimaryKey(
                f"Primary key must be complete; expected {tbl.pk_cols} but got {primary_key}"
            )

        t_iter = self.matching(**primary_key)
        first = next(t_iter, None)
        if first is None:
            return None
        should_be_no_next = next(t_iter, None)
        if should_be_no_next is not None:
            raise BadPrimaryKey(f"More than one item found for supposed primary key {primary_key}")
        return first

    def list(self, **where: ty.Any) -> ty.List[T]:
        """List all items in the table where key/column = value."""
        return list(self.matching(**where))


def autometa_factory(
    src: ty.Callable[[], DbPathAndTableName]
) -> ty.Callable[[ty.Optional[int]], ThreadLocalLazy[TableMeta]]:
    """Use this factory to defer the connection and other settings (e.g., mmap_size) within each thread"""

    def _autometa(mmap_size: ty.Optional[int] = None) -> ThreadLocalLazy[TableMeta]:
        def _get_table_meta():
            db_path, table_name = src()
            conn = row_connect(db_path)
            # test if table even exists:
            if table_name not in get_tables(conn):
                raise ValueError(f"Table {table_name} not found in {db_path}")

            pk_cols = set(primary_key_cols(table_name, conn))
            if not pk_cols:
                raise BadPrimaryKey(f"Found no primary key cols for table {table_name}")
            colnames = set(column_names(table_name, conn))
            if not colnames:
                raise UnknownColumns(f"Found no columns for table {table_name}")

            if mmap_size:
                _logger.info(f"Setting sqlite mmap size to {mmap_size}")
                conn.execute(f"PRAGMA mmap_size={mmap_size};")

            return TableMeta(conn, table_name, pk_cols, colnames)

        return ThreadLocalLazy(_get_table_meta)

    return _autometa


def struct_table_from_source(
    from_item: ty.Callable[[ty.Mapping[str, ty.Any]], T],
    table_source: ty.Callable[[], TableSource],
    **kwargs,
) -> StructTable[T]:
    def extract_path_and_name() -> DbPathAndTableName:
        return str(table_source().db_src.path()), table_source().table_name

    return StructTable(from_item, autometa_factory(extract_path_and_name), **kwargs)
