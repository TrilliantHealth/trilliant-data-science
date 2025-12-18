import contextlib
import datetime
import itertools
import json
import logging
import os
import sys
import typing as ty
from functools import lru_cache, wraps
from pathlib import Path
from typing import Callable, Optional, Type

import attr
import cattrs.preconf.json
import pkg_resources
from filelock import FileLock
from typing_inspect import get_args, get_origin, is_literal_type, is_optional_type, is_union_type

from thds.core.types import StrOrPath
from thds.tabularasa.schema.dtypes import DType
from thds.tabularasa.sqlite3_compat import sqlite3

DEFAULT_ATTR_SQLITE_CACHE_SIZE = 100_000
DEFAULT_MMAP_BYTES = int(os.environ.get("TABULA_RASA_DEFAULT_MMAP_BYTES", 8_589_934_592))  # 8 GB

PARAMETERIZABLE_BUILTINS = sys.version_info >= (3, 9)

if not PARAMETERIZABLE_BUILTINS:
    _builtin_to_typing = {
        list: ty.List,
        set: ty.Set,
        frozenset: ty.FrozenSet,
        tuple: ty.Tuple,
        dict: ty.Dict,
    }

    def get_generic_origin(t) -> ty.Optional[ty.Type]:
        org = get_origin(t)
        return None if org is None else _builtin_to_typing.get(org, org)  # type: ignore

else:
    get_generic_origin = get_origin


LITERAL_SQLITE_TYPES = {int, float, bool, str, type(None), datetime.date, datetime.datetime}


CONVERTER = cattrs.preconf.json.make_converter()


def structure_date(s: str, dt: ty.Type[datetime.date] = datetime.date) -> datetime.date:
    return dt.fromisoformat(s)


CONVERTER.register_structure_hook(datetime.date, structure_date)
CONVERTER.register_unstructure_hook(datetime.date, datetime.date.isoformat)

T = ty.TypeVar("T")
Record = ty.TypeVar("Record", bound=attr.AttrsInstance)


@lru_cache(None)
def sqlite_postprocessor_for_type(t: ty.Type[T]) -> Optional[Callable[[ty.AnyStr], Optional[T]]]:
    """Construct a parser converting an optional JSON string from a sqlite query to a python value of
    type T"""
    t = resolve_newtypes(t)

    if is_primitive_type(t):
        return None

    if is_optional_type(t):

        def parse(s: ty.AnyStr) -> Optional[T]:
            if s is None:
                return None
            raw = json.loads(s)
            return CONVERTER.structure(raw, t)

    else:

        def parse(s: ty.AnyStr) -> Optional[T]:
            raw = json.loads(s)
            return CONVERTER.structure(raw, t)

    return parse


@lru_cache(None)
def sqlite_preprocessor_for_type(t: ty.Type[T]) -> Optional[Callable[[T], Optional[str]]]:
    """Prepare a value of type T for inserting into a sqlite TEXT/JSON column by serializing it as
    JSON"""
    t = resolve_newtypes(t)

    if is_primitive_type(t):
        return None

    if is_optional_type(t):

        def unparse(value: T) -> Optional[str]:
            if value is None:
                return None
            raw = CONVERTER.unstructure(value, t)
            return json.dumps(raw, check_circular=False, indent=None)

    else:

        def unparse(value: T) -> Optional[str]:
            raw = CONVERTER.unstructure(value, t)
            return json.dumps(raw, check_circular=False, indent=None)

    return unparse


def resolve_newtypes(t: Type) -> Type:
    supertype = getattr(t, "__supertype__", None)
    if supertype is None:
        origin = get_generic_origin(t)
        if origin is None:
            return t
        args = get_args(t)
        if not args:
            return t
        args_resolved = tuple(resolve_newtypes(a) for a in args)
        return origin[args_resolved]
    return resolve_newtypes(supertype)


def is_primitive_type(t: Type) -> bool:
    if t in LITERAL_SQLITE_TYPES:
        return True
    elif is_optional_type(t) and is_primitive_type(nonnull_type_of(t)):
        return True
    elif is_literal_type(t):
        return True
    else:
        return False


def nonnull_type_of(t: Type) -> Type:
    if not is_union_type(t):
        return t
    types = get_args(t)
    if type(None) not in types:
        return t
    nonnull_types = tuple(t_ for t_ in types if t_ is not type(None))  # noqa
    return ty.Union[nonnull_types]  # type: ignore


def to_local_path(path: StrOrPath, package: Optional[str] = None) -> Path:
    if package is None:
        return Path(path)
    else:
        return Path(pkg_resources.resource_filename(package, str(path)))


def set_bulk_write_mode(con: sqlite3.Connection) -> sqlite3.Connection:
    logger = logging.getLogger(__name__)
    logger.debug("Setting pragmas for bulk write optimization")
    # https://www.sqlite.org/pragma.html#pragma_synchronous
    _log_exec_sql(logger, con, "PRAGMA synchronous = 0")  # OFF

    return con


def unset_bulk_write_mode(con: sqlite3.Connection) -> sqlite3.Connection:
    logger = logging.getLogger(__name__)
    logger.debug("Setting pragmas for bulk write optimization")
    _log_exec_sql(logger, con, "PRAGMA synchronous = 2")  # FULL (default)

    return con


@contextlib.contextmanager
def bulk_write_connection(
    db_path: StrOrPath, db_package: Optional[str] = None, close: bool = True
) -> ty.Generator[sqlite3.Connection, None, None]:
    """Context manager to set/unset bulk write mode on a sqlite connection. Sets pragmas for efficient bulk writes,
    such as loosening synchronous and locking modes. If `close` is True, the connection will be closed on exit.
    To avoid bulk insert routines being run by other processes concurrently, we also acquire a file lock on the
    database file on entry and release it on exit. Other processes attempting to perform bulk writes to the same file
    will block until the lock is released. In the case of tabularasa init-sqlite, the semantics then imply that those
    workers will perform no writes at all, since metadata will indicate that the data in the file is up-to-date.
    """
    db_path_ = to_local_path(db_path, db_package).absolute()
    lock_path = db_path_.with_suffix(".lock")
    lock = FileLock(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.info("PID %d: Acquiring lock on %s", os.getpid(), lock_path)
    with lock:
        con = sqlite_connection(db_path, db_package, read_only=False)
        set_bulk_write_mode(con)
        try:
            yield con
        finally:
            unset_bulk_write_mode(con)

            if close:
                con.close()

            if lock_path.exists():
                os.remove(lock_path)


def sqlite_connection(
    db_path: StrOrPath,
    package: Optional[str] = None,
    *,
    mmap_size: Optional[int] = None,
    read_only: bool = False,
) -> sqlite3.Connection:
    db_full_path = to_local_path(db_path, package)

    logger = logging.getLogger(__name__)
    logger.info(f"Connecting to sqlite database: {db_full_path}")
    # sqlite3.PARSE_DECLTYPES will cover parsing dates/datetimes from the db
    con = sqlite3.connect(
        db_full_path.absolute(), detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=not read_only
    )
    if mmap_size is not None:
        logger.info(f"Setting sqlite mmap size to {mmap_size}")
        _log_exec_sql(logger, con, f"PRAGMA mmap_size={mmap_size};")

    return con


def _log_exec_sql(
    logger: logging.Logger, con: sqlite3.Connection, statement: str, level: int = logging.DEBUG
) -> None:
    logger.log(level, "sqlite: %s", statement)
    con.execute(statement)


@lru_cache(None)
def sqlite_constructor_for_record_type(cls):
    """Wrap an `attrs` record class to allow it to accept raw JSON strings from sqlite queries in place
    of collection types. If not fields of the class have collection types, simply return the record class
    unwrapped"""
    postprocessors = [sqlite_postprocessor_for_type(type_) for type_ in cls.__annotations__.values()]
    if not any(postprocessors):
        return cls

    @wraps(cls)
    def cons(*args):
        return cls(*(v if f is None else f(v) for f, v in zip(postprocessors, args)))

    return cons


class AttrsSQLiteDatabase:
    """Base interface for loading package resources as record iterators"""

    def __init__(
        self,
        package: ty.Optional[str],
        db_path: StrOrPath,
        cache_size: ty.Optional[int] = DEFAULT_ATTR_SQLITE_CACHE_SIZE,
        mmap_size: int = DEFAULT_MMAP_BYTES,
    ):
        if cache_size is not None:
            self.sqlite_index_query = lru_cache(cache_size)(self.sqlite_index_query)  # type: ignore

        self._sqlite_con = sqlite_connection(
            db_path,
            package,
            mmap_size=mmap_size,
            read_only=True,
        )

    def sqlite_index_query(
        self, clazz: ty.Callable[..., Record], query: str, args: ty.Tuple
    ) -> ty.List[Record]:
        result = self._sqlite_con.execute(query, args).fetchall()
        return [clazz(*r) for r in result]

    def sqlite_pk_query(
        self, clazz: ty.Callable[..., Record], query: str, args: ty.Tuple
    ) -> ty.Optional[Record]:
        # Note: when we create PK indexes on our sqlite tables, we enforce a UNIQUE constraint, so if the
        # build succeeds then we're guaranteed 0 or 1 results here
        result = self.sqlite_index_query(clazz, query, args)
        return result[0] if result else None

    @ty.overload
    def sqlite_bulk_query(
        self,
        clazz: ty.Callable[..., Record],
        query: str,
        args: ty.Collection[ty.Tuple],
        single_col: ty.Literal[False],
    ) -> ty.Iterator[Record]: ...

    @ty.overload
    def sqlite_bulk_query(
        self,
        clazz: ty.Callable[..., Record],
        query: str,
        args: ty.Collection,
        single_col: ty.Literal[True],
    ) -> ty.Iterator[Record]: ...

    def sqlite_bulk_query(
        self, clazz: ty.Callable[..., Record], query: str, args: ty.Collection, single_col: bool
    ) -> ty.Iterator[Record]:
        """Note: this method is intentionally left un-cached; it makes a tradeoff: minimize the number of disk acesses
        and calls into sqlite at the cost of potentially re-loading the same records multiple times in case multiple
        calls pass overlapping keys. Since it isn't cached, it can also be lazyly evaluated as an iterator. Callers are
        encouraged to take advantage of this laziness where it may be useful."""
        if single_col:
            args_ = args if isinstance(args, (list, tuple)) else list(args)
        else:
            args_ = list(itertools.chain.from_iterable(args))
        cursor = self._sqlite_con.execute(query, args_)
        for row in cursor:
            yield clazz(*row)


# SQL pre/post processing


def load_date(datestr: Optional[bytes]) -> Optional[datetime.date]:
    return None if datestr is None else datetime.datetime.fromisoformat(datestr.decode()).date()


def load_datetime(datestr: Optional[bytes]) -> Optional[datetime.datetime]:
    return None if datestr is None else datetime.datetime.fromisoformat(datestr.decode())


sqlite3.register_converter(DType.BOOL.sqlite, lambda b: bool(int(b)))
sqlite3.register_converter(DType.DATE.sqlite, load_date)
sqlite3.register_converter(DType.DATETIME.sqlite, load_datetime)
sqlite3.register_adapter(datetime.date, datetime.date.isoformat)
sqlite3.register_adapter(datetime.datetime, datetime.datetime.isoformat)
