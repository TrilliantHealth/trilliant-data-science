import contextlib
import datetime
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
from typing_inspect import get_args, get_origin, is_literal_type, is_optional_type, is_union_type

from thds.core.types import StrOrPath
from thds.tabularasa.schema.dtypes import DType
from thds.tabularasa.sqlite3_compat import sqlite3

DEFAULT_ATTR_SQLITE_CACHE_SIZE = 100_000
DEFAULT_MMAP_BYTES = int(os.environ.get("TABULA_RASA_DEFAULT_MMAP_BYTES", 8_589_934_592))  # 8 GB
DISABLE_WAL_MODE = bool(os.environ.get("REF_D_DISABLE_SQLITE_WAL_MODE", False))

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


def set_bulk_write_mode(con: sqlite3.Connection) -> sqlite3.Connection:
    logger = logging.getLogger(__name__)
    logger.debug("Setting pragmas for bulk write optimization")
    # https://www.sqlite.org/pragma.html#pragma_synchronous
    _log_exec_sql(logger, con, "PRAGMA synchronous = 0")  # OFF
    # https://www.sqlite.org/pragma.html#pragma_journal_mode
    if not DISABLE_WAL_MODE:
        _log_exec_sql(logger, con, "PRAGMA journal_mode = WAL")
    # https://www.sqlite.org/pragma.html#pragma_locking_mode
    _log_exec_sql(logger, con, "PRAGMA locking_mode = EXCLUSIVE")

    return con


def unset_bulk_write_mode(con: sqlite3.Connection) -> sqlite3.Connection:
    logger = logging.getLogger(__name__)
    logger.debug("Setting pragmas for bulk write optimization")
    # https://www.sqlite.org/pragma.html#pragma_journal_mode
    # resetting this to the default. This is a property of the database, rather than the connection.
    # the other settings are connection-specific.
    # according to the docs, the WAL journal mode should be disabled before the locking mode is restored,
    # else any attempt to do so is a no-op.
    _log_exec_sql(logger, con, "PRAGMA journal_mode = DELETE")
    # https://www.sqlite.org/pragma.html#pragma_synchronous
    _log_exec_sql(logger, con, "PRAGMA synchronous = 2")  # FULL (default)
    # https://www.sqlite.org/pragma.html#pragma_locking_mode
    _log_exec_sql(logger, con, "PRAGMA locking_mode = NORMAL")

    return con


@contextlib.contextmanager
def bulk_write_context(con: sqlite3.Connection, close: bool):
    set_bulk_write_mode(con)
    try:
        yield con
    finally:
        unset_bulk_write_mode(con)
        if close:
            con.close()


def sqlite_connection(
    db_path: StrOrPath,
    package: Optional[str] = None,
    *,
    mmap_size: Optional[int] = None,
    read_only: bool = False,
):
    if package is None:
        db_filename = db_path
    else:
        db_filename = pkg_resources.resource_filename(package, str(db_path))

    db_full_path = str(Path(db_filename).absolute())

    logger = logging.getLogger(__name__)
    logger.info(f"Connecting to sqlite database: {db_filename}")
    # sqlite3.PARSE_DECLTYPES will cover parsing dates/datetimes from the db
    con = sqlite3.connect(
        db_full_path, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=not read_only
    )
    if mmap_size is not None:
        logger.info(f"Setting sqlite mmap size to {mmap_size}")
        _log_exec_sql(logger, con, f"PRAGMA mmap_size={mmap_size};")

    return con


def _log_exec_sql(
    logger: logging.Logger, con: sqlite3.Connection, statement: str, level: int = logging.DEBUG
):
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
