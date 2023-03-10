import datetime
import json
import logging
import os
import sys
import typing
from functools import lru_cache
from pathlib import Path
from typing import AnyStr, Callable, Optional, Type, TypeVar

import cattrs.preconf.json
import pkg_resources
from typing_inspect import get_args, get_origin, is_literal_type, is_optional_type, is_union_type

from thds.tabularasa.sqlite3_compat import sqlite3

T = TypeVar("T")

DISABLE_WAL_MODE = bool(os.environ.get("REF_D_DISABLE_SQLITE_WAL_MODE", False))

PARAMETERIZABLE_BUILTINS = sys.version_info >= (3, 9)

if not PARAMETERIZABLE_BUILTINS:
    _builtin_to_typing = {
        list: typing.List,
        set: typing.Set,
        frozenset: typing.FrozenSet,
        tuple: typing.Tuple,
        dict: typing.Dict,
    }

    def get_generic_origin(t) -> Optional[Type]:
        org = get_origin(t)
        return None if org is None else _builtin_to_typing.get(org, org)  # type: ignore

else:
    get_generic_origin = get_origin


LITERAL_SQLITE_TYPES = {int, float, bool, str, type(None), datetime.date, datetime.datetime}


CONVERTER = cattrs.preconf.json.make_converter()


def structure_date(s: str, dt: Type[datetime.date] = datetime.date) -> datetime.date:
    return dt.fromisoformat(s)


CONVERTER.register_structure_hook(datetime.date, structure_date)
CONVERTER.register_unstructure_hook(datetime.date, datetime.date.isoformat)


@lru_cache(None)
def sqlite_postprocessor_for_type(t: Type[T]) -> Optional[Callable[[AnyStr], Optional[T]]]:
    """Construct a parser converting an optional JSON string from a sqlite query to a python value of
    type T"""
    t = resolve_newtypes(t)

    if is_primitive_type(t):
        return None

    if is_optional_type(t):

        def parse(s: AnyStr) -> Optional[T]:
            if s is None:
                return None
            raw = json.loads(s)
            return CONVERTER.structure(raw, t)

    else:

        def parse(s: AnyStr) -> Optional[T]:
            raw = json.loads(s)
            return CONVERTER.structure(raw, t)

    return parse


@lru_cache(None)
def sqlite_preprocessor_for_type(t: Type[T]) -> Optional[Callable[[T], Optional[str]]]:
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
    return typing.Union[nonnull_types]  # type: ignore


def sqlite_connection(
    db_path: str,
    package: Optional[str] = None,
    *,
    mmap_size: Optional[int] = None,
    bulk_write_mode: bool = False,
):
    if package is None:
        db_filename = db_path
    else:
        db_filename = pkg_resources.resource_filename(package, db_path)

    db_full_path = str(Path(db_filename).absolute())

    logger = logging.getLogger(__name__)
    logger.debug(f"Connecting to sqlite database: {db_filename}")
    # sqlite3.PARSE_DECLTYPES will cover parsing dates/datetimes from the db
    con = sqlite3.connect(db_full_path, detect_types=sqlite3.PARSE_DECLTYPES)
    if mmap_size is not None:
        logger.info(f"Setting sqlite mmap size to {mmap_size}")
        _log_exec_sql(logger, con, f"PRAGMA mmap_size={mmap_size};")
    if bulk_write_mode:
        logger.debug("Setting pragmas for bulk write optimization")
        # https://www.sqlite.org/pragma.html#pragma_synchronous
        _log_exec_sql(logger, con, "PRAGMA synchronous = OFF")
        # https://www.sqlite.org/pragma.html#pragma_journal_mode
        if not DISABLE_WAL_MODE:
            _log_exec_sql(logger, con, "PRAGMA journal_mode = WAL")
        # https://www.sqlite.org/pragma.html#pragma_locking_mode
        _log_exec_sql(logger, con, "PRAGMA locking_mode = EXCLUSIVE")

    return con


def _log_exec_sql(
    logger: logging.Logger, con: sqlite3.Connection, statement: str, level: int = logging.DEBUG
):
    logger.log(level, "sqlite: %s", statement)
    con.execute(statement)
