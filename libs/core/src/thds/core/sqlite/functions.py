import hashlib
import inspect
import sqlite3
import typing as ty


def _slowish_hash_str_function(__string: str) -> int:
    # Raymond Hettinger says that little-endian is slightly faster, though that was 2021.
    # I have also tested this myself and found it to be true.
    # https://bugs.python.org/msg401661
    return int.from_bytes(hashlib.md5(__string.encode()).digest()[:7], byteorder="little")


_THE_HASH_FUNCTION = _slowish_hash_str_function


# If you need it to be faster, you can 'replace' this with your own implementation.
def set_hash_function(f: ty.Callable[[str], int]) -> None:
    global _THE_HASH_FUNCTION
    _THE_HASH_FUNCTION = f


def _pyhash_values(*args) -> int:
    _args = (x if isinstance(x, str) else str(x) for x in args)
    concatenated = "".join(_args)
    hash_value = _THE_HASH_FUNCTION(concatenated)
    return hash_value


def _has_param_kind(signature, kind) -> bool:
    return any(p.kind == kind for p in signature.parameters.values())


def _num_parameters(f: ty.Callable) -> int:
    signature = inspect.signature(f)
    if _has_param_kind(signature, inspect.Parameter.VAR_KEYWORD):
        raise NotImplementedError("**kwargs in sqlite functions is not supported")
    elif _has_param_kind(signature, inspect.Parameter.VAR_POSITIONAL):
        return -1
    else:
        return len(signature.parameters.keys())


_FUNCTIONS = [_pyhash_values]


def register_functions_on_connection(
    conn: sqlite3.Connection,
    *,
    functions: ty.Collection[ty.Callable] = _FUNCTIONS,
) -> sqlite3.Connection:
    """By default registers our default functions.

    Returns the connection itself, for chaining.
    """
    for f in _FUNCTIONS:
        narg = _num_parameters(f)
        # SPOOKY: we're registering a function here with SQLite, and the SQLite name will
        # be the same as its name in Python.  Be very careful that you do not register two
        # different functions with the same name - you can read their docs on what will
        # happen, but it would be far better to just not ever do this.
        conn.create_function(name=f.__name__, narg=narg, func=f, deterministic=True)
    return conn
