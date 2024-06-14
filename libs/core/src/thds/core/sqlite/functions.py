import inspect
import sqlite3
import typing as ty

from thds.stable_sampling.python import hash_str


def _hash(*args) -> int:
    _args = (x if isinstance(x, str) else str(x) for x in args)
    return hash_str("".join(_args))


_FUNCTIONS = [_hash]


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


def register_functions(conn: sqlite3.Connection) -> None:
    for f in _FUNCTIONS:
        narg = _num_parameters(f)
        conn.create_function(name=f.__name__.strip("_"), narg=narg, func=f, deterministic=True)
