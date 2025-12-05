from functools import partial, wraps
from typing import Callable, Dict, Type, TypeVar, cast, overload

T = TypeVar("T")
U = TypeVar("U")
_D = TypeVar("_D")
_Type = TypeVar("_Type", bound=Type)

_MISSING = object()


class Registry(Dict[T, U]):
    @overload
    def register(self, key: T) -> Callable[[U], U]: ...

    @overload
    def register(self, key: T, value: U) -> U: ...

    def register(self, key: T, value=_MISSING):
        if value is _MISSING:

            def decorator(value: U) -> U:
                self[key] = value
                return value

            return decorator
        else:
            self[key] = value
            return value

    def cache(self, func: Callable[[T], U]) -> Callable[[T], U]:
        cached = partial(_check_cache, self, func)
        return wraps(func)(cached)


def _check_cache(cache: Dict[T, U], func: Callable[[T], U], key: T) -> U:
    value = cache.get(key, cast(U, _MISSING))
    if value is _MISSING:
        value = cache[key] = func(key)
    return value
