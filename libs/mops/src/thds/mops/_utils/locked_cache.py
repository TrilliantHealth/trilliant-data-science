import functools
import typing as ty
from threading import Lock, RLock
from typing import Optional, Union

from cachetools import keys

try:
    from cachetools.func import _CacheInfo  # type: ignore
except ImportError:
    # this moved between 5.2.1 and 5.3.
    from cachetools import _CacheInfo  # type: ignore


F = ty.TypeVar("F", bound=ty.Callable)


def locked_cached(
    cache: ty.Any, typed: bool = False, lock: Optional[Union[RLock, Lock]] = None
) -> ty.Callable[[F], F]:
    """Like cachetools.func._cache, except it locks the actual
    function call but does _not_ lock reading from the cache the first
    time, so most of the time, cache hits are nearly free, but you
    don't call the function more than once for the same arguments.
    """
    maxsize = cache.maxsize

    def decorator(func: F) -> F:
        key = keys.typedkey if typed else keys.hashkey
        hits = misses = 0
        _lock = lock or RLock()

        def wrapper(*args, **kwargs):  # type: ignore
            nonlocal hits, misses
            k = key(*args, **kwargs)

            # optimistic lookup on a cache that is threadsafe for reads
            try:
                v = cache[k]
                hits += 1
                return v
            except KeyError:
                with _lock:
                    try:
                        v = cache[k]
                        hits += 1
                        return v
                    except KeyError:
                        misses += 1

                    v = func(*args, **kwargs)
                    # in case of a race, prefer the item already in the cache
                    try:
                        return cache.setdefault(k, v)
                    except ValueError:
                        return v  # value too large

        def cache_info() -> _CacheInfo:
            with _lock:
                maxsize = cache.maxsize
                currsize = cache.currsize
            return _CacheInfo(hits, misses, maxsize, currsize)

        def cache_clear() -> None:
            nonlocal hits, misses
            with _lock:
                try:
                    cache.clear()
                finally:
                    hits = misses = 0

        wrapper.cache_info = cache_info  # type: ignore
        wrapper.cache_clear = cache_clear  # type: ignore
        wrapper.cache_parameters = lambda: {"maxsize": maxsize, "typed": typed}  # type: ignore
        functools.update_wrapper(wrapper, func)
        return ty.cast(F, wrapper)

    return decorator
