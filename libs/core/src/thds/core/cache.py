import functools
import inspect
import sys
import threading
import typing as ty

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec


class _HashedTuple(tuple):
    """A tuple that ensures that hash() will be called no more than once
    per element, since cache decorators will hash the key multiple
    times on a cache miss.  See also _HashedSeq in the standard
    library functools implementation.

    """

    __hashvalue: ty.Optional[int] = None

    def __hash__(self, hash=tuple.__hash__) -> int:
        hashvalue = self.__hashvalue
        if hashvalue is None:
            self.__hashvalue = hashvalue = hash(self)
        return hashvalue

    def __add__(self, other, add=tuple.__add__) -> "_HashedTuple":
        return _HashedTuple(add(self, other))

    def __radd__(self, other, add=tuple.__add__) -> "_HashedTuple":
        return _HashedTuple(add(other, self))

    def __getstate__(self) -> ty.Dict:
        return {}


# used for separating keyword arguments; we do not use an object
# instance here so identity is preserved when pickling/unpickling
_kwmark = (_HashedTuple,)


def hashkey(*args, **kwargs) -> _HashedTuple:
    """Return a cache key for the specified hashable arguments."""

    if kwargs:
        return _HashedTuple(args + sum(sorted(kwargs.items()), _kwmark))
    else:
        return _HashedTuple(args)


# above keying code borrowed from `cachetools`: https://github.com/tkem/cachetools/tree/master
# I have added some type information


def make_bound_hashkey(f: ty.Callable) -> ty.Callable[..., _HashedTuple]:
    """Makes a hashkey function that binds its `*args, **kwargs` to the function signature of `f`.

    The resulting bound hashkey function makes cache keys that are robust to variations in how arguments are passed to
    the cache-wrapped `f`. Note that `*args`, by definition, are order dependent.
    """
    signature = inspect.signature(f)

    def bound_hashkey(*args, **kwargs) -> _HashedTuple:
        bound_arguments = signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        return hashkey(*bound_arguments.args, **bound_arguments.kwargs)

    return bound_hashkey


class _CacheInfo(ty.NamedTuple):
    # typed version of what is in `functools`
    hits: int
    misses: int
    maxsize: ty.Optional[int]
    currsize: int


P = ParamSpec("P")
R = ty.TypeVar("R")
L = ty.TypeVar(
    "L", bound=ty.Union[threading.Lock, threading.RLock, threading.Semaphore, threading.BoundedSemaphore]
)


def _locking_cache_factory(
    cache_lock: L,
    make_func_lock: ty.Callable[[_HashedTuple], L],
) -> ty.Callable[[ty.Callable[P, R]], ty.Callable[P, R]]:
    def decorator(func: ty.Callable[P, R]) -> ty.Callable[P, R]:
        cache: ty.Dict[_HashedTuple, R] = {}
        keys_to_func_locks: ty.Dict[_HashedTuple, L] = {}
        hits = misses = 0
        bound_hashkey = make_bound_hashkey(func)
        sentinel = ty.cast(R, object())  # unique object used to signal cache misses

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            nonlocal hits, misses

            key = bound_hashkey(*args, **kwargs)
            maybe_value = cache.get(key, sentinel)
            if maybe_value is not sentinel:
                hits += 1
                return maybe_value

            if key not in keys_to_func_locks:
                with cache_lock:
                    if key not in keys_to_func_locks:
                        # just here to guard against a potential race condition
                        keys_to_func_locks[key] = make_func_lock(key)

            with keys_to_func_locks[key]:
                maybe_value = cache.get(key, sentinel)
                if maybe_value is not sentinel:
                    hits += 1
                    return maybe_value

                misses += 1
                result = func(*args, **kwargs)
                cache[key] = result

                return result

        def cache_info() -> _CacheInfo:
            with cache_lock:
                return _CacheInfo(hits, misses, None, len(cache))

        def clear_cache() -> None:
            nonlocal hits, misses
            with cache_lock:
                cache.clear()
                keys_to_func_locks.clear()
                hits = misses = 0

        wrapper.cache_info = cache_info  # type: ignore[attr-defined]
        wrapper.clear_cache = clear_cache  # type: ignore[attr-defined]

        return wrapper

    return decorator


def locking_cache(
    func: ty.Optional[ty.Callable[P, R]] = None,
    *,
    cache_lock: ty.Optional[L] = None,
    make_func_lock: ty.Optional[ty.Callable[[_HashedTuple], L]] = None,
):
    """A threadsafe, simple, unbounded cache.

    Unlike common cache implementations, such as `functools.cache` or `cachetools.cached({})`,
    `locking_cache` makes sure only one invocation of the wrapped function will occur per key across concurrent
    threads.

    When using `locking_cache` to call the same function with the same args concurrently, care should be taken
    that the wrapped function handles exceptions gracefully. A worst-case scenario exists where the wrapped function
    *F* is long-running and deterministically errors towards the end of its run. If this exception raising *F* is
    called with the same args *N* times, *F* will run (and error) in serial, *N* times.

    Users can optionally supply their own `cache_lock` and `make_func_lock` callable that returns a lock based on the
    cache key. By default, the `cache_lock` is a `Lock` and each unique cache key gets a unique `Lock`.
    """

    def default_make_func_lock(_key: _HashedTuple) -> threading.Lock:
        return threading.Lock()

    decorator = _locking_cache_factory(
        cache_lock or threading.Lock(), make_func_lock or default_make_func_lock
    )
    if func:
        return decorator(func)
    return decorator
