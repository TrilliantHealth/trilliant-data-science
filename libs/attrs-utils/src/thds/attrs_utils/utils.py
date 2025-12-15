import functools
import typing as ty

F = ty.TypeVar("F", bound=ty.Callable)


def signature_preserving_cache(func: F, size: ty.Optional[int] = None) -> F:
    """Decorator to apply `functools.lru_cache` while preserving the decorated function's signature
    (which is otherwise lost when using `lru_cache` directly)"""

    cached_func = functools.lru_cache(size)(func)
    return ty.cast(F, cached_func)
