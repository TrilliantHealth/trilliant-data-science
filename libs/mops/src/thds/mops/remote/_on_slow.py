import typing as ty
from functools import wraps
from timeit import default_timer

F = ty.TypeVar("F", bound=ty.Callable)


def on_slow(slow_seconds: float, callback: ty.Callable[[float], None]) -> ty.Callable[[F], F]:
    def deco(f: F) -> F:
        @wraps(f)
        def wrapper(*args, **kwargs):
            start_time = default_timer()
            r = f(*args, **kwargs)
            elapsed_s = default_timer() - start_time
            if elapsed_s > slow_seconds:
                callback(elapsed_s)
            return r

        return ty.cast(F, wrapper)

    return deco
