import typing as ty
from functools import wraps
from timeit import default_timer

from thds.core import log

from .colorize import colorized

F = ty.TypeVar("F", bound=ty.Callable)
logger = log.getLogger(__name__)
_SLOW = colorized(fg="yellow", bg="black")
LogSlow = lambda s: logger.warning(_SLOW(s))  # noqa: E731


def on_slow(callback: ty.Callable[[float], None], slow_seconds: float = 3.0) -> ty.Callable[[F], F]:
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
