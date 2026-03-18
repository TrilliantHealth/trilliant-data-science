import typing as ty
from functools import wraps
from timeit import default_timer

from thds.core import log
from thds.termtool.colorize import colorized, make_colorized_out

F = ty.TypeVar("F", bound=ty.Callable)
logger = log.getLogger(__name__)
_SLOW = colorized(fg="yellow", bg="black")
LogSlow = make_colorized_out(_SLOW, out=logger.warning)


class _OnSlow:
    """Usable as both a decorator and a context manager.

    As a decorator: ``on_slow(cb)(some_func)`` — wraps the function.
    As a context manager: ``with on_slow(cb):`` — times the block.
    """

    def __init__(self, callback: ty.Callable[[float], None], slow_seconds: float):
        self._callback = callback
        self._slow_seconds = slow_seconds
        self._start = 0.0

    def __call__(self, f: F) -> F:
        @wraps(f)
        def wrapper(*args: ty.Any, **kwargs: ty.Any) -> ty.Any:
            with _OnSlow(self._callback, self._slow_seconds):
                return f(*args, **kwargs)

        return ty.cast(F, wrapper)

    def __enter__(self) -> "_OnSlow":
        self._start = default_timer()
        return self

    def __exit__(self, *exc: ty.Any) -> None:
        elapsed = default_timer() - self._start
        if elapsed > self._slow_seconds:
            self._callback(elapsed)


def on_slow(callback: ty.Callable[[float], None], slow_seconds: float = 3.0) -> _OnSlow:
    return _OnSlow(callback, slow_seconds)
