import time
import typing as ty
from datetime import timedelta

T = ty.TypeVar("T")


def null_safe_iter(it: ty.Optional[ty.Iterable[T]]) -> ty.Iterator[T]:
    """Iterate the iterable if it is not None"""
    if it is not None:
        yield from it


_Frequency = ty.Union[timedelta, float]


def _to_float(t: _Frequency) -> float:
    return t.total_seconds() if isinstance(t, timedelta) else t


def titrate(it: ty.Iterable[T], *, at_rate: _Frequency, until_nth: int) -> ty.Iterator[T]:
    it_, freq = iter(it), _to_float(at_rate)

    for i, x in enumerate(it_):
        yield x

        if i + 1 < until_nth:
            time.sleep(freq)
