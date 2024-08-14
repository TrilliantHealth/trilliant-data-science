import math
import typing as ty
from datetime import timedelta
from functools import wraps
from timeit import default_timer

from typing_extensions import ParamSpec

from thds.core import log, scope


def _name(obj: ty.Any) -> str:
    if hasattr(obj, "__class__"):
        return obj.__class__.__name__
    if hasattr(obj, "__name__"):
        return obj.__name__
    if obj is not None:
        return str(obj)[:20]
    return "item"


def _mag_10(num: float) -> float:
    assert num > 0
    return math.floor(math.log(abs(num), 10))


logger = log.getLogger(__name__.replace("ud.shared.", ""))
T = ty.TypeVar("T")
_progress_scope = scope.Scope("progress")


@_progress_scope.bound
def report(
    it: ty.Iterable[T],
    *,
    name: str = "",
    roughly_every_s: timedelta = timedelta(seconds=20),
) -> ty.Iterator[T]:
    """Reports when both intervals have been met."""
    iterator = iter(it)
    total = 0
    try:
        first_item = next(iterator)
        yield first_item
        total = 1
    except StopIteration:
        first_item = None

    name = name or _name(first_item)

    _progress_scope.enter(log.logger_context(t=name))
    report_every = 0
    start = default_timer()
    last_report = 0

    def calc_report_every(total: int, factor: float) -> int:
        decimal_smoothing = 10 ** _mag_10(total / factor)
        report_every = int(int((total / factor) / decimal_smoothing) * decimal_smoothing)
        assert report_every > 0, str((total, decimal_smoothing))
        return report_every

    for total, item in enumerate(iterator, start=2):
        yield item

        if not report_every and default_timer() - start > (roughly_every_s.total_seconds() * 0.5):
            report_every = calc_report_every(total, 0.5)
        elif report_every and (total % report_every == 0) and (total - last_report >= report_every):
            now = default_timer()
            elapsed = now - start
            logger.info(f"Processed {total:12,d} in {elapsed:6,.1f}s at {total / elapsed:10,.0f}/s")
            last_report = total
            report_every = calc_report_every(total, elapsed / roughly_every_s.total_seconds())

    _log = logger.info if total > 0 else logger.warning
    elapsed = default_timer() - start
    _log(f"FINISHED {total:12,d} {name} in {elapsed:6,.1f}s at {total / elapsed:10,.0f}/s")


P = ParamSpec("P")


def report_gen(f: ty.Callable[P, ty.Iterator[T]], **kwargs: ty.Any) -> ty.Callable[P, ty.Iterator[T]]:
    @wraps(f)
    def _report_gen(*args: P.args, **kwargs: P.kwargs) -> ty.Iterator[T]:
        yield from report(f(*args, **kwargs))

    return _report_gen
