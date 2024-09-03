import math
import typing as ty
from datetime import timedelta
from functools import partial, wraps
from timeit import default_timer

from typing_extensions import ParamSpec

from thds.core import log, scope


def _smooth_number(num):
    if num == 0:
        return 0

    # Determine the order of magnitude of the number
    magnitude = int(math.log10(abs(num)))

    # Find the nearest power of 10
    power_of_10 = 10**magnitude

    # Scale down the number to the range 1-10
    scaled_num = num / power_of_10

    # Round scaled number to nearest 1, 2, or 5 times 10^n
    if scaled_num < 1.5:
        return 1 * power_of_10
    elif scaled_num < 3:
        return 2 * power_of_10
    elif scaled_num < 7:
        return 5 * power_of_10
    return 10 * power_of_10


logger = log.getLogger(__name__.replace("ud.shared.", ""))
T = ty.TypeVar("T")
_progress_scope = scope.Scope("progress")


def calc_report_every(target_interval: float, total: int, sec_elapsed: float) -> int:
    seconds_per_item = sec_elapsed / total
    rate = 1 / seconds_per_item
    target_rate = 1 / target_interval
    return _smooth_number(int(rate / target_rate) or 1)


def _name(obj: ty.Any) -> str:
    if hasattr(obj, "__class__"):
        return obj.__class__.__name__
    if hasattr(obj, "__name__"):
        return obj.__name__
    if obj is not None:
        return str(obj)[:20]
    return "item"


@_progress_scope.bound
def report(
    it: ty.Iterable[T],
    *,
    name: str = "",
    roughly_every_s: timedelta = timedelta(seconds=20),
) -> ty.Iterator[T]:
    """Report round-number progress roughly every so often..."""
    iterator = iter(it)
    total = 0
    start = default_timer()
    report_every = 0
    last_report = 0
    frequency = roughly_every_s.total_seconds()

    try:
        first_item = next(iterator)
        name = name or _name(first_item)
        _progress_scope.enter(log.logger_context(P=name))
        yield first_item
        total = 1
    except StopIteration:
        pass

    for total, item in enumerate(iterator, start=2):
        yield item

        if not report_every:
            elapsed = default_timer() - start
            if elapsed > frequency * 0.5:
                report_every = calc_report_every(frequency, total, elapsed)
        elif report_every and (total % report_every == 0) and (total - last_report >= report_every):
            elapsed = default_timer() - start
            # once we have our first report_every value, don't get the time on every iteration
            if total >= elapsed:
                rate_str = f"{total / elapsed:10,.0f}/s"
            else:
                rate_str = f"{elapsed / total:10,.0f}s/{name}"
            logger.info(f"Processed {total:12,d} in {elapsed:6,.1f}s at {rate_str}")
            last_report = total
            report_every = calc_report_every(frequency, total, elapsed)

    _log = logger.info if total > 0 else logger.warning
    elapsed = default_timer() - start
    _log(f"FINISHED {total:12,d} {name} in {elapsed:6,.1f}s at {total / elapsed:10,.0f}/s")


P = ParamSpec("P")


def _report_gen(f: ty.Callable[P, ty.Iterator[T]], *args: P.args, **kwargs: P.kwargs) -> ty.Iterator[T]:
    yield from report(f(*args, **kwargs))


def report_gen(f: ty.Callable[P, ty.Iterator[T]]) -> ty.Callable[P, ty.Iterator[T]]:
    return wraps(f)(partial(_report_gen, f))
