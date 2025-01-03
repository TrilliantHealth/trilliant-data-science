"""A more composable retry decorator."""

import random
import time
import typing as ty
from functools import wraps
from logging import getLogger
from timeit import default_timer

F = ty.TypeVar("F", bound=ty.Callable)


IsRetryable = ty.Callable[[Exception], bool]
RetryStrategy = ty.Iterable[IsRetryable]
RetryStrategyFactory = ty.Callable[[], RetryStrategy]


def expo(
    *, retries: int, delay: float = 1.0, backoff: int = 2, jitter: bool = True
) -> ty.Callable[[], ty.Iterator[float]]:
    """End iteration after yielding 'retries' times.

    The first retry is immediate (i.e. 0). Subsequent retries will follow the schedule
    established by the exponential backoff algorithm. The default algorithm is 1, 3, 7,
    15, etc., but also adds jitter.

    If you want infinite exponential values, pass a negative number for 'retries'.
    """

    def expo_() -> ty.Iterator[float]:
        count = 0
        accum_jitter = 0.0
        while retries < 0 or count < retries:
            expo_delay = (backoff**count * delay) - delay  # first retry is immediate
            if jitter:
                jitter_delay = random.uniform(0.5, 1.5) * expo_delay
                yield jitter_delay + accum_jitter
                accum_jitter = expo_delay - jitter_delay
            else:
                yield expo_delay
            count += 1

    return expo_


def sleep(
    mk_seconds_iter: ty.Callable[[], ty.Iterable[float]],
    sleeper: ty.Callable[[float], ty.Any] = time.sleep,
) -> ty.Callable[[], ty.Iterator[str]]:
    """A common base strategy for separating retries by sleeps.

    Yield once prior to the first sleep, and once before each sleep.
    In other words, the total number of yields is the length of the input iterable (if it is finite).
    """

    def sleep_() -> ty.Iterator[str]:
        start = default_timer()

        so_far = 0.0
        for i, secs in enumerate(mk_seconds_iter(), start=1):
            yield f"attempt {i} after {so_far:.2f}s"
            so_far = default_timer() - start
            sleeper(secs)

    return sleep_


def retry(retry_strategy_factory: RetryStrategyFactory) -> ty.Callable[[F], F]:
    """Uses your retry strategy every time an exception is raised.
    Your iterable can therefore provide different handling for each
    incrementing error, as well as configurable delays between errors,
    etc.

    If the retry_strategy iterator itself ends (or is empty to begin
    with), the function will be called one final time.
    """

    def _retry_decorator(func: F) -> F:
        @wraps(func)
        def retry_wrapper(*args, **kwargs):
            for i, is_retryable in enumerate(retry_strategy_factory(), start=1):
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    if not is_retryable(ex):
                        raise ex
                    getLogger(__name__).info("Retry #%d for %s due to exception %s", i, func, ex)
            # one final retry that, if it fails, will not get caught and retried.
            return func(*args, **kwargs)

        return ty.cast(F, retry_wrapper)

    return _retry_decorator


def retry_regular(
    is_retryable: IsRetryable,
    intervals_factory: ty.Callable[[], ty.Iterable[ty.Any]],
) -> ty.Callable[[F], F]:
    return retry(lambda: (is_retryable for _ in intervals_factory()))


def retry_sleep(
    is_retryable: IsRetryable,
    seconds_iter: ty.Callable[[], ty.Iterable[float]],
) -> ty.Callable[[F], F]:
    """E.g. retry_sleep(expo(retries=5)) to get max 6 calls to the function."""
    return retry_regular(is_retryable, sleep(seconds_iter))


def is_exc(*exc_types: ty.Type[Exception]) -> IsRetryable:
    def _is_exc_retryable(exc: Exception) -> bool:
        return isinstance(exc, exc_types)

    return _is_exc_retryable
