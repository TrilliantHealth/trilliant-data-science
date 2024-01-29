"""A more composable retry decorator."""
import time
import typing as ty
from functools import wraps

F = ty.TypeVar("F", bound=ty.Callable)


IsRetryable = ty.Callable[[Exception], bool]
RetryStrategy = ty.Iterable[IsRetryable]
RetryStrategyFactory = ty.Callable[[], RetryStrategy]


def expo(*, retries: int, delay: float = 1.0, backoff: int = 2) -> ty.Iterator[float]:
    """End iteration after yielding 'retries' times.

    If you want infinite exponential values, pass a negative number for 'retries'.
    """
    count = 0
    while retries < 0 or count < retries:
        yield backoff**count * delay
        count += 1


def sleep(
    seconds_iter: ty.Iterable[float], sleeper: ty.Callable[[float], ty.Any] = time.sleep
) -> ty.Callable[[], ty.Iterator]:
    """A common base strategy for separating retries by sleeps."""

    def sleep_() -> ty.Iterator:
        for secs in seconds_iter:
            yield
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
            for is_retryable in retry_strategy_factory():
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    if not is_retryable(ex):
                        raise ex
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
    seconds_iter: ty.Iterable[float],
) -> ty.Callable[[F], F]:
    """E.g. retry_sleep(expo(retries=5))"""
    return retry_regular(is_retryable, sleep(seconds_iter))


def is_exc(*exc_types: ty.Type[Exception]) -> IsRetryable:
    def _is_exc_retryable(exc: Exception) -> bool:
        return isinstance(exc, exc_types)

    return _is_exc_retryable
