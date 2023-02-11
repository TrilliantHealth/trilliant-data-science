"""A more composable retry decorator."""
import time
import typing as ty
from functools import wraps

F = ty.TypeVar("F", bound=ty.Callable)


IsRetryable = ty.Callable[[Exception], bool]
RetryStrategy = ty.Iterable[IsRetryable]
RetryStrategyFactory = ty.Callable[[], RetryStrategy]


def expo(*, tries: int, delay: float = 1.0, backoff: int = 2) -> ty.Iterator[float]:
    """End iteration after 'tries'.
    If you want infinite exponential values, pass a negative number for 'tries'.
    """
    count = 0
    while tries < 0 or count < tries:
        yield backoff**count * delay
        count += 1


def sleep(
    seconds_iter: ty.Iterable[float], sleeper: ty.Callable[[float], ty.Any] = time.sleep
) -> ty.Callable[[], ty.Iterator]:
    """A common base strategy for separating retries by sleeps."""

    def sleep_() -> ty.Iterator:
        yield
        for secs in seconds_iter:
            sleeper(secs)
            yield

    return sleep_


def retry(retry_strategy_factory: RetryStrategyFactory) -> ty.Callable[[F], F]:
    """Uses your retry strategy every time an exception is raised.
    Your iterable can therefore provide different handling for each
    incrementing error, as well as configurable delays between errors,
    etc.
    """

    def _retry_decorator(func: F) -> F:
        @wraps(func)
        def retry_wrapper(*args, **kwargs):
            for is_retryable in retry_strategy_factory():
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if not is_retryable(e):
                        raise

        return ty.cast(F, retry_wrapper)

    return _retry_decorator


def retry_regular(
    is_retryable: IsRetryable,
    intervals_factory: ty.Callable[[], ty.Iterable[ty.Any]],
) -> ty.Callable[[F], F]:
    return retry(lambda: (is_retryable for _ in intervals_factory()))
