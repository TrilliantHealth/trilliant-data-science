import functools
import time
import typing as ty
from concurrent.futures import ThreadPoolExecutor

from typing_extensions import ParamSpec

from thds.core import cache

P = ParamSpec("P")
R = ty.TypeVar("R")


def count_calls(f: ty.Callable[P, R]) -> ty.Callable[P, R]:
    calls = 0

    @functools.wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        nonlocal calls
        calls += 1
        return f(*args, **kwargs)

    wrapper.calls = calls  # type: ignore[attr-defined]

    return wrapper


def add_one(i: int) -> int:
    return i + 1


def slow_add_one(i: int) -> int:
    time.sleep(1)
    return add_one(i)


def test_threadsafe_cache_calls_same_args_once() -> None:
    cached_add_one = cache.threadsafe_cache(count_calls(add_one))

    lst = [1] * 500 + [2] * 500

    with ThreadPoolExecutor() as exc:
        exc.map(cached_add_one, lst)

    assert cached_add_one.cache_info().hits == 998  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().misses == 2  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().currsize == 2  # type: ignore[attr-defined]


def test_thread_safe_cache_runs_diff_args_parallel() -> None:
    cached_slow_add_one = cache.threadsafe_cache(count_calls(slow_add_one))

    lst = [1, 2]

    start = time.perf_counter()
    with ThreadPoolExecutor() as exc:
        exc.map(cached_slow_add_one, lst)
    stop = time.perf_counter()

    assert stop - start < 2
    # concurrent runtime is less than serial runtime
