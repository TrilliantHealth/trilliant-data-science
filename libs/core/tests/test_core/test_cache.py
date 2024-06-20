import time
from concurrent.futures import ThreadPoolExecutor

from thds.core import cache


def crazy_signature_1(i1: int, i2: int, *args: int, a: str, b: str, **kwargs) -> None:
    pass


def test_bound_hashkey1() -> None:
    bound_hashey = cache.make_bound_hashkey(crazy_signature_1)
    assert bound_hashey(1, 2, 3, 4, a="a", b="b", c="c", d="d") == bound_hashey(
        1, 2, 3, 4, b="b", a="a", d="d", c="c"
    )


def crazy_signature_2(i1: int, i2: int, *args: int, a: str = "a", b: str = "b") -> None:
    pass


def test_bound_hashkey2() -> None:
    bound_hashey = cache.make_bound_hashkey(crazy_signature_2)
    assert bound_hashey(1, 2, 3, 4, a="a", b="b") == bound_hashey(1, 2, 3, 4)


def add_one(i: int) -> int:
    return i + 1


def test_threadsafe_cache() -> None:
    cached_add_one = cache.threadsafe_cache(add_one)

    assert cached_add_one.cache_info().currsize == 0  # type: ignore[attr-defined]
    cached_add_one(1)
    assert cached_add_one.cache_info().currsize == 1  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().misses == 1  # type: ignore[attr-defined]
    cached_add_one(1)
    assert cached_add_one.cache_info().currsize == 1  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().hits == 1  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().misses == 1  # type: ignore[attr-defined]


def test_threadsafe_cache_calls_same_args_once() -> None:
    cached_add_one = cache.threadsafe_cache(add_one)
    lst = [1] * 256 + [2] * 256 + [3] * 256 + [4] * 256

    with ThreadPoolExecutor() as exc:
        exc.map(cached_add_one, lst, chunksize=32)

    assert cached_add_one.cache_info().hits == 1020  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().misses == 4  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().currsize == 4  # type: ignore[attr-defined]


def slow_add_one(i: int) -> int:
    time.sleep(1)
    return add_one(i)


def test_thread_safe_cache_runs_diff_args_parallel() -> None:
    cached_slow_add_one = cache.threadsafe_cache(slow_add_one)

    lst = [1, 2, 3, 4]

    start = time.perf_counter()
    with ThreadPoolExecutor() as exc:
        exc.map(cached_slow_add_one, lst, chunksize=4)
    stop = time.perf_counter()

    assert stop - start < 2
    # concurrent runtime is _way_ less than serial runtime


def test_cache_clear() -> None:
    cached_add_one = cache.threadsafe_cache(add_one)

    assert cached_add_one.cache_info().currsize == 0  # type: ignore[attr-defined]
    cached_add_one(1)
    assert cached_add_one.cache_info().currsize == 1  # type: ignore[attr-defined]
    cached_add_one.clear_cache()  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().currsize == 0  # type: ignore[attr-defined]
