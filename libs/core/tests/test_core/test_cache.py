import random
import time
from concurrent.futures import ThreadPoolExecutor
from threading import RLock

from thds.core import cache


def crazy_signature_1(i1: int, i2: int, *args: int, a: str, b: str, **kwargs) -> None:
    pass


def test_bound_hashkey1() -> None:
    bound_hashey = cache.make_bound_hashkey(crazy_signature_1)
    assert bound_hashey((1, 2, 3, 4), dict(a="a", b="b", c="c", d="d")) == bound_hashey(
        (1, 2, 3, 4), dict(b="b", a="a", d="d", c="c")
    )


def crazy_signature_2(i1: int, i2: int, *args: int, a: str = "a", b: str = "b") -> None:
    pass


def test_bound_hashkey2() -> None:
    bound_hashey = cache.make_bound_hashkey(crazy_signature_2)
    assert bound_hashey((1, 2, 3, 4), dict(a="a", b="b")) == bound_hashey((1, 2, 3, 4), {})


def add_one(i: int) -> int:
    return i + 1


def test_locking() -> None:
    cached_add_one = cache.locking(add_one)

    assert cached_add_one.cache_info().currsize == 0  # type: ignore[attr-defined]
    cached_add_one(1)
    assert cached_add_one.cache_info().currsize == 1  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().misses == 1  # type: ignore[attr-defined]
    cached_add_one(1)
    assert cached_add_one.cache_info().currsize == 1  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().hits == 1  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().misses == 1  # type: ignore[attr-defined]


def test_parametrized_locking() -> None:
    cached_add_one = cache.locking(cache_lock=RLock(), make_func_lock=lambda key: RLock())(add_one)

    assert cached_add_one.cache_info().currsize == 0  # type: ignore[attr-defined]
    cached_add_one(1)
    assert cached_add_one.cache_info().currsize == 1  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().misses == 1  # type: ignore[attr-defined]
    cached_add_one(1)
    assert cached_add_one.cache_info().currsize == 1  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().hits == 1  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().misses == 1  # type: ignore[attr-defined]


def test_locking_clear_cache() -> None:
    cached_add_one = cache.locking(add_one)

    assert cached_add_one.cache_info().currsize == 0  # type: ignore[attr-defined]
    cached_add_one(1)
    assert cached_add_one.cache_info().currsize == 1  # type: ignore[attr-defined]
    cached_add_one.clear_cache()  # type: ignore[attr-defined]
    assert cached_add_one.cache_info().currsize == 0  # type: ignore[attr-defined]


def slow_add_one(i: int) -> int:
    time.sleep(1)
    return add_one(i)


def test_locking_calls_same_args_once_diff_args_parallel() -> None:
    cached_slow_add_one = cache.locking(slow_add_one)
    lst = [1, 2, 3, 4] * 256
    random.shuffle(lst)

    start = time.perf_counter()
    with ThreadPoolExecutor() as exc:
        exc.map(cached_slow_add_one, lst)
    stop = time.perf_counter()

    assert cached_slow_add_one.cache_info().currsize == 4  # type: ignore[attr-defined]
    assert stop - start < 4
    # concurrent runtime is less than serial runtime of the 4 function invocations


def deadlocker(deco) -> int:
    n = 0

    @deco
    def inner() -> int:
        nonlocal n
        if n < 2:
            n += 1
            inner()

        return n

    return inner()


def test_locking_supports_recursive_calls_w_rlock() -> None:
    assert deadlocker(cache.locking(make_func_lock=lambda _key: RLock()))


class _Bounded(dict):
    """A minimal newest-N mapping, standing in for `cachetools.LRUCache` so
    these tests don't need `cachetools` (which `thds.core` deliberately
    doesn't depend on)."""

    maxsize = 2

    def __setitem__(self, key, value) -> None:
        super().__setitem__(key, value)
        while len(self) > self.maxsize:
            super().pop(next(iter(self)))


def test_locking_accepts_a_bounded_cache() -> None:
    calls = []

    @cache.locking(make_cache=_Bounded)
    def square(n: int) -> int:
        calls.append(n)
        return n * n

    assert [square(n) for n in (1, 2, 3)] == [1, 4, 9]
    assert square.cache_info().currsize == 2  # type: ignore[attr-defined]
    assert square.cache_info().maxsize == 2  # type: ignore[attr-defined]

    assert square(3) == 9  # still cached
    assert calls == [1, 2, 3]

    assert square(1) == 1  # evicted, so recomputed
    assert calls == [1, 2, 3, 1]


def test_locking_defaults_to_unbounded_dict() -> None:
    @cache.locking
    def double(n: int) -> int:
        return n * 2

    for n in range(50):
        double(n)
    assert double.cache_info().currsize == 50  # type: ignore[attr-defined]
    assert double.cache_info().maxsize is None  # type: ignore[attr-defined]


def test_locking_bounded_cache_is_still_single_flight() -> None:
    """Eviction must not cost the single-flight property - the whole reason
    this decorator exists rather than `cachetools.cached`."""
    invocations = 0

    @cache.locking(make_cache=_Bounded)
    def slow(_n: int) -> int:
        nonlocal invocations
        invocations += 1
        time.sleep(0.2)
        return 1

    with ThreadPoolExecutor() as exc:
        list(exc.map(slow, [7] * 8))

    assert invocations == 1
