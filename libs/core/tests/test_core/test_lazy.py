import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from thds.core.lazy import Lazy, threadlocal_lazy


def test_lazy():
    counter = 0

    def up():
        nonlocal counter
        counter += 1
        return counter

    lazy_counter = Lazy(up)

    assert lazy_counter() == 1
    assert counter == 1
    assert lazy_counter() == 1
    assert counter == 1


def test_lazy_is_threadsafe():
    counter = 0

    def up():
        nonlocal counter
        counter += 1
        return counter

    lazy_counter = Lazy(up)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futs = [executor.submit(lazy_counter) for i in range(10)]
    for fut in futs:
        assert fut.result() == 1
    assert counter == 1


def test_thread_local_lazy():
    calls = list()

    @threadlocal_lazy
    def three():
        time.sleep(1)  # sleep in order to force all threads to spawn.
        calls.append(1)
        return 3

    with ThreadPoolExecutor(max_workers=10) as executor:
        futs = [executor.submit(three) for i in range(20)]
    results = [fut.result() for fut in futs]
    tot = sum(results)
    assert tot == 6 * 10
    assert len(calls) == 10


def test_lazy_is_friendlier_now():
    class Foo:
        def __init__(self):
            self.bar = "baz"

    lazy_foo = Lazy(Foo)

    with pytest.raises(
        AttributeError,
        match=r"did you mean to instantiate the object before access, i.e. `\(\)\.bar`\?",
    ):
        assert lazy_foo.bar  # type: ignore

    assert "baz" == lazy_foo().bar
