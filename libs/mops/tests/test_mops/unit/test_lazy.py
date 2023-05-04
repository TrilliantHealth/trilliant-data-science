from concurrent.futures import ThreadPoolExecutor

from thds.mops._lazy import Lazy


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
