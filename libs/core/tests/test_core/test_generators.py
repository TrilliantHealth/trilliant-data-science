import pytest

from thds.core.generators import iterator_sender, return_wrapper


def generator_that_receives():
    j = 1
    all_js = list()
    while True:
        try:
            j = yield 2 * j
            all_js.append(j)
        except GeneratorExit:
            break
    return all_js  # noqa: B901


def test_iterator_sender():
    # iterator_sender ignores the yielded values _from_ the generator
    assert [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] == iterator_sender(generator_that_receives(), range(10))


def test_return_wrapper_when_exception_happens():
    def generator_that_raises():
        for i in range(10):
            yield i
            if i == 5:
                raise ValueError("I can't count that high!")

        return "four"  # noqa: B901

    with pytest.raises(ValueError):
        with return_wrapper(generator_that_raises()) as gen:
            for i in gen:
                assert i < 6
