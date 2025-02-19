import pytest

from ._util import adls_shim


def func_that_raises(a: int):
    raise ValueError(f"{a} is just no good at all!!")


@adls_shim
def func_that_calls_other_func_that_raises(a: int):
    return func_that_raises(a)


def test_that_remote_exceptions_can_be_reraised_locally():
    with pytest.raises(ValueError):
        func_that_calls_other_func_that_raises(2)
