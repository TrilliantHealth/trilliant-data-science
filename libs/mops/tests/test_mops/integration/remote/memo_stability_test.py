from thds.mops.remote.core import invocation_unique_key

from ._util import adls_shell


@adls_shell
def mul(a: int, b: float = 4.2) -> float:
    in_un_key = invocation_unique_key()
    assert in_un_key
    assert in_un_key.endswith("c/c470c619e86f77fdda807f68bd101ef58c0d1a42e9c464e08578e8d7a5dda0b")
    return a * b


def test_memoization_of_args_kwargs_is_stable_across_different_looking_call_signatures():
    assert 16.8 == mul(4, 4.2)
    assert 16.8 == mul(4, b=4.2)
    assert 16.8 == mul(a=4, b=4.2)
    assert 16.8 == mul(b=4.2, a=4)
    assert 16.8 == mul(a=4)
