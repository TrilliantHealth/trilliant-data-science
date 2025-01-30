from thds.mops.pure import memoize_in

from ...config import TEST_TMP_URI


@memoize_in(TEST_TMP_URI)
def a_direct_func(a: int, b: int) -> int:
    return a * b


def test_memoize_in():
    assert 18 == a_direct_func(3, 6)
