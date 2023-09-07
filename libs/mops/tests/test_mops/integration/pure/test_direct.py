from thds.mops.pure import memoize_in


@memoize_in("adls://thdsscratch/tmp/")
def a_direct_func(a: int, b: int) -> int:
    return a * b


def test_memoize_in():
    assert 18 == a_direct_func(3, 6)
