from thds.mops.remote import memoize_direct


@memoize_direct("adls://thdsscratch/tmp/")
def a_direct_func(a: int, b: int) -> int:
    return a * b


def test_memoize_direct():
    assert 18 == a_direct_func(3, 6)
