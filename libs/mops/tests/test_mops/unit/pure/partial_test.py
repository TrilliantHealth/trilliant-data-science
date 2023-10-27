from functools import partial

from thds.mops.pure.core.partial import unwrap_partial


def bar(x, y, z, a=1, b=2, c=3) -> str:
    return f"{x} {y} {z} {a} {b} {c}"


def test_unwrap_partial():
    p = partial(partial(bar, 1, a=2), 2, b=4)

    f, args, kwargs = unwrap_partial(p, (3,), dict(c=6))
    assert f is bar
    assert args == (1, 2, 3)
    assert kwargs == dict(a=2, b=4, c=6)

    assert p(3, c=6) == "1 2 3 2 4 6"
    assert f(*args, **kwargs) == "1 2 3 2 4 6"
