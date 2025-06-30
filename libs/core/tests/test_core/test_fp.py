from thds.core import fp


def f1(a: str) -> str:
    return a + "->f1"


def f2(b: str) -> str:
    return b + "->f2"


def test_compose():
    assert fp.compose(f1, f2)("!") == "!->f2->f1"


def test_pipe():
    assert fp.pipe(f1, f2)("!") == "!->f1->f2"
