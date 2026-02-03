from thds.core import decos


def test_decos_composes_top_to_bottom(capsys):
    def foo(a):
        try:
            print("in foo")
            return a + 1
        finally:
            print("out foo")

    def bardeco(f):
        def wrapped(*args, **kwargs):
            print("in bardeco")
            try:
                return f(*args, **kwargs)
            finally:
                print("out bardeco")

        return wrapped

    def bazdeco(f):
        def wrapped(*args, **kwargs):
            print("in bazdeco")
            try:
                return f(*args, **kwargs)
            finally:
                print("out bazdeco")

        return wrapped

    assert 2 == decos.compose(
        bardeco,
        bazdeco,
    )(foo)(1)

    assert (
        "in bardeco\nin bazdeco\nin foo\nout foo\nout bazdeco\nout bardeco\n" == capsys.readouterr().out
    )


def test_decos_compose_directly():
    def foo(a):
        return a + 1

    def bardeco(f):
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    def bazdeco(f):
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    assert 2 == decos.compose(bardeco, bazdeco, f=foo)(1)
