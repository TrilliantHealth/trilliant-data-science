import typing as ty

from attrs import define

from core.atacama import neo


def test_set_field():
    @define
    class Foo:
        s: ty.Set[int]

    FooS = neo(Foo)

    foo = FooS().load(dict(s=[2, 1, 2, 3]))
    assert foo.s == {1, 2, 3}

    foo_dict = FooS().dump(foo)
    assert foo_dict["s"] == [1, 2, 3]
