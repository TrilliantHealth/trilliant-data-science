"""Tests for pure.magic().shared() — content-addressed argument serialization."""

import pytest

from thds.mops.pure import _magic, magic
from thds.mops.pure._magic.shared_args import SharedArgRegistrar


@pytest.fixture
def clear_magic():
    _magic.api._MAGIC_CONFIG = _magic.sauce.new_config()  # type: ignore
    yield


def test_shared_chaining_returns_magic(clear_magic):
    """shared() is chainable and the result is still a Magic instance."""

    @magic().shared("big_obj")
    def func(big_obj: list, other: str) -> str:
        return other

    from thds.mops.pure._magic.sauce import Magic

    assert isinstance(func, Magic)


def test_shared_variadic_call_syntax(clear_magic):
    """shared("a", "b") works — exercises the variadic call path."""

    @magic().shared("mapping", "lookups")
    def func(sources: list, mapping: dict, lookups: list) -> int:
        return 0

    assert func._shared is not None
    assert func._shared._arg_names == ("mapping", "lookups")


def test_shared_no_args_is_noop(clear_magic):
    """With no shared args, _shared is None (zero overhead at call time)."""

    @magic()
    def func(x: int) -> int:
        return x

    assert func._shared is None


def test_shared_stores_arg_names(clear_magic):
    """SharedArgRegistrar is created with the right arg names."""

    @magic().shared("mapping")
    def func(sources: list, mapping: dict) -> int:
        return 0

    assert func._shared is not None
    assert func._shared._arg_names == ("mapping",)


def test_shared_rejects_invalid_arg_names(clear_magic):
    with pytest.raises(TypeError, match="do not match any parameter"):

        @magic().shared("nonexistent")
        def func(x: int) -> int:
            return x


def test_shared_arg_registrar_extracts_by_name():
    """SharedArgRegistrar extracts named args from both positional and keyword calls."""

    def fn(a: int, b: str, c: list) -> None:
        pass

    registered: list[tuple[str, object]] = []

    class _FakeRunner:
        def shared(self, **kwargs: object) -> None:
            registered.extend(kwargs.items())

    registrar = SharedArgRegistrar(fn, ("b", "c"))

    big_list = [1, 2, 3]
    registrar.register(_FakeRunner(), (42, "hello"), {"c": big_list})  # type: ignore
    assert ("b", "hello") in registered
    assert ("c", big_list) in registered


def test_shared_arg_registrar_skips_none():
    """None values are not registered (optional args that weren't passed)."""

    def fn(x: int, mapping: object = None) -> None:
        pass

    registered: list[tuple[str, object]] = []

    class _FakeRunner:
        def shared(self, **kwargs: object) -> None:
            registered.extend(kwargs.items())

    registrar = SharedArgRegistrar(fn, ("mapping",))
    registrar.register(_FakeRunner(), (1,), {})  # type: ignore

    assert registered == []


def test_shared_arg_registrar_positional():
    """Works when the named arg is passed positionally."""

    def fn(sources: list, mapping: dict) -> None:
        pass

    registered: list[tuple[str, object]] = []

    class _FakeRunner:
        def shared(self, **kwargs: object) -> None:
            registered.extend(kwargs.items())

    the_mapping = {"a": 1}
    registrar = SharedArgRegistrar(fn, ("mapping",))
    registrar.register(_FakeRunner(), ([1, 2], the_mapping), {})  # type: ignore

    assert ("mapping", the_mapping) in registered
