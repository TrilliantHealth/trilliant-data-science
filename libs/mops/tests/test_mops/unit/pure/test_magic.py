from datetime import datetime

import pytest

from thds.mops.pure import _magic, magic
from thds.mops.pure.runner.simple_shims import samethread_shim, subprocess_shim


@pytest.fixture
def clear_magic():
    _magic.api._MAGIC_CONFIG = _magic.sauce.new_config()  # type: ignore
    yield


def test_magic_shim_configuration(clear_magic):
    # Test direct shim application
    @magic()
    def func1():
        pass

    assert samethread_shim is func1._shimbuilder(func1, tuple(), dict())  # type: ignore

    # Test shim override
    magic.shim("subprocess", func1)
    assert subprocess_shim is func1._shimbuilder(func1, tuple(), dict())  # type: ignore

    # Test turning off mops
    magic.shim("off", func1)
    assert func1._shim_builder_or_off is None


def test_magic_blob_root_configuration(clear_magic):
    @magic()
    def func1():
        pass

    assert func1._get_blob_root() == _magic.sauce._local_root()

    # Test override
    new_uri = "adls://new/container"
    magic.blob_root(new_uri, func1)
    assert func1._get_blob_root() == new_uri


def test_magic_pipeline_id_configuration(clear_magic):
    @magic()
    def func2():
        pass

    assert func2._pipeline_id == "magic"

    @magic(pipeline_id="test_pipeline")
    def func1():
        pass

    assert func1._pipeline_id == "test_pipeline"

    # Test override
    magic.pipeline_id("new_pipeline", func1)
    assert func1._pipeline_id == "new_pipeline"


def test_magic_mask_override(clear_magic):
    # Set up a module hierarchy simulation
    @magic()
    def module_func():
        pass

    magic.shim("samethread", module_func)
    magic.off("tests", mask=True)  # Should override everything

    assert module_func._shim_builder_or_off is None


def test_magic_off_context(clear_magic):
    @magic()
    def func7():
        return "test"

    assert not func7._is_off()

    # Test that the function would run directly in the off context
    with func7.off():
        assert func7._is_off()

    # Test that it returns to normal after context
    assert not func7._is_off()


def test_magic_decorator_allows_all_config(clear_magic):
    pipeline_id = "test/mops" + datetime.utcnow().isoformat()

    @magic("subprocess", blob_root="file://~/.mops", pipeline_id=pipeline_id)
    def func1(a: int, b: str):
        return a * b

    assert func1._get_blob_root() == "file://~/.mops"
    assert func1._pipeline_id == pipeline_id
    assert subprocess_shim is func1._shimbuilder(func1, tuple(), dict())  # type: ignore


def test_pipeline_id_from_docstring(clear_magic):
    @magic()
    def func1():
        """pipeline-id: test_pipeline"""
        pass

    assert func1._pipeline_id == "test_pipeline"


def test_repr(clear_magic):
    @magic()
    def func1():
        pass

    f1_repr = repr(func1)
    print(f1_repr)
    assert f1_repr.startswith(
        "Magic('tests.test_mops.unit.pure.test_magic.func1', shim=<static_shim_builder for <function samethread_shim"
    )
    assert f1_repr.endswith("/.mops', pipeline_id='magic')")
