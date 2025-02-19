"""This tests the majority of thds.mops.pure without needing a Kubernetes cluster."""

import logging
import typing as ty
from datetime import datetime
from pathlib import Path

import pytest

from thds.mops import tempdir
from thds.mops.pure import MemoizingPicklingRunner, Shell, set_pipeline_id, use_runner

from ...config import TEST_TMP_URI
from ._util import _subprocess_remote, adls_shim, clear_cache

# this hopefully keeps the tmp bucket easier to navigate/understand
set_pipeline_id("test/shim/" + datetime.utcnow().isoformat())


@adls_shim
def crazy_stuff(a: float, b: float, c: float, d: int):
    print("craaaaazy")
    return a * b / c + d


def test_crazy_stuff():
    """This will actually cause crazy_stuff to be run in a completely
    separate process, with the arguments to it being transmitted over
    ADLS and the return value also being sent back across ADLS.
    """
    val = crazy_stuff(4, 3, 2, 1)
    assert val == 7.0


@adls_shim
def func_with_paths(a_path: Path, b: int) -> Path:
    # the Path gets transferred as a temp path - we can read its
    # contents but not use it in any other way.
    assert isinstance(a_path, Path), a_path
    assert a_path.exists()
    path_str = open(a_path).read()

    result = float(path_str) + b
    # we can now return the result as a Path, and the file will get
    # transferred to a tempfile on the orchestrator.
    outpath = tempdir() / "out.txt"
    with open(outpath, "w") as f:
        f.write(str(result))
    return outpath


def test_func_with_paths():
    the_path = Path(__file__).parent / "a_path.txt"
    val_path = func_with_paths(the_path, 3)
    with open(val_path, "r") as f:
        assert float(f.read()) == 9.4

    # run it a second time with the same arguments and we'll
    # short-circuit the actual execution
    val_path = func_with_paths(the_path, 3)
    with open(val_path, "r") as f:
        assert float(f.read()) == 9.4


def test_repeated_pipeline_id_reuses_results(caplog):
    caplog.set_level(logging.INFO)
    clear_cache()
    test_func_with_paths()
    exists = False
    for record in caplog.records:
        if "already exists" in record.msg:
            exists = True
    assert exists, [r.msg for r in caplog.records]


@adls_shim
def raises_exception():
    raise ValueError("Oh No!")


def test_func_raising_exception():
    with pytest.raises(ValueError):
        raises_exception()


def add2(i: int) -> int:
    return i + 2


def test_shim_builder(caplog):
    def build_shim(f: ty.Callable, args, kwargs) -> Shell:
        assert f is add2
        if args[0] == 1:
            return _subprocess_remote

        def _just_raise(*args):
            raise ValueError("i can only add 2 to 1")

        return _just_raise

    a2 = use_runner(MemoizingPicklingRunner(build_shim, TEST_TMP_URI))(add2)  # type: ignore

    with pytest.raises(ValueError):
        assert a2(0) == 2
    assert a2(1) == 3
    with pytest.raises(ValueError):
        assert a2(2) == 3
