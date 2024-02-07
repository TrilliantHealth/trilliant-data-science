import os
import unittest.mock
from pathlib import Path

from thds.core import tmp


def test_if_on_ci_we_put_tempfiles_in_home_directory():
    """Because if we don't, we have all kinds of weird linking race conditions that I don't want to deal with."""
    if os.getenv("CI"):
        assert tmp.tempdirs_on_different_filesystem()
    else:
        assert not tmp.tempdirs_on_different_filesystem()


def test_mock_tempdir_when_not_on_same_fs():
    # this isn't actually possible to test locally in a meaningful way (i.e. without
    # mocking the actual core logic), but on CI it should actually change what happens.
    with unittest.mock.patch("thds.core.tmp._are_same_fs", return_value=False):
        with tmp.temppath_same_fs(Path(__file__)) as p:
            assert p.parent.exists()
            assert p.parent == Path(__file__).parent  # because we didn't have to look farther


def test_mock_tempdir_when_on_same_fs():
    # this isn't actually possible to test locally in a meaningful way (i.e. without
    # mocking the actual core logic), but on CI it should actually change what happens.
    with unittest.mock.patch("thds.core.tmp._are_same_fs", return_value=True):
        with tmp.temppath_same_fs(Path(__file__)) as p:
            assert p.parent.exists()
            assert p.parent != Path(__file__).parent


def test_deletes_file():
    with unittest.mock.patch("thds.core.tmp._are_same_fs", return_value=False):
        with tmp.temppath_same_fs(Path(__file__)) as p:
            with open(p, "w") as f:
                f.write("hello")
                assert p.exists()
        assert not p.exists()


def test_deletes_dir():
    with unittest.mock.patch("thds.core.tmp._are_same_fs", return_value=False):
        with tmp.temppath_same_fs(Path(__file__)) as p:
            p.mkdir(exist_ok=True, parents=True)
            foo = p / "foo"
            foo.write_text("goodbye")
            assert foo.exists()
        assert not p.exists()
