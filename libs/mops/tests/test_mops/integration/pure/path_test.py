from pathlib import Path

import pytest

from thds.mops import tempdir
from thds.mops.pure.core.serialize_paths import NotAFileError

from ._util import adls_shell


@adls_shell
def _consume_a_path_and_return_one(path: Path) -> Path:
    tmp_out = tempdir() / "out"
    with open(path) as p:
        with open(tmp_out, "w") as w:
            # reverse the bytes of the file:
            w.write(p.read()[::-1])

    return tmp_out


def test_that_nonfile_fails():
    with pytest.raises(NotAFileError):
        _consume_a_path_and_return_one(Path(__file__).parent / "not-a-file")


def test_that_directory_fails():
    with pytest.raises(NotAFileError):
        _consume_a_path_and_return_one(Path(__file__).parent)


def test_that_normal_path_works():
    pth = tempdir() / "foooooo"

    with open(pth, "w") as w:
        w.write("hello world")

    out_pth = _consume_a_path_and_return_one(pth)
    with open(out_pth) as r:
        assert r.read() == "dlrow olleh"
