from pathlib import Path

import pytest

from thds.mops import tempdir
from thds.mops.pure.core.serialize_paths import NotAFileError

from ._util import adls_shim


@adls_shim
def _consume_a_path_and_return_one(path: Path) -> Path:
    tmp_out = tempdir() / "out"
    with open(path) as p:
        print("reversing contents of file and writing to tmp out path:", tmp_out)
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


@adls_shim
def _write_foo_into_path(pth: Path) -> Path:
    print(f"Will write to: {pth}")
    with open(pth, "w") as w:
        w.write("foo")
    return pth


def test_that_relative_path_does_not_error_if_not_file():
    # relative paths that don't exist or aren't files are serialized as relative paths
    try:
        not_a_file = Path("not-a-file")
        assert not not_a_file.is_absolute()
        not_a_file.unlink(missing_ok=True)
        out_pth = _write_foo_into_path(not_a_file)
        assert not_a_file.exists()  # we did actually write to this file!
        assert out_pth.read_text() == "foo"
    finally:
        not_a_file.unlink()
