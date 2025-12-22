import typing as ty
from pathlib import Path

from thds.core import uri_assign


def test_default_no_assign():
    assert not uri_assign.for_path(Path(".out/some/random/path.txt"))


def test_no_prefix_no_uri_assign():
    assert not uri_assign.replace_working_dirs_with_prefix("", Path("some/random/path.txt"))


def test_with_prefix_uri_assign():
    assert "foo/bar/some/random/path.txt" == uri_assign.replace_working_dirs_with_prefix(
        "foo/bar", Path("some/random/path.txt")
    )


def test_removes_default_dot_out_dir():
    assert "foo/bar/some/random/path.txt" == uri_assign.replace_working_dirs_with_prefix(
        "foo/bar",
        Path(".out/some/random/path.txt"),
    )


def test_works_with_absolute_path():
    assert "foo/bar/some/random/path.txt" == uri_assign.replace_working_dirs_with_prefix(
        "foo/bar",
        Path("/absolute/path/to/some/random/path.txt"),
        working_dirs=(Path("/absolute/path/to"),),
    )


def _prefixed_uri_hook(prefix: str, working_dirs: ty.Sequence[Path] = tuple()):
    return lambda path: uri_assign.replace_working_dirs_with_prefix(
        prefix, path, working_dirs=working_dirs or []
    )


def test_assign_empty_string_or_empty_path():
    with uri_assign.add_hook(_prefixed_uri_hook("foo/bar")):
        assert "foo/bar" == uri_assign.for_path("")
        assert "foo/bar" == uri_assign.for_path(Path(""))
        assert "foo/bar" == uri_assign.for_path(Path("."))
        assert "foo/bar" == uri_assign.for_path(Path.cwd())
        # all of the above are equivalent to asking for the current working directory aka prefix.


def test_assign_respects_hook_registration():
    assert not uri_assign.for_path("some/random/path.txt")

    with uri_assign.add_hook(_prefixed_uri_hook("foo/bar")):
        assert "foo/bar/some/random/path.txt" == uri_assign.for_path("some/random/path.txt")

    assert not uri_assign.for_path("some/random/path.txt")


def test_assign_respects_latest_hook_registration():
    with uri_assign.add_hook(_prefixed_uri_hook("foo/bar")):
        with uri_assign.add_hook(_prefixed_uri_hook("foo/bar/baz")):
            assert "foo/bar/baz/some/random/path.txt" == uri_assign.for_path("some/random/path.txt")
        with uri_assign.add_hook(_prefixed_uri_hook("foo/bar", working_dirs=[Path("baz")])):
            assert "foo/bar/some/random/path.txt" == uri_assign.for_path("baz/some/random/path.txt")
        assert "foo/bar/some/random/path.txt" == uri_assign.for_path("some/random/path.txt")
        assert "foo/bar/baz/some/random/path.txt" == uri_assign.for_path("baz/some/random/path.txt")
