from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from thds.core import scope
from thds.core.source import tree, tree_from_directory


def test_logical_tree_replication_operations():
    # Setup test data, which represents stuff in a global cache (most likely)
    local_paths = [
        Path("~/sa/cont/duck/horse/foo/bar/baz.txt").expanduser(),
        Path("~/sa/cont/duck/horse/foo/bar/a.txt").expanduser(),
        Path("~/sa/cont/duck/horse/foo/bar/b.txt").expanduser(),
        Path("~/sa/cont/duck/horse/foo/quux/a.txt").expanduser(),
    ]
    logical_local_root = Path("~/sa/cont/duck").expanduser()
    dest_dir = Path("/tmp/myapp")

    # Expected results
    expected_logical_dest = Path("/tmp/myapp/duck")
    expected_operations = [
        (
            Path("~/sa/cont/duck/horse/foo/bar/baz.txt").expanduser(),
            Path("/tmp/myapp/duck/horse/foo/bar/baz.txt"),
        ),
        (
            Path("~/sa/cont/duck/horse/foo/bar/a.txt").expanduser(),
            Path("/tmp/myapp/duck/horse/foo/bar/a.txt"),
        ),
        (
            Path("~/sa/cont/duck/horse/foo/bar/b.txt").expanduser(),
            Path("/tmp/myapp/duck/horse/foo/bar/b.txt"),
        ),
        (
            Path("~/sa/cont/duck/horse/foo/quux/a.txt").expanduser(),
            Path("/tmp/myapp/duck/horse/foo/quux/a.txt"),
        ),
    ]

    logical_dest, operations = tree._logical_tree_replication_operations(
        local_paths, logical_local_root, dest_dir
    )

    # Assertions
    assert logical_dest == expected_logical_dest
    assert operations == expected_operations


class Test_tree_from_directory:
    @scope.bound
    def test_it_raises_when_given_a_file(self):
        tmp = Path(scope.enter(TemporaryDirectory()))

        a_file = tmp / "a_file.txt"
        a_file.touch()

        with pytest.raises(NotADirectoryError):
            tree_from_directory(a_file)

    @scope.bound
    def test_it_raises_when_given_a_non_existent_dir(self):
        tmp = Path(scope.enter(TemporaryDirectory()))

        a_dir = tmp / "a_dir"  # doesn't actually exist

        with pytest.raises(FileNotFoundError):
            tree_from_directory(a_dir)

    @scope.bound
    def test_it_returns_a_source_tree_when_given_a_dir(self):
        tmp = Path(scope.enter(TemporaryDirectory()))
        file_1 = tmp / "1.txt"
        file_2 = tmp / "2.txt"

        contents = "SourceTrees are sew kewllll"
        # the function that takes a hash of the file contents is tested elsewhere.
        # here, we're just going to assert that the hashes of these two files are equivalent.

        for file in (file_1, file_2):
            with open(file, "w", encoding="utf-8") as f:
                f.write(contents)

        source_tree = tree_from_directory(tmp)

        assert len(source_tree.sources) == 2
        assert source_tree.sources[0].hash == source_tree.sources[1].hash
