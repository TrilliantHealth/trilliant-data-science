from pathlib import Path

from thds.core.source import tree


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
