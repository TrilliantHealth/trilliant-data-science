import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from thds.core import scope
from thds.core.source import Source, tree, tree_from_directory


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


_LOGICAL_ROOT = "dir"
_SOURCE_A = Source(uri=f"scheme://bucket/{_LOGICAL_ROOT}/a.parquet")
_SOURCE_B = Source(uri=f"scheme://bucket/{_LOGICAL_ROOT}/b.parquet")


# --- serde ---


def test_serde_roundtrip_multiple_sources():
    st = tree.SourceTree(sources=[_SOURCE_A, _SOURCE_B], higher_logical_root=_LOGICAL_ROOT)
    restored = tree.from_json(tree.to_json(st))
    assert len(restored.sources) == 2
    assert restored.sources[0].uri == _SOURCE_A.uri
    assert restored.sources[1].uri == _SOURCE_B.uri
    assert restored.higher_logical_root == _LOGICAL_ROOT


def test_serde_roundtrip_single_source():
    st = tree.SourceTree(sources=[_SOURCE_A], higher_logical_root=_LOGICAL_ROOT)
    restored = tree.from_json(tree.to_json(st))
    assert len(restored.sources) == 1
    assert restored.sources[0].uri == _SOURCE_A.uri
    assert restored.higher_logical_root == _LOGICAL_ROOT


def test_serde_roundtrip_empty_higher_logical_root():
    st = tree.SourceTree(sources=[_SOURCE_A], higher_logical_root="")
    restored = tree.from_json(tree.to_json(st))
    assert restored.higher_logical_root == ""


def test_serde_to_dict_structure():
    st = tree.SourceTree(sources=[_SOURCE_A], higher_logical_root=_LOGICAL_ROOT)
    d = tree.to_dict(st)
    assert "sources" in d
    assert "higher_logical_root" in d
    assert len(d["sources"]) == 1
    assert d["sources"][0]["uri"] == _SOURCE_A.uri


def test_serde_from_mapping_missing_higher_logical_root_defaults_empty():
    m = {"sources": [{"uri": _SOURCE_A.uri, "size": 0}]}
    restored = tree.from_mapping(m)
    assert restored.higher_logical_root == ""


def test_serde_to_json_is_valid_json():
    st = tree.SourceTree(sources=[_SOURCE_A], higher_logical_root=_LOGICAL_ROOT)
    parsed = json.loads(tree.to_json(st))
    assert isinstance(parsed, dict)


def test_serde_write_to_json_file(tmp_path: Path):
    st = tree.SourceTree(sources=[_SOURCE_A, _SOURCE_B], higher_logical_root=_LOGICAL_ROOT)
    outfile = tmp_path / "tree.json"
    changed = tree.write_to_json_file(st, outfile)
    assert changed is True
    restored = tree.from_json(outfile.read_text())
    assert len(restored.sources) == 2
    assert restored.higher_logical_root == _LOGICAL_ROOT


def test_serde_write_to_json_file_no_change(tmp_path: Path):
    st = tree.SourceTree(sources=[_SOURCE_A], higher_logical_root=_LOGICAL_ROOT)
    outfile = tmp_path / "tree.json"
    tree.write_to_json_file(st, outfile)
    changed = tree.write_to_json_file(st, outfile)
    assert changed is False


# --- higher_logical_root_uri ---


def test_higher_logical_root_uri_multiple_sources():
    st = tree.SourceTree(sources=[_SOURCE_A, _SOURCE_B], higher_logical_root=_LOGICAL_ROOT)
    assert st.higher_logical_root_uri == f"scheme://bucket/{_LOGICAL_ROOT}"


def test_higher_logical_root_uri_single_source():
    st = tree.SourceTree(sources=[_SOURCE_A], higher_logical_root=_LOGICAL_ROOT)
    assert st.higher_logical_root_uri == f"scheme://bucket/{_LOGICAL_ROOT}"


def test_higher_logical_root_uri_single_source_nested():
    st = tree.SourceTree(
        sources=[Source(uri="scheme://bucket/a/b/dir/only.parquet")],
        higher_logical_root=_LOGICAL_ROOT,
    )
    assert st.higher_logical_root_uri == f"scheme://bucket/a/b/{_LOGICAL_ROOT}"


def test_higher_logical_root_uri_empty():
    st = tree.SourceTree(sources=[_SOURCE_A], higher_logical_root="")
    # empty string — endswith("") is always true, so no assertion error
    assert st.higher_logical_root_uri == f"scheme://bucket/{_LOGICAL_ROOT}"


def test_higher_logical_root_uri_single_source_mismatch():
    st = tree.SourceTree(
        sources=[Source(uri="scheme://bucket/other/only.parquet")],
        higher_logical_root=_LOGICAL_ROOT,
    )
    with pytest.raises(AssertionError, match="Expected the uri ends with"):
        st.higher_logical_root_uri


def test_higher_logical_root_uri_multiple_sources_mismatch():
    st = tree.SourceTree(
        sources=[
            Source(uri="scheme://bucket/other/a.parquet"),
            Source(uri="scheme://bucket/other/b.parquet"),
        ],
        higher_logical_root=_LOGICAL_ROOT,
    )
    with pytest.raises(AssertionError, match="Expected the uri ends with"):
        st.higher_logical_root_uri
