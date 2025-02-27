import pytest

from thds.core import logical_root


def test_finds_default_common_prefix():
    uris = [
        "adls://duck/horse/foo/bar/baz",
        "adls://duck/horse/foo/a/c",
        "adls://duck/horse/foo/bar/quux",
    ]
    assert logical_root.find(uris) == "adls://duck/horse/foo"


def test_finds_explicit_higher_root():
    uris = ["adls://duck/horse/foo/bar/baz", "adls://duck/horse/foo/a/c"]
    assert logical_root.find(uris, "horse") == "adls://duck/horse"
    assert logical_root.find(uris, "duck") == "adls://duck"


def test_multi_component_higher_root():
    uris = [
        "adls://duck/horse/hippo/horse/foo/bar/baz",
        "adls://duck/horse/hippo/horse/foo/a/c",
        "adls://duck/horse/hippo/horse/foo/bar/quux",
    ]
    assert logical_root.find(uris, "duck/horse") == "adls://duck"


def test_empty_uri_list_returns_empty_str():
    assert logical_root.find([]) == ""


def test_no_common_prefix_raises():
    with pytest.raises(ValueError, match="Paths have no common prefix"):
        logical_root.find(["adls://duck/horse/foo", "gcs://duck/horse/foo"])


def test_nonexistent_higher_root_raises():
    uris = ["adls://duck/horse/foo/bar", "adls://duck/horse/foo/baz"]
    with pytest.raises(ValueError, match="Higher root 'nonexistent' not found"):
        logical_root.find(uris, "nonexistent")


def test_nonexistent_higher_root_raises_with_no_uris():
    with pytest.raises(ValueError, match="Higher root 'nonexistent' not found"):
        logical_root.find(list(), "nonexistent")
