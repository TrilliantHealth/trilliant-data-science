from typing import List

import pytest

from thds.mops.pure.core.file_blob_store import FileBlobStore


@pytest.fixture
def file_blob_store() -> FileBlobStore:
    return FileBlobStore()


@pytest.mark.parametrize(
    "uri, expected_parts",
    [
        pytest.param("", [], id="empty case", marks=pytest.mark.xfail(raises=AssertionError)),
        pytest.param(
            "file:///nested/directory/structure/that/is/very/deep/file.txt",
            ["file:///", "nested", "directory", "structure", "that", "is", "very", "deep", "file.txt"],
            id="deep path",
        ),
        pytest.param("file:///nested/directory", ["file:///", "nested", "directory"], id="simple case"),
        pytest.param(
            "file:///foo/bar//baz",
            ["file:///", "foo", "bar", "baz"],
            id="redundant slashes",
        ),
    ],
)
def test_file_blob_store_split(
    file_blob_store: FileBlobStore, uri: str, expected_parts: List[str]
) -> None:
    assert file_blob_store.split(uri) == expected_parts
