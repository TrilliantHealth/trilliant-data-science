import os
from concurrent.futures import ThreadPoolExecutor

from thds.core.files import path_from_uri, to_uri
from thds.mops.pure.core.file_blob_store import FileBlobStore


def test_create_then_conflict(tmp_path):
    store = FileBlobStore()
    uri = to_uri(tmp_path).rstrip("/") + "/thing"
    assert store.put_unless_exists(uri, b"first") is True
    assert store.put_unless_exists(uri, b"second") is False
    assert path_from_uri(uri).read_bytes() == b"first"


def test_concurrent_creates_have_one_winner(tmp_path):
    store = FileBlobStore()
    uri = to_uri(tmp_path).rstrip("/") + "/contested"
    with ThreadPoolExecutor(32) as pool:
        outcomes = list(pool.map(lambda i: store.put_unless_exists(uri, b"x"), range(32)))
    assert sum(outcomes) == 1


def test_partial_write_failure_leaves_name_retryable(tmp_path, monkeypatch):
    store = FileBlobStore()
    uri = to_uri(tmp_path).rstrip("/") + "/manifest-chunk"
    real_link = os.link

    def failing_link(src, dst, **kw):
        raise OSError("simulated failure between body write and publication")

    monkeypatch.setattr(os, "link", failing_link)
    try:
        store.put_unless_exists(uri, b"complete body")
        raise AssertionError("expected the simulated failure to propagate")
    except OSError:
        pass
    monkeypatch.setattr(os, "link", real_link)

    assert not path_from_uri(uri).exists()  # a failed attempt published nothing
    assert store.put_unless_exists(uri, b"complete body") is True
    assert path_from_uri(uri).read_bytes() == b"complete body"
    assert [p.name for p in path_from_uri(uri).parent.iterdir()] == ["manifest-chunk"]  # no temp residue


def test_dot_prefixed_blob_is_listed(tmp_path):
    store = FileBlobStore()
    root = to_uri(tmp_path).rstrip("/")
    assert store.put_unless_exists(root + "/.hidden-but-real", b"body") is True
    assert [entry.uri.rsplit("/", 1)[-1] for entry in store.list(root)] == [".hidden-but-real"]
