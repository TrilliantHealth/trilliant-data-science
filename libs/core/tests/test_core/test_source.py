import dataclasses
from pathlib import Path

import pytest

from thds.core.hashing import Hash
from thds.core.source import Source
from thds.core.source._construct import from_file, from_uri, to_uri
from thds.core.source._download import SourceHashMismatchError, _get_download_handler


def test_source_from_file_is_openable(temp_file):
    tfile = temp_file("some text")

    assert open(from_file(tfile)).read() == "some text"


def test_source_from_file_gives_path(temp_file):
    tfile = temp_file("other text")

    assert from_file(tfile).path().resolve() == tfile.resolve()


def test_from_file_allows_source(temp_file):
    # In practice this is not a realistic use case, but it should work.
    tfile = temp_file("other text")
    source1 = from_file(tfile)
    source2 = from_file(source1)

    assert source1 == source2


def test_resolve_local_source(temp_file):
    tfile = temp_file("YO")

    source = from_file(tfile, None, to_uri(tfile))
    assert source.hash
    object.__setattr__(source, "__cached_path", None)  # hack necessary to test caching

    assert open(source).read() == "YO"
    assert open(source).read() == "YO"
    # running this twice makes sure we cover the optimized reuse of the cached path.


def test_from_file_fails_if_path_not_exists():
    with pytest.raises(FileNotFoundError):
        from_file(Path("does-not-exist"))


def test_hash_mismatch_detected_upon_creation_of_file_source(temp_file):
    tfile = temp_file("some text")

    with pytest.raises(SourceHashMismatchError):
        from_file(tfile, hash=Hash("sha256", b"not-the-right-hash"))


def test_from_uri_redirects_to_from_file_for_file_scheme(temp_file):
    tfile = temp_file("whatever")

    assert from_file(tfile) == from_uri(f"file://{tfile}")


def test_no_downloader_found_for_random_scheme():
    with pytest.raises(ValueError):
        _get_download_handler("random://whatever")


def test_local_file_downloader_raises_file_not_found():
    with pytest.raises(FileNotFoundError):
        _get_download_handler("file:///does-not-exist")(None)


def test_hash_not_checked_if_not_present(temp_file):
    source = Source(to_uri(temp_file("foobar")))
    assert open(source).read() == "foobar"


def test_from_uri_works():
    assert from_uri("foo://bar/baz").uri == "foo://bar/baz"


def test_source_is_hashable():
    assert hash(from_uri("foo://bar/baz")) == hash(from_uri("foo://bar/baz"))
    assert from_uri("foo://bar/baz") == from_uri("foo://bar/baz")


def test_source_is_immutable(temp_file):
    s = from_file(temp_file("car"))
    with pytest.raises(dataclasses.FrozenInstanceError):
        s.uri = "not allowed"  # type: ignore


def test_set_cached_path_does_not_error_if_none(temp_file):
    s = from_file(temp_file("dog"))
    s._set_cached_path(None)
    assert not s.cached_path
