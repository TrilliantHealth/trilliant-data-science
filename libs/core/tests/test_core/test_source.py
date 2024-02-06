import tempfile
import typing as ty
import uuid
from pathlib import Path

import pytest

from thds.core.hashing import Hash
from thds.core.source import SourceHashMismatchError, _get_download_handler, from_file, from_uri


@pytest.fixture
def temp_file() -> ty.Iterator[ty.Callable[[str], Path]]:
    with tempfile.TemporaryDirectory() as tempdir:

        def make_temp_file(some_text: str) -> Path:
            p = Path(tempdir) / ("cfile-" + uuid.uuid4().hex)
            with open(p, "w") as f:
                f.write(some_text)
            return p

        yield make_temp_file


def test_source_from_file_is_openable(temp_file):
    tfile = temp_file("some text")

    assert open(from_file(tfile)).read() == "some text"


def test_source_from_file_gives_path(temp_file):
    tfile = temp_file("other text")

    assert from_file(tfile).path().resolve() == tfile.resolve()


def test_resolve_local_source(temp_file):
    tfile = temp_file("YO")

    source = from_file(tfile)
    source._local_path = None

    assert open(source).read() == "YO"
    assert open(source).read() == "YO"
    # running this twice makes sure we cover the optimized path.


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
    source = from_file(temp_file("foobar"))
    source.hash = None
    source._local_path = None
    assert open(source).read() == "foobar"


def test_from_uri_works():
    assert from_uri("foo://bar/baz").uri == "foo://bar/baz"
