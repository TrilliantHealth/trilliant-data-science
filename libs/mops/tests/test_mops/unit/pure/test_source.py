# testing pure.core.source
import uuid

import pytest

from thds.core import files, source
from thds.core.hash_cache import filehash
from thds.core.hashing import Hash
from thds.mops import tempdir
from thds.mops.pure.core import deferred_work, output_naming
from thds.mops.pure.core.source import (
    _HASHREF_MAP,
    _hash_to_str,
    create_source_at_uri,
    hashref_context,
    prepare_source_argument,
    prepare_source_result,
    source_from_hashref,
    source_from_source_result,
)
from thds.mops.pure.core.uris import ACTIVE_STORAGE_ROOT

from ...config import TEST_DATA_TMP_URI

_TEST_DIR = tempdir() / "mops-test-source"
_TEST_DIR.mkdir(exist_ok=True)


@pytest.fixture
def prep():
    """These things are necessary before our Hashref/Source code can do anything."""
    with (
        deferred_work.open_context(),
        ACTIVE_STORAGE_ROOT.set(f"file://{_TEST_DIR}"),
        _HASHREF_MAP.set({}),
    ):
        yield


# testing argument scenarios:
# 1. we have a local source — prepare populates hashref map, resolve via map
# 2. we have a local source, upload works, resolve via map on remote side
# 3. we have a remote-only source with a Hash
# 4. we have a URI-only remote source


def test_local_source_roundtrip(prep, temp_file):
    """Prepare a local source, then resolve it from the hashref map."""
    test_file = temp_file("local source roundtrip")
    initial_source = source.from_file(test_file)
    source_arg = prepare_source_argument(initial_source)
    assert isinstance(source_arg, Hash)
    assert source_arg == source.from_file(test_file).hash
    deferred_work.perform_all()

    reconstituted = source_from_hashref(source_arg)
    assert reconstituted.hash == initial_source.hash
    assert test_file.read_text() == reconstituted.path().read_text()


def test_local_source_with_uploads(prep, temp_file):
    """Prepare and upload a local source, then resolve via map as if truly remote."""
    test_file = temp_file("local source with uploads")
    initial_source = source.from_file(test_file)
    assert initial_source.cached_path
    assert initial_source.hash
    source_arg = prepare_source_argument(initial_source)
    assert isinstance(source_arg, Hash)
    deferred_work.perform_all()

    # resolve via hashref map — the remote URI should point to uploaded data
    reconstituted = source_from_hashref(source_arg)
    assert reconstituted.hash == initial_source.hash
    assert reconstituted.size == initial_source.size
    assert test_file.read_text() == reconstituted.path().read_text()


def test_remote_source_with_hash(prep, temp_file):
    test_file = temp_file("remote source with hash")
    initial_source = source.Source(
        files.to_uri(test_file), filehash("sha256", test_file)
    )  # hash but no local Path

    source_arg = prepare_source_argument(initial_source)
    assert isinstance(source_arg, Hash)

    reconstituted = source_from_hashref(source_arg)
    assert reconstituted.hash == initial_source.hash
    assert test_file.read_text() == reconstituted.path().read_text()


def test_remote_source_with_no_hash_just_communicates_uri(prep, temp_file):
    test_file = temp_file("remote source with NO hash")
    initial_source = source.Source(files.to_uri(test_file))  # no hash, no local Path

    source_arg = prepare_source_argument(initial_source)
    assert isinstance(source_arg, str)
    deferred_work.perform_all()

    reconstituted_source = source.from_uri(source_arg)
    assert test_file.read_text() == reconstituted_source.path().read_text()


# testing result scenarios:
# 1. we have a local source and we return to a local environment
# 2. we have a local source and we return to a remote environment
# 3. we have a remote-only source. (this needs to be an integration test)

_VALID_MEMO_URI = "file://foo/bar/mops2-mpf/pipelineid/module--function/argskwargshash"


def test_local_source_returned_to_local(prep, temp_file):
    with output_naming.uri_assignment_context(_VALID_MEMO_URI):
        test_file = temp_file("local source returned to local")
        initial_source = source.from_file(test_file)
        source_result = prepare_source_result(initial_source)
        deferred_work.perform_all()

        out_src = source_from_source_result(*source_result)
        assert out_src.path().read_text() == initial_source.path().read_text()


def test_local_source_returned_to_remote(prep, temp_file):
    with output_naming.uri_assignment_context(_VALID_MEMO_URI):
        test_file = temp_file("local source returned to remote")
        initial_source = source.from_file(test_file)
        source_result = prepare_source_result(initial_source)
        deferred_work.perform_all()

        orig_text = test_file.read_text()
        test_file.unlink()  # delete the original file so we look like we're remote
        out_src = source_from_source_result(*source_result)
        assert out_src.path().read_text() == orig_text


@pytest.mark.integration
def test_remote_source_returned_to_remote(prep, temp_file):
    initial_source = source.from_uri(
        "adls://thdsdatasets/prod-datasets/test/read-only/DONT_DELETE_THESE_FILES.txt"
    )
    source_result = prepare_source_result(initial_source)
    out_src = source_from_source_result(*source_result)
    assert out_src.path().read_text() == initial_source.path().read_text()


@pytest.mark.integration
def test_create_source_from_local(prep, temp_file):
    test_file = temp_file("create source from local")
    dest_uri = f"{TEST_DATA_TMP_URI}pure-core-source/{uuid.uuid4().hex}"
    source = create_source_at_uri(test_file, dest_uri)

    assert source.path().resolve() == test_file.resolve()

    orig_text = test_file.read_text()
    test_file.unlink()
    assert open(source).read() == orig_text


@pytest.mark.integration
def test_local_source_with_remote(prep, temp_file):
    test_file = temp_file("local source with remote")
    dest_uri = f"{TEST_DATA_TMP_URI}pure-core-source-result/{uuid.uuid4().hex}"
    source = create_source_at_uri(test_file, dest_uri)

    orig_text = test_file.read_text()
    test_file.unlink()

    re_source = source_from_source_result(*prepare_source_result(source))
    assert open(re_source).read() == orig_text


@pytest.mark.integration
def test_from_file_with_uri(prep, temp_file):
    test_file = temp_file("local source with remote from_file(uri=...)")
    dest_uri = f"{TEST_DATA_TMP_URI}pure-core-source-result/{uuid.uuid4().hex}"
    source_ = source.from_file(test_file, uri=dest_uri)  # does not do the upload

    orig_text = test_file.read_text()

    re_source = source_from_source_result(*prepare_source_result(source_))
    deferred_work.perform_all()

    test_file.unlink()  # make it go away so we can test
    assert open(re_source).read() == orig_text


# hashref map (embedded in invocation header) tests


def test_hashref_map_resolves_without_network(prep, temp_file):
    """When a hashref map is set, source_from_hashref resolves directly from it."""
    test_file = temp_file("hashref map test")
    initial_source = source.from_file(test_file)
    assert initial_source.hash

    remote_uri = files.to_uri(test_file)
    hashref_map = {
        _hash_to_str(initial_source.hash): {"uri": remote_uri, "size": initial_source.size},
    }

    with hashref_context(hashref_map):
        reconstituted = source_from_hashref(initial_source.hash)

    assert reconstituted.hash == initial_source.hash
    assert reconstituted.uri == remote_uri
    assert reconstituted.size == initial_source.size


def test_hashref_map_miss_raises(prep, temp_file):
    """When the hashref map doesn't contain the hash, raise KeyError."""
    test_file = temp_file("hashref map miss")
    initial_source = source.from_file(test_file)
    assert initial_source.hash

    with hashref_context({}):
        with pytest.raises(KeyError):
            source_from_hashref(initial_source.hash)


def test_no_hashref_context_raises(prep, temp_file):
    """When hashref_context is None (default), raise ValueError."""
    test_file = temp_file("no hashref map context")
    initial_source = source.from_file(test_file)
    assert initial_source.hash

    with hashref_context(None):
        with pytest.raises(ValueError, match="without a hashref map context"):
            source_from_hashref(initial_source.hash)


def test_prepare_source_argument_collects_hashref_mapping(prep, temp_file):
    """prepare_source_argument populates the hashref map when one is open."""
    test_file = temp_file("collect mapping test")
    initial_source = source.from_file(test_file)

    collected = _HASHREF_MAP()
    assert collected is not None
    assert initial_source.hash is not None

    prepare_source_argument(initial_source)

    hash_str = _hash_to_str(initial_source.hash)
    assert hash_str in collected
    assert collected[hash_str]["size"] == initial_source.size
    assert collected[hash_str]["uri"]
