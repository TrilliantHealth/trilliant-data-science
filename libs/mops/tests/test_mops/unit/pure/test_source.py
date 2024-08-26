# testing pure.core.source
import uuid

import pytest

from thds.core import source
from thds.core.hash_cache import filehash
from thds.core.hashing import Hash
from thds.mops import tempdir
from thds.mops.pure.core import deferred_work, output_naming
from thds.mops.pure.core.source import (
    _hashref_uri,
    create_source_at_uri,
    perform_source_uploads,
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
    with deferred_work.open_context(), ACTIVE_STORAGE_ROOT.set(f"file://{_TEST_DIR}"):
        yield


# testing argument scenarios:
# 1. we have a local source and we don't trigger uploads
# 2. we have a local source and we _do_ trigger uploads
# 3. we have a remote-only source with a Hash
# 4. we have a URI-only remote source
# 5. ? we have a URI-only source but it's a file and we could compute a hash?


def test_local_source_no_uploads(prep, temp_file):  # 1
    test_file = temp_file("local source no uploads")
    initial_source = source.from_file(test_file)
    source_arg = prepare_source_argument(initial_source)
    assert isinstance(source_arg, Hash)
    assert source_arg == source.from_file(test_file).hash

    reconstituted_source = source_from_hashref(source_arg)
    assert reconstituted_source.hash == initial_source.hash
    assert test_file.resolve() == reconstituted_source.path().resolve()


def test_local_source_with_uploads(prep, temp_file):  # 2
    test_file = temp_file("local source with uploads")
    initial_source = source.from_file(test_file)
    assert initial_source.cached_path
    assert initial_source.hash
    source_arg = prepare_source_argument(initial_source)
    assert isinstance(source_arg, Hash)
    perform_source_uploads()

    # now, delete the local source ref (pretend that we're truly remote)
    # and see that we can still reconstitute the Source
    source.path_from_uri(_hashref_uri(initial_source.hash, "local")).unlink()
    reconstituted_source = source_from_hashref(source_arg)
    assert reconstituted_source.hash == initial_source.hash
    assert test_file.resolve() != reconstituted_source.path().resolve()
    # origin and 'downloaded' files actually live in different places
    assert test_file.read_text() == reconstituted_source.path().read_text()
    # but their contents are identical


def test_remote_source_with_hash(prep, temp_file):  # 3
    test_file = temp_file("remote source with hash")
    initial_source = source.Source(
        source.to_uri(test_file), filehash("sha256", test_file)
    )  # hash but no local Path

    source_arg = prepare_source_argument(initial_source)
    assert isinstance(source_arg, Hash)

    perform_source_uploads()
    # hashrefs are now also gated behind the 'uploads'
    # abstraction, in order to avoid unnecessarily uploading things
    # when we may already be able to fetch a result.

    reconstituted_source = source_from_hashref(source_arg)
    assert reconstituted_source.hash == initial_source.hash
    assert test_file.read_text() == reconstituted_source.path().read_text()


def test_remote_source_with_no_hash_just_communicates_uri(prep, temp_file):  # 4
    test_file = temp_file("remote source with NO hash")
    initial_source = source.Source(source.to_uri(test_file))  # no hash, no local Path

    source_arg = prepare_source_argument(initial_source)
    assert isinstance(source_arg, str)
    perform_source_uploads()  # just to show that we can 'not' do uploads if there are none.

    reconstituted_source = source.from_uri(source_arg)
    assert test_file.read_text() == reconstituted_source.path().read_text()


# testing result scenarios:
# 1. we have a local source and we return to a local environment
# 2. we have a local source and we return to a remote environment
# 3. we have a remote-only source. (this needs to be an integration test)


def test_local_source_returned_to_local(prep, temp_file):  # 1
    with output_naming.PipelineFunctionUniqueKey.set(
        "test"
    ), output_naming.FunctionArgumentsHashUniqueKey.set("abcdefg"):
        test_file = temp_file("local source returned to local")
        initial_source = source.from_file(test_file)
        source_result = prepare_source_result(initial_source)

        out_src = source_from_source_result(*source_result)
        assert out_src.path().read_text() == initial_source.path().read_text()


def test_local_source_returned_to_remote(prep, temp_file):  # 2
    with output_naming.PipelineFunctionUniqueKey.set(
        "test"
    ), output_naming.FunctionArgumentsHashUniqueKey.set("123456"):
        test_file = temp_file("local source returned to remote")
        initial_source = source.from_file(test_file)
        source_result = prepare_source_result(initial_source)

        orig_text = test_file.read_text()
        test_file.unlink()  # delete the original file so we look like we're remote
        out_src = source_from_source_result(*source_result)
        assert out_src.path().read_text() == orig_text


@pytest.mark.integration
def test_remote_source_returned_to_remote(prep, temp_file):  # 3
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
