import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from thds.adls import defaults, errors, resource
from thds.mops.pure.adls import DestFileContext, SrcFileContext, dest, local_src, src, src_from_dest
from thds.mops.pure.adls.srcdest.download import download_serialized
from thds.mops.pure.adls.srcdest.parse_serialized import (
    read_possible_serialized,
    resource_from_serialized,
)
from thds.mops.pure.adls.srcdest.srcf import _srcfile_from_serialized
from thds.mops.pure.pickling._pickle import gimme_bytes
from thds.mops.srcdest import DestFile, Serialized, SrcFile

from ...config import TEST_TMP_URI
from ._util import adls_shell, runner

TEST_TIME = datetime.utcnow().isoformat()
# at least two of these times will be seen each time the tests are
# run.  one for the local-running process, and then others for the
# remote-running processes that directly access TEST_PREFIX instead of
# it being provided to them.
TOP_DIR = "test-adls-src-dest-files/"
TEST_PREFIX = TOP_DIR + TEST_TIME
TEST_ROOT = defaults.env_root("dev") / TEST_PREFIX


def dest_context():
    return DestFileContext(TEST_ROOT, "")


def src_context():
    return SrcFileContext(TEST_ROOT)


def put_into_dest_remotely(dest_file: DestFile) -> DestFile:
    assert isinstance(dest_file, DestFile), dest_file
    with dest_file as dfname:
        with open(dfname, "w") as f:
            f.write("testing123")
            assert os.path.exists(dfname)
    return dest_file


def read_from_remote_srcfile(src_file: SrcFile) -> str:
    with src_file as src_name:
        with open(src_name) as f:
            return f.read()


def test_fully_local():
    mkdest = dest_context()

    path = "tests/test_mops/integration/pure/local_file_fully_local"
    dest_f = put_into_dest_remotely(mkdest(path))

    with open(str(dest_f)) as f:
        assert "testing123" == f.read()

    src = src_from_dest(dest_f)
    assert src
    assert "testing123" == read_from_remote_srcfile(src)

    os.remove(str(dest_f))
    # os.remove(path)


def test_via_remote():
    mkdest = dest_context()

    path = "tests/test_mops/integration/pure/local_file_via_remote.txt"
    dest_f = adls_shell(put_into_dest_remotely)(mkdest(path))

    with open(str(dest_f)) as f:
        rfp = json.loads(f.read())
        assert "adls://" in rfp["uri"]
        assert "pure/local_file_via_remote.txt" in rfp["uri"]

    src = src_from_dest(dest_f)
    assert src
    assert "testing123" == adls_shell(read_from_remote_srcfile)(src)

    os.remove(str(dest_f))


def test_first_time_upload_of_src_file():
    sd_context = src_context()
    with tempfile.NamedTemporaryFile(mode="w", prefix="created-on-orchestrator") as f:
        f.write("some random data")
        f.flush()
        f.seek(0)
        source = sd_context("first-time-upload", f.name)
        assert source
        assert "some random data" == adls_shell(read_from_remote_srcfile)(source)

    # also assert that SrcFiles can be created directly from existing ADLS paths
    ahr = resource_from_serialized(source._serialized_remote_pointer)
    assert src(ahr.fqn, ahr.md5b64)
    dir = ahr.fqn.path[: ahr.fqn.path.find(f.name)]
    print("dir", dir)


def test_context_doesnt_let_you_pick_up_nonexistent_src_files():
    with pytest.raises(FileNotFoundError):
        src_context()("wont-get-created-and-doesnt-exist", "file-that-doesnt-exist.txt")


def test_exception_if_serialized_is_invalid():
    with pytest.raises(TypeError):
        download_serialized(json.dumps(dict(type="ADLS", nope="not there")), "wonthappen.txt")  # type: ignore


def test_no_get_remote_if_not_unicode():
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(b"\xff\x00\x00\x00\x00\x00\x00\x00\x01\x01\x01\x01\x01\x01")
        tmp.seek(0)
        assert None is read_possible_serialized(tmp.name)  # type: ignore


def test_serialized_path_not_dict():
    with pytest.raises(TypeError):
        download_serialized("[1, 2, 3]", "never.txt")  # type: ignore


@adls_shell
def _remote_doesnt_write_to_dest(dst_file):
    return dst_file  # never written


def test_unused_remote_destination():
    mkdest = dest_context()
    assert not os.path.exists(str(_remote_doesnt_write_to_dest(mkdest("whatever.txt"))))


def test_src_file_is_reentrant():
    sd_context = src_context()

    src = sd_context("yoyo", Path(__file__).parent / "a_path.txt")
    with src as f1:
        with src as f2:
            assert f1 == f2


def _remote_dest_file_creator() -> DestFile:
    """Proves that even if the orchestrator doesn't provide you a DestFile,
    you can create one yourself and hand it back, if you use remote_dest.
    """
    destfile = dest(TEST_ROOT / "just/somewhere.txt")
    with destfile as path:
        with open(path, "w") as f:
            f.write("howdy")
    return destfile


def test_dest_file_created_remotely():
    dest = adls_shell(_remote_dest_file_creator)()
    assert not os.path.exists(str(dest))


def test_src_file_serialization_includes_deterministic_md5b64():
    sd_context = src_context()
    src = sd_context("includes-deterministic-md5b64", Path(__file__).parent / "a_path.txt")
    src._upload_if_not_already_remote()
    assert json.loads(src._serialized_remote_pointer)["md5b64"] == "QQgXnnVU6fpk9lmLJxqqnA=="
    assert src._uploader is None
    assert src._local_filename == ""


def test_remote_src_file_is_identical_on_serialization_to_local_src_file():
    """This test 'proves' that, whether this is a first-time upload of
    your SrcFile or a later re-use of the remote-only bytes, you will
    get an identical SrcFile payload during pickle serialization,
    which is critical for providing maximum memoization over time when
    using SrcFiles the way they're intended to be used.

    We accomplish this essentially by reducing the SrcFile internal
    attributes to nothing but those required to successfully download
    the file on the worker, as soon as the upload has been completed.
    """
    dumper = runner._get_stateful_dumper(os.environ.get("PYTEST_CURRENT_TEST"))

    srcfile = local_src(TEST_ROOT / "a_path.txt", Path(__file__).parent / "a_path.txt")
    srcfile._upload_if_not_already_remote()

    ahr = resource_from_serialized(srcfile._serialized_remote_pointer)

    rem_src = src(ahr.fqn, ahr.md5b64)
    assert rem_src._serialized_remote_pointer == srcfile._serialized_remote_pointer
    assert gimme_bytes(dumper, srcfile) == gimme_bytes(dumper, rem_src)


def test_from_serialized_sad_paths():
    with pytest.raises(ValueError):
        _srcfile_from_serialized("")  # type: ignore
    with pytest.raises(TypeError):
        _srcfile_from_serialized('{"type": "S3", "bucket": "foo", "key": "bar"}')  # type: ignore
    with pytest.raises(TypeError):
        # is missing 'key'
        _srcfile_from_serialized(Serialized('{"type": "ADLS", "sa": "thdsscratch", "container": "tmp"}'))

    with pytest.raises(errors.BlobNotFoundError):
        _srcfile_from_serialized(
            Serialized(f'{"uri": "{TEST_TMP_URI}/this-path-should-never-exist.txt"}')
        )

    sd_context = src_context()
    src = sd_context("a_path.txt", Path(__file__).parent / "a_path.txt")
    src._upload_if_not_already_remote()

    ahr = resource_from_serialized(src._serialized_remote_pointer)
    assert ahr.md5b64 != "broken"

    with pytest.raises(ValueError):
        # valueError here means we failed md5 validation
        _srcfile_from_serialized(Serialized(resource.AHR(ahr.fqn, "broken").serialized))
