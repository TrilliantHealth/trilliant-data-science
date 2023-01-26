import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from thds.mops.config import adls_remote_tmp_container, adls_remote_tmp_sa
from thds.mops.remote import AdlsDatasetContext, DestFile, SrcFile
from thds.mops.remote.adls_remote_files import (
    AdlsDirectory,
    _download_serialized,
    _get_remote_serialized,
    adls_dataset_context,
    adls_remote_src,
)

from ._util import adls_shell

TEST_TIME = datetime.utcnow().isoformat()
# at least two of these times will be seen each time the tests are
# run.  one for the local-running process, and then others for the
# remote-running processes that directly access TEST_PREFIX instead of
# it being provided to them.
TOP_DIR = "test-adls-src-dest-files/"
TEST_PREFIX = TOP_DIR + TEST_TIME


def _context():
    return AdlsDatasetContext(
        AdlsDirectory(adls_remote_tmp_sa(), adls_remote_tmp_container(), TEST_PREFIX),
        "tests/test_mops/integration",  # ignore this part of the local file path
    )


def put_into_dest_remotely(dest_file: DestFile) -> DestFile:
    with dest_file as dfname:
        with open(dfname, "w") as f:
            f.write("testing123")
    return dest_file


def read_from_remote_srcfile(src_file: SrcFile) -> str:
    with src_file as src_name:
        with open(src_name) as f:
            return f.read()


def test_fully_local():
    sd_context = _context()

    path = "tests/test_mops/integration/remote/local_file_fully_local"
    dest_f = put_into_dest_remotely(sd_context.dest(path))

    with open(str(dest_f)) as f:
        assert "testing123" == f.read()

    src = sd_context.src(dest_f)
    assert src
    assert "testing123" == read_from_remote_srcfile(src)

    os.remove(str(dest_f))


def test_via_remote():
    sd_context = _context()

    path = "tests/test_mops/integration/remote/local_file_via_remote.txt"

    dest_f = adls_shell(put_into_dest_remotely)(sd_context.dest(path))

    with open(str(dest_f)) as f:
        rfp = json.loads(f.read())
        assert rfp["type"] == "ADLS"
        assert "tests/test_mops/integration" not in rfp["key"]
        assert "remote/local_file_via_remote.txt" in rfp["key"]

    src = sd_context.src(str(dest_f))
    assert src
    assert "testing123" == adls_shell(read_from_remote_srcfile)(src)

    os.remove(str(dest_f))


def test_first_time_upload_of_src_file():
    sd_context = _context()
    with tempfile.NamedTemporaryFile(mode="w", prefix="created-on-orchestrator") as f:
        f.write("some random data")
        f.flush()
        f.seek(0)
        source = sd_context.src(f.name)
        assert source
        assert "some random data" == adls_shell(read_from_remote_srcfile)(source)

    # also assert that SrcFiles can be created directly from existing ADLS paths
    src_p = json.loads(source._serialized_remote_pointer)
    assert adls_remote_src(src_p["sa"], src_p["container"], src_p["key"])
    dir = src_p["key"][: src_p["key"].find(f.name)]
    print("dir", dir)
    adls_dataset_context(dir).remote_src(f.name)


def test_context_doesnt_let_you_pick_up_nonexistent_src_files():
    with pytest.raises(FileNotFoundError):
        _context().src("file-that-doesnt-exist.txt")


def test_exception_if_serialized_is_invalid():
    with pytest.raises(ValueError):
        _download_serialized(json.dumps(dict(type="ADLS", nope="not there")), "wonthappen.txt")  # type: ignore


def test_no_get_remote_if_not_unicode():
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(b"\xff\x00\x00\x00\x00\x00\x00\x00\x01\x01\x01\x01\x01\x01")
        tmp.seek(0)
        assert "" == _get_remote_serialized(tmp.name)  # type: ignore


def test_serialized_path_not_dict():
    with pytest.raises(ValueError):
        _download_serialized("[1, 2, 3]", "never.txt")  # type: ignore


@adls_shell
def _remote_doesnt_write_to_dest(dst_file):
    return dst_file  # never written


def test_unused_remote_destination():
    sd_context = _context()

    assert not os.path.exists(str(_remote_doesnt_write_to_dest(sd_context.dest("whatever.txt"))))


def test_src_file_is_reentrant():
    sd_context = _context()

    src = sd_context.src(Path(__file__).parent / "a_path.txt")
    with src as f1:
        with src as f2:
            assert f1 == f2


def remote_dest_file_creator() -> DestFile:
    """Proves that even if the orchestrator doesn't provide you a DestFile,
    you can create one yourself and hand it back, if you use remote_dest.
    """
    sd_context = _context()
    dest = sd_context.remote_dest("just/somewhere.txt")
    with dest as path:
        with open(path, "w") as f:
            f.write("howdy")
    return dest


def test_dest_file_created_remotely():
    try:
        dest = adls_shell(remote_dest_file_creator)()
        assert os.path.exists(str(dest))
    finally:
        shutil.rmtree(TOP_DIR)
