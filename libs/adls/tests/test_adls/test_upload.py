import logging
import tempfile
from pathlib import Path
from random import randint

from thds.adls import ADLSFileSystem
from thds.adls._upload import upload_decision_and_settings
from thds.adls.global_client import get_global_client

HW = Path(__file__).parent.parent / "data/hello_world.txt"


def test_basic_upload_without_cache(caplog):
    fs = ADLSFileSystem("thdsscratch", "tmp")

    remote_path = f"test/hello_world/{randint(0, 2**63-1)}.txt"
    with caplog.at_level(logging.DEBUG):
        # synchronous file not found
        assert upload_decision_and_settings(
            get_global_client(fs.account_name, fs.file_system).get_file_client(remote_path),
            HW,
        ).upload_required
        assert "Too small to bother" in caplog.text

    with caplog.at_level(logging.DEBUG):
        # async file not found, plus upload
        fs.put_file(HW, remote_path)
        assert "failed to get length" in caplog.text
        assert "No remote file properties" in caplog.text

    # now assert that upload uploaded the correct bytes
    with tempfile.TemporaryDirectory() as dir:
        hw = Path(dir) / "hw.txt"
        fs.fetch_file(remote_path, hw)
        with open(hw) as f:
            assert f.read() == open(HW).read()

    # same filename, different bytes
    with tempfile.TemporaryDirectory() as dir:
        fb = Path(dir) / "foobar.txt"
        with open(fb, "wb") as pw:
            pw.write(b"some other data")
        with caplog.at_level(logging.DEBUG):
            # synchronous bytes don't match
            assert upload_decision_and_settings(
                get_global_client(fs.account_name, fs.file_system).get_file_client(remote_path),
                fb,
                min_size_for_remote_check=0,
            ).upload_required
            assert "Remote file exists but MD5 does not match" in caplog.text
        with caplog.at_level(logging.DEBUG):
            # async bytes don't match
            fs.put_file(fb, remote_path)
            assert "Remote file exists but MD5 does not match" in caplog.text


def test_cached_large_upload(caplog):
    fs = ADLSFileSystem("thdsdatasets", "prod-datasets")

    remote_path = "test/writable/a_small_parquet_file.parquet"
    with tempfile.TemporaryDirectory() as dir:
        local_path = Path(dir) / "11mb.parquet"
        fs.fetch_file(remote_path, local_path)
        with caplog.at_level(logging.INFO):
            fs.put_file(local_path, remote_path)  # this should get skipped after md5 calc.
            assert "already exists and has matching checksum" in caplog.text
