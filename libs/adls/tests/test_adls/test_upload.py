import io
import logging
import tempfile
import typing as ty
from datetime import datetime
from pathlib import Path
from random import randint

from thds.adls import ADLSFileSystem, hashes
from thds.adls._upload import upload_decision_and_metadata
from thds.adls.global_client import get_global_fs_client
from thds.adls.upload import upload
from thds.core import hashing

HW = Path(__file__).parent.parent / "data/hello_world.txt"


def test_basic_upload_without_cache(caplog):
    fs = ADLSFileSystem("thdsscratch", "tmp")

    remote_path = f"test/hello_world/{randint(0, 2**63-1)}.txt"
    with caplog.at_level(logging.DEBUG, logger="thds.adls._upload"):
        # synchronous file not found
        assert upload_decision_and_metadata(
            get_global_fs_client(  # type: ignore[arg-type]
                # TODO - look at above type ignore
                fs.account_name,
                fs.file_system,
            ).get_file_client(remote_path),
            HW,
        ).upload_required
        assert "Too small to bother" in caplog.text

    with caplog.at_level(logging.DEBUG, logger="thds.adls._upload"):
        # async file not found, plus upload
        fs.put_file(HW, remote_path)
        assert "failed to get length" in caplog.text
        assert "No remote properties" in caplog.text

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
        with caplog.at_level(logging.DEBUG, logger="thds.adls._upload"):
            # synchronous bytes don't match
            assert upload_decision_and_metadata(
                get_global_fs_client(fs.account_name, fs.file_system)  # type: ignore[arg-type]
                # TODO - look at above type ignore
                .get_file_client(remote_path).get_file_properties,
                fb,
                min_size_for_remote_check=0,
            ).upload_required
            assert "Remote file exists but hash does not match" in caplog.text
        with caplog.at_level(logging.DEBUG, logger="thds.adls._upload"):
            # async bytes don't match
            fs.put_file(fb, remote_path)
            assert "Remote file exists but hash does not match" in caplog.text


def test_cached_large_upload(caplog):
    fs = ADLSFileSystem("thdsdatasets", "prod-datasets")

    src_path = "test/read-only/a_small_parquet_file.parquet"
    dest_path = f"test/writable/a_small_parquet_file_{hashes.default_hasher().name.lower()}.parquet"
    with tempfile.TemporaryDirectory() as dir:
        local_path = Path(dir) / "11mb.parquet"
        fs.fetch_file(src_path, local_path)
        with caplog.at_level(logging.INFO):
            fs.put_file(local_path, dest_path)  # this should get skipped after checksum calc.
            assert "already exists and has matching checksum" in caplog.text


now = datetime.now()


class Test_upload:
    def test_upload_path(self, tmp_path, request):
        src = Path(tmp_path / "1.txt")
        src.write_text("1234")

        uploaded = upload(f"adls://thdsscratch/vtmp/adls/upload/{now}-{request.node.name}.txt", src)

        assert uploaded.hash
        assert uploaded.hash.bytes == hashing.hash_using(src, hashes.default_hasher()).digest()

    def test_upload_bytes(self, request):
        bytes_ = "1234".encode()

        uploaded = upload(f"adls://thdsscratch/vtmp/adls/upload/{now}-{request.node.name}.txt", bytes_)

        assert uploaded.hash
        assert uploaded.hash.bytes == hashing.hash_using(bytes_, hashes.default_hasher()).digest()

    def test_upload_bytes_io(self, request):
        buffer = io.BytesIO(b"1234")
        uploaded = upload(f"adls://thdsscratch/vtmp/adls/upload/{now}-{request.node.name}.txt", buffer)

        buffer.seek(0)
        expected_hash = hashing.hash_using(buffer, hashes.default_hasher()).digest()
        assert uploaded.hash and uploaded.hash.bytes == expected_hash

    def test_upload_iter_bytes(self, request):
        def _yield_bytes() -> ty.Iterator[bytes]:
            for i in range(1, 5):
                yield str(i).encode()

        uploaded = upload(
            f"adls://thdsscratch/vtmp/adls/upload/{now}-{request.node.name}.txt", _yield_bytes()
        )

        assert uploaded.hash is None
        # we can't take a hash of an Iterator, because it will consume it pre-upload

        assert uploaded.path().read_bytes() == b"".join(_yield_bytes())
