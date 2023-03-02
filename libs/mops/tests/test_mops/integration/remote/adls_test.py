import pytest

from thds.mops.remote._adls import AdlsFileSystem, BlobNotFoundError


def test_get_bytes_better_blob_not_found():
    fs = AdlsFileSystem("thdsscratch", "tmp")
    with pytest.raises(BlobNotFoundError) as e:
        fs.get_bytes("path-that-should-never-ever-ever-exist.hex", "Hexdump")
        assert (
            e.message  # type: ignore
            == "Hexdump not found: thdsscratch tmp /path-that-should-never-ever-ever-exist.hex"
        )
