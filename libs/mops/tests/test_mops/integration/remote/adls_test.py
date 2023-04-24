import pytest

from thds.mops.remote._adls import AdlsFileSystem, BlobNotFoundError
from thds.mops.remote._content_aware_uri_serde import _get_bytes


def test_get_bytes_better_blob_not_found():
    with pytest.raises(
        BlobNotFoundError,
        match="Hexdump not found: adls://thdsscratch/tmp/path-that-should-never-ever-ever-exist.hex",
    ):
        _get_bytes(
            AdlsFileSystem(),
            "adls://thdsscratch/tmp/path-that-should-never-ever-ever-exist.hex",
            type_hint="Hexdump",
        )
