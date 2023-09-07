import pytest

from thds.adls.errors import BlobNotFoundError
from thds.mops.pure.pickling._pickle import get_bytes


def test_get_bytes_better_blob_not_found():
    with pytest.raises(
        BlobNotFoundError,
        match="Blob not found: adls://thdsscratch/tmp/path-that-should-never-ever-ever-exist.hex",
    ):
        get_bytes(
            "adls://thdsscratch/tmp/path-that-should-never-ever-ever-exist.hex",
            type_hint="Hexdump",
        )
