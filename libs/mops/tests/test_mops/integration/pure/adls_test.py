import pytest

from thds.adls.errors import BlobNotFoundError
from thds.mops.pure.pickling._pickle import get_bytes

from ...config import TEST_TMP_URI


def test_get_bytes_better_blob_not_found():
    with pytest.raises(
        BlobNotFoundError,
        match=f"Blob not found: {TEST_TMP_URI}path-that-should-never-ever-ever-exist.hex",
    ):
        get_bytes(
            f"{TEST_TMP_URI}path-that-should-never-ever-ever-exist.hex",
            type_hint="Hexdump",
        )
