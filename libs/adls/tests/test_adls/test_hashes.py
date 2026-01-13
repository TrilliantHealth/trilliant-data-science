import hashlib
import os
import tempfile
import typing as ty

from attrs import define

from thds.adls import hashes
from thds.core.hashing import Hash


@define
class FakeContentSettings:
    content_md5: ty.Optional[bytearray]


@define
class FakeFileProperties:
    name: str
    metadata: dict
    content_settings: FakeContentSettings
    etag: str = ""


def test_basic_preferred_hash():
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        # make a biggish temp file
        temp_file.write(os.urandom(1024 * 1024 * 10))
        temp_file_path = temp_file.name
        # Calculate the hash using the preferred algorithm
        assert hashes.hash_cache.filehash(hashes.PREFERRED_ALGOS[0], temp_file_path) is not None


def test_extract_hashes_from_props_with_md5_only():
    m = hashlib.md5()
    m.update("abc".encode("utf-8"))
    md5_bytes = m.digest()

    props = FakeFileProperties(
        "a_file",
        metadata=dict(),
        content_settings=FakeContentSettings(content_md5=bytearray(md5_bytes)),
    )
    assert hashes.extract_hashes_from_props(props) == {
        "md5": Hash("md5", md5_bytes),
    }
