import hashlib
import typing as ty
from pathlib import Path

from attrs import define

from thds.adls import hashes
from thds.adls._etag import (
    ETAG_FAKE_HASH_NAME,
    add_to_etag_cache,
    extract_etag_bytes,
    hash_file_fake_etag,
)
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


# --- Unit tests for _etag module ---


def test_extract_etag_bytes_standard_format():
    """ADLS etags are quoted hex strings like '"0x8DE4EC15EF69095"'"""
    etag = '"0x8DE4EC15EF69095"'
    result = extract_etag_bytes(etag)
    # The hex value 0x8DE4EC15EF69095 should be converted to bytes
    assert isinstance(result, bytes)
    assert len(result) > 0
    # Round-trip check: the hex representation should match
    assert result == int("0x8DE4EC15EF69095", 16).to_bytes(len(result), byteorder="big")


def test_extract_etag_bytes_various_lengths():
    """Test etag extraction with various lengths.

    The byte length is derived from the STRIPPED string length,
    so both quoted and unquoted etags produce the same byte representation.
    """
    test_cases = [
        # (etag_str, expected_bytes) - length = (len(stripped) - 2 + 1) // 2
        # where stripped = etag_str.strip('"')
        ('"0x1"', bytes.fromhex("01")),  # stripped='0x1', len=3, (3-2+1)//2 = 1 byte
        ("0x1", bytes.fromhex("01")),  # same value, no quotes, same result
        ('"0xFF"', bytes.fromhex("FF")),  # stripped='0xFF', len=4, (4-2+1)//2 = 1 byte
        ("0xFF", bytes.fromhex("FF")),  # same value, no quotes, same result
        ('"0x1234"', bytes.fromhex("1234")),  # stripped='0x1234', len=6, (6-2+1)//2 = 2 bytes
        ("0x1234", bytes.fromhex("1234")),  # same value, no quotes, same result
        ('"0xABCDEF"', bytes.fromhex("ABCDEF")),  # stripped='0xABCDEF', len=8, (8-2+1)//2 = 3 bytes
        ("0xABCDEF", bytes.fromhex("ABCDEF")),  # same value, no quotes, same result
    ]
    for etag_str, expected in test_cases:
        result = extract_etag_bytes(etag_str)
        assert (
            result == expected
        ), f"Failed for {etag_str}: got {result.hex()}, expected {expected.hex()}"


def test_etag_cache_round_trip(tmp_path: Path):
    """Test that add_to_etag_cache and hash_file_fake_etag work together"""
    # Create a test file
    test_file = tmp_path / "test_file.txt"
    test_file.write_bytes(b"hello world for etag test")

    # Simulate what happens during download: we have a remote etag
    fake_etag_bytes = bytes.fromhex("DEADBEEF1234")

    # Add to cache (this is what happens after download)
    result_hash = add_to_etag_cache(test_file, fake_etag_bytes)
    assert result_hash.algo == ETAG_FAKE_HASH_NAME
    assert result_hash.bytes == fake_etag_bytes

    # Now verify we can retrieve it (this is what happens on cache hit check)
    retrieved_hash = hash_file_fake_etag(test_file)
    assert retrieved_hash is not None
    assert retrieved_hash.algo == ETAG_FAKE_HASH_NAME
    assert retrieved_hash.bytes == fake_etag_bytes


def test_etag_cache_returns_none_for_unknown_file(tmp_path: Path):
    """Test that hash_file_fake_etag returns None for files not in cache"""
    unknown_file = tmp_path / "unknown.txt"
    unknown_file.write_bytes(b"this file was never cached")

    result = hash_file_fake_etag(unknown_file)
    assert result is None


def test_etag_cache_returns_none_for_nonexistent_file(tmp_path: Path):
    """Test that hash_file_fake_etag returns None for files that don't exist"""
    nonexistent = tmp_path / "does_not_exist.txt"
    result = hash_file_fake_etag(nonexistent)
    assert result is None


def test_etag_cache_content_sensitive(tmp_path: Path):
    """Test that different file contents get different cache entries"""
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    file1.write_bytes(b"content one")
    file2.write_bytes(b"content two")

    etag1 = bytes.fromhex("AAAA")
    etag2 = bytes.fromhex("BBBB")

    add_to_etag_cache(file1, etag1)
    add_to_etag_cache(file2, etag2)

    # Each file should get its own cached etag
    assert hash_file_fake_etag(file1).bytes == etag1  # type: ignore
    assert hash_file_fake_etag(file2).bytes == etag2  # type: ignore


# --- Tests for extract_hashes_from_props with etag ---


def test_extract_hashes_from_props_with_etag_only():
    """When no MD5 or metadata hashes, etag should be used"""
    props = FakeFileProperties(
        "a_file",
        metadata=dict(),
        content_settings=FakeContentSettings(content_md5=None),
        etag='"0x8DE4EC15EF69095"',
    )
    result = hashes.extract_hashes_from_props(props)

    # Should have exactly one hash: the etag-based fake hash
    assert len(result) == 1
    assert ETAG_FAKE_HASH_NAME in result
    etag_hash = result[ETAG_FAKE_HASH_NAME]
    assert etag_hash.algo == ETAG_FAKE_HASH_NAME
    assert etag_hash.bytes == extract_etag_bytes('"0x8DE4EC15EF69095"')


def test_extract_hashes_from_props_etag_is_last_resort():
    """When MD5 is present, it should be preferred over etag"""
    m = hashlib.md5()
    m.update(b"test")
    md5_bytes = m.digest()

    props = FakeFileProperties(
        "a_file",
        metadata=dict(),
        content_settings=FakeContentSettings(content_md5=bytearray(md5_bytes)),
        etag='"0x8DE4EC15EF69095"',
    )
    result = hashes.extract_hashes_from_props(props)

    # Should have both MD5 and etag, but MD5 comes first (is preferred)
    assert "md5" in result
    assert ETAG_FAKE_HASH_NAME in result
    # The dict maintains insertion order, so first key should be md5
    first_key = next(iter(result))
    assert first_key == "md5"


def test_extract_hashes_from_props_metadata_preferred_over_etag():
    """When metadata hash (xxh3) is present, it should be preferred over etag"""
    from thds.core import hashing

    xxh3_hash = hashing.b64(b"some_xxh3_hash_bytes")

    props = FakeFileProperties(
        "a_file",
        metadata={"hash_xxh3_128_b64": xxh3_hash},
        content_settings=FakeContentSettings(content_md5=None),
        etag='"0x8DE4EC15EF69095"',
    )
    result = hashes.extract_hashes_from_props(props)

    # Should have both xxh3 and etag, but xxh3 comes first (is preferred)
    assert "xxh3_128" in result
    assert ETAG_FAKE_HASH_NAME in result
    first_key = next(iter(result))
    assert first_key == "xxh3_128"


def test_create_hash_metadata_if_missing_skips_etag():
    """Etag-based hashes should never be written to metadata"""
    from azure.storage.filedatalake import FileProperties

    # Create a mock file properties
    props = FileProperties()
    props["name"] = "test"
    props["metadata"] = {}

    etag_hash = Hash(ETAG_FAKE_HASH_NAME, bytes.fromhex("DEADBEEF"))

    result = hashes.create_hash_metadata_if_missing(props, etag_hash)
    # Should return empty dict, refusing to write etag to metadata
    assert result == {}
