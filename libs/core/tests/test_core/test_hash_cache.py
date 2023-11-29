import hashlib
from pathlib import Path

import pytest

from thds.core import hash_cache, hashing

TEST_DIR = Path(__file__).parent


def test_file_hashes_correctly_and_consistently():
    assert hashing.hash_using(
        TEST_DIR / "test_hash_cache.py", hashlib.sha256()
    ).digest() == hash_cache.hash_file(TEST_DIR / "test_hash_cache.py", hashlib.sha256())
    assert hashing.hash_using(
        TEST_DIR / "test_hash_cache.py", hashlib.md5()
    ).digest() == hash_cache.hash_file(TEST_DIR / "test_hash_cache.py", hashlib.md5())


def test_directory_fails_to_hash():
    with pytest.raises(IsADirectoryError):
        hash_cache.hash_file(TEST_DIR, hashlib.sha256())
