"""We're specifically testing that this library selects the 'correct'
thing to hash when passes the very broad types that it accepts.
"""
import hashlib
from pathlib import Path

from thds.core.hashing import hash_using

HW = Path(__file__).parent.parent / "data/hello_world.txt"
HW_SHA256 = "3ad08644ab5aa81626a4c41c253a82da9a7cd9843f74486b8b492dff8e8ab690"


def test_file_hashes():
    assert HW_SHA256 == hash_using(HW, hashlib.sha256()).hexdigest()


def test_bytes_hash():
    with open(HW, "rb") as f:
        assert HW_SHA256 == hash_using(f.read(), hashlib.sha256()).hexdigest()


def test_readable_hashes():
    with open(HW, "rb") as f:
        assert HW_SHA256 == hash_using(f, hashlib.sha256()).hexdigest()
