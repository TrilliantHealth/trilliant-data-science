"""We're specifically testing that this library selects the 'correct'
thing to hash when passes the very broad types that it accepts.
"""

import hashlib
from pathlib import Path

from thds.core.hashing import hash_anything, hash_using

HW = Path(__file__).parent.parent / "data/hello_world.txt"
HW_SHA256 = (
    "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447"  # includes trailing newline
)


def test_file_hashes():
    assert HW_SHA256 == hash_using(HW, hashlib.sha256()).hexdigest()


def test_bytes_hash():
    with open(HW, "rb") as f:
        assert HW_SHA256 == hash_using(f.read(), hashlib.sha256()).hexdigest()


def test_readable_hashes():
    with open(HW, "rb") as f:
        assert HW_SHA256 == hash_using(f, hashlib.sha256()).hexdigest()


def test_hash_anything_doesnt_die_on_typerror():
    assert None is hash_anything((b"23423423423423423423",), hashlib.md5())


def test_hash_anything_doesnt_die_on_fnf_error():
    assert None is hash_anything("file/not/exists", hashlib.md5())
