import hashlib

from thds.core.hashing import Hash, db64, hash_using


def hex_md5_str(string: str) -> str:
    return hash_using(string.encode(), hashlib.md5()).hexdigest()


def to_hash(md5b64: str) -> Hash:
    """Convert a base64-encoded MD5 hash to a hex string."""
    assert md5b64, "MD5 base64 string cannot be empty"
    return Hash(algo="md5", bytes=db64(md5b64))
