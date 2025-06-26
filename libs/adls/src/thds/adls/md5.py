import hashlib

from thds.core.hashing import hash_using


def hex_md5_str(string: str) -> str:
    return hash_using(string.encode(), hashlib.md5()).hexdigest()
