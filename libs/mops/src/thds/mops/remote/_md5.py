"""Why MD5 when it's no longer a good choice for most use cases?
Because Azure/ADLS support Content-MD5 but nothing else, and I don't
want to lie to them and get us confused later.

Thankfully, there are no real security concerns for us with purely
internal code and data sets.
"""
import hashlib
from typing import Optional

from ._hash import hash_anything, hash_using


def try_md5(data) -> Optional[bytes]:
    """Ideally, we calculate an MD5 sum for all data that we upload.

    The only circumstances under which we cannot do this are if the
    stream does not exist in its entirety before the upload begins.
    """
    res = hash_anything(data, hashlib.md5())
    if res:
        return res.digest()
    return None


def md5_readable(data) -> bytes:
    """Raise exception if it cannot be read."""
    return hash_using(data, hashlib.md5()).digest()
