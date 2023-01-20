"""Why MD5 when it's no longer a good choice for most use cases?
Because Azure/ADLS support Content-MD5 but nothing else, and I don't
want to lie to them and get us confused later.

Thankfully, there are no real security concerns for us with purely
internal code and data sets.
"""
import hashlib
import io
import threading
from typing import Optional

# https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
# I have written this code too many times to write it again. Why isn't this in the stdlib?

_MD5_SEMAPHORE = threading.BoundedSemaphore(10)
# Python threads don't allow for significant CPU parallelism, so
# allowing for more than a few of these per process is a recipe for
# getting nothing done.


def md5_readable_bytes(bytes_readable) -> bytes:
    """Calculate MD5 of a file efficiently with respect to memory."""
    with _MD5_SEMAPHORE:
        hash_md5 = hashlib.md5()
        for chunk in iter(lambda: bytes_readable.read(4096), b""):
            hash_md5.update(chunk)
        return hash_md5.digest()


def md5_something(data) -> Optional[bytes]:
    """Ideally, we calculate an MD5 sum for all data that we upload.

    The only circumstances under which we cannot do this are if the
    stream does not exist in its entirety before the upload begins.
    """
    if hasattr(data, "read") and hasattr(data, "seek"):
        try:
            return md5_readable_bytes(data)
        finally:
            data.seek(0)
    elif isinstance(data, bytes):
        return md5_readable_bytes(io.BytesIO(data))
    try:
        with open(data, "rb") as readable:
            return md5_readable_bytes(readable)
    except FileNotFoundError:
        # it's unlikely we can operate on this data?
        return None
