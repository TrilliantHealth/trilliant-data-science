"""Why MD5 when it's no longer a good choice for most use cases?
Because Azure/ADLS support Content-MD5 but nothing else, and I don't
want to lie to them and get us confused later.

Thankfully, there are no real security concerns for us with purely
internal code and data sets.

That said, please _do not_ use MD5 for non-Azure things. Prefer SHA256
if at all possible.
"""
import hashlib
import typing as ty
from pathlib import Path

from thds.core.hash_cache import hash_file
from thds.core.hashing import SomehowReadable, hash_anything, hash_using
from thds.core.types import StrOrPath

AnyStrSrc = ty.Union[SomehowReadable, ty.Iterable[ty.AnyStr]]
# this type closely corresponds to what the underlying DataLakeStorageClient will accept for upload_data.


def md5_file(file: StrOrPath) -> bytes:
    """Raise exception if it cannot be read.

    Safely caches the hash on your local filesystem (uses mtimes to
    determine staleness).
    """
    return hash_file(file, hashlib.md5())


def hex_md5_str(string: str) -> str:
    return hash_using(string.encode(), hashlib.md5()).hexdigest()


def try_md5(data: AnyStrSrc) -> ty.Optional[bytes]:
    """Ideally, we calculate an MD5 sum for all data that we upload.

    The only circumstances under which we cannot do this are if the
    stream does not exist in its entirety before the upload begins.
    """
    if isinstance(data, Path):
        return md5_file(data)
    res = hash_anything(data, hashlib.md5())
    if res:
        return res.digest()
    return None


def is_reasonable_b64(md5: str):
    if len(md5) == 22:
        return True
    if len(md5) == 24 and md5.endswith("=="):
        return True
    return False


def check_reasonable_md5b64(maybe_md5: str):
    if not is_reasonable_b64(maybe_md5):
        raise ValueError(f"MD5 '{maybe_md5}' is not a reasonable MD5.")
