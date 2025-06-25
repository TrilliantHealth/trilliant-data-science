import typing as ty
from pathlib import Path

from blake3 import blake3

from thds.core import hash_cache
from thds.core.hashing import SomehowReadable, hash_anything
from thds.core.types import StrOrPath


def blake3_file(file: StrOrPath) -> bytes:
    """Calculate the BLAKE3 hash of a file."""
    path = Path(file).resolve()
    size_bytes = path.stat().st_size

    if size_bytes > 10 * 2**20:  # 10 MB
        hasher = blake3(max_threads=blake3.AUTO)
    else:  # i just don't think we want thread pools for tiny files
        hasher = blake3()

    return hash_cache.hash_file(path, hasher)  # type: ignore[arg-type]


AnyStrSrc = ty.Union[SomehowReadable, ty.Iterable[ty.AnyStr]]
# this type closely corresponds to what the underlying DataLakeStorageClient will accept for upload_data.


def try_blake3(data: AnyStrSrc) -> ty.Optional[bytes]:
    if isinstance(data, Path):
        return blake3_file(data)

    res = hash_anything(data, blake3(max_threads=blake3.AUTO))  # type: ignore[type-var]
    if res:
        return res.digest()
    return None
