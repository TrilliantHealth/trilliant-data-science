import typing as ty
from pathlib import Path

from blake3 import blake3

from thds.core import hash_cache, hashing, source
from thds.core.hashing import SomehowReadable, hash_anything
from thds.core.types import StrOrPath


def _simple_blake3(algo: str, file: StrOrPath) -> hashing.Hasher:
    assert algo == "blake3", "simple_blake3 only supports the 'blake3' algorithm"
    path = Path(file).resolve()
    size_bytes = path.stat().st_size

    if size_bytes > 10 * 2**20:  # 10 MB
        hasher = blake3(max_threads=blake3.AUTO)
    else:  # i just don't think we want thread pools for tiny files
        hasher = blake3()
    return hasher  # type: ignore[return-value]


def register_blake3():
    # this makes all Source objects created from files in an application where they have imported thds.adls
    # use blake3 hashing by default, so that we only compute a single hash for every uploaded file.
    source.set_file_autohash("blake3", _simple_blake3)


def blake3_file(file: StrOrPath) -> hashing.Hash:
    """Calculate the BLAKE3 hash of a file."""
    # we pass this off to hash_file _so that_ the hash will be cached.
    register_blake3()

    return hash_cache.filehash("blake3", file)


AnyStrSrc = ty.Union[SomehowReadable, ty.Iterable[ty.AnyStr]]
# this type closely corresponds to what the underlying DataLakeStorageClient will accept for upload_data.


def try_blake3(data: AnyStrSrc) -> ty.Optional[bytes]:
    if isinstance(data, Path):
        return blake3_file(data).bytes

    res = hash_anything(data, blake3(max_threads=blake3.AUTO))  # type: ignore[type-var]
    if res:
        return res.digest()
    return None
