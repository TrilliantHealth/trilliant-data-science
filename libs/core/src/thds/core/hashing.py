"""
https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
I have written this code too many times to write it again. Why isn't this in the stdlib?
"""

import base64
import contextlib
import hashlib
import io
import os
import typing as ty
from pathlib import Path

from .types import StrOrPath

_CHUNK_SIZE = int(os.getenv("THDS_CORE_HASHING_CHUNK_SIZE", 2**18))
# https://stackoverflow.com/questions/17731660/hashlib-optimal-size-of-chunks-to-be-used-in-md5-update
# i've done some additional benchmarking, and slightly larger chunks (256 KB) are faster
# when the files are larger, and those are the ones we care about most since they take the longest.


class Hasher(ty.Protocol):
    """This may be incomplete as far as hashlib is concerned, but it covers everything we use."""

    @property
    def name(self) -> str:
        """The name of the hashing algorithm, e.g. 'sha256'."""
        ...

    def update(self, __byteslike: ty.Union[bytes, bytearray, memoryview]) -> None:
        """Update the hash object with the bytes-like object."""
        ...

    def digest(self) -> bytes:
        ...


H = ty.TypeVar("H", bound=Hasher)
SomehowReadable = ty.Union[ty.AnyStr, ty.IO[ty.AnyStr], Path]


def hash_readable_chunks(bytes_readable: ty.IO[bytes], hasher: H) -> H:
    """Return thing you can call .digest or .hexdigest on.

    E.g.:

    hash_readable_chunks(open(Path('foo/bar'), 'rb'), hashlib.sha256()).hexdigest()
    """
    for chunk in iter(lambda: bytes_readable.read(_CHUNK_SIZE), b""):
        hasher.update(chunk)  # type: ignore
    return hasher


@contextlib.contextmanager
def attempt_readable(thing: SomehowReadable) -> ty.Iterator[ty.IO[bytes]]:
    """Best effort: make this object a bytes-readable."""
    if hasattr(thing, "read") and hasattr(thing, "seek"):
        try:
            yield thing  # type: ignore
            return
        finally:
            thing.seek(0)  # type: ignore
    elif isinstance(thing, bytes):
        yield io.BytesIO(thing)
        return
    with open(thing, "rb") as readable:  # type: ignore
        yield readable


def hash_using(data: SomehowReadable, hasher: H) -> H:
    """This is quite dynamic - but if your data object is not readable
    bytes and is not openable as bytes, you'll get a
    FileNotFoundError, or possibly a TypeError or other gremlin.

    Therefore, you may pass whatever you want unless it's an actual
    string - if you want your actual string hashed, you should encode
    it as actual bytes first.
    """
    with attempt_readable(data) as readable:
        return hash_readable_chunks(readable, hasher)


def hash_anything(data: SomehowReadable, hasher: H) -> ty.Optional[H]:
    try:
        return hash_using(data, hasher)
    except (FileNotFoundError, TypeError):
        # it's unlikely we can operate on this data?
        return None


def b64(digest: ty.ByteString) -> str:
    """The string representation commonly used by Azure utilities.

    We use it in cases where we want to represent the same hash that
    ADLS will have in UTF-8 string (instead of bytes) format.
    """
    return base64.b64encode(digest).decode()


def db64(s: str) -> bytes:
    """Shorthand for the inverse of b64."""
    return base64.b64decode(s)


def _repr_bytes(bs: ty.ByteString) -> str:
    return f"db64('{b64(bs)}')"


class Hash(ty.NamedTuple):
    """Algorithm name needs to match something supported by hashlib.

    A good choice would be sha256. Use md5 if you have to.
    """

    algo: str
    # valid algorithm names listed here: https://docs.python.org/3/library/hashlib.html#constructors
    bytes: bytes

    def __repr__(self) -> str:
        return f"Hash(algo='{self.algo}', bytes={_repr_bytes(self.bytes)})"


_NAMED_HASH_CONSTRUCTORS: ty.Dict[str, ty.Callable[[str], Hasher]] = {}


def add_named_hash(algo: str, constructor: ty.Callable[[str], Hasher]) -> None:
    _NAMED_HASH_CONSTRUCTORS[algo] = constructor


def get_hasher(algo: str) -> Hasher:
    if algo in _NAMED_HASH_CONSTRUCTORS:
        return _NAMED_HASH_CONSTRUCTORS[algo](algo)

    return hashlib.new(algo)


def file(algo: str, pathlike: StrOrPath) -> bytes:
    """I'm so lazy"""
    return hash_using(pathlike, get_hasher(algo)).digest()
