"""
https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
I have written this code too many times to write it again. Why isn't this in the stdlib?
"""
import base64
import contextlib
import io
import os
import threading
import typing as ty
from pathlib import Path

# Python threads don't allow for significant CPU parallelism, so
# allowing for more than a few of these per process is a recipe for
# getting nothing done.
_SEMAPHORE = threading.BoundedSemaphore(int(os.getenv("THDS_CORE_HASHING_PARALLELISM", 4)))
_CHUNK_SIZE = int(os.getenv("THDS_CORE_HASHING_CHUNK_SIZE", 65536))
# https://stackoverflow.com/questions/17731660/hashlib-optimal-size-of-chunks-to-be-used-in-md5-update
# this may not apply to us as the architecture is 32 bit, but it's at
# least a halfway decent guess and benchmarking this ourselves would
# be a massive waste of time.

T = ty.TypeVar("T")
SomehowReadable = ty.Union[ty.AnyStr, ty.IO[ty.AnyStr], Path]


def hash_readable_chunks(bytes_readable: ty.IO[bytes], hasher: T) -> T:
    """Return thing you can call .digest or .hexdigest on.

    E.g.:

    hash_readable_chunks(open(Path('foo/bar'), 'rb'), hashlib.sha256()).hexdigest()
    """
    with _SEMAPHORE:
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


def hash_using(data: SomehowReadable, hasher: T) -> T:
    """This is quite dynamic - but if your data object is not readable
    bytes and is not openable as bytes, you'll get a
    FileNotFoundError, or possibly a TypeError or other gremlin.

    Therefore, you may pass whatever you want unless it's an actual
    string - if you want your actual string hashed, you should encode
    it as actual bytes first.
    """
    with attempt_readable(data) as readable:
        return hash_readable_chunks(readable, hasher)


def hash_anything(data: SomehowReadable, hasher: T) -> ty.Optional[T]:
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
