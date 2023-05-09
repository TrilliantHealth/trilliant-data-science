"""
https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
I have written this code too many times to write it again. Why isn't this in the stdlib?
"""
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
_CHUNK_SIZE = int(os.getenv("THDS_CORE_HASHING_CHUNK_SIZE", 4096))

T = ty.TypeVar("T")
SomehowReadable = ty.Union[ty.AnyStr, ty.IO[ty.AnyStr], Path]


def hash_readable_chunks(bytes_readable: ty.IO[bytes], hasher: T) -> T:
    """Return thing you can call .digest or .hexdigest on.

    E.g.:

    hash_readable_chunks(hashlib.sha256(), open(Path('foo/bar'), 'rb')).hexdigest()
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
        finally:
            thing.seek(0)  # type: ignore
    elif isinstance(thing, bytes):
        yield io.BytesIO(thing)
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
    except FileNotFoundError:
        # it's unlikely we can operate on this data?
        return None
