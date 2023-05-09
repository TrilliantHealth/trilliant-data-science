import io
import threading
import typing as ty

# https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
# I have written this code too many times to write it again. Why isn't this in the stdlib?

# Python threads don't allow for significant CPU parallelism, so
# allowing for more than a few of these per process is a recipe for
# getting nothing done.
_SEMAPHORE = threading.BoundedSemaphore(4)

T = ty.TypeVar("T")
AnyStrSrc = ty.Union[ty.AnyStr, ty.Iterable[ty.AnyStr], ty.IO[ty.AnyStr]]


def hash_readable_chunks(bytes_readable: ty.IO[bytes], hasher: T) -> T:
    """Return thing you can call .digest or .hexdigest on.

    E.g.:

    hash_readable_chunks(hashlib.sha256(), open(Path('foo/bar'), 'rb')).hexdigest()
    """
    with _SEMAPHORE:
        for chunk in iter(lambda: bytes_readable.read(4096), b""):
            hasher.update(chunk)  # type: ignore
        return hasher


def hash_using(data: AnyStrSrc, hasher: T) -> T:
    if hasattr(data, "read") and hasattr(data, "seek"):
        try:
            return hash_readable_chunks(data, hasher)  # type: ignore
        finally:
            data.seek(0)  # type: ignore
    elif isinstance(data, bytes):
        return hash_readable_chunks(io.BytesIO(data), hasher)
    with open(data, "rb") as readable:  # type: ignore
        return hash_readable_chunks(readable, hasher)


def hash_anything(data: AnyStrSrc, hasher: T) -> ty.Optional[T]:
    try:
        return hash_using(data, hasher)
    except FileNotFoundError:
        # it's unlikely we can operate on this data?
        return None
