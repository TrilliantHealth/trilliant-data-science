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


def hash_readable_chunks(bytes_readable: ty.IO[bytes], hasher: T) -> T:
    """Return thing you can call .digest or .hexdigest on.

    E.g.:

    hash_readable_chunks(hashlib.sha256(), open(Path('foo/bar'), 'rb')).hexdigest()
    """
    with _SEMAPHORE:
        for chunk in iter(lambda: bytes_readable.read(4096), b""):
            hasher.update(chunk)  # type: ignore
        return hasher


def hash_using(data, hasher: T) -> T:
    if hasattr(data, "read") and hasattr(data, "seek"):
        try:
            return hash_readable_chunks(data, hasher)
        finally:
            data.seek(0)
    elif isinstance(data, bytes):
        return hash_readable_chunks(io.BytesIO(data), hasher)
    with open(data, "rb") as readable:
        return hash_readable_chunks(readable, hasher)


def hash_anything(data, hasher: T) -> ty.Optional[T]:
    try:
        return hash_using(data, hasher)
    except FileNotFoundError:
        # it's unlikely we can operate on this data?
        return None


def nest(hashstr: str, split: int = 1) -> str:
    """A common pattern for formatting hash strings into directory
    paths for usability in various places. We borrow this pattern from
    DVC.

    Turns badbeef into b/adbeef.

    Default split is 1 because a human can fairly easily poke at 16
    directories if there's a debugging need to, for instance, count
    the number of total items. 256 (split=2) requires automation. And
    unlike DVC, we don't anticipate this being used for millions of
    'things', so each of the 16 top level directories will rarely
    contain more than several thousand items, which is pretty
    manageable for most systems.

    Another way to look at the split is to think about how many
    parallel list operations you'd like to be able to do. For most
    imaginable use cases, 16 parallel list operations would be
    plenty. If you think you'd need more - split at 2!
    """
    if split > 0 and split < len(hashstr):
        return f"{hashstr[:split]}/{hashstr[split:]}"
    return hashstr
