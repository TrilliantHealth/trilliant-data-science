import hashlib
import typing as ty
from pathlib import Path
from tempfile import NamedTemporaryFile

from thds.core.hashing import hash_using
from thds.core.log import getLogger

from ._hash import nest
from ._once import Once

Downloader = ty.Callable[[], Path]
logger = getLogger(__name__)


class PathContentAddresser:
    """Hashes the data at a path, but only once per unique resolved
    Path seen, because hashing a large file is expensive and such
    Paths are often shared across many invocations.

    In general, you will want only one instance of this per
    application/process, to take advantage of the caching behavior.

    This does imply that each use of a Path is, as documented in the
    README, a reference to an immutable, write-at-most-once file, at
    least during the lifetime of the process hosting this
    object. Passing the same Path multiple times with different
    contents, and expecting it to get hashed and uploaded each time,
    will not work.
    """

    def __init__(self):
        self.once = Once()
        self.paths_to_keys: ty.Dict[str, str] = dict()

    def __call__(self, path: Path) -> str:
        """Return a remote key for a path."""
        resolved = str(path.resolve())
        # we now put all paths at the hash of their own contents which
        # allows us to avoid uploading duplicated data even from two
        # different file paths that happen to share the same contents.
        #
        # This _also_ allows us to be more confident that memoization
        # bugs arising from reuse of Paths pointing to different
        # underlying file contents across separate process lifetimes
        # cannot happen - a given Path will be represented inside the
        # pickle by something that represents the (immutable) file
        # contents itself, rather than by a mutable reference (the
        # path).

        def _hash_and_remember_path():
            hexsha = hash_using(path, hashlib.sha256()).hexdigest()
            self.paths_to_keys[resolved] = f"path/{nest(hexsha)}"

        self.once.run_once(resolved, _hash_and_remember_path)
        return self.paths_to_keys[resolved]


class PathUnpickler:
    """A self-unserializing 'thunk' compatible with CallableUnpickler.

    Important: this object and its associated Downloader must also be
    trivially picklable.
    """

    def __init__(self, downloader: Downloader, remote_key: str):
        self.downloader = downloader
        self.remote_key = remote_key  # only for debugging

    def __call__(self) -> Path:
        return self.downloader()


class BlobStream(ty.Protocol):
    def local_to_remote(self, __path: Path, __key: str):
        ...  # pragma: no cover

    def get_downloader(self, __key: str) -> Downloader:
        ...  # pragma: no cover


def _pickle_file_path_as_upload(
    once: Once, path_keyer: PathContentAddresser, stream: BlobStream, local_src: Path
) -> ty.Optional[PathUnpickler]:
    if not local_src.exists():
        logger.warning(
            f"You asked us to pickle the path {local_src} "
            "but it does not exist, so it will be pickled as-is."
        )
        return None
    if not local_src.is_file():
        logger.warning(
            f"You asked us to pickle the Path {local_src} "
            "but it is not a file, so it will be pickled as-is."
        )
        return None

    remote_root = path_keyer(local_src)
    # I am creating a root 'directory' so that we can put debug info
    # side-by-side with the actual bytes, without interfering in any
    # way with the determinism of the hashed bytes themselves.
    remote_key = remote_root + "/_bytes"

    def upload():
        logger.info(
            f"Uploading Path {local_src} to {remote_key} - "
            "its contents will get 'unpickled' on the other side"
            " as a Path pointing to a local, read-only file."
        )
        stream.local_to_remote(local_src, remote_key)
        with NamedTemporaryFile("w") as tmp:
            tmp.write(str(local_src))
            tmp.flush()
            stream.local_to_remote(  # purely debug info
                Path(tmp.name),
                f"{remote_root}/debug_{str(local_src).replace('/', '_')}",
            )

    once.run_once(remote_key, upload)
    return PathUnpickler(stream.get_downloader(remote_key), remote_key)


class PathPickler:
    """Allow local file Paths to be pickled as streaming objects and then
    unpickled remotely by downloading them from a stream and then
    returning a Path object pointing to the downloaded file.
    """

    def __init__(self, stream: BlobStream, once: Once, path_addresser: PathContentAddresser):
        self.stream = stream
        self.once = once
        self.path_addresser = path_addresser

    def __call__(self, maybe_path: ty.Any) -> ty.Optional[PathUnpickler]:
        """Returns a persistent ID compatible with CallableUnpickler for any real file Path.

        The Persistent ID will actually be a thunk that is self-unpickling.
        """
        if isinstance(maybe_path, Path):
            return _pickle_file_path_as_upload(self.once, self.path_addresser, self.stream, maybe_path)
        return None
