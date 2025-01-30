import hashlib
import typing as ty
from functools import partial
from pathlib import Path
from tempfile import NamedTemporaryFile

from thds import humenc
from thds.core.hash_cache import hash_file
from thds.core.log import getLogger

from ..._utils import once
from . import deferred_work

Downloader = ty.Callable[[], Path]
logger = getLogger(__name__)
_1_MB = 2**20


def human_sha256b64_file_at_paths(path: Path) -> str:
    """Return a human-readable hash of the file at the given path."""
    assert path.exists(), path
    return humenc.encode(hash_file(path, hashlib.sha256()))


class _ProcessLockingPathContentAddresser:
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

    def __init__(self, once: once.Once):
        self.once = once
        self.paths_to_keys: ty.Dict[str, str] = dict()

    def __call__(self, path: Path) -> str:
        """Return a remote key (sha256 hash in human-base64) for a path."""
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

        def _hash_and_remember_path() -> None:
            self.paths_to_keys[resolved] = human_sha256b64_file_at_paths(path)

        self.once.run_once(resolved, _hash_and_remember_path)
        return self.paths_to_keys[resolved]


class PathStream(ty.Protocol):
    def local_to_remote(self, __path: Path, __key: str) -> None:
        ...  # pragma: no cover

    def get_downloader(self, __key: str) -> Downloader:
        ...  # pragma: no cover


class NotAFileError(ValueError):
    """We err on the side of caution in Mops 2.0 by never allowing
    Paths that are not actual files to be serialized on either side.

    This error is not intended to be caught; it is intended to inform
    the developer that they have made a coding mistake by passing an
    incorrect Path to a function that is supposed to be transferring
    execution via a Runner.

    In the future we might add support for directories if it is desired.
    """


def _serialize_file_path_as_upload(
    once: once.Once, path_keyer: _ProcessLockingPathContentAddresser, stream: PathStream, local_src: Path
) -> ty.Optional[Downloader]:
    if not local_src.exists():
        raise NotAFileError(f"You asked mops to upload the path {local_src}, but it does not exist.")
    if not local_src.is_file():
        raise NotAFileError(f"You asked mops to upload the Path {local_src}, but it is not a file.")

    remote_root = path_keyer(local_src)
    # I am creating a root 'directory' so that we can put debug info
    # side-by-side with the actual bytes, without interfering in any
    # way with the determinism of the hashed bytes themselves.
    remote_key = remote_root + "/_bytes"

    def upload() -> None:
        size = local_src.stat().st_size
        formatted_size = f"{size / _1_MB:,.2f} MB"
        log = logger.info if size > 10 * _1_MB else logger.debug
        log(
            f"Uploading Path {local_src} of size {formatted_size} to {remote_key} - "
            "its contents will get 'unpickled' on the other side"
            " as a Path pointing to a local, read-only file."
        )
        stream.local_to_remote(local_src, remote_key)
        with NamedTemporaryFile("w") as tmp:
            tmp.write(str(local_src))
            tmp.flush()
            stream.local_to_remote(  # purely debug info
                Path(tmp.name),
                f"{remote_root}/pathname-_{str(local_src).replace('/', '_')}",
            )

    logger.debug("Adding deferred upload of %s", remote_key)
    deferred_work.add(
        __name__,
        remote_key,
        partial(once.run_once, remote_key, upload),
    )
    return stream.get_downloader(remote_key)


class CoordinatingPathSerializer:
    """Allow local file Paths to be serialized as streaming objects and then
    deserialized remotely by downloading them from a stream and then
    returning a Path object pointing to the downloaded file.
    """

    def __init__(self, stream: PathStream, once: once.Once):
        self.stream = stream
        self.once = once
        self.path_addresser = _ProcessLockingPathContentAddresser(once)

    def __call__(self, maybe_path: ty.Any) -> ty.Optional[ty.Callable[[], Path]]:
        """Returns a persistent ID compatible with CallableUnpickler for any real file Path.

        The Persistent ID will actually be a thunk that is self-unpickling.
        """
        if isinstance(maybe_path, Path):
            return _serialize_file_path_as_upload(
                self.once, self.path_addresser, self.stream, maybe_path
            )
        return None
