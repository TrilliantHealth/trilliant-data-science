import datetime as dt
import os
import shutil
import tempfile
import typing as ty
from contextlib import contextmanager
from pathlib import Path

from thds.core import config, log
from thds.core.files import FILE_SCHEME, atomic_write_path, path_from_uri, remove_file_scheme, to_uri
from thds.core.link import link

from ..core.types import AnyStrSrc, BlobListing, BlobStore, Listings

MOPS_ROOT = config.item("control_root", default=Path.home() / ".mops")
_PUT_UNLESS_EXISTS_TEMP = ".~put-unless-exists~"  # infix of an in-flight temp; never a real blob name
logger = log.getLogger(__name__)


def _listings(root: Path) -> ty.Iterator[BlobListing]:
    """A file removed between `iterdir` and `stat` is skipped rather than failing the whole
    listing - concurrent writers and TTL cleanup make that a normal occurrence, and one
    vanished file should not cost a reader every other entry."""
    for path in root.iterdir():
        try:
            if _PUT_UNLESS_EXISTS_TEMP in path.name:
                continue

            if path.is_file():
                yield BlobListing(
                    to_uri(path), dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
                )
        except OSError:
            logger.debug("Skipping %s while listing; it went away.", path)


@contextmanager
def atomic_writable(desturi: str, mode: str = "wb") -> ty.Iterator[ty.IO[bytes]]:
    with atomic_write_path(desturi) as temppath:
        with open(temppath, mode) as f:
            yield f


def _link(path: Path, remote_uri: str) -> None:
    dest = path_from_uri(remote_uri)
    dest.parent.mkdir(parents=True, exist_ok=True)
    assert link(path, dest), f"Link {path} to {remote_uri} failed!"


def _put_bytes_to_file_uri(remote_uri: str, data: AnyStrSrc) -> None:
    """Write data to a local path. It is very hard to support all the same inputs that ADLS does. :("""
    path = None
    if isinstance(data, str):
        path = Path(data)
        if not path.exists():  # wasn't _actually_ a Path
            path = None
    elif isinstance(data, Path):
        path = data
    if path:
        _link(path, remote_uri)
    elif isinstance(data, bytes):
        with atomic_writable(remote_uri, "wb") as f:
            f.write(data)
    elif isinstance(data, str):
        with atomic_writable(remote_uri, "w") as f:
            f.write(data)  # type: ignore
    else:
        # if this fallback case fails, we may need to admit defeat for now,
        # and follow up by analyzing the failure and adding support for the input data type.
        with atomic_writable(remote_uri, "wb") as f:
            for block in data:  # type: ignore
                f.write(block)


class FileBlobStore(BlobStore):
    def control_root(self, uri: str) -> str:
        local_root = MOPS_ROOT()
        local_root.mkdir(exist_ok=True)
        return to_uri(local_root)

    def readbytesinto(self, remote_uri: str, stream: ty.IO[bytes], type_hint: str = "bytes") -> None:
        with path_from_uri(remote_uri).open("rb") as f:
            shutil.copyfileobj(f, stream)  # type: ignore

    def getfile(self, remote_uri: str) -> Path:
        p = path_from_uri(remote_uri)
        if not p.exists():
            logger.error(f"{remote_uri} does not exist. Parent = {p.parent}")
            try:
                logger.error(list(p.parent.glob("*")))
            except FileNotFoundError:
                logger.error(f"{p.parent} does not exist either!")
            raise FileNotFoundError(f"{remote_uri} does not exist")
        return p

    def putbytes(self, remote_uri: str, data: AnyStrSrc, type_hint: str = "bytes") -> None:
        """Upload data to a remote path."""
        logger.debug(f"Writing {type_hint} to {remote_uri}")
        _put_bytes_to_file_uri(remote_uri, data)

    def putfile(self, path: Path, remote_uri: str) -> None:
        _link(path, remote_uri)

    def exists(self, remote_uri: str) -> bool:
        return path_from_uri(remote_uri).exists()

    def put_unless_exists(self, remote_uri: str, data: bytes, *, type_hint: str = "bytes") -> bool:
        """Atomic create-if-absent: True only for the one caller that created the blob.

        The body is written to a same-directory temp file and the final name is claimed
        with an atomic no-replace hardlink, so the name is never visible with a partial
        body and a failed or interrupted attempt leaves the name safely retryable.

        The temp file is always unlinked, on both paths: `os.link` gives the inode a second
        name, so dropping the temp name after a successful claim leaves the body reachable at
        the final name. One temp file exists per in-flight caller in the directory, and none
        outlive the call.
        """
        path = path_from_uri(remote_uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=path.name + _PUT_UNLESS_EXISTS_TEMP, dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.link(tmp, path)
            return True
        except FileExistsError:
            return False
        finally:
            os.unlink(tmp)

    def list(self, prefix_uri: str, start_at: str = "") -> Listings:
        """The optional ListableBlobStore capability - see `core.types`.

        Non-recursive, matching AdlsBlobStore. Sorted, which a filesystem does not do on its
        own - `iterdir` yields in directory order.
        """
        root = path_from_uri(prefix_uri)
        if not root.is_dir():
            return []

        listed = sorted(_listings(root), key=lambda entry: entry.uri)
        # sort on the URI alone: BlobListing's tuple ordering would fall through to
        # modified_at on a tie, which cannot be compared when one side is None.
        return [entry for entry in listed if entry.uri >= start_at] if start_at else listed

    def join(self, *parts: str) -> str:
        return os.path.join(*parts)

    def split(self, uri: str) -> ty.List[str]:
        """Splits a given URI into its constituent parts"""
        path = remove_file_scheme(uri)
        # normalize the path to handle redundant slashes
        normalized_path = os.path.normpath(path)

        parts = normalized_path.split(os.sep)

        # remove any empty parts that might be created due to leading slashes
        parts = [part for part in parts if part]

        parts = [f"{FILE_SCHEME}/"] + parts

        return parts

    def is_blob_not_found(self, exc: Exception) -> bool:
        return isinstance(exc, FileNotFoundError)


_STATELESS_BLOB_STORE = FileBlobStore()


def get_file_blob_store(uri: str) -> ty.Optional[FileBlobStore]:
    if uri.startswith(FILE_SCHEME):
        return _STATELESS_BLOB_STORE

    # special case for things where somebody forgot the file:// scheme.
    # we're the 'first' registered blob store, so we're the last ones to be asked
    # and this shouldn't cause a significant performance penalty since everything else
    # with a scheme will get picked up first.
    return _STATELESS_BLOB_STORE
