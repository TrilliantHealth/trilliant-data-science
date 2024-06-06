import os
import shutil
import typing as ty
from contextlib import contextmanager
from pathlib import Path

from thds.core import log, tmp
from thds.core.files import FILE_SCHEME, path_from_uri, remove_file_scheme
from thds.core.link import link

from ..core.types import AnyStrSrc, BlobStore

logger = log.getLogger(__name__)


@contextmanager
def atomic_writable(desturi: str, mode: str = "wb"):
    destfile = path_from_uri(desturi)
    with tmp.temppath_same_fs(destfile) as temp_writable_path:
        with open(temp_writable_path, mode) as f:
            yield f
            destfile.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(temp_writable_path), destfile)


def _link(path: Path, remote_uri: str):
    dest = path_from_uri(remote_uri)
    dest.parent.mkdir(parents=True, exist_ok=True)
    assert link(path, dest), f"Link {path} to {remote_uri} failed!"


def _put_bytes_to_file_uri(remote_uri: str, data: AnyStrSrc):
    """Write data to a local path. It is very hard to support all the same inputs that ADLS does. :("""
    assert remote_uri.startswith(FILE_SCHEME)

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
            f.write(data)
    else:
        # if this fallback case fails, we may need to admit defeat for now,
        # and follow up by analyzing the failure and adding support for the input data type.
        with atomic_writable(remote_uri, "wb") as f:
            for block in data:  # type: ignore
                f.write(block)


class FileBlobStore(BlobStore):
    def readbytesinto(self, remote_uri: str, stream: ty.IO[bytes], type_hint: str = "bytes"):
        assert remote_uri.startswith(FILE_SCHEME)
        with path_from_uri(remote_uri).open("rb") as f:
            shutil.copyfileobj(f, stream)  # type: ignore

    def getfile(self, remote_uri: str) -> Path:
        assert remote_uri.startswith(FILE_SCHEME)
        p = path_from_uri(remote_uri)
        assert p.exists()
        return p

    def putbytes(self, remote_uri: str, data: AnyStrSrc, type_hint: str = "bytes"):
        """Upload data to a remote path."""
        logger.debug(f"Writing {type_hint} to {remote_uri}")
        _put_bytes_to_file_uri(remote_uri, data)

    def putfile(self, path: Path, remote_uri: str):
        assert remote_uri.startswith(FILE_SCHEME)
        _link(path, remote_uri)

    def exists(self, remote_uri: str) -> bool:
        assert remote_uri.startswith(FILE_SCHEME)
        return path_from_uri(remote_uri).exists()

    def join(self, *parts: str) -> str:
        return os.path.join(*parts)

    def split(self, uri: str) -> ty.List[str]:
        """Splits a given URI into its constituent parts"""
        assert uri.startswith(FILE_SCHEME)

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
