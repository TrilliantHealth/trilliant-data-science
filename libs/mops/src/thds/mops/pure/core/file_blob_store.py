import os
import shutil
import typing as ty
from contextlib import contextmanager
from pathlib import Path

from thds.core import config, log
from thds.core.files import FILE_SCHEME, atomic_write_path, path_from_uri, remove_file_scheme, to_uri
from thds.core.link import link

from ..core.types import AnyStrSrc, BlobStore

MOPS_CONTROL_ROOT = config.item("control_root", default=Path.home() / ".mops-local-root")
logger = log.getLogger(__name__)


@contextmanager
def atomic_writable(desturi: str, mode: str = "wb"):
    with atomic_write_path(desturi) as temppath:
        with open(temppath, mode) as f:
            yield f


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
    def control_root(self, uri: str) -> str:
        local_root = MOPS_CONTROL_ROOT()
        local_root.mkdir(exist_ok=True)
        return to_uri(local_root)

    def readbytesinto(self, remote_uri: str, stream: ty.IO[bytes], type_hint: str = "bytes"):
        assert remote_uri.startswith(FILE_SCHEME)
        with path_from_uri(remote_uri).open("rb") as f:
            shutil.copyfileobj(f, stream)  # type: ignore

    def getfile(self, remote_uri: str) -> Path:
        assert remote_uri.startswith(FILE_SCHEME)
        p = path_from_uri(remote_uri)
        if not p.exists():
            logger.error(f"{remote_uri} does not exist. Parent = {p.parent}")
            try:
                logger.error(list(p.parent.glob("*")))
            except FileNotFoundError:
                logger.error(f"{p.parent} does not exist either!")
            raise FileNotFoundError(f"{remote_uri} does not exist")
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


def get_file_blob_store(uri: str) -> ty.Optional[FileBlobStore]:
    if uri.startswith(FILE_SCHEME):
        return FileBlobStore()

    return None
