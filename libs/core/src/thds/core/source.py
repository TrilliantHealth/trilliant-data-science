"""Wrap openable, read-only data that is either locally-present or downloadable,

yet will not be downloaded (if non-local) until it is actually opened or unwrapped.
"""
import hashlib
import os
import typing as ty
from dataclasses import dataclass
from pathlib import Path

from .files import is_file, path_from_uri, set_read_only, to_uri
from .hash_cache import hash_file
from .hashing import Hash
from .types import StrOrPath


class Downloader(ty.Protocol):
    def __call__(self, hash: ty.Optional[Hash]) -> Path:
        """Closure over a URI that downloads a file to a local path and returns the path.
        The file may be placed anywhere as long as the file will be readable until the
        program exits.

        If the URI points to a missing file, this MUST raise any Exception that the
        underlying implementation desires. It MUST NOT return a Path pointing to a
        non-existent file.

        The Hash may be used to short-circuit a download that would result in downloading
        a file that does not match the expected hash, but the Downloader need not verify
        the Hash of the file downloaded after the fact, as that will be performed by
        default by the Source object.
        """


class DownloadHandler(ty.Protocol):
    def __call__(self, uri: str) -> ty.Optional[Downloader]:
        """Returns a Downloader containing the URI if this URI can be handled.  Returns
        None if this URI cannot be handled.
        """


_DOWNLOAD_HANDLERS: ty.Dict[str, DownloadHandler] = {}


def register_download_handler(key: str, handler: DownloadHandler):
    # key is not currently used for anything other than avoiding
    # having duplicates registered for whatever reason.
    _DOWNLOAD_HANDLERS[key] = handler


def LocalFileHandler(uri: str) -> ty.Optional[Downloader]:
    if not is_file(uri):
        return None

    def download(hash: ty.Optional[Hash]) -> Path:
        lpath = path_from_uri(uri)
        if not lpath.exists():
            raise FileNotFoundError(lpath)
        return lpath

    return download


register_download_handler("local_file", LocalFileHandler)


def _get_download_handler(uri: str) -> Downloader:
    for handler in _DOWNLOAD_HANDLERS.values():
        if downloader := handler(uri):
            return downloader
    raise ValueError(f"No SourcePath download handler for uri: {uri}")


class SourceHashMismatchError(ValueError):
    pass


def _check_hash(expected_hash: ty.Optional[Hash], path: Path) -> Hash:
    def _hash_file(algo: str) -> Hash:
        return Hash(algo, hash_file(path, hashlib.new(algo)))

    computed_hash = _hash_file(expected_hash.algo if expected_hash else "sha256")
    if expected_hash and expected_hash != computed_hash:
        raise SourceHashMismatchError(
            f"{expected_hash.algo} mismatch for {path};"
            f" got {computed_hash.bytes!r}, expected {expected_hash.bytes!r}"
        )
    return computed_hash


@dataclass
class Source(os.PathLike):
    """Source is meant to be a consistent in-memory representation for an abstract,
    **read-only** source of data that may not be present locally when an application
    starts.

    Do not call its constructor in application code. Use `from_path` or `from_uri` instead.

    By 'wrapping' read-only data in these objects, we can unify the code around how we
    unwrap and use them, which should allow us to more easily support different execution
    environments and sources of data.

    For instance, a Source could be a file on disk, but it could also be a file in
    ADLS.

    Furthermore, libraries which build on top of this one may use this representation to
    identify opportunities for optimization, by representing the Source in a stable
    and consistent format that allows different underlying data sources to fulfill the
    request for the data based on environmental context. A library could choose to
    transparently transform a local-path-based Source into a Source representing a
    remote file, without changing the semantics of the object as observed by the code.

    One reason a Hash is part of the interface is so that libraries interacting with the
    object can use the hash as a canonical 'name' for the data, if one is available.

    Another reason is that we can add a layer of consistency checking to data we're
    working with, at the cost of a few compute cycles. Since Sources are meant to represent
    read-only data, the Hash is a meaningful and persistent marker of data identity.
    """

    uri: str
    hash: ty.Optional[Hash] = None
    _local_path: ty.Optional[Path] = None

    def path(self) -> Path:
        """Any Source can be turned into a local file path.

        Remember that the resulting data is meant to be read-only. If you want to mutate
        the data, you should first make a copy.

        If not already present locally, this will incur a one-time download.
        """
        if self._local_path is None or not self._local_path.exists():
            self._local_path = _get_download_handler(self.uri)(self.hash)
            if self.hash:
                _check_hash(self.hash, self._local_path)
            set_read_only(self._local_path)

        assert self._local_path
        return self._local_path

    def __fspath__(self) -> str:
        return os.fspath(self.path())


def from_path(path: StrOrPath, hash: ty.Optional[Hash] = None) -> Source:
    """Create a read-only Source from a local Path that already exists."""
    path = path_from_uri(path) if isinstance(path, str) else path
    assert isinstance(path, Path)
    if not path.exists():
        raise FileNotFoundError(path)

    return Source(to_uri(path), _check_hash(hash, path), path)


def from_uri(uri: str, hash: ty.Optional[Hash] = None) -> Source:
    """Create a read-only Source from a URI. In general, this ought to already exist, but
    Source makes no contract that it always represents real data - only that the data it
    represents is read-only.

    It may be advantageous for a URI-handling library to provide its own
    implementation of this function, if it is capable of determining a Hash
    for the blob represented by the URI without downloading the blob.
    """
    if is_file(uri):
        return from_path(uri, hash=hash)
    return Source(uri=uri, hash=hash)
