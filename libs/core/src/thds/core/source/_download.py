"""Wrap openable, read-only data that is either locally-present or downloadable,

yet will not be downloaded (if non-local) until it is actually opened or unwrapped.
"""

import typing as ty
from pathlib import Path

from ..files import is_file_uri, path_from_uri
from ..hashing import Hash
from ._construct import hash_file


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


def _LocalFileHandler(uri: str) -> ty.Optional[Downloader]:
    if not is_file_uri(uri):
        return None

    def download_file(hash: ty.Optional[Hash]) -> Path:
        lpath = path_from_uri(uri)
        if not lpath.exists():
            raise FileNotFoundError(lpath)
        if hash:
            _check_hash(hash, lpath)
        return lpath

    return download_file


def register_download_handler(key: str, handler: DownloadHandler):
    # key is not currently used for anything other than avoiding
    # having duplicates registered for whatever reason.
    _DOWNLOAD_HANDLERS[key] = handler


_DOWNLOAD_HANDLERS: ty.Dict[str, DownloadHandler] = dict()
register_download_handler("local_file", _LocalFileHandler)


def _get_download_handler(uri: str) -> Downloader:
    for handler in _DOWNLOAD_HANDLERS.values():
        if downloader := handler(uri):
            return downloader
    raise ValueError(f"No SourcePath download handler for uri: {uri}")


class SourceHashMismatchError(ValueError):
    pass


def _check_hash(expected_hash: ty.Optional[Hash], path: Path) -> Hash:
    computed_hash = hash_file(path)
    if expected_hash and expected_hash != computed_hash:
        raise SourceHashMismatchError(
            f"{expected_hash.algo} mismatch for {path};"
            f" got {computed_hash.bytes!r}, expected {expected_hash.bytes!r}"
        )
    return computed_hash
