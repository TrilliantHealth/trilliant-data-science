import typing as ty
from os import PathLike
from pathlib import Path

from thds.core.hashing import Hash
from thds.core.link import link_or_copy

from ..files import is_file_uri, path_from_uri
from .src import Source


class Uploader(ty.Protocol):
    def __call__(self, local_path: Path, hash: ty.Optional[Hash], /) -> None:
        """Closure over a URI that uploads a Source from a local path to a destination URI.
        It may or may not do anything with the Hash object.

        If the Path points to a missing file, this MUST raise any Exception that the
        underlying implementation desires. If the destination URI cannot be written to,
        this MUST raise any Exception that the underlying implementation desires.
        """


class UploadHandler(ty.Protocol):
    def __call__(self, uri: str, /) -> ty.Optional[Uploader]:
        """Returns a Uploader containing the URI if this URI can be handled.  Returns
        None if this URI cannot be handled.
        """


def register_upload_handler(key: str, handler: UploadHandler):
    # key is not currently used for anything other than avoiding
    # having duplicates registered for whatever reason.
    _UPLOAD_HANDLERS[key] = handler


def _LocalFileHandler(uri: str) -> ty.Optional[Uploader]:
    if not is_file_uri(uri):
        return None  # not to be handled by this handler

    def upload_file(local_path: Path, hash: ty.Optional[Hash]) -> None:
        lpath = path_from_uri(uri)
        if not local_path.exists():
            raise FileNotFoundError(local_path)
        # For local file URIs, we simply link/copy the file.
        # if the URI _is_ the file's current location, link_or_copy is a no-op.
        lpath.parent.mkdir(parents=True, exist_ok=True)
        link_or_copy(local_path, lpath)

    return upload_file


_UPLOAD_HANDLERS: ty.Dict[str, UploadHandler] = dict()
register_upload_handler("local_file", _LocalFileHandler)


def _get_uploader(uri: str) -> Uploader:
    for handler in _UPLOAD_HANDLERS.values():
        if uploader := handler(uri):
            return uploader
    raise ValueError(f"No SourcePath upload handler for uri: {uri}")


def upload(source: ty.Union[Source, str, PathLike]) -> Source:
    """Uploads the given Source's local cached file to its URI.

    If a PathLike or str is given, it is first converted to a Source using
    source.from_file (the file must exist), so that `core.uri_assign` will be used to
    assign a URI.

    If the Source has no cached local file, raises a ValueError.

    If the Source has a Hash, the Hash will be validated against the local file
    before upload, and a failure will raise SourceHashMismatchError.
    """
    if not isinstance(source, Source):
        from ._construct import from_file

        source = from_file(source)

    if source.cached_path is None or not source.cached_path.exists():
        raise ValueError(f"Source {source.uri} has no local cached file to upload")

    _get_uploader(source.uri)(source.cached_path, source.hash)
    return source
