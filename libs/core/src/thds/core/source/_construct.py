import os
import sys
import typing as ty
from functools import partial
from pathlib import Path

from .. import log
from ..files import is_file_uri, path_from_uri, to_uri
from ..hash_cache import filehash
from ..hashing import Hash, Hasher, add_named_hash
from .src import Source

# Creation from local Files or from remote URIs

_AUTOHASH = "sha256"


def set_file_autohash(
    algo: str, hash_constructor: ty.Optional[ty.Callable[[str], Hasher]] = None
) -> None:
    """If you call this and provide a non-builtin hash algorithm, you must also provide a constructor for it."""
    if hash_constructor:
        hash_constructor(algo)  # this will raise if algo is not supported.
        add_named_hash(algo, hash_constructor)
    global _AUTOHASH
    _AUTOHASH = sys.intern(algo)


def hash_file(path: Path, algo: str = "") -> Hash:
    hash_algo = sys.intern(algo or _AUTOHASH)
    with log.logger_context(hash_for=f"source-{hash_algo}"):
        computed_hash = filehash(hash_algo, path)
        return computed_hash


def from_file(
    filename: ty.Union[str, os.PathLike], hash: ty.Optional[Hash] = None, uri: str = ""
) -> Source:
    """Create a read-only Source from a local file that already exists.

    If URI is passed, the local file will be read and hashed, but the final URI in the
    Source will be the one provided explicitly. NO UPLOAD IS PERFORMED. It is your
    responsibility to ensure that your file has been uploaded to the URI you provide.
    """
    path = path_from_uri(filename) if isinstance(filename, str) else Path(filename)
    if not path.exists():
        raise FileNotFoundError(path)

    file_hash = hash or hash_file(path)  # use automatic hash algo if not specified!
    if uri:
        src = from_uri(uri, file_hash)
    else:
        src = Source(to_uri(path), file_hash)
    src._set_cached_path(path)  # internally, it's okay to hack around immutability.
    return src


class FromUri(ty.Protocol):
    def __call__(self, hash: ty.Optional[Hash]) -> Source:
        """Closure over a URI that creates a Source from a URI.

        The Hash may be used to short-circuit creation that would result in creating
        a Source that cannot match the expected Hash, but this is not required,
        and the hash will be included in the Source object regardless, and will
        be validated (if non-nil) at the time of source data access.
        """
        ...


class FromUriHandler(ty.Protocol):
    def __call__(self, uri: str) -> ty.Optional[FromUri]:
        """Returns a FromUri object containing the URI if this URI can be handled.  Returns
        None if this URI cannot be handled.
        """
        ...


def register_from_uri_handler(key: str, handler: FromUriHandler):
    """If a library wants to customize how Sources are created from URIs that it handles,
    it can register a handler here.
    """
    # key is not currently used for anything other than avoiding
    # having duplicates registered for whatever reason.
    _FROM_URI_HANDLERS[key] = handler


_FROM_URI_HANDLERS: ty.Dict[str, FromUriHandler] = dict()
register_from_uri_handler(
    "local_file", lambda uri: partial(from_file, path_from_uri(uri)) if is_file_uri(uri) else None
)


def from_uri(uri: str, hash: ty.Optional[Hash] = None) -> Source:
    """Create a read-only Source from a URI. The data should already exist at this remote
    URI, although Source itself can make no guarantee that it always represents real data
    - only that any data it does represent is read-only.

    It may be advantageous for a URI-handling library to register a more specific
    implementation of this function, if it is capable of determining a Hash for the blob
    represented by the URI without downloading the blob.
    """
    for handler in _FROM_URI_HANDLERS.values():
        if from_uri_ := handler(uri):
            return from_uri_(hash)
    return Source(uri=uri, hash=hash)
