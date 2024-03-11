"""Bidirectional, context-sensitive translation: Source <--> (Hashref | URI).

Hashrefs - passing data blobs of many kinds into remote functions by their Hash where
possible, then using a separate lookup file per hash to tell us where the actual data is
stored.

- local file source containing a Hash - can be optimized with hashref
- remote file source containing a Hash - can be optimized with hashref
- remote file source only having URI - cannot be optimized - passed as a raw URI.

Decoupling hashref creation from potential upload is important because it lets us avoid
upload in cases where the Shell turns out to be a local machine shell.

We create hashrefs for Sources on the local machine in a shared location. Since this
data is immutable and content-addressed, there should be no serious concurrency objections
to this approach.

Then, if we cross a boundary into a Shell that will start execution on a different
machine, we serialize the local Path to content-addressed storage in the current active
storage root, and we then create a hashref in the active storage root (again, these
should be effectively immutable on the shared store even if they will mostly likely get
rewritten multiple times).

On the remote side, we will first check the local hashref location. It may very well not
exist at all.  If it does, we should attempt to follow it, but the referent may not
exist (for whatever reason) and in all cases we are able to fall back to looking for a
remote hashref and following its reference.

We are keeping the core business logic completely separate from pickling.  All
serialization methods will have to choose how to represent the information returned by
this module, but it should be able to call back into this module with that same state to
have a Source object returned to it while it performs low-level deserialization.
"""
import io
import typing as ty
from contextlib import contextmanager
from functools import partial
from pathlib import Path

from thds.core import hashing, log, source
from thds.core.files import is_file_uri, to_uri
from thds.core.source import Source
from thds.core.stack_context import StackContext
from thds.core.types import StrOrPath
from thds.mops._utils import human_b64

from .content_addressed import wordybin_content_addressed
from .output_naming import invocation_output_uri
from .uris import active_storage_root, lookup_blob_store

_REMOTE_HASHREF_PREFIX = "mops2-hashrefs"
_LOCAL_HASHREF_DIR = ".mops2-local-hashrefs"
logger = log.getLogger(__name__)


def _hash_to_str(hash: hashing.Hash) -> str:
    # i see no reason to not remain opinionated and "debug-friendly" with the user-visible
    # encoding of our hashes when they are being stored on a blob store/FS of some kind.
    return f"{hash.algo}-{human_b64.encode(hash.bytes)}"


def _hashref_uri(hash: hashing.Hash, type: ty.Literal["local", "remote"]) -> str:
    # the .txt extensions are just for user-friendliness during debugging
    if type == "remote":
        base_uri = active_storage_root()
        return lookup_blob_store(base_uri).join(
            base_uri, _REMOTE_HASHREF_PREFIX, _hash_to_str(hash) + ".txt"
        )
    local_hashref = Path.home() / _LOCAL_HASHREF_DIR / f"{_hash_to_str(hash)}.txt"
    return to_uri(local_hashref)


def _read_hashref(hashref_uri: str) -> str:
    """Return URI represented by this hashref. Performs IO."""
    uri_bytes = io.BytesIO()
    lookup_blob_store(hashref_uri).readbytesinto(hashref_uri, uri_bytes)
    uri = uri_bytes.getvalue().decode()
    assert uri, f"Hashref from {hashref_uri} is empty"
    return uri


def _write_hashref(hashref_uri: str, uri: str) -> None:
    """Write URI to this hashref. Performs IO."""
    assert uri, f"Should never encode hashref ({hashref_uri}) pointing to empty URI"
    lookup_blob_store(hashref_uri).putbytes(hashref_uri, uri.encode())


def source_from_hashref(hash: hashing.Hash) -> Source:
    """Re-create a Source from a Hash by looking up one of two Hashrefs and finding a
    valid Source for the data."""
    local_file_hashref_uri = _hashref_uri(hash, "local")
    try:
        # we might be on the same machine where this was originally invoked.
        # therefore, there may be a local path we can use directly.
        return source.from_file(_read_hashref(local_file_hashref_uri), hash=hash)
    except FileNotFoundError:
        # we are not on the same machine as the local ref. assume we need the remote URI.
        pass
    except Exception as e:
        if not lookup_blob_store(local_file_hashref_uri).is_blob_not_found(e):
            # blob not found is fine, but anything else is weird and we should raise.
            raise

    # no local file, so we assume there must be a remote URI.
    return source.from_uri(_read_hashref(_hashref_uri(hash, "remote")), hash=hash)


_PENDING_UPLOADS: StackContext[ty.Dict[hashing.Hash, ty.Callable[[], None]]] = StackContext(
    "PENDING_SOURCE_UPLOADS", dict()
)


@contextmanager
def source_argument_buffering() -> ty.Iterator[None]:
    """Enter this context before you begin serializing your invocation. If
    perform_source_uploads() is later called, any Sources replaced by Source Arguments
    during serialization will be uploaded to the remote store. The context should not be
    closed until after return from the Shell.

    The idea is that you'd call perform_source_uploads() inside your Shell which transfers
    execution to a remote environment, but _not_ call it if you're transferring execution
    to a local environment, as the upload will not be needed.

    This is not re-entrant. If this is called while the dictionary is non-empty, an
    exception will be raised. This is only because I can think of no reason why anyone
    would want it to be re-entrant, so it seems better to raise an error. If for some
    reason re-entrancy were desired, we could just silently pass if the dictionary already
    has pending uploads.

    """
    assert not _PENDING_UPLOADS(), "source_upload_buffering() is not re-entrant"
    with _PENDING_UPLOADS.set(dict()):
        yield


def perform_source_uploads() -> None:
    uploads = _PENDING_UPLOADS()
    if uploads:
        # TODO - remove these while uploading?
        logger.info(f"Uploading {len(uploads)} Sources so they'll be available remotely.")
        for _hash, upload in uploads.items():
            # TODO perform these uploads in a small thread pool?
            upload()


def _upload_and_create_remote_hashref(local_path: Path, remote_uri: str, hash: hashing.Hash) -> None:
    # exists only to provide a local (non-serializable) closure around local_path and remote_uri.
    lookup_blob_store(remote_uri).putfile(local_path, remote_uri)
    # make sure we never overwrite a hashref until it's actually going to be valid.
    _write_hashref(_hashref_uri(hash, "remote"), remote_uri)


def _auto_remote_uri(hash: hashing.Hash) -> str:
    """Pick a remote URI for a file/source that has the given hash.

    The underlying implementation is shared with the content-addressing that is used
    throughout mops.
    """
    return wordybin_content_addressed(hash).bytes_uri


def prepare_source_argument(source_: Source) -> ty.Union[str, hashing.Hash]:
    """For use on the orchestrator side, during serialization of the invocation.

    You either end up with a Hashref created under the current HASHREF_ROOT, or you end up
    with just a URI, which is not amenable to hashref optimization.
    """
    if not source_.hash:
        # we cannot optimize this one for memoization - just return the URI.
        return source_.uri

    local_path = source_.cached_path
    if local_path and local_path.exists():
        # create local hashref...
        _write_hashref(_hashref_uri(source_.hash, "local"), str(local_path))
        # then also register pending upload - if the URI is a local file, we need to determine a
        # remote URI for this thing automagically; otherwise, use whatever was already
        # specified by the Source itself.
        remote_uri = source_.uri if not is_file_uri(source_.uri) else _auto_remote_uri(source_.hash)
        _PENDING_UPLOADS()[source_.hash] = partial(
            _upload_and_create_remote_hashref, local_path, remote_uri, source_.hash
        )
    else:
        # only create a remote hashref, because this Source represents a non-local resource.
        _write_hashref(_hashref_uri(source_.hash, "remote"), source_.uri)

    return source_.hash


# RETURNING FROM REMOTE
#
# when returning a Source from a remote, we cannot avoid the upload.  this is because the
# uploaded data is part of the memoized result, and memoization by definition is available
# to all callers, even those on other machines/environments.
#
# A good example of where this is necessary is memoizing Person API test data in CI.  the
# code runs locally, but the goal is to create an output file that can be reused next time
# it runs (locally or in CI). And for that to be possible, the output _must_ be uploaded.
#
# This does not mean that the Source itself must be uploaded immediately upon creation;
# just that mops must detect Sources in the return value and must force an upload on them.
# In essence, this creates a bifurcated code path for Sources during serialization; if
# we're "on the way out", we avoid uploading until it is clear that the data will be used
# in a remote environment. Whereas "on the way back", we must always upload, and nothing
# can or should be deferred; upload should happen at the time of serialization.
#
# Nevertheless, a local caller should still be able to short-circuit the _download_ by
# using a locally-created File, if on the same machine where the local file was created.


class SourceResult(ty.NamedTuple):
    """Contains the fully-specified local URI and remote URI, plus (probably) a Hash.

    Everything is defined right here. No need for any kind of dynamic lookup, and
    optimization buys us nothing, since memoization only operates on arguments.
    """

    remote_uri: str
    hash: ty.Optional[hashing.Hash]
    file_uri: str


def prepare_source_result(source_: Source) -> SourceResult:
    """Call from within the remote side of an invocation, while serializing the function return value.

    Forces the Source to be present at a remote URI which will be available once
    returned to the orchestrator.

    The full output URI is auto-generated if one is not already provided, because we're
    guaranteed to be in a remote context, which provides an invocation output root URI
    where we can safely place any named output.
    """
    if not is_file_uri(source_.uri):
        if source_.cached_path:
            file_uri = to_uri(source_.cached_path)
        else:
            file_uri = ""
        logger.debug("Creating a SourceResult for a URI that is presumed to already be uploaded.")
        return SourceResult(source_.uri, source_.hash, file_uri)

    # by definition, if this is a file URI, it now needs to be uploaded, because we could
    # be transferring back to an orchestrator on a different machine, but also because a
    # future caller on a different machine could try to use this memoized result.
    local_path = source.path_from_uri(source_.uri)
    assert local_path.exists(), f"{local_path} does not exist"
    logger.debug("Automatically selecting a remote URI for a Source being returned.")
    remote_uri = invocation_output_uri(name=local_path.name)
    # the line above is a bit of opinionated magic. it uses the 'end' of the filename
    # to automagically assign a meaningful name to the output remote URI.
    #
    # If users do not like this automatically assigned remote URI name, they can construct
    # the Source themselves and provide a remote URI (as well as, optionally, a
    # local_path), and we will use their remote URI.
    lookup_blob_store(remote_uri).putfile(local_path, remote_uri)
    # upload must _always_ happen on remotely-returned Sources, as detailed above.
    # There is no advantage to waiting to upload past this point.
    return SourceResult(remote_uri, source_.hash, source_.uri)


def source_from_source_result(remote_uri: str, hash: ty.Optional[hashing.Hash], file_uri: str) -> Source:
    """Call when deserializing a remote function return value on the orchestrator side, to
    replace all SourceResults with the intended Source object.
    """
    local_path = source.path_from_uri(file_uri)
    if local_path.exists():
        try:
            return source.from_file(local_path, hash=hash)
        except Exception as e:
            logger.warning(f"Failed to use local path {local_path} for source {remote_uri}: {e}")
    return source.from_uri(remote_uri, hash=hash)


def create_source_at_uri(filename: StrOrPath, destination_uri: str) -> Source:
    """Public API for creating a Source with a manually-specified remote URI
    within a remote function invocation. Not generally recommended.

    Use this if you want to provide specific URI destination for a file that exists
    locally, rather than using the automagic naming behavior provided by creating a Source
    with `from_file`, which is standard.

    _Only_ use this if you are willing to immediately upload your data.

    """
    source_ = source.from_file(filename, uri=destination_uri)
    lookup_blob_store(destination_uri).putfile(Path(filename), destination_uri)
    return source_
