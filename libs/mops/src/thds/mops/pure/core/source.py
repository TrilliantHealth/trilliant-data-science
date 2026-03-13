"""Bidirectional, context-sensitive translation: Source <--> (Hashref | URI).

Source arguments with a Hash are passed by hash into remote functions. The invocation
file header embeds a JSON hashref map (hash → uri + size) so the remote side can
resolve all Sources from the single invocation download — zero additional network
round-trips.

- local file source containing a Hash - can be optimized with hashref
- remote file source containing a Hash - can be optimized with hashref
- remote file source only having URI - cannot be optimized - passed as a raw URI.

Decoupling the data upload from serialization is important because it lets us avoid
upload in cases where the Shim turns out to be a local machine shim.

We are keeping the core business logic completely separate from pickling.  All
serialization methods will have to choose how to represent the information returned by
this module, but it should be able to call back into this module with that same state to
have a Source object returned to it while it performs low-level deserialization.
"""

import sys
import typing as ty
from contextlib import contextmanager
from functools import partial
from pathlib import Path

from thds import humenc
from thds.core import hashing, log, source
from thds.core.files import is_file_uri, to_uri
from thds.core.source import Source
from thds.core.stack_context import StackContext
from thds.core.types import StrOrPath

from . import deferred_work
from .content_addressed import wordybin_content_addressed
from .output_naming import mops_uri_assignment
from .uris import lookup_blob_store

logger = log.getLogger(__name__)

# Maps hash-string → {"uri": ..., "size": ...} for all Sources prepared during serialization.
# Populated by prepare_source_argument on the orchestrator side, embedded as a JSON header
# in the invocation file, then set on the remote side so source_from_hashref can resolve
# without any per-hashref network calls.
_HASHREF_MAP: StackContext[None | dict[str, dict[str, ty.Any]]] = StackContext("HASHREF_MAP", None)
stacklocal_hashrefs = _HASHREF_MAP.__call__  # public interface that could later be intermediated


def _hash_to_str(hash: hashing.Hash) -> str:
    return f"{hash.algo}-{humenc.encode(hash.bytes)}"


@contextmanager
def hashref_context(
    hashref_map: None | dict[str, dict[str, ty.Any]],
) -> ty.Iterator[None]:
    """Set an in-memory hashref map so source_from_hashref can resolve without network."""
    with _HASHREF_MAP.set(hashref_map):
        yield


def _collect_hashref_mapping(hash: hashing.Hash, uri: str, size: int) -> None:
    """Record a hashref mapping during serialization (orchestrator side)."""
    m = _HASHREF_MAP()
    if m is not None:
        m[_hash_to_str(hash)] = {"uri": uri, "size": size}


def source_from_hashref(hash: hashing.Hash) -> Source:
    """Re-create a Source from a Hash using the in-memory hashref map
    (populated from the invocation file header).
    """
    m = _HASHREF_MAP()
    if m is None:
        raise ValueError(
            "source_from_hashref called without a hashref map context. "
            "This indicates a mops version mismatch between orchestrator and remote, or a bug."
        )

    entry = m.get(_hash_to_str(hash))
    if not entry:
        raise KeyError(
            f"Hash {_hash_to_str(hash)} not found in hashref map. "
            "This indicates the invocation was serialized without recording this hash."
        )

    return source.from_uri(entry["uri"], hash=hash, size=entry["size"])


def _upload_source_data(local_path: Path, remote_uri: str) -> None:
    lookup_blob_store(remote_uri).putfile(local_path, remote_uri)


def _auto_remote_arg_uri(hash: hashing.Hash) -> str:
    """Pick a remote URI for a file/source _input_ (argument) that has the given hash.

    The underlying implementation is shared with the content-addressing that is used
    throughout mops.
    """
    return wordybin_content_addressed(hash).bytes_uri


def prepare_source_argument(source_: Source) -> ty.Union[str, hashing.Hash]:
    """For use on the orchestrator side, during serialization of the invocation.

    You either end up with a Hash (recorded in the hashref map for the remote side),
    or you end up with just a URI, which is not amenable to hashref optimization.
    """
    if not source_.hash:
        return source_.uri

    local_path = source_.cached_path
    if local_path and local_path.exists():
        remote_uri = source_.uri if not is_file_uri(source_.uri) else _auto_remote_arg_uri(source_.hash)
        _collect_hashref_mapping(source_.hash, remote_uri, source_.size)
        deferred_work.add(
            __name__ + "-upload",
            source_.hash,
            partial(_upload_source_data, local_path, remote_uri),
        )
    else:
        # non-local resource — the URI is already remote, just record the mapping
        _collect_hashref_mapping(source_.hash, source_.uri, source_.size)

    return hashing.Hash(algo=sys.intern(source_.hash.algo), bytes=source_.hash.bytes)


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
# in a remote environment. Whereas "on the way back", we must always upload -- there, we
# defer uploads until everything is serialized, then we perform all deferred uploads in
# parallel, prior to writing the serialized result.
#
# Nevertheless, a local caller should still be able to short-circuit the _download_ by
# using a locally-created File, if on the same machine where the local file was created.


class SourceResult(ty.NamedTuple):
    """Contains the fully-specified local URI and remote URI, plus (probably) a Hash
    and a size.

    Everything is defined right here. No need for any kind of dynamic lookup, and
    optimization buys us nothing, since memoization only operates on arguments.
    """

    remote_uri: str
    hash: ty.Optional[hashing.Hash]
    file_uri: str

    size: int = 0
    # instances of older versions of this namedtuple will be missing this field.
    # we supply a default for backward-compatibility.


class DuplicateSourceBasenameError(ValueError):
    """This is not a catchable error - it will be raised inside the mops result-wrapping
    code, and is an indication that user code has attempted to return two file-only Source objects
    without URIs specified, and that those two files have the same basename.
    """


def _put_file_to_blob_store(local_path: Path, remote_uri: str) -> None:
    logger.info("Uploading Source to remote URI %s", remote_uri)
    lookup_blob_store(remote_uri).putfile(local_path, remote_uri)


def prepare_source_result(source_: Source, existing_uris: ty.Collection[str] = tuple()) -> SourceResult:
    """Call from within the remote side of an invocation, while serializing the function return value.

    Forces the Source to be present at a remote URI which will be available once
    returned to the orchestrator.

    The full output URI is auto-generated if one is not already provided, because we're
    guaranteed to be in a remote context, which provides an invocation output root URI
    where we can safely place any named output.
    """

    # pick a remote URI
    if not source_.uri or is_file_uri(source_.uri):
        assert source_.cached_path, (
            f"Source with no URI must have a local path to assign a remote URI from: {source_}"
        )
        logger.info(f"Assigning remote URI for Source with local path {source_.cached_path}")
        remote_uri = mops_uri_assignment(source_.cached_path)
    else:
        remote_uri = source_.uri
        logger.debug("Using existing remote URI on Source %s", remote_uri)

    # check it for duplication because we're nice - but someday this needs to move to uri_assign.
    if remote_uri in existing_uris:
        raise DuplicateSourceBasenameError(
            f"Duplicate blob store URI {remote_uri} found in SourceResultPickler."
            " This is usually an indication that you have two files with the same name in two different directories,"
            " and are trying to convert them into Source objects with automatically-assigned URIs."
            " Per the documentation, all output Source objects must either have unique basenames or "
            " must use a URI assignment algorithm that can take directory structure into account."
        )

    # if we have a file path, make sure that gets sent to the uploader.
    if source_.cached_path and Path(source_.cached_path).exists():
        file_uri = to_uri(source_.cached_path)
        deferred_work.add(
            __name__ + "-chosen-source-result",
            remote_uri,
            partial(_put_file_to_blob_store, source_.cached_path, remote_uri),
        )
    else:
        if source_.cached_path:
            logger.warning(
                "Source has cached_path '%s' but file no longer exists. "
                "This often indicates a temporary file was deleted before mops could upload it. "
                "Consider using a persistent output directory (e.g., '.out/') instead of temp files. "
                "See mops Source documentation for recommended patterns.",
                source_.cached_path,
            )
        logger.debug("Creating SourceResult for URI %s that is presumed to be uploaded.", remote_uri)
        file_uri = ""

    return SourceResult(remote_uri, source_.hash, file_uri, source_.size)


def source_from_source_result(
    remote_uri: str, hash: ty.Optional[hashing.Hash], file_uri: str, size: int
) -> Source:
    """Call when deserializing a remote function return value on the orchestrator side, to
    replace all SourceResults with the intended Source object.
    """
    if not file_uri:
        return source.from_uri(remote_uri, hash=hash, size=size)

    local_path = source.path_from_uri(file_uri)

    try:
        file_exists = local_path.exists()
    except PermissionError:
        file_exists = False  # this will happen if one of the intermediate directories is not readable

    if file_exists:
        try:
            # since there's a remote URI, it's possible a specific consumer might want to
            # get access to that directly, even though the default data access would still
            # be to use the local file.
            return source.from_file(local_path, hash=hash, uri=remote_uri)

        except Exception as e:
            logger.warning(
                f"Unable to reuse destination local path {local_path} when constructing Source {remote_uri}: {e}"
            )
    return source.from_uri(remote_uri, hash=hash, size=size)


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
