"""This module is roughly 10x as fast as ADLSFileSystem.get_directory_info.

Part of that is the parallelism, but part of it seems to be using the blob container
client instead of the file system client.
"""

import threading
import typing as ty

from azure.core.exceptions import ServiceRequestError, ServiceResponseError
from azure.storage.blob import BlobProperties

from thds.core import config, fretry, log, parallel, source, thunks

from . import blob_meta, global_client
from . import source as adls_source
from .fqn import AdlsFqn
from .uri import UriIsh, parse_any

R = ty.TypeVar("R")


logger = log.getLogger(__name__)

# Global ceiling on concurrent ADLS network calls across ALL of list_fast's
# parallelism. The fan-out is intentional (parallel subdir listings are ~10x
# faster than serial), but it is nested and recursive: one listing spawns a
# pool of per-subdir tasks, each of which spawns its own pool, and several
# listings (e.g. one per UA table) run at once. Capping each executor
# individually does nothing -- there are an unbounded number of executors, and
# they all share ONE cached connection pool per (account, container) of size
# `default_connection_pool_size` (default 100). When live threads exceed pool
# connections, the pool thrashes (connections discarded/reopened) and urllib3
# logs a "pool is full" WARNING per discard -- thousands per second, which is
# itself the CPU spin that locks the box up.
#
# A semaphore (rather than a bounded executor) is the right tool because it
# bounds in-flight *requests* regardless of how many threads/executors exist or
# how deeply they nest -- and it cannot deadlock the nested fan-out the way a
# fixed-size shared executor would (a parent task waiting on child tasks that
# can't get a worker). Threads still spawn freely; only this many may be issuing
# a request at once, so the pool is never oversubscribed. Sized to leave the
# pool a little headroom.
MAX_INFLIGHT_LISTING_REQUESTS = config.item("adls_max_inflight_listing_requests", default=80, parse=int)
_inflight = threading.Semaphore(MAX_INFLIGHT_LISTING_REQUESTS())

# Retry the spurious connection failures Azure raises (ServiceResponseError /
# ServiceRequestError wrap transport-level drops like BrokenPipe) -- the same
# "ConnectionError seems to be a thing now" failures downloads already retry
# (see download.py's _excs_to_retry). Without this, one dropped connection
# during a parallel listing reaches `failfast` and aborts the whole listing,
# cascading into "cannot schedule new futures after interpreter shutdown" as the
# nested executors tear down. NOT HttpResponseError -- that covers real 4xx/5xx.
# Applied to the operations that materialize a result, not the lazy generators.
_retry_listing = fretry.retry_regular(
    fretry.is_exc(ServiceResponseError, ServiceRequestError), fretry.n_times(3)
)


def _guarded(fn: ty.Callable[[], R]) -> R:
    """Run a single ADLS network call while holding a slot in the global
    in-flight semaphore, so total concurrent requests stay under the pool size."""
    with _inflight:
        return fn()


def _failfast_parallel(thunks: ty.Iterable[ty.Callable[[], R]]) -> ty.Iterator[R]:
    yield from (
        res
        for _, res in parallel.failfast(
            parallel.yield_all(parallel.create_keys(thunks), progress_logger=logger.debug)
        )
    )


@_retry_listing
def _direct_children(fqn: AdlsFqn) -> tuple[set[str], set[str]]:
    """Materialize (subdirs, files) directly under fqn. Retryable + guarded: the
    get_paths walk is consumed into sets here (so a transient drop retries this
    one directory's listing rather than escaping a half-consumed generator) while
    holding an in-flight slot."""

    def _walk() -> tuple[set[str], set[str]]:
        subdirs: set[str] = set()
        files: set[str] = set()
        for child in global_client.get_global_fs_client(fqn.sa, fqn.container).get_paths(
            path=fqn.path, recursive=False
        ):
            (subdirs if child.is_directory else files).add(child.name)

        return subdirs, files

    return _guarded(_walk)


def multilayer_yield_blob_properties(fqn: AdlsFqn, layers: int = 1) -> ty.Iterator[BlobProperties]:
    """A fast way to find all blobs in a directory tree; we do this in parallel on
    subdirs, with as much nesting as necessary (though 1 level should be enough for most cases).

    Does not maintain order.

    Does return directories; if you want to filter those out, you can use filter with is_dir.
    """
    if layers <= 0:
        # directly yield the blobs
        yield from blob_meta.yield_blob_props(
            global_client.get_global_blob_container_client(fqn.sa, fqn.container),
            fqn.path.rstrip("/") + "/",
        )
        return

    # This code seems more complex than it needs to be...  Why is it like this?
    #
    # Well, it's an optimization.  The underlying Azure API being used (i.e.,
    # call to list_blobs) limits the number of paths returned with each
    # invocation and each subsequent request must use a continuation token
    # received from the previous request to get the next "page" (it paginates serially).
    #
    # Running this sequentially for tens of thousands of blobs is slow if we just
    # iterate over the paths recursively. Therefore, we first list/get the
    # top-level subdirectories (not recursively), and then in parallel ask
    # the API to list the parquet paths of each subdirectory in parallel,
    # greatly speeding things up.
    #
    # Because there may be parquet files in the top-level directory, we also
    # gather those up and get `BlobProperties` for those individually (this is
    # what azure's SDK offers us as an option -_-.  At least we can do each of
    # these gets in parallel...), then convert each of them to a adls.source_tree.BlobMeta.
    #
    # API reference: https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list?view=rest-storageservices-datalakestoragegen2-2019-12-12

    # you cannot limit recursion depth using the container client, so to speed things up
    # we use the file system client to get the direct children, and then use the container client
    # to list within each final subdir
    subdirs, files = _direct_children(fqn)

    blob_container_client = global_client.get_global_blob_container_client(fqn.sa, fqn.container)

    @_retry_listing
    def _get_blob_props(blob_name: str) -> BlobProperties:
        return _guarded(lambda: blob_container_client.get_blob_client(blob_name).get_blob_properties())

    for blob_props_iter in (
        # only the top-level FILES need a per-blob properties fetch; subdirs are
        # enumerated by the recursive list below, so fetching their "blob"
        # properties is a wasted request (and there can be hundreds of them).
        _failfast_parallel(thunks.thunking(_get_blob_props)(name) for name in files),
        *(
            _failfast_parallel(
                # we use list_blobs (defined below) rather than yield here because returning
                # an iterator across a thread boundary doesn't work
                thunks.thunking(_list_blob_props)(AdlsFqn(fqn.sa, fqn.container, subdir), layers - 1)
                for subdir in subdirs
            )
        ),
    ):
        yield from blob_props_iter


def multilayer_yield_blob_meta(fqn: AdlsFqn, layers: int = 1) -> ty.Iterator[blob_meta.BlobMeta]:
    for blob_props in multilayer_yield_blob_properties(fqn, layers):
        yield blob_meta.to_blob_meta(blob_props)


@_retry_listing
def _list_blob_props(fqn: AdlsFqn, layers: int = 1) -> list[BlobProperties]:
    """Only for use within multilayer_yield_blob_properties. Materializes the
    listing (the `list(...)`) so @_retry_listing guards the whole iteration, not
    just the generator's construction.

    At the recursion base (layers<=0) this is one `list_blobs` paginated scan of a
    single leaf subdir, so we hold one in-flight slot for it. Above the base it
    just drives further nested listings that take their own slots, so we don't
    hold a slot here (that would double-count and could starve the nested calls)."""
    if layers <= 0:
        return _guarded(lambda: list(multilayer_yield_blob_properties(fqn, layers)))

    return list(multilayer_yield_blob_properties(fqn, layers))


def multilayer_yield_sources(
    fqn_or_uri: UriIsh,
    layers: int = 1,
    filter_: ty.Callable[[blob_meta.BlobMeta], bool] = lambda _: True,
) -> ty.Iterator[source.Source]:
    """
    if you want to list directories and files, use `multilayer_yield_blob_meta` instead
    """
    fqn = parse_any(fqn_or_uri)
    root = fqn.root()
    for blob in multilayer_yield_blob_meta(fqn, layers):
        if not blob_meta.is_dir(blob) and filter_(blob):
            # ^ a "dir" Source would not make sense
            yield adls_source.from_adls(root / blob.path, hash=blob.hash, size=blob.size)
