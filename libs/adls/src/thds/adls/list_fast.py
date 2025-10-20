"""This module is roughly 10x as fast as ADLSFileSystem.get_directory_info.

Part of that is the parallelism, but part of it seems to be using the blob container
client instead of the file system client.
"""

import typing as ty

from thds.core import log, parallel, source, thunks

from . import blob_meta, global_client
from . import source as adls_source
from .fqn import AdlsFqn
from .uri import UriIsh, parse_any

R = ty.TypeVar("R")


logger = log.getLogger(__name__)


def _failfast_parallel(thunks: ty.Iterable[ty.Callable[[], R]]) -> ty.Iterator[R]:
    yield from (
        res
        for _, res in parallel.failfast(
            parallel.yield_all(parallel.create_keys(thunks), progress_logger=logger.debug)
        )
    )


def multilayer_yield_blob_meta(fqn: AdlsFqn, layers: int = 1) -> ty.Iterator[blob_meta.BlobMeta]:
    """A fast way to find all blobs in a directory tree; we do this in parallel on
    subdirs, with as much nesting as necessary (though 1 level should be enough for most cases).

    Does not maintain order.

    Does return directories; if you want to filter those out, you can use filter with is_dir.
    """
    if layers <= 0:
        # directly yield the blobs
        yield from blob_meta.yield_blob_meta(
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
    subdirs, files = set(), set()  # direct children of the top-level directory
    for child_path_props in global_client.get_global_fs_client(fqn.sa, fqn.container).get_paths(
        path=fqn.path, recursive=False
    ):
        if child_path_props.is_directory:
            subdirs.add(child_path_props.name)
        else:
            files.add(child_path_props.name)

    blob_container_client = global_client.get_global_blob_container_client(fqn.sa, fqn.container)

    def _get_blob_meta(blob_name: str) -> blob_meta.BlobMeta:
        return blob_meta.to_blob_meta(
            blob_container_client.get_blob_client(blob_name).get_blob_properties()
        )

    for blob_meta_iter in (
        _failfast_parallel((thunks.thunking(_get_blob_meta)(file) for file in files)),
        *(
            _failfast_parallel(
                # we use list_blobs (defined below) rather than yield here because returning
                # an iterator across a thread boundary doesn't work
                thunks.thunking(_list_blob_meta)(AdlsFqn(fqn.sa, fqn.container, subdir), layers - 1)
                for subdir in subdirs
            )
        ),
    ):
        yield from blob_meta_iter


def _list_blob_meta(fqn: AdlsFqn, layers: int = 1) -> list[blob_meta.BlobMeta]:
    """Only for use within multi_layer_yield_blobs."""
    return list(multilayer_yield_blob_meta(fqn, layers))


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
