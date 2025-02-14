import concurrent.futures
import os
import typing as ty
from dataclasses import dataclass
from pathlib import Path

from azure.storage.blob import ContainerClient

from thds.core import Source, hashing, logical_root, parallel

from . import fqn, global_client, source, uri


@dataclass
class BlobMeta:
    path: str
    size: int
    md5: ty.Optional[hashing.Hash]


# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python#azure-storage-blob-containerclient-list-blobs
# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobproperties?view=azure-python
# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.contentsettings?view=azure-python
def list_blob_meta(container_client: ContainerClient, root_dir: str) -> ty.List[BlobMeta]:
    """Gets the path (relative to the SA/container root), size, and MD5 hash of all blobs in a directory."""
    return [
        BlobMeta(
            blob_props.name,  # type: ignore
            blob_props.size,  # type: ignore
            (
                hashing.Hash("md5", bytes(blob_props.content_settings.content_md5))
                if blob_props.content_settings.content_md5
                else None
            ),
        )
        for blob_props in container_client.list_blobs(name_starts_with=root_dir)
        if blob_props.size > 0  # type: ignore  # container client lists directories as blobs with size 0
    ]


_MAX_DOWNLOAD_PARALLELISM = 90


@dataclass
class SourceTree(os.PathLike):
    """Represent a fixed set of sources (with hashes where available) as a list of
    sources, plus the (optional) logical root of the tree, so that they can be 'unwrapped'
    as a local directory structure.
    """

    sources: ty.List[Source]
    higher_logical_root: str = ""
    # there may be cases where, rather than identifying the 'lowest common prefix'
    # of a set of sources/URIs, we may wish to represent a 'higher' root for the sake of some consuming system.
    # in those cases, this can be specified and we'll find the lowest common prefix _above_ that.

    def path(self) -> Path:
        """Return a local path to a directory that corresponds to the logical root.

        This incurs a download of _all_ sources explicitly represented by the list.
        """
        return Path(
            logical_root.find(
                (
                    str(p)
                    for _, p in parallel.failfast(
                        parallel.yield_all(
                            ((src, src.path) for src in self.sources),
                            executor_cm=concurrent.futures.ThreadPoolExecutor(
                                max_workers=_MAX_DOWNLOAD_PARALLELISM
                            ),
                        )
                    )
                ),
                self.higher_logical_root,
            )
        )

    def __fspath__(self) -> str:  # implement the os.PathLike protocol
        return str(self.path())


def from_path(adls_path: uri.UriIsh) -> SourceTree:
    """Creates a SourceTree object where the logical root is the final piece of the
    provided adls path.
    """
    root_fqn = uri.parse_any(adls_path)

    container_client = global_client.get_global_blob_container_client(root_fqn.sa, root_fqn.container)
    container_root = root_fqn.root()
    return SourceTree(
        sources=[
            source.from_adls(container_root / blob_meta.path, hash=blob_meta.md5)
            for blob_meta in list_blob_meta(container_client, root_fqn.path)
        ],
        higher_logical_root=fqn.split(root_fqn)[-1],
    )
