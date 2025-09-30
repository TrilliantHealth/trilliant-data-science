import typing as ty
from dataclasses import dataclass

from azure.storage.blob import BlobProperties, ContainerClient

from thds.core import hashing
from thds.core.source.tree import SourceTree

from . import fqn, global_client, hashes, source, uri

# TODO refactor BlobMeta into its own module.


@dataclass
class BlobMeta:
    path: str
    size: int
    hash: ty.Optional[hashing.Hash]
    metadata: dict[str, str]


def to_blob_meta(blob_props: BlobProperties) -> BlobMeta:
    return BlobMeta(
        blob_props.name,
        blob_props.size,
        next(iter(hashes.extract_hashes_from_props(blob_props).values()), None),
        blob_props.metadata or {},
    )


def yield_blob_meta(container_client: ContainerClient, root_dir: str) -> ty.Iterator[BlobMeta]:
    for blob_props in container_client.list_blobs(name_starts_with=root_dir, include=["metadata"]):
        # `list_blobs` does not include metadata by default, so we need to explicitly specify including it
        yield to_blob_meta(blob_props)


# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python#azure-storage-blob-containerclient-list-blobs
# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobproperties?view=azure-python
# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.contentsettings?view=azure-python
def list_blob_meta(
    container_client: ContainerClient, root_dir: str, match_suffix: str = ""
) -> ty.List[BlobMeta]:
    """Gets the path (relative to the SA/container root), size, and _a_ hash (if available) of all blobs in a directory."""
    return [
        blob_meta
        for blob_meta in yield_blob_meta(container_client, root_dir)
        if blob_meta.size > 0
        # container client lists directories as blobs with size 0
        and blob_meta.path.endswith(match_suffix)
    ]


def from_path(adls_path: uri.UriIsh, match_suffix: str = "") -> SourceTree:
    """Creates a SourceTree object where the logical root is the final piece of the
    provided adls path.
    """
    root_fqn = uri.parse_any(adls_path)

    container_client = global_client.get_global_blob_container_client(root_fqn.sa, root_fqn.container)
    container_root = root_fqn.root()
    return SourceTree(
        sources=[
            source.from_adls(container_root / blob_meta.path, hash=blob_meta.hash)
            for blob_meta in list_blob_meta(container_client, root_fqn.path, match_suffix=match_suffix)
        ],
        higher_logical_root=fqn.split(root_fqn)[-1],
    )
