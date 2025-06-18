import typing as ty
from dataclasses import dataclass

from azure.storage.blob import BlobProperties, ContainerClient

from thds.core import hashing
from thds.core.source.tree import SourceTree

from . import fqn, global_client, source, uri


@dataclass
class BlobMeta:
    path: str
    size: int
    md5: ty.Optional[hashing.Hash]


def to_blob_meta(blob_props: BlobProperties) -> BlobMeta:
    return BlobMeta(
        blob_props.name,
        blob_props.size,
        (
            hashing.Hash("md5", bytes(blob_props.content_settings.content_md5))
            if blob_props.content_settings.content_md5
            else None
        ),
    )


# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python#azure-storage-blob-containerclient-list-blobs
# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobproperties?view=azure-python
# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.contentsettings?view=azure-python
def list_blob_meta(
    container_client: ContainerClient, root_dir: str, match_suffix: str = ""
) -> ty.List[BlobMeta]:
    """Gets the path (relative to the SA/container root), size, and MD5 hash of all blobs in a directory."""
    return [
        to_blob_meta(blob_props)
        for blob_props in container_client.list_blobs(name_starts_with=root_dir)
        if blob_props.size > 0
        # container client lists directories as blobs with size 0
        and blob_props.name.endswith(match_suffix)
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
            source.from_adls(container_root / blob_meta.path, hash=blob_meta.md5)
            for blob_meta in list_blob_meta(container_client, root_fqn.path, match_suffix=match_suffix)
        ],
        higher_logical_root=fqn.split(root_fqn)[-1],
    )
