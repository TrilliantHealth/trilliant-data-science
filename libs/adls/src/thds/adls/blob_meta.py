import typing as ty
from dataclasses import dataclass

from azure.storage.blob import BlobProperties, ContainerClient

from thds.core import hashing

from . import hashes


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


def is_dir(blob_meta: BlobMeta) -> bool:
    return blob_meta.metadata.get("hdi_isfolder", "false") == "true"


# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python#azure-storage-blob-containerclient-list-blobs
# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobproperties?view=azure-python
# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.contentsettings?view=azure-python
def yield_blob_meta(container_client: ContainerClient, root_dir: str) -> ty.Iterator[BlobMeta]:
    for blob_props in container_client.list_blobs(name_starts_with=root_dir, include=["metadata"]):
        # `list_blobs` does not include metadata by default, so we need to explicitly specify including it
        yield to_blob_meta(blob_props)
