from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient

from thds.core import cache

from . import conf
from .shared_credential import SharedCredential


def adls_fs_client(storage_account: str, container: str) -> FileSystemClient:
    """No context managers - is this better?"""
    return DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=SharedCredential(),
        max_single_get_size=conf.MAX_SINGLE_GET_SIZE(),  # for downloads
        max_chunk_get_size=conf.MAX_CHUNK_GET_SIZE(),  # for downloads
        max_block_size=conf.MAX_BLOCK_SIZE(),  # for uploads
    ).get_file_system_client(file_system=container)


"""Singletons are scary, but in practice this appears to be the
best approach for all applications.

This avoids creating a client at a module level and is
thread-safe.
"""
get_global_client = cache.locking(adls_fs_client)
# deprecated name - prefer get_global_fs_client
get_global_fs_client = get_global_client


def adls_blob_container_client(storage_account: str, container: str) -> ContainerClient:
    """This seems to support an atomic write operation,
    which is in theory going to be faster than two separate network requests.
    """
    return BlobServiceClient(
        account_url=f"https://{storage_account}.blob.core.windows.net",
        credential=SharedCredential(),
        max_single_get_size=conf.MAX_SINGLE_GET_SIZE(),  # for downloads
        max_chunk_get_size=conf.MAX_CHUNK_GET_SIZE(),  # for downloads
        max_block_size=conf.MAX_BLOCK_SIZE(),  # for uploads
        max_single_put_size=conf.MAX_SINGLE_PUT_SIZE(),  # for_uploads
    ).get_container_client(container)


get_global_blob_container_client = cache.locking(adls_blob_container_client)
