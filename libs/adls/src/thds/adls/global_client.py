import requests
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient

from thds.core import cache, config

from . import conf
from .shared_credential import SharedCredential

DEFAULT_CONNECTION_POOL_SIZE = config.item("default_connection_pool_size", default=100, parse=int)
# see docstring below about why we are setting this to be large.


def _connpool_session(connection_pool_size: int) -> requests.Session:
    """We do lot of multithreaded use of connections to ADLS. For large runs,
    having the default connection pool size of 10 is not enough, because each thread will create a
    new connection if an unused one is not available from the pool, and we often have more than 10 threads
    communicating at a time.

    The connection pool will not start out this large, so don't fear that we're introducing new overhead here.
    We will just throw away fewer connections that were used only once and replaced by a newer one.

    The only reason you'd really want to keep this low is if the host you're talking to
    can't handle this many simultaneous connections, but my belief is that ADLS should be
    able to handle thousands (probably hundreds of thousands?) of connections at once.
    """
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=connection_pool_size, pool_maxsize=connection_pool_size
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def adls_fs_client(
    storage_account: str, container: str, connpool_size: int = DEFAULT_CONNECTION_POOL_SIZE()
) -> FileSystemClient:
    """No context managers - is this better?"""
    return DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=SharedCredential(),
        session=_connpool_session(connection_pool_size=connpool_size),
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


def adls_blob_container_client(
    storage_account: str, container: str, connpool_size: int = DEFAULT_CONNECTION_POOL_SIZE()
) -> ContainerClient:
    """This seems to support an atomic write operation,
    which is in theory going to be faster than two separate network requests.
    """
    return BlobServiceClient(
        account_url=f"https://{storage_account}.blob.core.windows.net",
        credential=SharedCredential(),
        session=_connpool_session(connection_pool_size=connpool_size),
        max_single_get_size=conf.MAX_SINGLE_GET_SIZE(),  # for downloads
        max_chunk_get_size=conf.MAX_CHUNK_GET_SIZE(),  # for downloads
        max_block_size=conf.MAX_BLOCK_SIZE(),  # for uploads
        max_single_put_size=conf.MAX_SINGLE_PUT_SIZE(),  # for_uploads
    ).get_container_client(container)


get_global_blob_container_client = cache.locking(adls_blob_container_client)
