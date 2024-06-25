import threading
import typing as ty

from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient

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


_LOCK = threading.Lock()
C = ty.TypeVar("C")


def _mk_get_global_client(
    clients: ty.Dict[ty.Tuple[str, str], C], mk_client: ty.Callable[[str, str], C]
) -> ty.Callable[[str, str], C]:
    def get_global_client(
        sa: str,
        container: str,
    ) -> C:
        """Singletons are scary, but in practice this appears to be the
        best approach for all applications.

        This avoids creating a client at a module level and is
        thread-safe.
        """
        key = (sa, container)
        if key not in clients:
            with _LOCK:
                if key not in clients:
                    clients[key] = mk_client(*key)
        return clients[key]

    return get_global_client


_GLOBAL_FS_CLIENTS: ty.Dict[ty.Tuple[str, str], FileSystemClient] = dict()
get_global_client = _mk_get_global_client(_GLOBAL_FS_CLIENTS, adls_fs_client)
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


_GLOBAL_BLOB_CLIENTS: ty.Dict[ty.Tuple[str, str], ContainerClient] = dict()
get_global_blob_container_client = _mk_get_global_client(
    _GLOBAL_BLOB_CLIENTS, adls_blob_container_client
)
