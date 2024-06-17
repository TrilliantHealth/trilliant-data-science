import threading
import typing as ty

from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient

from .conf import MAX_BLOCK_SIZE, MAX_CHUNK_GET_SIZE, MAX_SINGLE_GET_SIZE
from .shared_credential import SharedCredential


def adls_fs_client(storage_account: str, container: str) -> FileSystemClient:
    """No context managers - is this better?"""
    return DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=SharedCredential(),
        max_single_get_size=MAX_SINGLE_GET_SIZE(),  # for downloads
        max_chunk_get_size=MAX_CHUNK_GET_SIZE(),  # for downloads
        max_block_size=MAX_BLOCK_SIZE(),  # for uploads
    ).get_file_system_client(file_system=container)


_LOCK = threading.Lock()
_GLOBAL_CLIENTS: ty.Dict[ty.Tuple[str, str], FileSystemClient] = dict()


def get_global_client(sa: str, container: str) -> FileSystemClient:
    """Singletons are scary, but in practice this appears to be the
    best approach for all applications.

    This avoids creating a client at a module level and is
    thread-safe.
    """
    key = (sa, container)
    if key not in _GLOBAL_CLIENTS:
        with _LOCK:
            if key not in _GLOBAL_CLIENTS:
                _GLOBAL_CLIENTS[key] = adls_fs_client(*key)
    return _GLOBAL_CLIENTS[key]
