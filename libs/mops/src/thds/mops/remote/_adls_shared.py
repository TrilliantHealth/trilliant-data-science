import threading
import typing as ty

from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient

from ._azure import SharedCredential


def adls_fs_client(storage_account: str, container: str) -> FileSystemClient:
    """No context managers - is this better?"""
    return DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=SharedCredential,
    ).get_file_system_client(file_system=container)


_LOCK = threading.Lock()
_GLOBAL_CLIENTS: ty.Dict[ty.Tuple[str, str], FileSystemClient] = dict()


def get_global_client(sa: str, container: str) -> FileSystemClient:
    """Arguably this should not be used directly, but in practice this
    appears to be the best approach for all applications.
    """
    key = (sa, container)
    if key not in _GLOBAL_CLIENTS:
        with _LOCK:
            if key not in _GLOBAL_CLIENTS:
                _GLOBAL_CLIENTS[key] = adls_fs_client(*key)
    return _GLOBAL_CLIENTS[key]
