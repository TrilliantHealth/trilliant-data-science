from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient

from ._azure import SharedCredential


def adls_fs_client(storage_account: str, container: str) -> FileSystemClient:
    """No context managers - is this better?"""
    return DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=SharedCredential,
    ).get_file_system_client(file_system=container)
