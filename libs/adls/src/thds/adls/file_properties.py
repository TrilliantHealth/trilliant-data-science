from azure.storage.filedatalake import FileProperties

from .fqn import AdlsFqn
from .global_client import get_global_fs_client


def is_directory(info: FileProperties) -> bool:
    # from https://github.com/Azure/azure-sdk-for-python/issues/24814#issuecomment-1159280840
    return str(info.metadata.get("hdi_isfolder", "")).lower() == "true"


def get_file_properties(fqn: AdlsFqn) -> FileProperties:
    return get_global_fs_client(fqn.sa, fqn.container).get_file_client(fqn.path).get_file_properties()
