from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.storage.blob import BlobProperties
from azure.storage.filedatalake import FileProperties

from .errors import translate_azure_error
from .fqn import AdlsFqn
from .global_client import get_global_blob_container_client, get_global_fs_client


def is_directory(info: FileProperties) -> bool:
    # from https://github.com/Azure/azure-sdk-for-python/issues/24814#issuecomment-1159280840
    return str(info.get("metadata", dict()).get("hdi_isfolder", "")).lower() == "true"


def get_file_properties(fqn: AdlsFqn) -> FileProperties:
    return get_global_fs_client(fqn.sa, fqn.container).get_file_client(fqn.path).get_file_properties()


def get_blob_properties(fqn: AdlsFqn) -> BlobProperties:
    return (
        get_global_blob_container_client(fqn.sa, fqn.container)
        .get_blob_client(fqn.path)
        .get_blob_properties()
    )


# At some point it may make sense to separate file and blob property modules,
# but they also are very closely tied together. AFAIK all files are blobs, and given our usage of ADLS,
# I don't know if we ever deal with things that are blobs but not files.


def exists(fqn: AdlsFqn) -> bool:
    try:
        get_blob_properties(fqn)
        # could generally use `get_file_properties` interchangeably,
        # but blobs are a lower-level primitive than the ADLS file abstraction
        return True
    except ResourceNotFoundError:
        return False
    except AzureError as err:
        translate_azure_error(get_global_fs_client(fqn.sa, fqn.container), fqn.path, err)
