from azure.storage.filedatalake import FileProperties


def is_directory(info: FileProperties) -> bool:
    # from https://github.com/Azure/azure-sdk-for-python/issues/24814#issuecomment-1159280840
    return str(info.metadata.get("hdi_isfolder", "")).lower() == "true"
