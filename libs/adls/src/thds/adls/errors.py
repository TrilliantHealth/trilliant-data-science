import azure.core.exceptions

from .fqn import AdlsFqn


class BlobNotFoundError(azure.core.exceptions.HttpResponseError):
    def __init__(self, fqn: AdlsFqn, type_hint: str = "Blob"):
        super().__init__(f"{type_hint} not found: {fqn}")


def is_blob_not_found(exc: Exception) -> bool:
    return (
        isinstance(exc, azure.core.exceptions.HttpResponseError) and exc.status_code == 404
    ) or isinstance(exc, BlobNotFoundError)
