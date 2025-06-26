import typing as ty
from contextlib import contextmanager

from azure.core.exceptions import AzureError, HttpResponseError

from thds.core.log import getLogger

from .fqn import AdlsFqn

logger = getLogger(__name__)


class BlobNotFoundError(HttpResponseError):
    def __init__(self, fqn: AdlsFqn, type_hint: str = "Blob"):
        super().__init__(f"{type_hint} not found: {fqn}")


class BlobPropertiesValidationError(ValueError):
    """Raised when the properties of a blob do not match the expected values."""


class HashMismatchError(BlobPropertiesValidationError):
    """Raised when the hash of a file does not match the expected value."""


class ContentLengthMismatchError(BlobPropertiesValidationError):
    """Raised when the content length of a file does not match the expected value as retrieved from the server."""


def is_blob_not_found(exc: Exception) -> bool:
    return (isinstance(exc, HttpResponseError) and exc.status_code == 404) or isinstance(
        exc, BlobNotFoundError
    )


def translate_blob_not_found(hre: HttpResponseError, sa: str, container: str, path: str) -> ty.NoReturn:
    if is_blob_not_found(hre):
        raise BlobNotFoundError(AdlsFqn.of(sa, container, path)) from hre
    raise hre


@contextmanager
def blob_not_found_translation(fqn: AdlsFqn) -> ty.Iterator[None]:
    try:
        yield
    except HttpResponseError as hre:
        translate_blob_not_found(hre, *fqn)


def translate_azure_error(client, key: str, err: AzureError) -> ty.NoReturn:
    """We reserve the right to translate others in the future."""
    fqn = AdlsFqn.of(client.account_name, client.file_system_name, key)
    if is_blob_not_found(err):
        raise BlobNotFoundError(fqn) from err
    logger.error("Failed when operating on %s", fqn)
    raise err
