import typing as ty
from contextlib import contextmanager

from azure.core.exceptions import HttpResponseError

from .fqn import AdlsFqn


class BlobNotFoundError(HttpResponseError):
    def __init__(self, fqn: AdlsFqn, type_hint: str = "Blob"):
        super().__init__(f"{type_hint} not found: {fqn}")


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
