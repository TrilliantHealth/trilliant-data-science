"""Mostly generic ADLS utilities for non-async operation."""
import logging
import typing as ty
from pathlib import Path

import azure.core.exceptions
from azure.storage.filedatalake import DataLakeFileClient, FileSystemClient

from thds.adls import AdlsFqn, AdlsRoot, join, resource
from thds.adls._upload import upload_decision_and_settings
from thds.adls.cached_up_down import download_to_cache, upload_through_cache
from thds.adls.errors import BlobNotFoundError, is_blob_not_found
from thds.adls.global_client import get_global_client
from thds.core import scope
from thds.core.log import getLogger, logger_context

from ..colorize import colorized
from ..fretry import expo, retry_regular, sleep
from ._on_slow import on_slow
from .types import AnyStrSrc, BlobStore

T = ty.TypeVar("T")
ToBytes = ty.Callable[[T, ty.BinaryIO], ty.Any]
FromBytes = ty.Callable[[ty.BinaryIO], T]
_5_MB = 5 * 2**20
_SLOW_CONNECTION_WORKAROUND = 14400  # seconds

# suppress very noisy INFO logs in azure library.
# This mirrors thds.adls.
getLogger("azure.core").setLevel(logging.WARNING)

logger = getLogger(__name__)


def yield_files(fsc: FileSystemClient, adls_root: str) -> ty.Iterable[ty.Any]:
    """Yield files (including directories) from the root."""
    with fsc as client:
        try:
            yield from client.get_paths(adls_root)
        except azure.core.exceptions.ResourceNotFoundError as rnfe:
            if rnfe.response.status_code == 404:
                return  # no paths
            raise


def yield_filenames(fsc: FileSystemClient, adls_root: str) -> ty.Iterable[str]:
    """Yield only real file (not directory) names recursively from the root."""
    for azure_file in yield_files(fsc, adls_root):
        if not azure_file.get("is_directory"):
            yield azure_file["name"]


def _selective_upload_path(path: Path, fqn: AdlsFqn):
    if path.stat().st_size > _5_MB:
        upload_through_cache(fqn, path)
    else:
        resource.upload(fqn, path)


def is_creds_failure(exc: Exception) -> bool:
    return isinstance(exc, azure.core.exceptions.HttpResponseError) and not is_blob_not_found(exc)


_azure_creds_retry = retry_regular(is_creds_failure, sleep(expo(retries=9, delay=1.0)))
# sometimes Azure Cli credentials expire but would succeed if retried
# and the azure library does not seem to retry these on its own.

LOG_SLOW_TRANSFER_S = 3  # seconds
_SLOW = colorized(fg="yellow", bg="black")
LogSlow = lambda s: logger.warning(_SLOW(s))  # noqa: E731


class AdlsFileSystem(BlobStore):
    def _client(self, fqn: AdlsFqn) -> DataLakeFileClient:
        return get_global_client(fqn.sa, fqn.container).get_file_client(fqn.path)

    @_azure_creds_retry
    @scope.bound
    def readbytesinto(self, remote_uri: str, stream: ty.IO[bytes], type_hint: str = "bytes"):
        fqn = AdlsFqn.parse(remote_uri)
        scope.enter(logger_context(download=fqn))
        logger.debug(f"<----- downloading {type_hint}")
        try:
            on_slow(
                LOG_SLOW_TRANSFER_S,
                lambda elapsed_s: LogSlow(f"Took {int(elapsed_s)}s to download {type_hint}"),
            )(lambda: self._client(fqn).download_file().readinto(stream))()
        except azure.core.exceptions.HttpResponseError as e:
            if is_blob_not_found(e):
                raise BlobNotFoundError(fqn, type_hint) from e
            raise

    @_azure_creds_retry
    @scope.bound
    def getfile(self, remote_uri: str) -> Path:
        scope.enter(logger_context(download="mops-getfile"))
        return download_to_cache(AdlsFqn.parse(remote_uri))

    @_azure_creds_retry
    @scope.bound
    def putbytes(self, remote_uri: str, data: AnyStrSrc, type_hint: str = "bytes"):
        """Upload data to a remote path."""
        fqn = AdlsFqn.parse(remote_uri)
        scope.enter(logger_context(upload=fqn))
        file_client = self._client(fqn)
        decision = upload_decision_and_settings(file_client, data)
        if not decision.upload_required:
            return remote_uri
        logger.info(f"======> uploading {type_hint}")
        on_slow(LOG_SLOW_TRANSFER_S, lambda secs: LogSlow(f"Took {int(secs)}s to upload {type_hint}"))(
            lambda: file_client.upload_data(
                data,
                overwrite=True,
                connection_timeout=_SLOW_CONNECTION_WORKAROUND,
                content_settings=decision.content_settings,
            )
        )()
        return remote_uri

    @_azure_creds_retry
    @scope.bound
    def putfile(self, path: Path, remote_uri: str):
        scope.enter(logger_context(upload="mops-putfile"))
        _selective_upload_path(path, AdlsFqn.parse(remote_uri))

    @_azure_creds_retry
    @scope.bound
    def exists(self, remote_uri: str) -> bool:
        fqn = AdlsFqn.parse(remote_uri)
        scope.enter(logger_context(exists=fqn))
        return on_slow(
            1.2,
            lambda secs: LogSlow(f"Took {int(secs)}s to check if file exists."),
        )(lambda: self._client(fqn).exists())()

    def join(self, *parts: str) -> str:
        return join(*parts)

    def is_blob_not_found(self, exc: Exception) -> bool:
        return is_blob_not_found(exc)


UriIsh = ty.Union[AdlsRoot, AdlsFqn, str]
UriResolvable = ty.Union[UriIsh, ty.Callable[[], UriIsh]]


def to_lazy_uri(resolvable: UriResolvable) -> ty.Callable[[], str]:
    if isinstance(resolvable, str):
        return lambda: str(resolvable)
    if isinstance(resolvable, (AdlsRoot, AdlsFqn)):
        return lambda: str(resolvable)
    return lambda: str(resolvable())  # type: ignore
