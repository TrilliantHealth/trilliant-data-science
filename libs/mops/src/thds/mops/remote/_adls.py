"""Mostly generic ADLS utilities for non-async operation."""
import base64
import logging
import typing as ty

import azure.core.exceptions
from azure.storage.blob import ContentSettings
from azure.storage.filedatalake import DataLakeFileClient, FileSystemClient

from thds.adls import AdlsFqn, join
from thds.core import scope
from thds.core.log import getLogger, logger_context

from ..colorize import colorized
from ..config import adls_skip_already_uploaded_check_if_smaller_than_bytes
from ..fretry import expo, retry_regular, sleep
from ._adls_shared import get_global_client
from ._md5 import try_md5
from ._on_slow import on_slow
from .types import AnyStrSrc, BlobStore

T = ty.TypeVar("T")
ToBytes = ty.Callable[[T, ty.BinaryIO], ty.Any]
FromBytes = ty.Callable[[ty.BinaryIO], T]


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


def _get_checksum_content_settings(data) -> ty.Optional[ContentSettings]:
    """Ideally, we calculate an MD5 sum for all data that we upload.

    The only circumstances under which we cannot do this are if the
    stream does not exist in its entirety before the upload begins.
    """
    md5 = try_md5(data)
    if md5:
        return ContentSettings(content_md5=md5)
    return None


def _content_settings_unless_checksum_already_present(
    fc: DataLakeFileClient, data: AnyStrSrc
) -> ty.Optional[ContentSettings]:
    local_content_settings = _get_checksum_content_settings(data)
    if not local_content_settings:
        return ContentSettings()  # nothing we can do
    try:
        if len(data) < adls_skip_already_uploaded_check_if_smaller_than_bytes():  # type: ignore
            logger.debug("Too small to bother with an early call - let's just upload...")
            return local_content_settings
    except TypeError:
        pass
    try:
        props = fc.get_file_properties()
        if props.content_settings.content_md5 == local_content_settings.content_md5:
            logger.info(f"Remote file {props.name} already exists and has matching checksum")
            return None
    except azure.core.exceptions.ResourceNotFoundError:
        pass  # just means the file doesn't exist remotely, so obviously the checksum doesn't match
    return local_content_settings


class BlobNotFoundError(Exception):
    def __init__(self, fqn: AdlsFqn, type_hint: str = "Blob"):
        super().__init__(f"{type_hint} not found: {fqn}")


def is_blob_not_found(exc: Exception) -> bool:
    return (
        isinstance(exc, azure.core.exceptions.HttpResponseError) and exc.status_code == 404
    ) or isinstance(exc, BlobNotFoundError)


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
    def read(self, remote_uri: str, stream: ty.IO[bytes], type_hint: str = "bytes"):
        fqn = AdlsFqn.parse(remote_uri)
        scope.enter(logger_context(download=fqn))
        logger.info(f"<----- downloading {type_hint}")
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
    def put(self, remote_uri: str, data: AnyStrSrc, type_hint: str = "bytes") -> str:
        """Upload data to a remote path and return the fully-qualified URI."""
        fqn = AdlsFqn.parse(remote_uri)
        scope.enter(logger_context(upload=fqn))
        file_client = self._client(fqn)
        content_settings_if_upload_needed = _content_settings_unless_checksum_already_present(
            file_client, data
        )
        if not content_settings_if_upload_needed:
            return remote_uri
        logger.info(f"======> uploading {type_hint}")
        on_slow(LOG_SLOW_TRANSFER_S, lambda secs: LogSlow(f"Took {int(secs)}s to upload {type_hint}"),)(
            lambda: file_client.upload_data(
                data,
                overwrite=True,
                content_settings=content_settings_if_upload_needed,
            )
        )()
        return remote_uri

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


def b64(digest: bytes) -> str:
    """This is the string representation used by ADLS.

    We use it in cases where we want to represent the same hash that
    ADLS will have in UTF-8 string (instead of bytes) format.
    """
    return base64.b64encode(digest).decode()
