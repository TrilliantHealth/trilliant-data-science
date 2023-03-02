"""Mostly generic ADLS utilities for non-async operation."""
import io
import json
import logging
import os
import typing as ty

import azure.core.exceptions
from azure.storage.blob import ContentSettings
from azure.storage.filedatalake import DataLakeFileClient, FileSystemClient

from thds.adls import AdlsFqn
from thds.core import scope
from thds.core.log import getLogger, logger_context

from ..colorize import colorized
from ..config import adls_skip_already_uploaded_check_if_smaller_than_bytes
from ..fretry import expo, retry_regular, sleep
from ._adls_shared import adls_fs_client
from ._md5 import md5_something
from ._on_slow import on_slow
from .remote_file import Serialized

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


AnyStrSrc = ty.Union[ty.AnyStr, ty.Iterable[ty.AnyStr], ty.IO[ty.AnyStr]]


def _get_checksum_content_settings(data) -> ty.Optional[ContentSettings]:
    """Ideally, we calculate an MD5 sum for all data that we upload.

    The only circumstances under which we cannot do this are if the
    stream does not exist in its entirety before the upload begins.
    """
    md5 = md5_something(data)
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
    def __init__(self, sa: str, container: str, path: str, type_hint: str = "Blob"):
        super().__init__(f"{type_hint} not found: {AdlsFqn(sa, container, path)}")


def is_blob_not_found(exc: Exception) -> bool:
    return (
        isinstance(exc, azure.core.exceptions.HttpResponseError) and exc.status_code == 404
    ) or isinstance(exc, BlobNotFoundError)


def is_creds_failure(exc: Exception) -> bool:
    return isinstance(exc, azure.core.exceptions.HttpResponseError) and not is_blob_not_found(exc)


_azure_creds_retry = retry_regular(is_creds_failure, sleep(expo(tries=10, delay=1.0)))
# sometimes Azure Cli credentials expire but would succeed if retried
# and the azure library does not seem to retry these on its own.

LOG_SLOW_TRANSFER_S = 3  # seconds
_SLOW = colorized(fg="yellow", bg="black")
LogSlow = lambda s: logger.warning(_SLOW(s))  # noqa: E731


class AdlsFileSystem:
    def __init__(self, storage_account: str, container: str):
        self.storage_account = storage_account
        self.container = container
        self._client = adls_fs_client(storage_account, container)

    @property
    def client(self) -> FileSystemClient:
        return self._client

    def __getstate__(self):
        """AdlsFileSystem needs to be picklable because it gets
        transmitted to the remote for use passing things back.  But
        the _client cannot be pickled since it contains a lock, so we
        need to remove that before pickling.
        """
        return {k: v for k, v in self.__dict__.items() if k != "_client"}

    def __setstate__(self, d):
        self.__dict__.update(d)
        self._client = adls_fs_client(self.storage_account, self.container)

    @_azure_creds_retry
    @scope.bound
    def readinto(self, remote_path: str, stream: ty.IO[bytes], type_hint: str = "bytes"):
        scope.enter(logger_context(download=remote_path))
        logger.info(f"<----- downloading {type_hint} from {self.storage_account} {self.container}")
        try:
            on_slow(
                LOG_SLOW_TRANSFER_S,
                lambda elapsed_s: LogSlow(f"Took {int(elapsed_s)}s to download {type_hint}"),
            )(lambda: self.client.get_file_client(remote_path).download_file().readinto(stream))()
        except azure.core.exceptions.HttpResponseError as e:
            if is_blob_not_found(e):
                raise BlobNotFoundError(
                    type_hint, self.storage_account, self.container, remote_path
                ) from e
            raise

    @_azure_creds_retry
    @scope.bound
    def put_bytes(self, remote_path: str, data: AnyStrSrc, type_hint: str = "bytes"):
        scope.enter(logger_context(upload=remote_path))
        file_client = self.client.get_file_client(remote_path)
        content_settings_if_upload_needed = _content_settings_unless_checksum_already_present(
            file_client, data
        )
        if not content_settings_if_upload_needed:
            return
        logger.info(f"======> uploading {type_hint} to {self.storage_account} {self.container}")
        on_slow(LOG_SLOW_TRANSFER_S, lambda secs: LogSlow(f"Took {int(secs)}s to upload {type_hint}"),)(
            lambda: file_client.upload_data(
                data,
                overwrite=True,
                content_settings=content_settings_if_upload_needed,
            )
        )()

    @_azure_creds_retry
    @scope.bound
    def file_exists(self, remote_path: str) -> bool:
        scope.enter(logger_context(exists=remote_path))
        return on_slow(
            1.2,
            lambda secs: LogSlow(f"Took {int(secs)}s to check if file exists."),
        )(lambda: self.client.get_file_client(remote_path).exists())()

    def get_bytes(self, remote_path: str, type_hint: str = "bytes") -> bytes:
        with io.BytesIO() as tb:
            self.readinto(remote_path, tb, type_hint=type_hint)
            tb.seek(0)
            return tb.read()


def represent_adls_path(storage_account: str, container: str, key: str) -> Serialized:
    """Canonical fully-qualified representation for a given ADLS file."""
    assert '{"type": "ADLS"' not in key, key
    return Serialized(json.dumps(dict(type="ADLS", sa=storage_account, container=container, key=key)))


def join(prefix: str, suffix: str) -> str:
    prefix = prefix.rstrip("/")
    suffix = suffix.lstrip("/")
    return f"{prefix}/{suffix}"


def download_to(fs: AdlsFileSystem, key: str, local_dest: os.PathLike):
    with open(local_dest, "wb") as file_:
        fs.readinto(key, file_)


def upload_to(fs: AdlsFileSystem, key: str, local_src: os.PathLike):
    with open(local_src, "rb") as f:
        fs.put_bytes(key, f)


def upload_and_represent(
    sa: str, container: str, directory: str, relative_path: str, local_src: os.PathLike
) -> Serialized:
    key = join(directory, relative_path)
    upload_to(AdlsFileSystem(sa, container), key, local_src)
    return represent_adls_path(sa, container, key)
