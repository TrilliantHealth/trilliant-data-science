"""Mostly generic ADLS utilities for non-async operation."""
import contextlib
import io
import json
import logging
import os
import threading
import typing as ty
from timeit import default_timer

import azure.core.exceptions
from azure.storage.blob import ContentSettings
from azure.storage.filedatalake import DataLakeFileClient, DataLakeServiceClient, FileSystemClient
from retry import retry

from thds.core.log import getLogger

from ..config import adls_max_clients, adls_skip_already_uploaded_check_if_smaller_than_bytes
from ._azure import SharedCredential
from ._md5 import md5_something
from .remote_file import Serialized

T = ty.TypeVar("T")
ToBytes = ty.Callable[[T, ty.BinaryIO], ty.Any]
FromBytes = ty.Callable[[ty.BinaryIO], T]


# suppress very noisy INFO logs in azure library.
# This mirrors thds.adls.
getLogger("azure.core").setLevel(logging.WARNING)

logger = getLogger(__name__)

_SimultaneousDownloadsSemaphore = threading.BoundedSemaphore(int(adls_max_clients()))
_SimultaneousUploadsSemaphore = threading.BoundedSemaphore(int(adls_max_clients()))


@contextlib.contextmanager
def AdlsFileSystemClient(storage_account: str, container: str) -> ty.Iterator[FileSystemClient]:
    # exclude_shared_token_cache_credential=True avoids MacOS keychain prompts
    with SharedCredential as credential:
        service_client = DataLakeServiceClient(
            account_url=f"https://{storage_account}.dfs.core.windows.net",
            credential=credential,
        )
        with service_client:
            with service_client.get_file_system_client(file_system=container) as file_system_client:
                yield file_system_client


def yield_files(fsc: ty.ContextManager[FileSystemClient], adls_root: str) -> ty.Iterable[ty.Any]:
    """Yield files (including directories) from the root."""
    with fsc as client:
        try:
            yield from client.get_paths(adls_root)
        except azure.core.exceptions.ResourceNotFoundError as rnfe:
            if rnfe.response.status_code == 404:
                return  # no paths
            raise


def yield_filenames(fsc: ty.ContextManager[FileSystemClient], adls_root: str) -> ty.Iterable[str]:
    """Yield only real file (not directory) names recursively from the root."""
    for azure_file in yield_files(fsc, adls_root):
        if not azure_file.get("is_directory"):
            yield azure_file["name"]


@contextlib.contextmanager
def _AdlsFileClient(
    storage_account: str, container: str, remote_path: str
) -> ty.Iterator[DataLakeFileClient]:
    with AdlsFileSystemClient(storage_account, container) as fs:
        with fs.get_file_client(remote_path) as file_client:
            yield file_client


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


_azure_creds_retry = retry(
    exceptions=azure.core.exceptions.HttpResponseError,
    tries=10,
    backoff=2,
    delay=1.0,
    logger=logger,  # type: ignore
)
# sometimes Azure Cli credentials expire but would succeed if retried
# and the azure library does not retry these on its own.

LOG_SLOW_TRANSFER_S = 10  # seconds


class AdlsFileSystem:
    def __init__(self, storage_account: str, container: str):
        self.storage_account = storage_account
        self.container = container

    @_azure_creds_retry
    def readinto(self, remote_path: str, stream: ty.IO[bytes], type_hint: str = "bytes"):
        with _SimultaneousDownloadsSemaphore:
            with _AdlsFileClient(self.storage_account, self.container, remote_path) as file_client:
                logger.info(
                    f"<----- downloading {type_hint} from {self.storage_account} {self.container} {remote_path}"
                )
                start_time = default_timer()
                file_client.download_file().readinto(stream)
                elapsed_s = default_timer() - start_time
                if elapsed_s > LOG_SLOW_TRANSFER_S:
                    logger.info(
                        f"Took {int(elapsed_s)} seconds to download {type_hint} from {remote_path}"
                    )

    @_azure_creds_retry
    def put_bytes(self, remote_path: str, data: AnyStrSrc, type_hint: str = "bytes"):
        with _AdlsFileClient(self.storage_account, self.container, remote_path) as file_client:
            content_settings_if_upload_needed = _content_settings_unless_checksum_already_present(
                file_client, data
            )
            if content_settings_if_upload_needed:
                # we allow unlimited 'small' operations to proceed in
                # parallel, but we gate actual uploads behind the
                # semaphore to concentrate our bandwidth on fewer
                # active uploads, reducing median and mean wait time.
                with _SimultaneousUploadsSemaphore:
                    logger.info(
                        f"======> uploading {type_hint} to {self.storage_account} {self.container} {remote_path}"
                    )
                    start_time = default_timer()
                    file_client.upload_data(
                        data,
                        overwrite=True,
                        content_settings=content_settings_if_upload_needed,
                    )
                    elapsed_s = default_timer() - start_time
                    if elapsed_s > LOG_SLOW_TRANSFER_S:
                        logger.info(
                            f"Took {int(elapsed_s)} seconds to upload {type_hint} to {remote_path}"
                        )

    @_azure_creds_retry
    def file_exists(self, remote_path: str) -> bool:
        with _AdlsFileClient(self.storage_account, self.container, remote_path) as file_client:
            return file_client.exists()

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
