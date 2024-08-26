"""This abstraction matches what is required by the BlobStore abstraction in remote.core.uris"""

import logging
import typing as ty
from pathlib import Path

from azure.core.exceptions import HttpResponseError
from azure.storage.filedatalake import DataLakeFileClient

from thds.adls import AdlsFqn, join, resource
from thds.adls.cached_up_down import download_to_cache, upload_through_cache
from thds.adls.errors import blob_not_found_translation, is_blob_not_found
from thds.adls.global_client import get_global_fs_client
from thds.adls.ro_cache import Cache
from thds.core import config, fretry, link, log, scope

from ..._utils.on_slow import LogSlow, on_slow
from ..core.types import AnyStrSrc, BlobStore

T = ty.TypeVar("T")
ToBytes = ty.Callable[[T, ty.BinaryIO], ty.Any]
FromBytes = ty.Callable[[ty.BinaryIO], T]
_5_MB = 5 * 2**20


# suppress very noisy INFO logs in azure library.
# This mirrors thds.adls.
log.getLogger("azure.core").setLevel(logging.WARNING)
logger = log.getLogger(__name__)


def _selective_upload_path(path: Path, fqn: AdlsFqn):
    if path.stat().st_size > _5_MB:
        upload_through_cache(fqn, path)
    else:
        resource.upload(fqn, path)


def is_creds_failure(exc: Exception) -> bool:
    return isinstance(exc, HttpResponseError) and not is_blob_not_found(exc)


_azure_creds_retry = fretry.retry_sleep(is_creds_failure, fretry.expo(retries=9, delay=1.0))
# sometimes Azure Cli credentials expire but would succeed if retried
# and the azure library does not seem to retry these on its own.


class AdlsBlobStore(BlobStore):
    def _client(self, fqn: AdlsFqn) -> DataLakeFileClient:
        return get_global_fs_client(fqn.sa, fqn.container).get_file_client(fqn.path)

    @_azure_creds_retry
    @scope.bound
    def readbytesinto(self, remote_uri: str, stream: ty.IO[bytes], type_hint: str = "bytes"):
        fqn = AdlsFqn.parse(remote_uri)
        scope.enter(log.logger_context(download=fqn))
        logger.debug(f"<----- downloading {type_hint}")
        with blob_not_found_translation(fqn):
            on_slow(
                lambda elapsed_s: LogSlow(f"Took {int(elapsed_s)}s to download {type_hint}"),
            )(lambda: self._client(fqn).download_file().readinto(stream))()

    @_azure_creds_retry
    @scope.bound
    def getfile(self, remote_uri: str) -> Path:
        scope.enter(log.logger_context(download="mops-getfile"))
        return download_to_cache(AdlsFqn.parse(remote_uri))

    @_azure_creds_retry
    @scope.bound
    def putbytes(self, remote_uri: str, data: AnyStrSrc, type_hint: str = "bytes"):
        """Upload data to a remote path."""
        resource.upload(AdlsFqn.parse(remote_uri), data)
        return remote_uri

    @_azure_creds_retry
    @scope.bound
    def putfile(self, path: Path, remote_uri: str):
        scope.enter(log.logger_context(upload="mops-putfile"))
        _selective_upload_path(path, AdlsFqn.parse(remote_uri))

    @_azure_creds_retry
    @scope.bound
    def exists(self, remote_uri: str) -> bool:
        fqn = AdlsFqn.parse(remote_uri)
        scope.enter(log.logger_context(exists=fqn))
        return on_slow(
            lambda secs: LogSlow(f"Took {int(secs)}s to check if file exists."),
            slow_seconds=1.2,
        )(lambda: self._client(fqn).exists())()

    def join(self, *parts: str) -> str:
        return join(*parts)

    def split(self, uri: str) -> ty.List[str]:
        fqn = AdlsFqn.parse(uri)
        return [str(fqn.root()), *fqn.path.split("/")]

    def is_blob_not_found(self, exc: Exception) -> bool:
        return is_blob_not_found(exc)


class DangerouslyCachingStore(AdlsBlobStore):
    """This BlobStore will cache _everything_ locally
    and anything it finds locally it will return without question.

    This maximally avoids network operations if for some reason you feel the need
    to do that, but it will 100% lead to false positive cache hits.

    It should only be used 'under supervision', e.g. during development, and never in any
    automated context.
    """

    def __init__(self, root: str):
        self._cache = Cache(Path(root).resolve(), ("ref", "hard"))

    def exists(self, remote_uri: str) -> bool:
        cache_path = self._cache.path(AdlsFqn.parse(remote_uri))
        if cache_path.exists():
            return True
        return super().exists(remote_uri)

    def readbytesinto(self, remote_uri: str, stream: ty.IO[bytes], type_hint: str = "bytes"):
        fqn = AdlsFqn.parse(remote_uri)
        cache_path = self._cache.path(fqn)
        if not cache_path.exists():
            link.link(download_to_cache(fqn), cache_path)
        with cache_path.open("rb") as f:
            stream.write(f.read())

    def getfile(self, remote_uri: str) -> Path:
        cache_path = self._cache.path(AdlsFqn.parse(remote_uri))
        if cache_path.exists():
            return cache_path
        outpath = super().getfile(remote_uri)
        link.link(outpath, cache_path)
        return outpath


DANGEROUSLY_CACHING_ROOT = config.item("cache-dangerously-root", default="")


def get_store() -> BlobStore:
    if root := DANGEROUSLY_CACHING_ROOT():
        return DangerouslyCachingStore(root)
    return AdlsBlobStore()
