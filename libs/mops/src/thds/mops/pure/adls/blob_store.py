"""This abstraction matches what is required by the BlobStore abstraction in pure.core.uris"""

import logging
import typing as ty
from pathlib import Path

from azure.core.exceptions import HttpResponseError
from azure.storage.filedatalake import DataLakeFileClient

from thds import adls
from thds.adls.errors import blob_not_found_translation, is_blob_not_found
from thds.adls.global_client import get_global_fs_client
from thds.core import config, fretry, home, link, log, scope

from ..._utils.on_slow import LogSlow, on_slow
from ..core.types import DISABLE_CONTROL_CACHE, AnyStrSrc, BlobStore

T = ty.TypeVar("T")
ToBytes = ty.Callable[[T, ty.BinaryIO], ty.Any]
FromBytes = ty.Callable[[ty.BinaryIO], T]
_5_MB = 5 * 2**20


# suppress very noisy INFO logs in azure library.
# This mirrors thds.adls.
log.getLogger("azure.core").setLevel(logging.WARNING)
logger = log.getLogger(__name__)


def _selective_upload_path(path: Path, adls_uri: str) -> None:
    if path.stat().st_size > _5_MB:
        adls.upload_through_cache(adls_uri, path)
    else:
        adls.upload(adls_uri, path)


def is_creds_failure(exc: Exception) -> bool:
    return isinstance(exc, HttpResponseError) and not is_blob_not_found(exc)


_azure_creds_retry = fretry.retry_sleep(is_creds_failure, fretry.expo(retries=9, delay=1.0))
# sometimes Azure Cli credentials expire but would succeed if retried
# and the azure library does not seem to retry these on its own.


class AdlsBlobStore(BlobStore):
    def control_root(self, uri: str) -> str:
        return str(adls.fqn.parse(uri).root())

    def _client(self, fqn: adls.AdlsFqn) -> DataLakeFileClient:
        return adls.get_global_fs_client(fqn.sa, fqn.container).get_file_client(fqn.path)

    @_azure_creds_retry
    @scope.bound
    def readbytesinto(self, remote_uri: str, stream: ty.IO[bytes], type_hint: str = "bytes") -> None:
        fqn = adls.fqn.parse(remote_uri)
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
        return adls.download_to_cache(remote_uri)

    @_azure_creds_retry
    @scope.bound
    def putbytes(
        self, remote_uri: str, data: AnyStrSrc, type_hint: str = "application/octet-stream"
    ) -> None:
        """Upload data to a remote path."""
        adls.upload(remote_uri, data, content_type=type_hint)

    @_azure_creds_retry
    @scope.bound
    def putfile(self, path: Path, remote_uri: str) -> None:
        scope.enter(log.logger_context(upload="mops-putfile"))
        _selective_upload_path(path, remote_uri)

    @_azure_creds_retry
    @scope.bound
    def exists(self, remote_uri: str) -> bool:
        fqn = adls.fqn.parse(remote_uri)
        scope.enter(log.logger_context(exists=fqn))
        return on_slow(
            lambda secs: LogSlow(f"Took {int(secs)}s to check if file exists."),
            slow_seconds=1.2,
        )(lambda: self._client(fqn).exists())()

    def join(self, *parts: str) -> str:
        return adls.fqn.join(*parts).rstrip("/")

    def split(self, uri: str) -> ty.List[str]:
        fqn = adls.fqn.parse(uri)
        return [str(fqn.root()), *fqn.path.split("/")]

    def is_blob_not_found(self, exc: Exception) -> bool:
        return is_blob_not_found(exc)

    def list(self, uri: str) -> ty.List[str]:
        fqn = adls.fqn.parse(uri)
        return [
            str(adls.fqn.AdlsFqn(fqn.sa, fqn.container, path.name))
            for path in get_global_fs_client(fqn.sa, fqn.container).get_paths(fqn.path, recursive=False)
        ]


class DangerouslyCachingStore(AdlsBlobStore):
    """This BlobStore will cache _everything_ locally
    and anything it finds locally it will return without question.

    This maximally avoids network operations if for some reason you feel the need
    to do that, but it will 100% lead to false positive cache hits, because it is no longer
    checking the hash of the locally-cached file against what ADLS itself advertises.

    It is now believed that this is not as dangerous as originally thought, because mops
    control files are not intended to be mutable, and if mutated in some kind of
    distributed systems context (e.g. two parallel runs of the same thing), the results
    are intended to be at least 'equivalent' even when they're not byte-identical.

    Therefore, this is now enabled by default and considered generally safe to use in any
    environment, including automated ones.
    """

    def __init__(self, root: Path):
        self._cache = adls.Cache(root.resolve(), ("ref", "hard"))

    def exists(self, remote_uri: str) -> bool:
        cache_path = self._cache.path(adls.fqn.parse(remote_uri))
        if cache_path.exists():
            return True
        return super().exists(remote_uri)

    def readbytesinto(self, remote_uri: str, stream: ty.IO[bytes], type_hint: str = "bytes") -> None:
        # readbytesinto is used for _almost_ everything in mops - but almost everything is a 'control file'
        # of some sort. We use a completely separate cache for all of these things, because
        # in previous implementations, none of these things would have been cached at all.
        # (see comment on getfile below...)
        fqn = adls.fqn.parse(remote_uri)
        cache_path = self._cache.path(fqn)
        if not cache_path.exists():
            adls.download.download_or_use_verified(
                get_global_fs_client(fqn.sa, fqn.container), fqn.path, cache_path, cache=self._cache
            )
        with cache_path.open("rb") as f:
            stream.write(f.read())

    def getfile(self, remote_uri: str) -> Path:
        # (continued from comment on readbytesinto...)
        #
        # whereas, for getfile, it is really only used for optimizations on larger file
        # downloads (e.g. Paths), and those were previously subject to long-term caching.
        # So for getfile, our primary source will be the parent implementation of getfile,
        # including any caching it already did.
        #
        # We still dangerously short-circuit the hash check, and we make a 'cheap copy'
        # (a link, usually) to our separate cache directory so that it's possible to
        # completely empty this particular mops cache (and all its 'dangerous' behavior)
        # simply by deleting that one cache directory.
        cache_path = self._cache.path(adls.fqn.parse(remote_uri))
        if cache_path.exists():
            return cache_path
        outpath = super().getfile(remote_uri)
        link.link(outpath, cache_path)
        return outpath


_DEFAULT_CONTROL_CACHE = config.item(
    "thds.mops.pure.adls.control_cache_root", default=home.HOMEDIR() / ".mops-adls-control-cache"
)


def get_adls_blob_store(uri: str) -> ty.Optional[AdlsBlobStore]:
    if not uri.startswith(adls.ADLS_SCHEME):
        return None

    if DISABLE_CONTROL_CACHE() or not _DEFAULT_CONTROL_CACHE():
        return AdlsBlobStore()

    return DangerouslyCachingStore(_DEFAULT_CONTROL_CACHE())
