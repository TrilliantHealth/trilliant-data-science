import contextlib
import enum
import os
import shutil
import threading
import typing as ty
from pathlib import Path

import aiohttp.http_exceptions
import requests.exceptions
from azure.core.exceptions import AzureError, HttpResponseError, ResourceModifiedError
from azure.storage.filedatalake import DataLakeFileClient, FileProperties, FileSystemClient, aio

from thds.core import fretry, hash_cache, hashing, log, scope, tmp
from thds.core.types import StrOrPath

from . import azcopy, errors, etag, hashes
from ._progress import report_download_progress
from .download_lock import download_lock
from .fqn import AdlsFqn
from .ro_cache import Cache, from_cache_path_to_local, from_local_path_to_cache

logger = log.getLogger(__name__)


def _check_size(dpath: Path, expected_size: ty.Optional[int]) -> None:
    actual_size = os.path.getsize(dpath)
    if expected_size is not None and actual_size != expected_size:
        raise errors.ContentLengthMismatchError(
            f"Downloaded file {dpath} has size {actual_size} but expected {expected_size}"
        )


@contextlib.contextmanager
def _atomic_download_and_move(
    fqn: AdlsFqn,
    dest: StrOrPath,
    properties: ty.Optional[FileProperties] = None,
) -> ty.Iterator[azcopy.download.DownloadRequest]:
    known_size = properties.size if properties else None
    with tmp.temppath_same_fs(dest) as dpath:
        logger.debug("Downloading %s", fqn)
        if azcopy.download.should_use_azcopy(known_size or -1):
            yield azcopy.download.DownloadRequest(dpath, known_size)
        else:
            with open(dpath, "wb") as down_f:
                yield azcopy.download.SdkDownloadRequest(
                    dpath, known_size, report_download_progress(down_f, str(fqn), known_size or 0)
                )
        _check_size(dpath, known_size)
        try:
            os.rename(dpath, dest)  # will succeed even if dest is read-only
        except OSError as oserr:
            if "Invalid cross-device link" in str(oserr):
                # this shouldn't ever happen because of temppath_same_fs, but just in case...
                logger.warning('Failed to move "%s" to "%s" - copying instead', dpath, dest)
                shutil.copyfile(dpath, dest)
                logger.info('Copied "%s" to "%s"', dpath, dest)
            else:
                logger.error('Failed to move "%s" to "%s" - raising', dpath, dest)
                raise


# Async is weird.
#
# You cannot easily call an async function from within a standard/non-async function.
# And while you _can_ call a synchronous function from within an async one,
# it's highly discouraged if that function is doing network I/O,
# because your sync network IO will block the entire async green thread,
# grinding all 'async' work to a halt. It's very unneighborly.
#
# Unfortunately, this means that it's quite difficult to share the implementation
# of complex logic between async and non-async users. You can't use callbacks to abstract
# I/O, because those would themselves need to be either async or not.
#
# What you can do is a sort of 'functional core, imperative shell' approach,
# where the I/O parts are performed in a top level imperative shell, and the functional
# (logic) parts are performed in the shared core. As with many such things,
# the trick then is how to structure the functional core such that it 'makes sense' to a reader.
#
# One traditional means of doing this is breaking the functional core up into
# several functions to be called before and after the network calls.
# However, that does tend to impair readability, as those core functions
# each only do part of the work, and sometimes the 'part' doesn't make as much
# sense on its own as you might like.
#
# This is (to me) an entirely novel approach, and as such is an experiment.
# By writing a coroutine (the logic directly below) as the functional core,
# we can make the main 'logic' of the system readable in one go.
# What is required is a willingness to read the `yield` statements as
# essentially the 'inverse' of async/await - yield means "send this value to
# my controller and wait for their response'. Once the response is sent, we can
# resume where we left off, and the logic flows reasonably nicely.
#
# One _additional_ advantage of this approach is that certain bits of IO
# can actually be re-requested at any time, and the coroutine's controller
# can in a sense 'cache' those responses. So instead of the core logic
# having to keep track of whether it has performed the IO, it can request the result
# again and rely on the controller to re-send the previously fetched result.


class _FileResult(ty.NamedTuple):
    hash: hashing.Hash
    hit: ty.Optional[Path]


def _attempt_cache_hit(
    expected_hash: ty.Optional[hashing.Hash],
    fqn: AdlsFqn,
    local_path: StrOrPath,
    cache: ty.Optional[Cache],
) -> ty.Optional[_FileResult]:
    if not expected_hash:
        return None

    hash_path_if_exists = hashes.hash_path_for_algo(expected_hash.algo)

    with log.logger_context(hash_for="before-download-dest"):
        local_hash = hash_path_if_exists(local_path)
    if local_hash == expected_hash:
        logger.debug("Local path matches %s - no need to look further", expected_hash.algo)
        if cache:
            cache_path = cache.path(fqn)
            with log.logger_context(hash_for="before-download-cache"):
                if local_hash != hash_path_if_exists(cache_path):
                    # only copy if the cache is out of date
                    from_local_path_to_cache(local_path, cache_path, cache.link)
            return _FileResult(local_hash, hit=cache_path)
        return _FileResult(local_hash, hit=Path(local_path))

    if local_hash:
        logger.debug(
            "Local path exists but does not match expected %s %s",
            expected_hash.algo,
            expected_hash.bytes,
        )
    if cache:
        cache_path = cache.path(fqn)
        cache_hash = hash_path_if_exists(cache_path)
        if cache_hash == expected_hash:  # file in cache matches!
            from_cache_path_to_local(cache_path, local_path, cache.link)
            return _FileResult(cache_hash, hit=cache_path)

        if cache_hash:
            logger.debug(
                "Cache path exists but does not match expected %s %s",
                expected_hash.algo,
                expected_hash.bytes,
            )
    return None


class _IoRequest(enum.Enum):
    FILE_PROPERTIES = "file_properties"


IoRequest = ty.Union[_IoRequest, azcopy.download.DownloadRequest]
IoResponse = ty.Union[FileProperties, None]


_dl_scope = scope.Scope("adls.download")


def _download_or_use_verified_cached_coroutine(  # noqa: C901
    fqn: AdlsFqn,
    local_path: StrOrPath,
    expected_hash: ty.Optional[hashing.Hash] = None,
    cache: ty.Optional[Cache] = None,
) -> ty.Generator[IoRequest, IoResponse, _FileResult]:
    """Make a file on ADLS available at the local path provided.

    When we download from ADLS we want to know for sure that we have
    the bytes we expected.  Sometimes we have a hash upfront that we
    want to verify.  Other times, we simply want to rely on the hash
    ADLS has. If we have both, we should check anything we have the
    opportunity to check.

    Because we're verifying everything, we can optionally offer two
    sorts of verified caching.

    1. With no local cache, we can at least verify whether the file is
    present at the local path and contains the expected bytes.  If it
    does, there's no need to re-download.

    2. With a cache provided, we can also check that cache, and if the
    file is present in the local cache, we can either hard/soft link or copy
    it into the expected location, depending on the Cache configuration.

    If the file is not present in either location, we finally download
    the file from ADLS and then verify that the local hash matches
    both what we expected and what ADLS told us it would be, if any.

    The downloaded file will always placed into the cache if a cache
    is provided.  The local path, if different from the cache path,
    will either be linked or copied to, as selected by
    `cache.link`. `link=True` will save storage space but is not what
    you want if you intend to modify the file.

    Files placed in the cache will be marked as read-only to prevent
    _some_ types of accidents. This will not prevent you from
    accidentally or maliciously moving files on top of existing cached
    files, but it will prevent you from opening those files for
    writing in a standard fashion.

    Raises StopIteration when complete. StopIteration.value.hit will
    be the Path to the cached file if there was a cache hit, and None
    if a download was required. `.value` will also contain the Hash of
    the downloaded file, which may be used as desired.
    """
    if not local_path:
        raise ValueError("Must provide a destination path.")

    _dl_scope.enter(log.logger_context(dl=fqn, pid=os.getpid(), tid=threading.get_ident()))
    file_properties = None

    if not expected_hash:
        # we don't know what we expect, so attempt to retrieve
        # expectations from ADLS itself.
        file_properties = yield _IoRequest.FILE_PROPERTIES
        if file_properties:
            # critically, we expect the _first_ one in this list to be the fastest to verify.
            expected_hash = next(iter(hashes.extract_hashes_from_props(file_properties).values()), None)

    def attempt_cache_hit() -> ty.Optional[_FileResult]:
        return _attempt_cache_hit(
            expected_hash=expected_hash, cache=cache, fqn=fqn, local_path=local_path
        )

    # attempt cache hits before taking a lock, to avoid contention for existing files.
    if file_result := attempt_cache_hit():
        return file_result  # noqa: B901

    # No cache hit, so its time to prepare to download. if a cache was provided, we will
    # _put_ the resulting file in it.

    file_lock = str(cache.path(fqn) if cache else local_path)
    # create lockfile name from the (shared) cache path if present, otherwise the final
    # destination.  Non-cache users may then still incur multiple downloads in parallel,
    # but if you wanted to coordinate then you should probably have been using the global
    # cache in the first place.
    _dl_scope.enter(download_lock(file_lock))

    # re-attempt cache hit - we may have gotten the lock after somebody else downloaded
    if file_result := attempt_cache_hit():
        logger.info("Got cache hit on the second attempt, after acquiring lock for %s", fqn)
        return file_result  # noqa: B901

    logger.debug("Unable to find a cached version anywhere that we looked...")
    file_properties = yield _IoRequest.FILE_PROPERTIES

    # if any of the remote hashes match the expected hash, verify that one.
    # otherwise, verify the first remote hash in the list, since that's the fastest one.
    all_remote_hashes = hashes.extract_hashes_from_props(file_properties)
    remote_hash_to_match = all_remote_hashes.get(expected_hash.algo) if expected_hash else None
    with hashes.verify_hashes_before_and_after_download(
        remote_hash_to_match,
        expected_hash,
        fqn,
        local_path,
    ):  # download new data directly to local path
        with _atomic_download_and_move(fqn, local_path, file_properties) as tmpwriter:
            yield tmpwriter

    if cache:
        from_local_path_to_cache(local_path, cache.path(fqn), cache.link)

    hash_to_set_if_missing = expected_hash or remote_hash_to_match
    if not hash_to_set_if_missing or hash_to_set_if_missing.algo not in hashes.PREFERRED_ALGOS:
        hash_to_set_if_missing = hash_cache.filehash(hashes.PREFERRED_ALGOS[0], local_path)
    assert hash_to_set_if_missing, "We should have a preferred hash to set at this point."
    return _FileResult(hash_to_set_if_missing, hit=None)


# So ends the crazy download caching coroutine.
#
# Below this point are several helper functions, and after that are the two
# (async and non-async) coroutine controllers. While you can still see duplication
# between the two controllers, it is clearly much less code than would otherwise
# have to be duplicated in order to maintain an async and non-async
# implementation in parallel.


def _prep_download_coroutine(
    fs_client: FileSystemClient,
    remote_key: str,
    local_path: StrOrPath,
    expected_hash: ty.Optional[hashing.Hash] = None,
    cache: ty.Optional[Cache] = None,
) -> ty.Tuple[
    ty.Generator[IoRequest, IoResponse, _FileResult],
    IoRequest,
    ty.Optional[FileProperties],
    DataLakeFileClient,
]:
    co = _download_or_use_verified_cached_coroutine(
        AdlsFqn(ty.cast(str, fs_client.account_name), fs_client.file_system_name, remote_key),
        local_path,
        expected_hash=expected_hash,
        cache=cache,
    )
    return co, co.send(None), None, fs_client.get_file_client(remote_key)


def _excs_to_retry() -> ty.Callable[[Exception], bool]:
    """These are exceptions that we observe to be spurious failures worth retrying."""
    return fretry.is_exc(
        *list(
            filter(
                None,
                (
                    requests.exceptions.ConnectionError,
                    aiohttp.http_exceptions.ContentLengthError,
                    aiohttp.client_exceptions.ClientPayloadError,
                    getattr(
                        aiohttp.client_exceptions, "SocketTimeoutError", None
                    ),  # not present in aiohttp < 3.10 - Databricks installs 3.8.
                ),
            )
        )
    )


def _log_nonfatal_hash_error_exc(exc: Exception, url: str) -> None:
    """Azure exceptions are very noisy."""
    msg = "Unable to set hash for %s: %s"
    exception_txt = str(exc)
    log, extra_txt = (
        (logger.debug, type(exc).__name__)
        if ("AuthorizationPermissionMismatch" in exception_txt or "ConditionNotMet" in exception_txt)
        else (logger.warning, exception_txt)
    )
    log(msg, url, extra_txt)


@_dl_scope.bound
def download_or_use_verified(
    fs_client: FileSystemClient,
    remote_key: str,
    local_path: StrOrPath,
    *,
    expected_hash: ty.Optional[hashing.Hash] = None,
    cache: ty.Optional[Cache] = None,
) -> ty.Optional[Path]:
    """Download a file or use the existing, cached copy if one exists in the cache and is verifiable.

    Note that you will get a logged warning if `local_path` already exists when you call
    this function.
    """
    file_properties = None
    try:
        co, co_request, file_properties, dl_file_client = _prep_download_coroutine(
            fs_client, remote_key, local_path, expected_hash, cache
        )
        _dl_scope.enter(dl_file_client)  # on __exit__, will release the connection to the pool
        while True:
            if co_request == _IoRequest.FILE_PROPERTIES:
                if not file_properties:
                    # only fetch these if they haven't already been requested
                    file_properties = dl_file_client.get_file_properties()
                co_request = co.send(file_properties)
            elif isinstance(co_request, azcopy.download.DownloadRequest):
                # coroutine is requesting download
                fretry.retry_regular(_excs_to_retry(), fretry.n_times(2))(
                    # retry n_times(2) means _retry_ twice.
                    azcopy.download.sync_fastpath
                )(dl_file_client, co_request)
                co_request = co.send(None)
            else:
                raise ValueError(f"Unexpected coroutine request: {co_request}")
    except StopIteration as si:
        if meta := hashes.create_hash_metadata_if_missing(file_properties, si.value.hash):
            try:
                logger.info(f"Setting missing hash for {remote_key}")
                assert file_properties
                dl_file_client.set_metadata(meta, **etag.match_etag(file_properties))
            except (HttpResponseError, ResourceModifiedError) as ex:
                _log_nonfatal_hash_error_exc(ex, dl_file_client.url)
        return si.value.hit
    except AzureError as err:
        errors.translate_azure_error(fs_client, remote_key, err)


_async_dl_scope = scope.AsyncScope("adls.download.async")


@_dl_scope.bound
@_async_dl_scope.async_bound
@fretry.retry_regular_async(
    fretry.is_exc(errors.ContentLengthMismatchError), fretry.iter_to_async(fretry.n_times(2))
)
async def async_download_or_use_verified(
    fs_client: aio.FileSystemClient,
    remote_key: str,
    local_path: StrOrPath,
    *,
    expected_hash: ty.Optional[hashing.Hash] = None,
    cache: ty.Optional[Cache] = None,
) -> ty.Optional[Path]:
    file_properties = None
    try:
        co, co_request, file_properties, dl_file_client = _prep_download_coroutine(
            fs_client, remote_key, local_path, expected_hash, cache
        )
        await _async_dl_scope.async_enter(dl_file_client)  # type: ignore[arg-type]
        # on __aexit__, will release the connection to the pool
        while True:
            if co_request == _IoRequest.FILE_PROPERTIES:
                if not file_properties:
                    # only fetch these if they haven't already been requested
                    file_properties = await dl_file_client.get_file_properties()  # type: ignore[misc]
                    # TODO - check above type ignore
                co_request = co.send(file_properties)
            elif isinstance(co_request, azcopy.download.DownloadRequest):
                # coroutine is requesting download
                await fretry.retry_regular_async(
                    _excs_to_retry(), fretry.iter_to_async(fretry.n_times(2))
                )(
                    # retry n_times(2) means _retry_ twice.
                    azcopy.download.async_fastpath
                )(
                    dl_file_client, co_request
                )
                co_request = co.send(None)
            else:
                raise ValueError(f"Unexpected coroutine request: {co_request}")

    except StopIteration as si:
        if meta := hashes.create_hash_metadata_if_missing(file_properties, si.value.hash):
            try:
                logger.info(f"Setting missing Hash for {remote_key}")
                assert file_properties
                await dl_file_client.set_metadata(meta, **etag.match_etag(file_properties))  # type: ignore[misc]
                # TODO - check above type ignore
            except (HttpResponseError, ResourceModifiedError) as ex:
                _log_nonfatal_hash_error_exc(ex, dl_file_client.url)
        return si.value.hit
    except AzureError as err:
        errors.translate_azure_error(fs_client, remote_key, err)
