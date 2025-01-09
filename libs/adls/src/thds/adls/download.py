import contextlib
import enum
import os
import shutil
import typing as ty
from base64 import b64decode

from azure.core.exceptions import AzureError, HttpResponseError
from azure.storage.filedatalake import (
    ContentSettings,
    DataLakeFileClient,
    FileProperties,
    FileSystemClient,
)

from thds.core import log, scope, tmp
from thds.core.hashing import b64
from thds.core.types import StrOrPath

from ._progress import report_download_progress
from .conf import CONNECTION_TIMEOUT, DOWNLOAD_FILE_MAX_CONCURRENCY
from .download_lock import download_lock
from .errors import translate_azure_error
from .etag import match_etag
from .fqn import AdlsFqn
from .md5 import check_reasonable_md5b64, md5_file
from .ro_cache import Cache, from_cache_path_to_local, from_local_path_to_cache

logger = log.getLogger(__name__)


class MD5MismatchError(Exception):
    """Indicates that something needs to be done by the developer to correct a hash mismatch."""


@contextlib.contextmanager
def _atomic_download_and_move(
    fqn: AdlsFqn,
    dest: StrOrPath,
    properties: ty.Optional[FileProperties] = None,
) -> ty.Iterator[ty.IO[bytes]]:
    with tmp.temppath_same_fs(dest) as dpath:
        with open(dpath, "wb") as f:
            known_size = (properties.size or 0) if properties else 0
            logger.debug("Downloading %s", fqn)
            yield report_download_progress(f, str(dest), known_size)
        try:
            os.rename(dpath, dest)  # will succeed even if dest is read-only
        except OSError as oserr:
            if "Invalid cross-device link" in str(oserr):
                # this shouldn't ever happen because of temppath_same_fs, but just in case...
                shutil.copyfile(dpath, dest)
            else:
                raise


@contextlib.contextmanager
def _verify_md5s_before_and_after_download(
    remote_md5b64: str, expected_md5b64: str, fqn: AdlsFqn, local_dest: StrOrPath
) -> ty.Iterator[None]:
    if expected_md5b64:
        check_reasonable_md5b64(expected_md5b64)
    if remote_md5b64:
        check_reasonable_md5b64(remote_md5b64)
    if remote_md5b64 and expected_md5b64 and remote_md5b64 != expected_md5b64:
        raise MD5MismatchError(
            f"ADLS thinks the MD5 of {fqn} is {remote_md5b64}, but we expected {expected_md5b64}."
            " This may indicate that we need to update a hash in the codebase."
        )

    yield  # perform download

    with log.logger_context(hash_for="after-download"):
        local_md5b64 = b64(md5_file(local_dest))
    check_reasonable_md5b64(local_md5b64)  # must always exist
    if remote_md5b64 and remote_md5b64 != local_md5b64:
        raise MD5MismatchError(
            f"The MD5 of the downloaded file {local_dest} is {local_md5b64},"
            f" but the remote ({fqn}) says it should be {remote_md5b64}."
            f" This may indicate that ADLS has an erroneous MD5 for {fqn}."
        )
    if expected_md5b64 and local_md5b64 != expected_md5b64:
        raise MD5MismatchError(
            f"The MD5 of the downloaded file {local_dest} is {local_md5b64},"
            f" but we expected it to be {expected_md5b64}."
            f" This probably indicates a corrupted download of {fqn}"
        )
    all_hashes = dict(local=local_md5b64, remote=remote_md5b64, expected=expected_md5b64)
    assert 1 == len(set(filter(None, all_hashes.values()))), all_hashes


def _md5b64_path_if_exists(path: StrOrPath) -> ty.Optional[str]:
    if not path or not os.path.exists(path):  # does not exist if it's a symlink with a bad referent.
        return None
    return b64(md5_file(path))


def _remote_md5b64(file_properties: FileProperties) -> str:
    if file_properties.content_settings.content_md5:
        return b64(file_properties.content_settings.content_md5)
    return ""


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


class _IoRequest(enum.Enum):
    FILE_PROPERTIES = "file_properties"


IoRequest = ty.Union[_IoRequest, ty.IO[bytes]]
IoResponse = ty.Union[FileProperties, None]


class _FileResult(ty.NamedTuple):
    md5b64: str
    hit: bool


_dl_scope = scope.Scope("adls.download")


def _download_or_use_verified_cached_coroutine(  # noqa: C901
    fqn: AdlsFqn,
    local_path: StrOrPath,
    md5b64: str = "",
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
    be True if there was a cache hit, and False if a download was
    required. `.value` will also contain the md5b64 of the downloaded
    file, which may be used as desired.
    """
    if not local_path:
        raise ValueError("Must provide a destination path.")

    _dl_scope.enter(log.logger_context(dl=fqn))
    file_properties = None
    if not md5b64:
        # we don't know what we expect, so attempt to retrieve an
        # expectation from ADLS itself.
        file_properties = yield _IoRequest.FILE_PROPERTIES
        md5b64 = _remote_md5b64(file_properties)

    def attempt_cache_hit() -> ty.Optional[_FileResult]:
        if not md5b64:
            return None

        check_reasonable_md5b64(md5b64)
        with log.logger_context(hash_for="before-download-dest"):
            local_md5b64 = _md5b64_path_if_exists(local_path)
        if local_md5b64 == md5b64:
            logger.debug("Local path matches MD5 - no need to look further")
            if cache:
                cache_path = cache.path(fqn)
                with log.logger_context(hash_for="before-download-cache"):
                    if local_md5b64 != _md5b64_path_if_exists(cache_path):
                        # only copy if the cache is out of date
                        from_local_path_to_cache(local_path, cache_path, cache.link)
            return _FileResult(local_md5b64, hit=True)

        if local_md5b64:
            logger.debug("Local path exists but does not match expected md5 %s", md5b64)
        if cache:
            cache_path = cache.path(fqn)
            cache_md5b64 = _md5b64_path_if_exists(cache_path)
            if cache_md5b64 == md5b64:  # file in cache matches!
                from_cache_path_to_local(cache_path, local_path, cache.link)
                return _FileResult(cache_md5b64, hit=True)

            if cache_md5b64:
                logger.debug("Cache path exists but does not match expected md5 %s", md5b64)
        return None

    # attempt cache hit before taking a lock, to avoid contention for existing files.
    if file_result := attempt_cache_hit():
        return file_result  # noqa: B901

    _dl_scope.enter(download_lock(str(cache.path(fqn) if cache else local_path)))
    # create lockfile name from the (shared) cache path if present, otherwise the final
    # destination.  Non-cache users may then still incur multiple downloads in parallel,
    # but if you wanted to coordinate then you should probably have been using the global
    # cache in the first place.

    # re-attempt cache hit - we may have gotten the lock after somebody else downloaded
    if file_result := attempt_cache_hit():
        logger.debug("Got cache hit on the second attempt, after acquiring lock for %s", fqn)
        return file_result  # noqa: B901

    logger.debug("Unable to find a cached version anywhere that we looked...")
    file_properties = yield _IoRequest.FILE_PROPERTIES
    # no point in downloading if we've asked for hash X but ADLS only has hash Y.
    with _verify_md5s_before_and_after_download(
        _remote_md5b64(file_properties),
        md5b64,
        fqn,
        local_path,
    ):  # download new data directly to local path
        with _atomic_download_and_move(fqn, local_path, file_properties) as tmpwriter:
            yield tmpwriter
    if cache:
        from_local_path_to_cache(local_path, cache.path(fqn), cache.link)
    return _FileResult(md5b64 or b64(md5_file(local_path)), hit=False)


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
    md5b64: str = "",
    cache: ty.Optional[Cache] = None,
) -> ty.Tuple[
    ty.Generator[IoRequest, IoResponse, _FileResult],
    IoRequest,
    ty.Optional[FileProperties],
    DataLakeFileClient,
]:
    co = _download_or_use_verified_cached_coroutine(
        AdlsFqn(fs_client.account_name, fs_client.file_system_name, remote_key),
        local_path,
        md5b64=md5b64,
        cache=cache,
    )
    return co, co.send(None), None, fs_client.get_file_client(remote_key)


def _set_md5_if_missing(
    file_properties: ty.Optional[FileProperties], md5b64: str
) -> ty.Optional[ContentSettings]:
    if not file_properties or file_properties.content_settings.content_md5:
        return None
    file_properties.content_settings.content_md5 = b64decode(md5b64)
    return file_properties.content_settings


@_dl_scope.bound
def download_or_use_verified(
    fs_client: FileSystemClient,
    remote_key: str,
    local_path: StrOrPath,
    md5b64: str = "",
    cache: ty.Optional[Cache] = None,
) -> bool:
    """Download a file or use the existing, cached copy if one exists in the cache and is verifiable.

    Note that you will get a logged warning if `local_path` already exists when you call
    this function.
    """
    file_properties = None
    try:
        co, co_request, file_properties, dl_file_client = _prep_download_coroutine(
            fs_client, remote_key, local_path, md5b64, cache
        )
        while True:
            if co_request == _IoRequest.FILE_PROPERTIES:
                if not file_properties:
                    # only fetch these if they haven't already been requested
                    file_properties = dl_file_client.get_file_properties()
                co_request = co.send(file_properties)
            else:  # needs file object
                dl_file_client.download_file(
                    max_concurrency=DOWNLOAD_FILE_MAX_CONCURRENCY(),
                    connection_timeout=CONNECTION_TIMEOUT(),
                ).readinto(co_request)
                co_request = co.send(None)
    except StopIteration as si:
        if cs := _set_md5_if_missing(file_properties, si.value.md5b64):
            try:
                logger.info(f"Setting missing MD5 for {remote_key}")
                assert file_properties
                dl_file_client.set_http_headers(cs, **match_etag(file_properties))
            except HttpResponseError as hre:
                logger.info(f"Unable to set MD5 for {remote_key}: {hre}")
        return si.value.hit
    except AzureError as err:
        translate_azure_error(fs_client, remote_key, err)


@_dl_scope.bound
async def async_download_or_use_verified(
    fs_client: FileSystemClient,
    remote_key: str,
    local_path: StrOrPath,
    md5b64: str = "",
    cache: ty.Optional[Cache] = None,
) -> bool:
    file_properties = None
    try:
        co, co_request, file_properties, dl_file_client = _prep_download_coroutine(
            fs_client, remote_key, local_path, md5b64, cache
        )
        while True:
            if co_request == _IoRequest.FILE_PROPERTIES:
                if not file_properties:
                    # only fetch these if they haven't already been requested
                    file_properties = await dl_file_client.get_file_properties()
                co_request = co.send(file_properties)
            else:  # needs file object
                reader = await dl_file_client.download_file(
                    max_concurrency=DOWNLOAD_FILE_MAX_CONCURRENCY(),
                    connection_timeout=CONNECTION_TIMEOUT(),
                )
                await reader.readinto(co_request)
                co_request = co.send(None)
    except StopIteration as si:
        if cs := _set_md5_if_missing(file_properties, si.value.md5b64):
            try:
                logger.info(f"Setting missing MD5 for {remote_key}")
                assert file_properties
                await dl_file_client.set_http_headers(cs, **match_etag(file_properties))
            except HttpResponseError as hre:
                logger.info(f"Unable to set MD5 for {remote_key}: {hre}")
        return si.value.hit
    except AzureError as err:
        translate_azure_error(fs_client, remote_key, err)
