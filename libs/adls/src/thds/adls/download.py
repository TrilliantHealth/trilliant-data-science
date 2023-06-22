import contextlib
import enum
import os
import shutil
import tempfile
import typing as ty
from pathlib import Path
from random import SystemRandom
from timeit import default_timer

from azure.core.exceptions import HttpResponseError
from azure.storage.filedatalake import DataLakeFileClient, FileProperties, FileSystemClient

from thds.core.hashing import b64
from thds.core.log import getLogger
from thds.core.types import StrOrPath

from .errors import BlobNotFoundError, is_blob_not_found
from .fqn import AdlsFqn
from .md5 import check_reasonable_md5b64, md5_readable
from .ro_cache import Cache, from_cache_path_to_local

logger = getLogger(__name__)


_TEMPDIRS_ON_DIFFERENT_FILESYSTEM = False
_1GB = 1 * 2**30  # log if hashing a file larger than this, since it will be slow.
_LONG_TRANSFER_S = 2


class MD5MismatchError(Exception):
    """Indicates that something needs to be done by the developer to correct a hash mismatch."""


@contextlib.contextmanager
def _atomic_download_and_move(fqn: AdlsFqn, dest: StrOrPath) -> ty.Iterator[ty.IO[bytes]]:
    global _TEMPDIRS_ON_DIFFERENT_FILESYSTEM
    with tempfile.TemporaryDirectory() as dir:
        # TODO lock dest
        if _TEMPDIRS_ON_DIFFERENT_FILESYSTEM:
            dpath = str(Path(dest).parent / ("tmp" + str(SystemRandom().random())[2:]))
        else:
            dpath = os.path.join(dir, "tmp")

        with open(dpath, "wb") as f:
            logger.debug(f"Downloading {fqn} to {dest}")
            started = default_timer()
            yield f
        elapsed = default_timer() - started
        size = os.path.getsize(dpath)
        log = logger.info if elapsed > _LONG_TRANSFER_S else logger.debug
        log(
            f"Downloaded {size:,} bytes in {elapsed:.1f}s at {int(size/10**6/elapsed):,.1f} Mbytes/sec to {dest}"
        )
        try:
            os.rename(dpath, dest)
        except OSError as oserr:
            if "Invalid cross-device link" in str(oserr):
                _TEMPDIRS_ON_DIFFERENT_FILESYSTEM = True  # don't make this mistake again
                shutil.copyfile(dpath, str(dest))
            else:
                raise


@contextlib.contextmanager
def _verify_md5s_before_and_after_download(
    remote_md5b64: str, md5b64: str, fqn: AdlsFqn, local_dest: StrOrPath
) -> ty.Iterator[None]:
    check_reasonable_md5b64(md5b64)
    if remote_md5b64 and remote_md5b64 != md5b64:
        raise MD5MismatchError(
            f"ADLS thinks the MD5 of {fqn} is {remote_md5b64}, but we wanted {md5b64}."
            " This may indicate that we need to update a hash in the codebase."
        )
    yield  # perform download
    local_md5b64 = b64(md5_readable(local_dest))
    if remote_md5b64 and remote_md5b64 != local_md5b64:
        raise MD5MismatchError(
            f"The MD5 of the downloaded file {local_dest} is {local_md5b64},"
            f" but the remote ({fqn}) says it should be {remote_md5b64}."
            f" This may indicate that ADLS has an erroneous MD5 for {fqn}."
        )
    if local_md5b64 != md5b64:
        raise MD5MismatchError(
            f"The MD5 of the downloaded file {local_dest} is {local_md5b64},"
            f" but we expected it to be {md5b64}."
            f" This probably indicates a corrupted download of {fqn}"
        )
    all_hashes = [local_md5b64, remote_md5b64, md5b64]
    assert 1 == len(set(filter(None, all_hashes))), all_hashes


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


def _download_or_use_verified_cached_coroutine(  # noqa: C901
    fqn: AdlsFqn,
    local_path: StrOrPath,
    md5b64: str = "",
    cache: ty.Optional[Cache] = None,
) -> ty.Generator[IoRequest, IoResponse, bool]:
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

    The downloaded file will be placed into the cache if it meets the
    provided cache's requirements. If it does, the local path, if
    different from the cache path, will either be linked or copied
    to, as selected by `cache.link`. `link=True` will save storage
    space but is not what you want if you intend to modify the file.

    Files placed in the cache will be marked as read-only to prevent
    _some_ types of accidents. This will not prevent you from
    accidentally or maliciously moving files on top of existing cached
    files, but it will prevent you from opening those files for
    writing in a standard fashion.

    Raises StopIteration when complete. The StopIteration.value will
    be True if there was a cache hit, and False if a download was required.

    """
    if not local_path:
        raise ValueError("Must provide a destination path.")

    def remote_md5b64(file_properties: FileProperties) -> str:
        if file_properties.content_settings.content_md5:
            return b64(file_properties.content_settings.content_md5)
        return ""

    if not md5b64:
        # we don't know what we expect, so attempt to retrieve an
        # expectation from ADLS itself.
        file_properties = yield _IoRequest.FILE_PROPERTIES
        md5b64 = remote_md5b64(file_properties)

    if not md5b64:
        # refuse to cache without md5 of some kind, either local or remote
        with _atomic_download_and_move(fqn, local_path) as tmpwriter:
            yield tmpwriter
            return False  # noqa: B901 # this is perfectly intentional

    # past this point, we have at least one md5 to verify against,
    # so an existing local file might be returned without a download.
    check_reasonable_md5b64(md5b64)

    def _md5b64_path_if_exists(path: StrOrPath) -> ty.Optional[str]:
        if not path or not os.path.exists(path):
            return None
        psize = Path(path).stat().st_size
        if psize > _1GB:
            logger.info(f"Hashing existing {psize/_1GB:.2f} GB file at {path}...")
        with open(path, "rb") as f:
            return b64(md5_readable(f))

    local_md5b64 = _md5b64_path_if_exists(local_path)
    if local_md5b64 == md5b64:
        return True

    if not cache:
        file_properties = yield _IoRequest.FILE_PROPERTIES
        with _verify_md5s_before_and_after_download(
            remote_md5b64(file_properties),
            md5b64,
            fqn,
            local_path,
        ):
            with _atomic_download_and_move(fqn, local_path) as tmpwriter:
                yield tmpwriter
        return False

    cache_path = cache.path(fqn)
    cache_md5b64 = _md5b64_path_if_exists(cache_path)
    if cache_md5b64 == md5b64:
        from_cache_path_to_local(cache_path, local_path, cache.link)
        return True

    file_properties = yield _IoRequest.FILE_PROPERTIES
    with _verify_md5s_before_and_after_download(
        remote_md5b64(file_properties),
        md5b64,
        fqn,
        cache_path,
    ):
        with _atomic_download_and_move(fqn, cache_path) as tmpwriter:
            yield tmpwriter

    from_cache_path_to_local(cache_path, local_path, cache.link)
    return False


# So ends the crazy download caching coroutine.
#
# Below this point is a helper function, and after that are the two
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
    ty.Generator[IoRequest, IoResponse, bool],
    IoRequest,
    ty.Optional[FileProperties],
    DataLakeFileClient,
]:
    co = _download_or_use_verified_cached_coroutine(
        AdlsFqn(fs_client.account_name, fs_client.file_system_name, remote_key),
        local_path,
        md5b64,
        cache=cache,
    )
    return co, co.send(None), None, fs_client.get_file_client(remote_key)


def _translate_blob_not_found(client, key: str, hre: HttpResponseError) -> ty.NoReturn:
    if is_blob_not_found(hre):
        raise BlobNotFoundError(AdlsFqn.of(client.account_name, client.file_system_name, key)) from hre
    raise


def download_or_use_verified(
    fs_client: FileSystemClient,
    remote_key: str,
    local_path: StrOrPath,
    md5b64: str = "",
    cache: ty.Optional[Cache] = None,
) -> bool:
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
                dl_file_client.download_file().readinto(co_request)
                co_request = co.send(None)
    except StopIteration as si:
        return si.value  # cache hit if True
    except HttpResponseError as hre:
        _translate_blob_not_found(fs_client, remote_key, hre)


async def async_download_or_use_verified(
    fs_client: FileSystemClient,
    remote_key: str,
    local_path: StrOrPath,
    md5b64: str = "",
    cache: ty.Optional[Cache] = None,
) -> bool:
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
                reader = await dl_file_client.download_file()
                await reader.readinto(co_request)
                co_request = co.send(None)
    except StopIteration as si:
        return si.value  # cache hit if True
    except HttpResponseError as hre:
        _translate_blob_not_found(fs_client, remote_key, hre)
