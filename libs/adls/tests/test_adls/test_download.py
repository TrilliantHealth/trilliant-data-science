import concurrent.futures
import logging
import typing as ty
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest
from azure.identity.aio import DefaultAzureCredential
from azure.storage.filedatalake import FileProperties, FileSystemClient, aio

from thds.adls import ADLSFileSystem, AdlsFqn, AdlsRoot, azcopy, cached, errors, hashes, md5
from thds.adls.download import (
    _download_or_use_verified_cached_coroutine,
    _IoRequest,
    async_download_or_use_verified,
    download_or_use_verified,
)
from thds.adls.download_lock import _clean_download_locks
from thds.adls.global_client import get_global_fs_client
from thds.adls.ro_cache import Cache, global_cache

__TMPCACHEDIR = TemporaryDirectory(prefix="cache-for-adls-tests--")
_TEST_CACHE = Cache(Path(__TMPCACHEDIR.name) / ".adls-md5-ro-cache", True)


def test_unit_download_coroutine_does_not_accept_empty_path():
    with pytest.raises(ValueError):
        _download_or_use_verified_cached_coroutine(AdlsFqn("foo", "bar", "baz"), "").send(None)


def test_unit_download_coroutine_no_cache_no_remote_md5b64(test_dest: Path):
    """without an md5, file properties will be requested. with no
    remote md5, no md5 checks will be performed and no cache will be used."""
    fake = AdlsFqn.parse("adls://does/not/exist.lol")
    co = _download_or_use_verified_cached_coroutine(fake, test_dest / "exist.lol", cache=_TEST_CACHE)

    request = co.send(None)
    assert request == _IoRequest.FILE_PROPERTIES
    # this time, we're asking because we don't have an md5

    request = co.send(FileProperties())
    assert request == _IoRequest.FILE_PROPERTIES
    # this time, we're asking because we're about to do a download and
    # we need to know whether to skip the download.

    wfp = co.send(FileProperties())
    assert isinstance(wfp, azcopy.download.SdkDownloadRequest)
    wfp.writer.write(b"ello")

    with pytest.raises(StopIteration) as si:
        co.send(None)

    assert not si.value.value.hit  # not a cache hit
    assert _TEST_CACHE.path(fake).exists()  # file _was_ downloaded _into_ cache, however.
    assert (test_dest / "exist.lol").exists()  # file also downloaded locally


def test_integration_download_to_local_and_reuse_from_there(
    global_test_fs_client: FileSystemClient, test_dest: Path
):
    real = "test/read-only/DONT_DELETE_THESE_FILES.txt"
    fqn = AdlsFqn(
        ty.cast(str, global_test_fs_client.account_name), global_test_fs_client.file_system_name, real
    )
    the_hash = md5.to_hash(md5b64="U3vtigRGuroWtJFEQ5dKoQ==")
    lcl = test_dest / "DONT_DELETE_THESE_FILES.txt"
    hit = download_or_use_verified(global_test_fs_client, real, lcl, expected_hash=the_hash)
    assert not hit
    assert lcl.exists()
    assert not _TEST_CACHE.path(fqn).exists()

    hit = download_or_use_verified(global_test_fs_client, real, lcl, expected_hash=the_hash)
    assert hit
    assert lcl.exists()
    assert not _TEST_CACHE.path(fqn).exists()


def test_integration_download_to_cache_with_no_expected_md5_and_reuse_from_there(
    global_test_fs_client: FileSystemClient, test_dest: Path
):
    remote = "test/read-only/DONT_DELETE_THESE_FILES.txt"
    fqn = AdlsFqn(
        ty.cast(str, global_test_fs_client.account_name), global_test_fs_client.file_system_name, remote
    )
    lcl = test_dest / "DONT_DELETE_THESE_FILES----use-cache.txt"
    hit = download_or_use_verified(
        global_test_fs_client, remote, lcl, expected_hash=None, cache=_TEST_CACHE
    )
    assert not hit
    assert lcl.exists()
    assert _TEST_CACHE.path(fqn).exists()

    hit = download_or_use_verified(
        global_test_fs_client, remote, lcl, expected_hash=None, cache=_TEST_CACHE
    )
    assert hit
    assert lcl.exists()
    assert hit.exists()
    assert _TEST_CACHE.path(fqn).exists()

    newlcl = test_dest / "DONT_DELETE---put-in-different-place-but-use-cache.txt"
    hit = download_or_use_verified(
        global_test_fs_client, remote, newlcl, expected_hash=None, cache=_TEST_CACHE
    )
    assert newlcl.exists()
    assert lcl.exists()  # still...
    assert _TEST_CACHE.path(fqn).exists()


def test_integration_handles_emoji_and_long_key(
    global_test_fs_client: FileSystemClient, test_dest: Path
):
    # this key is longer than 255 bytes, which is longer than most
    # local filesystems can accept.  therefore, we must truncate it in
    # a reliably-discoverable way.
    remote = "test/read-only/😀aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"  # noqa
    fqn = AdlsFqn(
        ty.cast(str, global_test_fs_client.account_name), global_test_fs_client.file_system_name, remote
    )
    md5h = md5.to_hash(md5b64="gEL83AfKoP2e3O1Y4RsBqQ==")
    lcl = test_dest / "benchmark_hashing.py"
    hit = download_or_use_verified(
        global_test_fs_client, remote, lcl, expected_hash=md5h, cache=_TEST_CACHE
    )
    assert not hit
    assert lcl.exists()
    assert _TEST_CACHE.path(fqn).exists()
    assert "md5-ff6343d3a291258f48cbe455145cbbfa-" in str(_TEST_CACHE.path(fqn))


def test_integration_md5_verification(global_test_fs_client: FileSystemClient, test_dest: Path):
    real = "test/read-only/DONT_DELETE_THESE_FILES.txt"
    md5h = md5.to_hash(md5b64="incorrect-MrMjF87w3GvA1==")
    lcl = test_dest / "DONT_DELETE_THESE_FILES.txt"
    with pytest.raises(errors.HashMismatchError):
        download_or_use_verified(global_test_fs_client, real, lcl, expected_hash=md5h)


def test_unit_md5_verification(test_dest: Path):
    made_it = False
    with pytest.raises(errors.HashMismatchError):
        local_dest = test_dest / "a-file.txt"
        with open(local_dest, "w") as f:
            f.write("hi")
        md5h = md5.to_hash(md5b64="WPMVPiXYwhMrMjF87w3GvA==")
        with hashes.verify_hashes_before_and_after_download(
            md5h,
            md5h,
            AdlsFqn("foo", "bar", "baz"),
            local_dest,
        ):
            made_it = True
    assert made_it


def _async_adls_fs_client(storage_account: str, container: str) -> aio.FileSystemClient:
    return aio.DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=DefaultAzureCredential(exclude_shared_token_cache_credential=True),
    ).get_file_system_client(file_system=container)


@pytest.mark.asyncio
async def test_integration_async(test_remote_root: AdlsRoot, test_dest: Path):
    remote = "test/read-only/DONT_DELETE_THESE_FILES.txt"
    lcl = test_dest / "DONT_DELETE_THESE_FILES----use-async.txt"

    async_client = _async_adls_fs_client(*test_remote_root)
    md5h = md5.to_hash(md5b64="U3vtigRGuroWtJFEQ5dKoQ==")
    hit = await async_download_or_use_verified(async_client, remote, lcl, expected_hash=md5h)
    assert not hit
    assert lcl.exists()

    hit = await async_download_or_use_verified(async_client, remote, lcl, expected_hash=md5h)
    assert hit
    assert lcl.exists()
    assert hit.exists()


@pytest.mark.asyncio
async def test_file_missing_hash_gets_one_assigned_after_download(
    tmp_remote_root: AdlsRoot, test_dest: Path
):
    fs_client = _async_adls_fs_client(*tmp_remote_root)
    key = "test/writable/missing-md5.txt"
    file_client = fs_client.get_file_client(key)
    await file_client.upload_data(b"hi-i-have-no-md5", overwrite=True)
    fp = await file_client.get_file_properties()
    assert not fp.content_settings.content_md5

    cache_hit = await async_download_or_use_verified(fs_client, key, test_dest / "missing-md5.txt")
    assert not cache_hit

    fp = await file_client.get_file_properties()
    assert not fp.content_settings.content_md5
    assert fp.metadata
    assert fp.metadata["hash_blake3_b64"] == "xbOHr0T91b3s1yjc24tkZ1y70caPbvkMBO7y7dyN2Rw="

    # should not error since the md5 should be correct
    cache_hit = await async_download_or_use_verified(fs_client, key, test_dest / "missing-md5.txt")
    assert cache_hit


def test_file_with_md5_doesnt_try_to_set_it(caplog: pytest.LogCaptureFixture, tmp_remote_root: AdlsRoot):
    fs = ADLSFileSystem(*tmp_remote_root)
    key = f"test/thds.adls/file_with_md5_doesnt_try_to_set_it/{uuid4().hex}"
    fs.put_file(Path(__file__).parent.parent / "data" / "hello_world.txt", key)

    with caplog.at_level(logging.INFO):
        with TemporaryDirectory() as td:
            download_or_use_verified(
                get_global_fs_client(fs.account_name, fs.file_system), key, Path(td) / "whatever"
            )

    for record in caplog.records:
        assert "missing MD5" not in record.getMessage()


@pytest.fixture
def random_test_file_fqn(
    test_dest: Path, tmp_remote_root: AdlsRoot, random_test_file_path: str
) -> ty.Iterator[AdlsFqn]:
    fs = ADLSFileSystem(*tmp_remote_root)
    fqn = AdlsFqn(fs.account_name, fs.file_system, random_test_file_path)

    random_file = test_dest / "random.txt"
    with open(random_file, "w") as f:
        f.write(uuid4().hex)

    fs.put_file(random_file, random_test_file_path)  # non-cached upload

    yield fqn

    fs.delete_file(fqn.path)  # clean up remote


def test_parallel_downloads_only_perform_a_single_download(
    caplog: pytest.LogCaptureFixture, random_test_file_fqn: AdlsFqn
):
    with caplog.at_level(logging.DEBUG, logger="thds.adls.download"):
        # we're not actually coordinating via shared memory,
        # but the easiest way to be able to configure the logs
        # so that we can see them in the test is to use threads
        # so that everything shares the same logging config.
        with concurrent.futures.ThreadPoolExecutor() as executor:
            list(executor.map(cached.download_to_cache, [random_test_file_fqn] * 10))

    download_count = 0
    reuse_count = 0
    for record in caplog.records:
        if "Downloading" in record.getMessage():
            download_count += 1
        elif "Local path matches blake3" in record.getMessage():
            reuse_count += 1

    global_cache().path(random_test_file_fqn).unlink()
    # don't need the cached file itself, so delete it before we assert

    assert download_count == 1
    assert reuse_count == 9


def test_clean_download_locks(caplog: pytest.LogCaptureFixture):
    num_deleted = _clean_download_locks()
    print("deleted num lockfiles: ", num_deleted)

    for record in caplog.records:
        assert not record.getMessage().startswith("Failed to clean download locks directory.")


def test_dont_use_azcopy_if_present(global_test_fs_client: FileSystemClient, test_dest: Path):
    with azcopy.download.DONT_USE_AZCOPY.set_local(True):
        # this is a weird test. we dynamically use azcopy if it's available because it's faster in high-bandwidth situations.
        # but we also have a fallback to the download_file part of the SDK.
        # what we are doing here is explicitly testing that code,
        # but weirdly, it may turn out not to matter, because if you're running this code in an environment without azcopy,
        # then all the integration tests above are testing the fallback code anyway.
        # Whatever. we just want to make sure all the code paths are tested wherever they can be.
        real = "test/read-only/DONT_DELETE_THESE_FILES.txt"
        md5h = md5.to_hash(md5b64="U3vtigRGuroWtJFEQ5dKoQ==")
        lcl = test_dest / "dont_use_azcopy_DONT_DELETE_THESE_FILES.txt"
        hit = download_or_use_verified(global_test_fs_client, real, lcl, expected_hash=md5h)
        assert not hit
        assert lcl.exists()
