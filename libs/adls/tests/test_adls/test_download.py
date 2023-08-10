import io
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest
from azure.identity.aio import DefaultAzureCredential
from azure.storage.filedatalake import FileProperties, aio

from thds.adls import ADLSFileSystem, AdlsFqn
from thds.adls.download import (
    MD5MismatchError,
    _download_or_use_verified_cached_coroutine,
    _IoRequest,
    _verify_md5s_before_and_after_download,
    async_download_or_use_verified,
    b64,
    download_or_use_verified,
)
from thds.adls.global_client import get_global_client
from thds.adls.ro_cache import Cache

__TMPCACHEDIR = TemporaryDirectory(prefix="cache-for-adls-tests--")
_TEST_CACHE = Cache(Path(__TMPCACHEDIR.name) / ".adls-md5-ro-cache", True)
__TMPDIR = TemporaryDirectory(prefix="for-adls-tests--")
_TEST_DEST = Path(__TMPDIR.name)

_TEST_REMOTE = "thdsdatasets", "prod-datasets"
_TMP_REMOTE = "thdsscratch", "tmp"
global_client = get_global_client(*_TEST_REMOTE)
# this location for test data has been blessed by Matt Eby himself.


def test_unit_download_coroutine_does_not_accept_empty_path():
    with pytest.raises(ValueError):
        _download_or_use_verified_cached_coroutine(AdlsFqn("foo", "bar", "baz"), "").send(None)


def test_unit_download_coroutine_no_cache_no_remote_md5b64():
    """without an md5, file properties will be requested. with no
    remote md5, no md5 checks will be performed and no cache will be used."""
    fake = AdlsFqn.parse("adls://does/not/exist.lol")
    co = _download_or_use_verified_cached_coroutine(fake, _TEST_DEST / "exist.lol", cache=_TEST_CACHE)

    request = co.send(None)
    assert request == _IoRequest.FILE_PROPERTIES
    # this time, we're asking because we don't have an md5

    request = co.send(FileProperties())
    assert request == _IoRequest.FILE_PROPERTIES
    # this time, we're asking because we're about to do a download and
    # we need to know whether to skip the download.

    wfp = co.send(FileProperties())
    assert isinstance(wfp, io.IOBase)
    wfp.write(b"ello")

    with pytest.raises(StopIteration) as si:
        co.send(None)

    assert not si.value.value.hit  # not a cache hit
    assert _TEST_CACHE.path(fake).exists()  # file _was_ downloaded _into_ cache, however.
    assert (_TEST_DEST / "exist.lol").exists()  # file also downloaded locally


def test_integration_download_to_local_and_reuse_from_there():
    real = "test/read-only/DONT_DELETE_THESE_FILES.txt"
    fqn = AdlsFqn(global_client.account_name, global_client.file_system_name, real)
    md5b64 = "U3vtigRGuroWtJFEQ5dKoQ=="
    lcl = _TEST_DEST / "DONT_DELETE_THESE_FILES.txt"
    hit = download_or_use_verified(global_client, real, lcl, md5b64)
    assert not hit
    assert lcl.exists()
    assert not _TEST_CACHE.path(fqn).exists()

    hit = download_or_use_verified(global_client, real, lcl, md5b64)
    assert hit
    assert lcl.exists()
    assert not _TEST_CACHE.path(fqn).exists()


def test_integration_download_to_cache_with_no_expected_md5_and_reuse_from_there():
    remote = "test/read-only/DONT_DELETE_THESE_FILES.txt"
    fqn = AdlsFqn(global_client.account_name, global_client.file_system_name, remote)
    md5b64 = ""
    lcl = _TEST_DEST / "DONT_DELETE_THESE_FILES----use-cache.txt"
    hit = download_or_use_verified(global_client, remote, lcl, md5b64, cache=_TEST_CACHE)
    assert not hit
    assert lcl.exists()
    assert _TEST_CACHE.path(fqn).exists()

    hit = download_or_use_verified(global_client, remote, lcl, md5b64, cache=_TEST_CACHE)
    assert hit
    assert lcl.exists()
    assert _TEST_CACHE.path(fqn).exists()

    newlcl = _TEST_DEST / "DONT_DELETE---put-in-different-place-but-use-cache.txt"
    hit = download_or_use_verified(global_client, remote, newlcl, md5b64, cache=_TEST_CACHE)
    assert newlcl.exists()
    assert lcl.exists()  # still...
    assert _TEST_CACHE.path(fqn).exists()


def test_integration_handles_emoji_and_long_key():
    # this key is longer than 255 bytes, which is longer than most
    # local filesystems can accept.  therefore, we must truncate it in
    # a reliably-discoverable way.
    remote = "test/read-only/😀aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"  # noqa
    fqn = AdlsFqn(global_client.account_name, global_client.file_system_name, remote)
    md5b64 = "gEL83AfKoP2e3O1Y4RsBqQ=="
    lcl = _TEST_DEST / "benchmark_hashing.py"
    hit = download_or_use_verified(global_client, remote, lcl, md5b64, cache=_TEST_CACHE)
    assert not hit
    assert lcl.exists()
    assert _TEST_CACHE.path(fqn).exists()
    assert len(str(_TEST_CACHE.path(fqn)).encode()) == 255
    assert str(_TEST_CACHE.path(fqn)).endswith("-md5-cfa12bd7609c74476efe66f0b5198e6b")


def test_integration_md5_verification():
    real = "test/read-only/DONT_DELETE_THESE_FILES.txt"
    md5b64 = "incorrect-MrMjF87w3GvA=="
    lcl = _TEST_DEST / "DONT_DELETE_THESE_FILES.txt"
    with pytest.raises(MD5MismatchError):
        download_or_use_verified(global_client, real, lcl, md5b64)


def test_unit_md5_verification():
    made_it = False
    with pytest.raises(MD5MismatchError):
        local_dest = _TEST_DEST / "a-file.txt"
        with open(local_dest, "w") as f:
            f.write("hi")
        with _verify_md5s_before_and_after_download(
            "WPMVPiXYwhMrMjF87w3GvA==",
            "WPMVPiXYwhMrMjF87w3GvA==",
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
async def test_integration_async():
    remote = "test/read-only/DONT_DELETE_THESE_FILES.txt"
    lcl = _TEST_DEST / "DONT_DELETE_THESE_FILES----use-async.txt"

    async_client = _async_adls_fs_client(*_TEST_REMOTE)
    hit = await async_download_or_use_verified(async_client, remote, lcl, "U3vtigRGuroWtJFEQ5dKoQ==")
    assert not hit
    assert lcl.exists()

    hit = await async_download_or_use_verified(async_client, remote, lcl, "U3vtigRGuroWtJFEQ5dKoQ==")
    assert hit
    assert lcl.exists()


@pytest.mark.asyncio
async def test_file_missing_md5_gets_one_assigned_after_download():
    fs_client = _async_adls_fs_client(*_TMP_REMOTE)
    key = "test/writable/missing-md5.txt"
    file_client = fs_client.get_file_client(key)
    await file_client.upload_data(b"hi-i-have-no-md5", overwrite=True)
    fp = await file_client.get_file_properties()
    assert not fp.content_settings.content_md5

    cache_hit = await async_download_or_use_verified(fs_client, key, _TEST_DEST / "missing-md5.txt")
    assert not cache_hit

    fp = await file_client.get_file_properties()
    assert b64(fp.content_settings.content_md5) == "8Wz15VCq6d73Z0+KUDNqVg=="

    # should not error since the md5 should be correct
    cache_hit = await async_download_or_use_verified(fs_client, key, _TEST_DEST / "missing-md5.txt")
    assert cache_hit


def test_file_with_md5_doesnt_try_to_set_it(caplog):
    fs = ADLSFileSystem("thdsscratch", "tmp")
    key = uuid4().hex
    fs.put_file(Path(__file__).parent.parent / "data" / "hello_world.txt", key)

    with caplog.at_level(logging.INFO):
        with TemporaryDirectory() as td:
            download_or_use_verified(
                get_global_client(fs.account_name, fs.file_system), key, Path(td) / "whatever"
            )

    for record in caplog.records:
        assert "missing MD5" not in record.getMessage()
