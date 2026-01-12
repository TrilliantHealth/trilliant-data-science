# --- Integration tests for etag-based download caching ---

import typing as ty
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from azure.identity.aio import DefaultAzureCredential
from azure.storage.filedatalake import aio

from thds.adls.download import async_download_or_use_verified
from thds.adls.fqn import AdlsRoot


def _async_adls_fs_client(storage_account: str, container: str) -> aio.FileSystemClient:
    return aio.DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=DefaultAzureCredential(exclude_shared_token_cache_credential=True),
    ).get_file_system_client(file_system=container)


class RemoteFileWithoutMd5(ty.NamedTuple):
    """A remote file uploaded without MD5, for testing etag-only scenarios."""

    fs_client: aio.FileSystemClient
    key: str
    content: bytes
    original_etag: str


@pytest_asyncio.fixture
async def remote_file_without_md5(
    tmp_remote_root: AdlsRoot,
) -> ty.AsyncIterator[RemoteFileWithoutMd5]:
    """Upload a file without MD5 and clean up after the test.

    This fixture uploads a file using the raw SDK (bypassing our upload logic)
    so that no content-md5 or metadata hashes are set. Only the etag is available.
    """
    fs_client = _async_adls_fs_client(*tmp_remote_root)
    unique_id = uuid4().hex
    key = f"test/thds.adls/etag-integration-test/{unique_id}.txt"
    content = f"etag test content {unique_id}".encode()

    file_client = fs_client.get_file_client(key)

    # Upload without MD5 - using raw SDK bypasses our upload logic
    await file_client.upload_data(content, overwrite=True)

    # Verify no hash exists and capture original etag
    fp = await file_client.get_file_properties()
    assert not fp.content_settings.content_md5, "File should have no content_md5"
    assert not fp.metadata, "File should have no metadata initially"
    assert fp.etag, "File should have an etag"

    yield RemoteFileWithoutMd5(fs_client, key, content, fp.etag)

    # Cleanup
    try:
        await file_client.delete_file()
    except Exception:
        pass


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_etag_cache_hit_same_path(
    remote_file_without_md5: RemoteFileWithoutMd5, test_dest: Path
):
    """Test that etag enables cache hits when downloading to the same path twice."""
    rf = remote_file_without_md5
    local_path = test_dest / f"etag_same_path_{uuid4().hex}.txt"

    # First download - populates local etag cache
    cache_hit_1 = await async_download_or_use_verified(
        rf.fs_client, rf.key, local_path, set_remote_hash=False
    )
    assert not cache_hit_1, "First download should not be a cache hit"
    assert local_path.exists()
    assert local_path.read_bytes() == rf.content

    # Verify etag is unchanged (we didn't set metadata)
    file_client = rf.fs_client.get_file_client(rf.key)
    fp = await file_client.get_file_properties()
    assert fp.etag == rf.original_etag, "Etag should be unchanged"
    assert not fp.metadata, "No metadata should have been set"

    # Second download to same path - should be a cache hit via etag
    cache_hit_2 = await async_download_or_use_verified(
        rf.fs_client, rf.key, local_path, set_remote_hash=False
    )
    assert cache_hit_2, "Second download should be a cache hit using etag"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_etag_cache_hit_different_paths_same_content(
    remote_file_without_md5: RemoteFileWithoutMd5, test_dest: Path
):
    """Test that etag cache enables hits when the same content exists at different local paths.

    The etag cache maps local content (xxhash) to the remote etag. So if we:
    1. Download to path A (populates etag cache keyed by content hash)
    2. Copy the file to path B (same content, same xxhash)
    3. Download to path B - should get cache hit because content hash matches cached etag
    """
    rf = remote_file_without_md5
    unique = uuid4().hex
    local_path_1 = test_dest / f"etag_path1_{unique}.txt"
    local_path_2 = test_dest / f"etag_path2_{unique}.txt"

    # First download to path 1 - populates etag cache
    await async_download_or_use_verified(rf.fs_client, rf.key, local_path_1, set_remote_hash=False)
    assert local_path_1.exists()

    # Copy content to path 2 (simulating having the same file content already)
    local_path_2.write_bytes(rf.content)

    # Download to path 2 - should be cache hit because content matches cached etag
    cache_hit = await async_download_or_use_verified(
        rf.fs_client, rf.key, local_path_2, set_remote_hash=False
    )
    assert cache_hit, "Should get cache hit when local content matches cached etag"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_etag_cache_miss_different_paths_no_local_file(
    remote_file_without_md5: RemoteFileWithoutMd5, test_dest: Path
):
    """Test that downloading to a new path (no existing file) triggers a fresh download."""
    rf = remote_file_without_md5
    unique = uuid4().hex
    local_path_1 = test_dest / f"etag_first_{unique}.txt"
    local_path_2 = test_dest / f"etag_second_{unique}.txt"

    # First download to path 1 - populates etag cache
    await async_download_or_use_verified(rf.fs_client, rf.key, local_path_1, set_remote_hash=False)
    assert local_path_1.exists()

    # Download to path 2 (doesn't exist) - must download, not a cache hit
    cache_hit = await async_download_or_use_verified(
        rf.fs_client, rf.key, local_path_2, set_remote_hash=False
    )
    assert not cache_hit, "No cache hit when local file doesn't exist"
    assert local_path_2.exists()
    assert local_path_2.read_bytes() == rf.content

    # But now path 2 exists, so third download should hit
    cache_hit_3 = await async_download_or_use_verified(
        rf.fs_client, rf.key, local_path_2, set_remote_hash=False
    )
    assert cache_hit_3, "Third download should be cache hit"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_etag_blob_api_list_then_dfs_api_download(
    tmp_remote_root: AdlsRoot, test_dest: Path
):
    """Regression test: ETags from Blob API listing must match DFS API download.

    This tests the specific scenario that was broken before the fix in _etag.py:
    - list_blobs (Blob API) returns ETags WITHOUT quotes: '0x8DE1DBAB15C03E1'
    - get_file_properties (DFS API) returns ETags WITH quotes: '"0x8DE1DBAB15C03E1"'

    Before the fix, extract_etag_bytes() would produce different byte representations
    for the same ETag value depending on whether it was quoted, causing HashMismatchError
    when a file was listed via Blob API but downloaded via DFS API.

    This is the exact flow used by list_fast + Source.path() in unified-asset.
    """
    from azure.storage.blob import ContainerClient

    from thds.adls._etag import ETAG_FAKE_HASH_NAME, extract_etag_bytes
    from thds.adls.global_client import get_global_fs_client
    from thds.adls.shared_credential import SharedCredential
    from thds.core.hashing import Hash

    storage_account, container = tmp_remote_root
    unique_id = uuid4().hex
    key = f"test/thds.adls/etag-blob-dfs-regression/{unique_id}.txt"
    content = f"blob-dfs etag regression test {unique_id}".encode()

    # Upload via DFS API (simulating external data that has no hash metadata)
    fs_client = get_global_fs_client(storage_account, container)
    file_client = fs_client.get_file_client(key)
    file_client.upload_data(content, overwrite=True)

    try:
        # Step 1: List via Blob API (like list_fast does) - returns UNQUOTED etag
        blob_container = ContainerClient(
            account_url=f"https://{storage_account}.blob.core.windows.net",
            container_name=container,
            credential=SharedCredential(),
        )
        blob_props = None
        for blob in blob_container.list_blobs(name_starts_with=key):
            blob_props = blob
            break

        assert blob_props is not None, "File should be listable via Blob API"
        blob_etag = blob_props.etag
        # Blob API list_blobs returns unquoted etags
        assert not blob_etag.startswith(
            '"'
        ), f"list_blobs should return unquoted etag, got {blob_etag!r}"

        # Step 2: Get properties via DFS API (like download does) - returns QUOTED etag
        dfs_props = file_client.get_file_properties()
        dfs_etag = dfs_props.etag
        # DFS API get_file_properties returns quoted etags
        assert dfs_etag
        assert dfs_etag.startswith(
            '"'
        ), f"get_file_properties should return quoted etag, got {dfs_etag!r}"

        # Step 3: Verify both ETags produce the SAME byte representation
        # This is the core of the regression test - before the fix, these would differ
        blob_etag_bytes = extract_etag_bytes(blob_etag)
        dfs_etag_bytes = extract_etag_bytes(dfs_etag)

        assert blob_etag_bytes == dfs_etag_bytes, (
            f"ETags from Blob API and DFS API should produce identical bytes!\n"
            f"  Blob API etag: {blob_etag!r} -> {blob_etag_bytes.hex()}\n"
            f"  DFS API etag:  {dfs_etag!r} -> {dfs_etag_bytes.hex()}"
        )

        # Step 4: Simulate the full flow: create expected hash from Blob API etag,
        # then download with DFS API verification
        expected_hash = Hash(ETAG_FAKE_HASH_NAME, blob_etag_bytes)
        local_path = test_dest / f"blob_dfs_regression_{unique_id}.txt"

        # This should NOT raise HashMismatchError
        # (before the fix, it would fail because blob_etag_bytes != dfs_etag_bytes)
        await async_download_or_use_verified(
            _async_adls_fs_client(storage_account, container),
            key,
            local_path,
            expected_hash=expected_hash,
            set_remote_hash=False,
        )

        assert local_path.exists()
        assert local_path.read_bytes() == content

    finally:
        # Cleanup
        try:
            file_client.delete_file()
        except Exception:
            pass
