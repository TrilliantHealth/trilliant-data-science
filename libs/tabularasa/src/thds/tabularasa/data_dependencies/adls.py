import os.path
from functools import lru_cache
from pathlib import Path
from typing import List, Optional, Protocol, Union

import attr

from thds.adls import ADLSFileSystem, fqn
from thds.core import fretry
from thds.tabularasa.schema.files import ADLSDataSpec, RemoteBlobStoreSpec

CACHE_DIR = ".cache/"


@attr.s(auto_attribs=True)
class ADLSFileIntegrityError(FileNotFoundError):
    adls_account: str
    adls_filesystem: str
    adls_path: str
    expected_md5: str
    md5: str

    def __str__(self):
        return (
            f"Unexpected contents for ADLS file: account={self.adls_account} "
            f"filesystem={self.adls_filesystem} path={self.adls_path} "
            f"expected_md5={self.expected_md5} md5={self.md5}"
        )


@attr.s(auto_attribs=True)
class ADLSDownloadResult:
    adls_account: str
    adls_filesystem: str
    adls_path: str
    local_path: Path


@lru_cache(None)
def adls_filesystem(account: str, filesystem: str, cache_dir: Optional[Union[Path, str]] = CACHE_DIR):
    return ADLSFileSystem(account, filesystem, cache_dir=cache_dir)


@fretry.retry_regular(fretry.is_exc(Exception), fretry.n_times(3))
def sync_adls_data(
    adls_spec: ADLSDataSpec, cache_dir: Optional[Union[Path, str]] = CACHE_DIR
) -> List[ADLSDownloadResult]:
    from .util import hash_file

    adls = adls_filesystem(adls_spec.adls_account, adls_spec.adls_filesystem, cache_dir)
    adls_paths = [adls_path.name for adls_path in adls_spec.paths]
    expected_hashes = [adls_path.md5 for adls_path in adls_spec.paths]
    cache_paths = adls.fetch_files(adls_paths)
    results = []
    for adls_path, cache_path, expected_md5 in zip(adls_paths, cache_paths, expected_hashes):
        download_result = ADLSDownloadResult(
            adls_account=adls_spec.adls_account,
            adls_filesystem=adls_spec.adls_filesystem,
            adls_path=adls_path,
            local_path=cache_path,
        )
        if expected_md5 is None:
            results.append(download_result)
            continue

        md5 = hash_file(cache_path)
        if md5 != expected_md5:
            raise ADLSFileIntegrityError(
                adls_account=adls_spec.adls_account,
                adls_filesystem=adls_spec.adls_filesystem,
                adls_path=adls_path,
                expected_md5=expected_md5,
                md5=md5,
            )

        results.append(download_result)
    if adls_spec.ordered:
        result_order = {os.path.basename(result.local_path): result for result in results}
        return [result_order[os.path.basename(path.name)] for path in adls_spec.paths]
    else:
        return results


class SupportsRemoteData(Protocol):
    md5: Optional[str] = None
    blob_store: Optional[RemoteBlobStoreSpec] = None


def get_remote_data_fqn(interface: SupportsRemoteData) -> fqn.AdlsFqn:
    if interface.md5 and interface.blob_store:
        return (
            fqn.of(
                storage_account=interface.blob_store.adls_account,
                container=interface.blob_store.adls_filesystem,
                path=interface.blob_store.path,
            )
            / interface.md5
        )
    raise ValueError("Getting a remote data path requires both the `md5` and `blob_store` to be set.")
