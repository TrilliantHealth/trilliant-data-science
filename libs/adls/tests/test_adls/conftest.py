import typing as ty
from pathlib import Path
from uuid import uuid4

import pytest
from azure.storage.filedatalake import FileSystemClient

from thds.adls.fqn import AdlsRoot
from thds.adls.global_client import get_global_fs_client

_TMPDIR = "for-adls-tests--"


@pytest.fixture(scope="session")
def test_dest(tmp_path_factory: pytest.TempPathFactory) -> ty.Iterator[Path]:
    p = tmp_path_factory.mktemp(_TMPDIR)
    yield p


_TEST_REMOTE = "thdsdatasets", "prod-datasets"
# this location for test data has been blessed by Matt Eby himself.
_TMP_REMOTE = "thdsscratch", "tmp"


@pytest.fixture(scope="session")
def test_remote_root() -> AdlsRoot:
    return AdlsRoot.of(*_TEST_REMOTE)


@pytest.fixture(scope="session")
def tmp_remote_root() -> AdlsRoot:
    return AdlsRoot.of(*_TMP_REMOTE)


@pytest.fixture
def global_test_fs_client() -> FileSystemClient:
    # could be session-scoped but not suitable for unit tests
    # the inner function call is cached anyway
    return get_global_fs_client(*_TEST_REMOTE)


@pytest.fixture
def global_tmp_fs_client() -> FileSystemClient:
    # could be session-scoped but not suitable for unit tests
    # the inner function call is cached anyway
    return get_global_fs_client(*_TMP_REMOTE)


@pytest.fixture
def random_test_file_path() -> str:
    random_part = "random-test-file-fqn-" + uuid4().hex
    return f"test/thds.adls/{random_part}"
