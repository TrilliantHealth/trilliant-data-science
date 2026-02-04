import datetime
import io
import json
import logging
import os
import subprocess
import typing as ty
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError
from importlib.resources import Package
from types import MappingProxyType
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture

from thds.core import git, meta

PACKAGE_NAME = "thds.test"
MODULE_NAME = "thds.test.module"
BRANCH_NAME = "feature/test_my-branch#37.50"
DOCKER_BRANCH_NAME = "feature-test_my-branch-37.50"
HIVE_BRANCH_NAME = "feature_test_my_branch_37_50"
COMMIT_HASH = "hash123"
USER_NAME = "test.user"
HIVE_USER_NAME = "test_user"
SEMVER_STRING = "2.1.20220919184213"
CALGITVER_STRING = "20220919.1842-abcdef1"


@contextmanager
def envvars(**kwargs: str) -> ty.Generator[None, None, None]:
    for kw, arg in kwargs.items():
        os.environ[kw] = arg

    try:
        yield
    finally:
        for kw in kwargs.keys():
            del os.environ[kw]


@pytest.fixture
def misc() -> meta.MiscType:
    return MappingProxyType({"bool": True, "float": 3.14, "int": 55, "str": "test"})


@pytest.fixture
def mock_getuser(mocker: MockFixture) -> MagicMock:
    mock = mocker.patch.object(meta, "getuser", autospec=True)
    mock.return_value = USER_NAME
    return mock


@pytest.fixture
def mock_git_commit(mocker: MockFixture) -> MagicMock:
    mock = mocker.patch.object(git, "_simple_run", autospec=True)
    mock.return_value = COMMIT_HASH
    return mock


@pytest.fixture
def mock_git_is_clean(mocker: MockFixture) -> MagicMock:
    mock = mocker.patch.object(git, "_simple_run", autospec=True)
    mock.return_value = True
    return mock


@pytest.fixture
def mock_git_branch(mocker: MockFixture) -> MagicMock:
    mock = mocker.patch.object(git, "_simple_run", autospec=True)
    mock.return_value = BRANCH_NAME
    return mock


@pytest.fixture
def mock_version(mocker: MockFixture) -> MagicMock:
    mock = mocker.patch.object(meta, "version", autospec=True)

    def get_version(pkg: Package) -> str:
        if pkg == PACKAGE_NAME or pkg == PACKAGE_NAME.replace(".", "_"):
            return SEMVER_STRING
        raise PackageNotFoundError

    mock.side_effect = get_version
    return mock


@pytest.fixture
def mock_base_package(mocker: MockFixture) -> MagicMock:
    mock = mocker.patch.object(meta, "version", autospec=True)

    def get_base_package(pkg: Package) -> str:
        if pkg == PACKAGE_NAME or pkg == PACKAGE_NAME.replace(".", "_"):
            return str(pkg)
        raise PackageNotFoundError

    mock.side_effect = get_base_package
    return mock


@pytest.fixture
def mock_open_text(
    mocker: MockFixture, metadata_unstructured: ty.Dict[str, ty.Union[str, bool, meta.MiscType]]
) -> MagicMock:
    mock = mocker.patch.object(meta, "open_text", autospec=True)

    @contextmanager
    def get_metadata(pkg: Package, _: str) -> ty.Generator[ty.IO[str], None, None]:
        if pkg == PACKAGE_NAME:
            yield io.StringIO(json.dumps(metadata_unstructured))
        else:
            raise FileNotFoundError

    mock.side_effect = get_metadata
    return mock


def test_format_name_git() -> None:
    assert meta.format_name(BRANCH_NAME, format="git") == BRANCH_NAME


def test_format_name_docker() -> None:
    assert meta.format_name(BRANCH_NAME, format="docker") == DOCKER_BRANCH_NAME


def test_format_name_hive() -> None:
    assert meta.format_name(BRANCH_NAME, format="hive") == HIVE_BRANCH_NAME


def test_format_name_unsupported() -> None:
    with pytest.raises(ValueError):
        meta.format_name(BRANCH_NAME, format="unsupported")  # type: ignore


def test_get_user_from_envvar(caplog) -> None:
    with envvars(THDS_USER=USER_NAME):
        with caplog.at_level(logging.DEBUG, logger="thds.core.meta"):
            assert meta.get_user() == USER_NAME
        assert "`get_user` reading from env var." in caplog.text


def test_get_hive_user_from_envvar(caplog) -> None:
    with envvars(THDS_USER=USER_NAME):
        with caplog.at_level(logging.DEBUG, logger="thds.core.meta"):
            assert meta.get_user(format="hive") == HIVE_USER_NAME
        assert "`get_user` reading from env var." in caplog.text


def test_get_user_no_user(caplog, mock_getuser: MagicMock) -> None:
    with caplog.at_level(logging.DEBUG, logger="thds.core.meta"):
        assert meta.get_user() == USER_NAME
    assert mock_getuser.called
    assert "`get_user` found no user data - getting system user." in caplog.text


def test_get_timestamp_str() -> None:
    timestamp = meta.get_timestamp(as_datetime=False)
    assert len(timestamp) == 14
    assert timestamp.isnumeric()


def test_get_timestamp_datetime() -> None:
    timestamp = meta.get_timestamp(as_datetime=True)
    assert isinstance(timestamp, datetime.datetime)
    assert timestamp.tzinfo == datetime.timezone.utc


def test_extract_timestamp_semver_str() -> None:
    assert meta.extract_timestamp(SEMVER_STRING, as_datetime=False) == "20220919184213"


def test_extract_timestamp_semver_datetime() -> None:
    assert meta.extract_timestamp(SEMVER_STRING, as_datetime=True) == datetime.datetime.strptime(
        SEMVER_STRING.split(".")[2], meta.TIMESTAMP_FORMAT
    ).replace(tzinfo=datetime.timezone.utc)


def test_extract_timestamp_semver_no_date_str() -> None:
    with pytest.raises(ValueError):
        meta.extract_timestamp(".".join(SEMVER_STRING.split(".")[:2]), as_datetime=False)


def test_extract_timestamp_semver_no_date_str2() -> None:
    with pytest.raises(ValueError):
        meta.extract_timestamp("1.1.1", as_datetime=False)


def test_extract_timestamp_semver_no_date_datetime() -> None:
    with pytest.raises(ValueError):
        meta.extract_timestamp(".".join(SEMVER_STRING.split(".")[:2]), as_datetime=True)


def test_extract_timestamp_semver_no_date_datetime2() -> None:
    with pytest.raises(ValueError):
        meta.extract_timestamp("1.1.1", as_datetime=True)


def test_extract_timestamp_calgitver_str() -> None:
    assert meta.extract_timestamp(CALGITVER_STRING, as_datetime=False) == "20220919184200"


def test_extract_timestamp_calgitver_datetime() -> None:
    assert meta.extract_timestamp(CALGITVER_STRING, as_datetime=True) == datetime.datetime(
        2022, 9, 19, 18, 42, 0, tzinfo=datetime.timezone.utc
    )


def test_extract_timestamp_bad_calgitver_str() -> None:
    with pytest.raises(ValueError):
        # '13' in the month spot
        meta.extract_timestamp("20221319.1842-abcdef1", as_datetime=False)


def test_extract_timestamp_unsupported_version_format() -> None:
    with pytest.raises(ValueError):
        meta.extract_timestamp("1")


def test_get_commit_from_envvar(caplog) -> None:
    with envvars(GIT_COMMIT=COMMIT_HASH):
        with caplog.at_level(logging.DEBUG, logger="thds.core.meta"):
            assert meta.get_commit() == COMMIT_HASH
        assert "`get_commit` reading from env var." in caplog.text


def test_get_commit_from_git_repo(caplog, mock_git_commit: MagicMock) -> None:
    with caplog.at_level(logging.DEBUG, logger="thds.core.git"):
        assert meta.get_commit() == COMMIT_HASH
    assert mock_git_commit.called
    assert "`get_commit` reading from Git repo." in caplog.text


def test_get_commit_no_commit(caplog, mock_git_commit: MagicMock) -> None:
    mock_git_commit.side_effect = subprocess.CalledProcessError(-1, [])
    with caplog.at_level(logging.WARNING, logger="thds.core.meta"):
        assert not meta.get_commit()
    assert mock_git_commit.called
    assert "`get_commit` found no commit." in caplog.text


def test_is_clean_from_clean_envvar(caplog) -> None:
    with envvars(GIT_IS_CLEAN="True"):
        with caplog.at_level(logging.DEBUG, logger="thds.core.meta"):
            assert meta.is_clean()
        assert "`is_clean` reading from env var." in caplog.text


def test_is_clean_from_dirty_envvar(caplog) -> None:
    with envvars(GIT_IS_DIRTY=""):
        with caplog.at_level(logging.DEBUG, logger="thds.core.meta"):
            assert meta.is_clean()
        assert "`is_clean` reading from env var." in caplog.text


def test_is_clean_from_git_repo(caplog, mock_git_is_clean: MagicMock) -> None:
    with caplog.at_level(logging.DEBUG, logger="thds.core.git"):
        assert not meta.is_clean()
    assert mock_git_is_clean.called
    assert "`is_clean` reading from Git repo." in caplog.text


def test_is_clean_no_dirtiness(caplog, mock_git_is_clean: MagicMock) -> None:
    mock_git_is_clean.side_effect = subprocess.CalledProcessError(-1, [])
    with caplog.at_level(logging.WARNING):
        assert not meta.is_clean()
    assert mock_git_is_clean.called
    assert "`is_clean` found no cleanliness - assume dirty." in caplog.text


def test_get_branch_from_envvar(caplog) -> None:
    with envvars(GIT_BRANCH=BRANCH_NAME):
        with caplog.at_level(logging.DEBUG, logger="thds.core.meta"):
            assert meta.get_branch() == BRANCH_NAME
        assert "`get_branch` reading from env var." in caplog.text


def test_get_hive_branch_from_envvar(caplog) -> None:
    with envvars(GIT_BRANCH=BRANCH_NAME):
        with caplog.at_level(logging.DEBUG, logger="thds.core.meta"):
            assert meta.get_branch(format="hive") == HIVE_BRANCH_NAME
        assert "`get_branch` reading from env var." in caplog.text


def test_get_branch_from_git_repo(caplog, mock_git_branch: MagicMock) -> None:
    with caplog.at_level(logging.DEBUG, logger="thds.core.git"):
        assert meta.get_branch() == BRANCH_NAME
    assert mock_git_branch.called
    assert "`get_branch` reading from Git repo." in caplog.text


def test_get_branch_no_branch(caplog, mock_git_branch: MagicMock) -> None:
    mock_git_branch.side_effect = subprocess.CalledProcessError(-1, [])
    with caplog.at_level(logging.WARNING):
        assert not meta.get_branch()
    assert mock_git_branch.called
    assert "`get_branch` found no branch." in caplog.text


def test_get_version_from_package(mock_version: MagicMock) -> None:
    assert meta.get_version(PACKAGE_NAME) == SEMVER_STRING
    assert mock_version.call_count == 1


def test_get_version_from_module(mock_version: MagicMock) -> None:
    assert meta.get_version(f"{PACKAGE_NAME}.module") == SEMVER_STRING
    assert mock_version.call_count == 2


def test_get_version_no_package(caplog, mock_version: MagicMock) -> None:
    with caplog.at_level(logging.WARNING):
        assert meta.get_version("unknown.package") == ""
    assert mock_version.call_count == 2
    assert "Could not find a version" in caplog.text


def test_get_base_package_from_package(mock_base_package: MagicMock) -> None:
    assert meta.get_base_package(PACKAGE_NAME) == PACKAGE_NAME
    assert mock_base_package.call_count == 1


def test_get_base_package_from_module(mock_base_package: MagicMock) -> None:
    assert meta.get_base_package(f"{PACKAGE_NAME}.module") == PACKAGE_NAME
    assert mock_base_package.call_count == 3


def test_get_base_package_no_package(caplog, mock_base_package: MagicMock) -> None:
    with caplog.at_level(logging.WARNING):
        assert meta.get_base_package("unknown.package") == ""
    assert mock_base_package.call_count == 4
    assert "Could not find the base package" in caplog.text


def test_get_repo_name():
    assert meta.get_repo_name() == "ds-monorepo"


def test_get_base_package_gives_nice_error_for_main():
    with pytest.raises(meta.NoBasePackageFromMain, match="nice introspection"):
        meta.get_base_package("__main__")
