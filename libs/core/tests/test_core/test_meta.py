import datetime
import logging
import os
import subprocess
import typing as ty
from contextlib import contextmanager
from types import MappingProxyType
from unittest.mock import Mock

import pytest
from pytest_mock import MockFixture

from thds.core import meta

BRANCH_NAME = "feature/test-branch#3750"
DOCKER_BRANCH_NAME = "feature-test-branch-3750"
HIVE_BRANCH_NAME = "feature_test_branch_3750"
COMMIT_HASH = "hash123"
USER_NAME = "test.user"
HIVE_USER_NAME = "test_user"
SEMVER_STRING = "2.1.20220919184213"
CALGITVER_STRING = "20220919.1842-abcdef1"


@contextmanager
def envvars(**kwargs: str) -> ty.Iterator[None]:
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
def metadata(misc: meta.MiscType) -> meta.Metadata:
    return meta.Metadata(
        git_commit=COMMIT_HASH,
        git_branch=BRANCH_NAME,
        git_is_clean="True",
        thds_user=USER_NAME,
        misc=misc,
    )


@pytest.fixture
def metadata_unstructured(
    misc: meta.MiscType,
) -> ty.Dict[str, ty.Union[meta.MetaPrimitiveType, meta.MiscType]]:
    return dict(
        git_commit=COMMIT_HASH,
        git_branch=BRANCH_NAME,
        git_is_clean="True",
        thds_user=USER_NAME,
        misc=dict(misc),
    )


@pytest.fixture
def mock_getuser(mocker: MockFixture) -> Mock:
    mock = mocker.patch("thds.core.meta.getuser")
    mock.return_value = USER_NAME
    return mock


@pytest.fixture
def mock_git_commit(mocker: MockFixture) -> Mock:
    mock = mocker.patch("thds.core.meta._simple_run")
    mock.return_value = COMMIT_HASH
    return mock


@pytest.fixture
def mock_git_is_clean(mocker: MockFixture) -> Mock:
    mock = mocker.patch("thds.core.meta._simple_run")
    mock.return_value = True
    return mock


@pytest.fixture
def mock_git_branch(mocker: MockFixture) -> Mock:
    mock = mocker.patch("thds.core.meta._simple_run")
    mock.return_value = BRANCH_NAME
    return mock


@pytest.fixture
def mock_simple_run(mocker: MockFixture) -> Mock:
    pass


@pytest.fixture
def mock_read_metadata(mocker: MockFixture, metadata: meta.Metadata) -> Mock:
    mock = mocker.patch("thds.core.meta.read_metadata")
    mock.return_value = metadata
    return mock


@pytest.fixture
def mock_read_empty_metadata(mocker: MockFixture) -> Mock:
    mock = mocker.patch("thds.core.meta.read_metadata")
    mock.return_value = meta.Metadata()
    return mock


def test_metadata(metadata: meta.Metadata) -> None:
    assert metadata.git_commit == COMMIT_HASH
    assert metadata.git_branch == BRANCH_NAME
    assert metadata.docker_branch == DOCKER_BRANCH_NAME
    assert metadata.hive_branch == HIVE_BRANCH_NAME
    assert metadata.thds_user == USER_NAME
    assert metadata.docker_user == USER_NAME
    assert metadata.hive_user == HIVE_USER_NAME
    assert not metadata.is_empty
    assert metadata.misc["bool"]
    with pytest.raises(TypeError):
        metadata.misc["new"] = "some_value"  # type: ignore


def test_empty_metadata() -> None:
    metadata = meta.Metadata()
    assert metadata.is_empty


def test_metadata_structure(
    metadata_unstructured: ty.Dict[str, ty.Union[meta.MetaPrimitiveType, meta.MiscType]],
    metadata: meta.Metadata,
) -> None:
    assert meta.meta_converter.structure(metadata_unstructured, meta.Metadata) == metadata


def test_metadata_unstructure(
    metadata: meta.Metadata,
    metadata_unstructured: ty.Dict[str, ty.Union[meta.MetaPrimitiveType, meta.MiscType]],
) -> None:
    assert meta.meta_converter.unstructure(metadata) == metadata_unstructured


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
        with caplog.at_level(logging.DEBUG):
            assert meta.get_user() == USER_NAME
        assert "`get_user` reading from env var." in caplog.text


def test_get_hive_user_from_envvar(caplog) -> None:
    with envvars(THDS_USER=USER_NAME):
        with caplog.at_level(logging.DEBUG):
            assert meta.get_user(format="hive") == HIVE_USER_NAME
        assert "`get_user` reading from env var." in caplog.text


def test_get_user_from_metadata(caplog, mock_read_metadata: Mock) -> None:
    with caplog.at_level(logging.DEBUG):
        assert meta.get_user("test") == USER_NAME
    assert mock_read_metadata.called
    assert "`get_user` reading from metadata." in caplog.text


def test_get_user_from_metadata_no_metadata(
    caplog, mock_read_metadata: Mock, mock_getuser: Mock
) -> None:
    mock_read_metadata.return_value = meta.Metadata()
    with caplog.at_level(logging.DEBUG):
        assert meta.get_user("test") == USER_NAME
    assert mock_getuser.called
    assert "`get_user` reading from metadata." in caplog.text
    assert "`get_user` found no user data - getting system user." in caplog.text


def test_get_user_no_user(caplog, mock_getuser: Mock) -> None:
    with caplog.at_level(logging.DEBUG):
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


def test_extract_timestamp_calver_str() -> None:
    assert meta.extract_timestamp(CALGITVER_STRING, as_datetime=False) == "20220919184200"


def test_extract_timestamp_calver_datetime() -> None:
    assert meta.extract_timestamp(CALGITVER_STRING, as_datetime=True) == datetime.datetime(
        2022, 9, 19, 18, 42, 0, tzinfo=datetime.timezone.utc
    )


def test_extract_timestamp_unsupported_version_format() -> None:
    with pytest.raises(ValueError):
        meta.extract_timestamp("1")


def test_get_commit_from_envvar(caplog) -> None:
    with envvars(GIT_COMMIT=COMMIT_HASH):
        with caplog.at_level(logging.DEBUG):
            assert meta.get_commit() == COMMIT_HASH
        assert "`get_commit` reading from env var." in caplog.text


def test_get_commit_from_git_repo(caplog, mock_git_commit: Mock) -> None:
    with caplog.at_level(logging.DEBUG):
        assert meta.get_commit() == COMMIT_HASH
    assert mock_git_commit.called
    assert "`get_commit` reading from Git repo." in caplog.text


def test_get_commit_from_metadata(caplog, mock_git_commit: Mock, mock_read_metadata: Mock) -> None:
    mock_git_commit.side_effect = subprocess.CalledProcessError(-1, [])
    with caplog.at_level(logging.DEBUG):
        assert meta.get_commit("test") == COMMIT_HASH
    assert mock_git_commit.called
    assert mock_read_metadata.called
    assert "`get_commit` reading from metadata." in caplog.text


def test_get_commit_from_metadata_no_metadata(
    caplog, mock_git_commit: Mock, mock_read_metadata: Mock
) -> None:
    mock_git_commit.side_effect = subprocess.CalledProcessError(-1, [])
    mock_read_metadata.return_value = meta.Metadata()
    with caplog.at_level(logging.DEBUG):
        with envvars(NO_GIT_FALLBACK="true"):
            assert not meta.get_commit("test")
    assert mock_git_commit.called
    assert mock_read_metadata.called
    assert "`get_commit` reading from metadata." in caplog.text
    assert "`get_commit` found no commit." in caplog.text


def test_get_commit_no_commit(caplog, mock_git_commit: Mock) -> None:
    mock_git_commit.side_effect = subprocess.CalledProcessError(-1, [])
    with caplog.at_level(logging.DEBUG):
        with envvars(NO_GIT_FALLBACK="true"):
            assert not meta.get_commit()
    assert mock_git_commit.called
    assert "`get_commit` found no commit." in caplog.text


def test_is_clean_from_envvar(caplog) -> None:
    with envvars(GIT_IS_CLEAN="True"):
        with caplog.at_level(logging.DEBUG):
            assert meta.is_clean()
        assert "`is_clean` reading from env var." in caplog.text


def test_is_clean_from_git_repo(caplog, mock_git_is_clean: Mock) -> None:
    with caplog.at_level(logging.DEBUG):
        assert not meta.is_clean()
    assert mock_git_is_clean.called
    assert "`is_clean` reading from Git repo." in caplog.text


def test_is_clean_no_dirtiness(caplog, mock_git_is_clean: Mock) -> None:
    mock_git_is_clean.side_effect = subprocess.CalledProcessError(-1, [])
    with caplog.at_level(logging.DEBUG):
        with envvars(NO_GIT_FALLBACK="true"):
            assert meta.is_clean()
    assert mock_git_is_clean.called
    assert "`is_clean` found no cleanliness - assume dirty." in caplog.text


def test_get_branch_from_envvar(caplog) -> None:
    with envvars(GIT_BRANCH=BRANCH_NAME):
        with caplog.at_level(logging.DEBUG):
            assert meta.get_branch() == BRANCH_NAME
        assert "`get_branch` reading from env var." in caplog.text


def test_get_hive_branch_from_envvar(caplog) -> None:
    with envvars(GIT_BRANCH=BRANCH_NAME):
        with caplog.at_level(logging.DEBUG):
            assert meta.get_branch(format="hive") == HIVE_BRANCH_NAME
        assert "`get_branch` reading from env var." in caplog.text


def test_get_branch_from_git_repo(caplog, mock_git_branch: Mock) -> None:
    with caplog.at_level(logging.DEBUG):
        assert meta.get_branch() == BRANCH_NAME
    assert mock_git_branch.called
    assert "`get_branch` reading from Git repo." in caplog.text


def test_get_branch_from_metadata(caplog, mock_git_branch: Mock, mock_read_metadata: Mock) -> None:
    mock_git_branch.side_effect = subprocess.CalledProcessError(-1, [])
    with caplog.at_level(logging.DEBUG):
        with envvars(NO_GIT_FALLBACK="true"):
            assert meta.get_branch("test") == BRANCH_NAME
    assert mock_git_branch.called
    assert "`get_branch` reading from metadata." in caplog.text


def test_get_branch_from_metadata_no_metadata(
    caplog, mock_git_branch: Mock, mock_read_metadata: Mock
) -> None:
    mock_git_branch.side_effect = subprocess.CalledProcessError(-1, [])
    mock_read_metadata.return_value = meta.Metadata()
    with caplog.at_level(logging.DEBUG):
        with envvars(NO_GIT_FALLBACK="true"):
            assert not meta.get_branch("test")
    assert mock_git_branch.called
    assert mock_read_metadata.called
    assert "`get_branch` reading from metadata." in caplog.text
    assert "`get_branch` found no branch." in caplog.text


def test_get_branch_no_branch(caplog, mock_git_branch: Mock) -> None:
    mock_git_branch.side_effect = subprocess.CalledProcessError(-1, [])
    with caplog.at_level(logging.DEBUG):
        with envvars(NO_GIT_FALLBACK="true"):
            assert not meta.get_branch()
    assert mock_git_branch.called
    assert "`get_branch` found no branch." in caplog.text


def test_is_deployed_with_metadata(mock_read_metadata: Mock) -> None:
    assert meta.is_deployed("test")


def test_is_deployed_with_empty_metadata(mock_read_empty_metadata: Mock) -> None:
    assert not meta.is_deployed("test")
