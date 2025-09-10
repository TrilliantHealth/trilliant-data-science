import logging
import typing as ty

import pytest
from azure.storage.blob._models import BlobProperties
from pytest_mock import MockFixture

from thds.adls import ADLSFileSystem, copy, file_properties, fqn, hashes


@pytest.fixture
def mock_copy_status(mocker: MockFixture) -> ty.Iterator[ty.Callable[[ty.Optional[str]], None]]:
    mock = mocker.patch.object(copy, "get_blob_properties", autospec=True)

    def set_status(status: ty.Optional[str]) -> None:
        blob_properties = BlobProperties()
        blob_properties.copy.status = status
        mock.return_value = blob_properties

    yield set_status


def test_unit_wait_for_copy(mock_copy_status: ty.Callable[[ty.Optional[str]], None]) -> None:
    fqn_ = fqn.AdlsFqn("account", "container", "path")
    mock_copy_status("success")

    assert copy.wait_for_copy(fqn_) == fqn_


@pytest.mark.parametrize(
    "status",
    [
        pytest.param(None, id="not a copy or not copying"),
        pytest.param("failed", id="copy failed"),
    ],
)
def test_unit_wait_for_copy_fails(
    mock_copy_status: ty.Callable[[ty.Optional[str]], None],
    status: ty.Optional[str],
) -> None:
    mock_copy_status(status)
    with pytest.raises(ValueError):
        copy.wait_for_copy(fqn.AdlsFqn("account", "container", "path"))


_FILE_TO_COPY_PATH = "test/read-only/DONT_DELETE_THESE_FILES.txt"
_FILE_TO_COPY_OVERWRITE_PATH = "test/read-only/integration_nppes.zip"

_FILE_TO_COPY_WITH_XXH3_128_IN_METADATA = "test/read-only/this-file-has-a-xxh3-128-in-its-metadata.txt"


@pytest.fixture
def copy_file_setup(
    test_remote_root: fqn.AdlsRoot,
    tmp_remote_root: fqn.AdlsRoot,
    random_test_file_path: str,
) -> ty.Iterator[copy.CopyInfo]:
    tmp_fs = ADLSFileSystem(*tmp_remote_root)
    src = fqn.AdlsFqn(*test_remote_root, path=_FILE_TO_COPY_PATH)
    dest = fqn.AdlsFqn(*tmp_remote_root, path=random_test_file_path)

    yield copy.copy_file(src, dest)

    tmp_fs.delete_file(dest.path)


@pytest.fixture
def file_to_overwrite_with(test_remote_root: fqn.AdlsRoot) -> fqn.AdlsFqn:
    return test_remote_root / _FILE_TO_COPY_OVERWRITE_PATH


def test_integration_copy_file(copy_file_setup: copy.CopyInfo) -> None:
    copy_info = copy_file_setup  # setup does an initial copy
    src_blob_props = file_properties.get_blob_properties(copy_info.src)
    dest_blob_props = file_properties.get_blob_properties(copy_info.dest)

    assert copy_info.copy_occurred
    assert dest_blob_props.copy.status == "success"
    assert dest_blob_props.content_settings.content_md5 == src_blob_props.content_settings.content_md5


def test_integration_copy_file_with_xxhash(
    test_remote_root, tmp_remote_root, random_test_file_path
) -> None:
    src = test_remote_root / _FILE_TO_COPY_WITH_XXH3_128_IN_METADATA
    dest = tmp_remote_root / (random_test_file_path + "-xxh3_128")

    src_hashes = hashes.extract_hashes_from_props(file_properties.get_blob_properties(src))
    assert "xxh3_128" in src_hashes

    first_copy = copy.copy_file(src, dest)
    assert first_copy.copy_occurred

    dest_hashes = hashes.extract_hashes_from_props(file_properties.get_blob_properties(dest))
    assert "xxh3_128" in dest_hashes
    assert dest_hashes["xxh3_128"] == src_hashes["xxh3_128"]

    second_copy = copy.copy_file(src, dest)
    assert not second_copy.copy_occurred


def test_integration_copy_file_same_src_dest(
    caplog: pytest.LogCaptureFixture,
    copy_file_setup: copy.CopyInfo,
) -> None:
    src, dest, copy_request1 = copy_file_setup  # setup does an initial copy

    with caplog.at_level(logging.INFO):
        _, _, copy_request2 = copy.copy_file(
            src, dest
        )  # dest is already the same as src, no copy will actually happen
        assert "same md5" in caplog.text

    assert not copy_request2


def test_integration_copy_file_silent_overwrite(
    copy_file_setup: copy.CopyInfo,
    file_to_overwrite_with: fqn.AdlsFqn,
) -> None:
    src, dest, copy_request1 = copy_file_setup  # setup does an initial copy
    _, _, copy_request2 = copy.copy_file(
        file_to_overwrite_with,
        dest,
        overwrite_method="silent",
    )

    assert copy_request2
    assert copy_request2["last_modified"] >= copy_request1["last_modified"]  # type: ignore[operator]

    src_blob_props = file_properties.get_blob_properties(src)
    dest_blob_props = file_properties.get_blob_properties(dest)

    assert dest_blob_props.copy.status == "success"
    assert dest_blob_props.content_settings.content_md5 != src_blob_props.content_settings.content_md5


def test_integration_copy_file_warn_on_overwrite(
    caplog: pytest.LogCaptureFixture,
    copy_file_setup: ty.Tuple[fqn.AdlsFqn, fqn.AdlsFqn, copy.CopyRequest],
    file_to_overwrite_with: fqn.AdlsFqn,
) -> None:
    src, dest, copy_request1 = copy_file_setup  # setup does an initial copy

    with caplog.at_level(logging.WARNING):
        _, _, copy_request2 = copy.copy_file(file_to_overwrite_with, dest, overwrite_method="warn")
        assert "will be overwritten" in caplog.text

    assert copy_request2
    assert copy_request2["last_modified"] >= copy_request1["last_modified"]  # type: ignore[operator]

    src_blob_props = file_properties.get_blob_properties(src)
    dest_blob_props = file_properties.get_blob_properties(dest)

    assert dest_blob_props.copy.status == "success"
    assert dest_blob_props.content_settings.content_md5 != src_blob_props.content_settings.content_md5


def test_integration_copy_file_skip_overwrite(
    caplog: pytest.LogCaptureFixture,
    copy_file_setup: ty.Tuple[fqn.AdlsFqn, fqn.AdlsFqn, copy.CopyRequest],
    file_to_overwrite_with: fqn.AdlsFqn,
) -> None:
    src, dest, copy_request1 = copy_file_setup  # setup does an initial copy

    with caplog.at_level(logging.WARNING):
        _, _, copy_request2 = copy.copy_file(file_to_overwrite_with, dest, overwrite_method="skip")
        assert "already exists, skipping" in caplog.text

    assert copy_request1
    assert not copy_request2

    src_blob_props = file_properties.get_blob_properties(src)
    dest_blob_props = file_properties.get_blob_properties(dest)

    assert dest_blob_props.copy.status == "success"
    assert dest_blob_props.content_settings.content_md5 == src_blob_props.content_settings.content_md5


def test_integration_copy_file_error_on_overwrite(
    copy_file_setup: ty.Tuple[fqn.AdlsFqn, fqn.AdlsFqn, copy.CopyRequest],
    file_to_overwrite_with: fqn.AdlsFqn,
) -> None:
    src, dest, copy_request = copy_file_setup  # setup does an initial copy

    with pytest.raises(ValueError):
        copy.copy_file(file_to_overwrite_with, dest, overwrite_method="error")
