import json
import os
import typing as ty
from pathlib import Path

from ..config import adls_remote_datasets_container, adls_remote_datasets_sa
from ._adls import (
    AdlsFileSystem,
    AdlsFileSystemClient,
    download_to,
    join,
    represent_adls_path,
    upload_and_represent,
    yield_filenames,
)
from .remote_file import DestFile, Serialized, SrcFile, StrOrPath

_AZURE_PLACEHOLDER_SIZE_LIMIT = 4096
# it is assumed that no placeholder will ever need to be larger than 4 KB.


def _try_parse_adls_path_repr(
    possible_json_adls_repr: str,
) -> ty.Optional[ty.Tuple[AdlsFileSystem, str]]:
    """Tightly coupled to represent_adls_path."""
    try:
        adls_dict = json.loads(possible_json_adls_repr)
        assert adls_dict["type"] == "ADLS"
        assert '{"type": "ADLS"' not in adls_dict["key"], adls_dict
        return AdlsFileSystem(adls_dict["sa"], adls_dict["container"]), adls_dict["key"]
    except (json.JSONDecodeError, TypeError, KeyError):
        return None


def _download_serialized(serialized: Serialized, local_dest: StrOrPath):
    local_filename = os.fspath(local_dest)
    fs_and_key = _try_parse_adls_path_repr(serialized)
    if not fs_and_key:
        raise ValueError(f"{serialized} does not represent a remote ADLS file")
    download_to(*fs_and_key, Path(local_filename))


def _get_remote_serialized(local_src: StrOrPath) -> Serialized:
    with open(local_src) as maybe_json_file:
        try:
            json_str = maybe_json_file.read(_AZURE_PLACEHOLDER_SIZE_LIMIT)
            if _try_parse_adls_path_repr(json_str):
                return Serialized(json_str)
        except UnicodeDecodeError:
            # TODO determine proper exception types to catch here
            pass
    return Serialized("")


def _split_on_working_dir_or_basename(filename: str, strip_local_prefix: str = "") -> ty.Tuple[str, str]:
    filename = os.path.abspath(filename)
    wdir = os.path.abspath(strip_local_prefix or os.getcwd())
    if not wdir.endswith("/"):
        wdir += "/"
    if filename.startswith(wdir):
        working_filename = filename[len(wdir) :]
        return os.path.dirname(working_filename), os.path.basename(working_filename)
    return os.path.dirname(filename), os.path.basename(filename)


class _AdlsUploadDirectoryAdjuster:
    """If the local prefix provided matches the beginning of the filename,
    we can place the file on ADLS with dramatically reduced nesting.
    """

    def __init__(
        self,
        upload: ty.Callable[[StrOrPath, str], Serialized],
        strip_local_prefix: str,
        local_src: StrOrPath,
    ):
        self.upload_file_to_remote_key = upload
        remote_dir, remote_name = _split_on_working_dir_or_basename(
            os.fspath(local_src), strip_local_prefix
        )
        self.remote_key = f"{remote_dir}/{remote_name}" if remote_dir else remote_name

    def __call__(self, local_src: StrOrPath) -> Serialized:
        return self.upload_file_to_remote_key(local_src, self.remote_key)


def adls_remote_src(storage_account: str, container: str, key: str, validate: bool = True) -> SrcFile:
    """Create a SrcFile from a fully-qualified set of ADLS info.

    You should really only disable validation for performance
    reasons. If your file does not in fact exist, you will just be
    deferring a very unpleasant error.
    """
    adls = AdlsFileSystem(storage_account, container)
    assert "{" not in key, key
    if validate and not adls.file_exists(key):
        raise ValueError(f"File {key} does not exist on ADLS in {storage_account} {container}")
    return SrcFile(
        _download_serialized,
        represent_adls_path(storage_account, container, key),
    )


class AdlsDirectory:
    """
    Note that this will be serialized in its entirety if a SrcFile or
    DestFile is passed to a remote function, so the state is
    deliberately kept very simple.
    """

    def __init__(self, sa: str, container: str, directory: str):
        self.sa = sa
        self.container = container
        self.directory = directory

    def upload(self, local_src: StrOrPath, remote_relative_path: str) -> Serialized:
        remote_serialization = upload_and_represent(
            self.sa, self.container, self.directory, remote_relative_path, Path(os.fspath(local_src))
        )
        serialization_len = len(remote_serialization.encode())
        assert (
            serialization_len <= _AZURE_PLACEHOLDER_SIZE_LIMIT
        ), "generated placeholder larger ({serialization_len}) than expected!"
        return remote_serialization


class AdlsDatasetContext:
    """Provides ADLS implementations of the DestFile/SrcFile abstraction.

    All Src/DestFiles are placed within the given ADLS Storage
    Account, Container, and directory prefix.

    Public methods are only to be called within a local orchestrator process.
    """

    def __init__(self, adls_dir: AdlsDirectory, strip_local_prefix: str = ""):
        """A given set of source and destination files which may be
        remote if activated via `pure_remote` will be stored
        underneath the provided remote_prefix within the specified SA and
        container.
        """
        self.adls_dir = adls_dir
        self.strip_local_prefix = strip_local_prefix

    def src(self, local_src: ty.Union[DestFile, StrOrPath]) -> SrcFile:
        """Return SrcFile for locally-existing path, or raise FileNotFoundError.

        May be absolute or relative to current working directory.
        """
        local_filepath = str(local_src)
        return SrcFile(
            _download_serialized,
            _get_remote_serialized(local_filepath),
            local_filepath,
            _AdlsUploadDirectoryAdjuster(
                self.adls_dir.upload, self.strip_local_prefix, os.fspath(local_src)
            ),
        )

    def dest(self, local_dest: StrOrPath) -> DestFile:
        """Return DestFile representing the given local path.

        May be absolute or relative to current working directory.
        """
        return DestFile(
            _AdlsUploadDirectoryAdjuster(
                self.adls_dir.upload,
                self.strip_local_prefix,
                local_dest,
            ),
            local_dest,
        )

    def remote_src(self, remote_relative_path: str) -> SrcFile:
        """Return SrcFile representing only an existing remote path."""
        return adls_remote_src(
            self.adls_dir.sa,
            self.adls_dir.container,
            join(self.adls_dir.directory, remote_relative_path),
        )

    def remote_dest(self, remote_relative_path: str) -> DestFile:
        """Create DestFile on remote process.

        Path if returned to orchestrator will be the process working
        directory plus the full ADLS path.
        """
        orchestrator_path = join(self.adls_dir.directory, remote_relative_path)
        return DestFile(
            _AdlsUploadDirectoryAdjuster(self.adls_dir.upload, "", orchestrator_path),
            orchestrator_path,
        )


def adls_dataset_context(remote_prefix: str, strip_local_prefix: str = "") -> AdlsDatasetContext:
    """This may be a nice default place to put your datasets."""
    return AdlsDatasetContext(
        AdlsDirectory(adls_remote_datasets_sa(), adls_remote_datasets_container(), remote_prefix),
        strip_local_prefix,
    )


def sync_remote_to_local_as_pointers(
    directory: str, local_root: str = ".", sa: str = "", container: str = ""
):  # pragma: nocover
    """If your orchestrator process somehow dies but all the runners
    succeeded, you can 'recover' the results easily with this
    function, making it easy to move to the next step in your pipeline.

    Mostly intended for interactive use.

    e.g.:

    sync_remote_to_local_as_pointers(
        'demand-forecast/peter-gaultney-df-orch-2022-07-25T19:04:20-1188-train-Radiology/.cache',
        sa='thdsdatasets',
        container='ml-ops',
    )
    """
    local_root_path = Path(local_root)
    sa = sa or adls_remote_datasets_sa()
    container = container or adls_remote_datasets_container()
    # normalize to start with no slash and end with a slash.
    directory = directory if directory.endswith("/") else (directory + "/")
    directory = directory[1:] if directory.startswith("/") else directory
    for azure_filename in yield_filenames(AdlsFileSystemClient(sa, container), directory):
        assert azure_filename.startswith(directory)
        path = local_root_path / azure_filename[len(directory) :]
        path.parent.mkdir(exist_ok=True, parents=True)
        print(path)
        with open(path, "w") as f:
            f.write(represent_adls_path(sa, container, azure_filename))


if __name__ == "__main__":  # pragma: nocover
    import sys

    sync_remote_to_local_as_pointers(sys.argv[1])

# TODO write utility for replacing local remote file pointers with the actual files.
