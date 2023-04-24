"""Composes the remote_file interface with the _adls implementation."""
import json
import os
import typing as ty
from pathlib import Path

from thds.adls import AdlsFqn, AdlsRoot

from ..config import get_datasets_storage_root
from ..exception import catch
from ._adls import b64, is_blob_not_found, join, yield_filenames
from ._adls_shared import get_global_client
from ._md5 import md5_readable
from ._uris import lookup_blob_store
from .remote_file import DestFile, Serialized, SrcFile, StrOrPath

_AZURE_PLACEHOLDER_SIZE_LIMIT = 4096
# it is assumed that no placeholder will ever need to be larger than 4 KB.


def _try_parse_adls_path_repr(
    possible_json_adls_repr: str,
) -> str:
    """Tightly coupled to _represent_adls_path."""
    try:
        adls_dict = json.loads(possible_json_adls_repr)
        assert adls_dict["type"] == "ADLS"
        assert '{"type": "ADLS"' not in adls_dict["key"], adls_dict
        return str(AdlsFqn(adls_dict["sa"], adls_dict["container"], adls_dict["key"]))
    except (json.JSONDecodeError, TypeError, KeyError):
        return ""


def _represent_adls_path(storage_account: str, container: str, key: str, **kwargs) -> Serialized:
    """Historical fully-qualified representation for a given ADLS file.

    At the time, our AdlsFqn approach did not exist.
    """
    assert '"type": "ADLS"' not in key, key
    return Serialized(
        json.dumps(dict(type="ADLS", sa=storage_account, container=container, key=key, **kwargs))
    )


def _upload_and_represent(
    sa: str, container: str, directory: str, relative_path: str, local_src: os.PathLike
) -> Serialized:
    key = join(directory, relative_path)
    with open(local_src, "rb") as file:
        uri = str(AdlsFqn(sa, container, key))
        lookup_blob_store(uri).put(uri, file)
        file.seek(0)
        # The primary reason for representing the md5 inside the
        # serialized file pointer is to add greater confidence in
        # memoization. This prevents memoizing results that are based
        # on a shared blob path but different blob contents.
        #
        # We use md5 base 64 so this is easy to verify against ADLS
        # without downloading the file.  we do not currently make use
        # of this validation but we could in the future.
        return _represent_adls_path(sa, container, key, md5b64=b64(md5_readable(file)))


def _download_serialized(serialized: Serialized, local_dest: StrOrPath):
    local_filename = os.fspath(local_dest)
    uri = _try_parse_adls_path_repr(serialized)
    if not uri:
        raise ValueError(f"{serialized} does not represent a remote ADLS file")

    with open(local_filename, "wb") as file:
        lookup_blob_store(uri).read(uri, file)
    ser_md5 = json.loads(serialized).get("md5b64")
    if ser_md5:
        # do additional validation. This is not required but it's
        # worth doing if the checksum is present in the
        # serialization.  The types of bugs this would catch would
        # be if a file uploaded to ADLS got modified after its
        # initial upload. DestFiles and SrcFiles are not designed
        # to be mutable - they should be write-once, as per the
        # documentation.
        file_md5 = b64(md5_readable(local_filename))
        assert file_md5 == ser_md5, (
            f"MD5 in Serialized remote file pointer ({ser_md5}) does not match"
            f" md5 of downloaded file from ADLS ({file_md5})"
        )


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


def _split_on_working_dir_or_basename(
    filename: str, current_working_dir: str = ""
) -> ty.Tuple[str, str]:
    filename = os.path.abspath(filename)
    wdir = os.path.abspath(current_working_dir or os.getcwd())
    if not wdir.endswith("/"):
        wdir += "/"
    if filename.startswith(wdir):
        working_filename = filename[len(wdir) :]
        return os.path.dirname(working_filename), os.path.basename(working_filename)
    return os.path.dirname(filename), os.path.basename(filename)


class _UploadDirectoryAdjuster:
    """If the local prefix provided matches the beginning of the filename,
    we can place the file in a location with dramatically reduced nesting.
    """

    def __init__(
        self,
        upload: ty.Callable[[StrOrPath, str], Serialized],
        current_working_dir: str,
        local_src: StrOrPath,
    ):
        self.upload_file_to_remote_key = upload
        remote_dir, remote_name = _split_on_working_dir_or_basename(
            os.fspath(local_src), current_working_dir
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

    def _nicer_blob_not_found_error(exc: Exception) -> bool:
        if is_blob_not_found(exc):
            raise ValueError(
                f"File {key} does not exist on ADLS in {storage_account} {container}"
            ) from exc
        return False

    adls_client = get_global_client(storage_account, container)
    assert "{" not in key, key
    serialized_meta = dict()
    if validate:
        with catch(_nicer_blob_not_found_error):
            props = adls_client.get_file_client(key).get_file_properties()
            if props.content_settings.content_md5:
                serialized_meta["md5b64"] = b64(props.content_settings.content_md5)

    return SrcFile(
        _download_serialized,
        _represent_adls_path(storage_account, container, key, **serialized_meta),
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
        remote_serialization = _upload_and_represent(
            self.sa,
            self.container,
            self.directory,
            remote_relative_path,
            Path(os.fspath(local_src)),
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

    def __init__(self, adls_dir: AdlsDirectory, current_working_dir: str = ""):
        """A given set of source and destination files which may be
        remote if activated via `pure_remote` will be stored
        underneath the provided remote_prefix within the specified SA and
        container.
        """
        self.adls_dir = adls_dir
        self.current_working_dir = current_working_dir

    def src(self, local_src: ty.Union[DestFile, StrOrPath]) -> SrcFile:
        """Return SrcFile for locally-existing path, or raise FileNotFoundError.

        If the local file is a remote file pointer, it will get
        recognized as such and will be downloaded upon entry to the
        SrcFile context in a local orchestrator.

        If the local file is not a remote file pointer (it is the file
        itself), no download will occur for local usage.

        In all cases, remote functions will download the file to a
        temporary Path upon first context entry.

        May be absolute or relative to current working directory.
        """
        local_filepath = str(local_src)
        return SrcFile(
            _download_serialized,
            _get_remote_serialized(local_filepath),
            local_filepath,
            # TODO insert lazy-hashing (sha256 to match Paths!) uploader here.
            _UploadDirectoryAdjuster(
                self.adls_dir.upload, self.current_working_dir, os.fspath(local_src)
            ),
        )

    def dest(self, local_dest: StrOrPath) -> DestFile:
        """Return DestFile representing the given local path.

        If a file already exists at this local path, it will be
        overwritten without warning upon return from a remote
        function.

        May be absolute or relative to current working directory.
        """
        return DestFile(
            _UploadDirectoryAdjuster(
                self.adls_dir.upload,
                self.current_working_dir,
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
            _UploadDirectoryAdjuster(self.adls_dir.upload, "", orchestrator_path),
            orchestrator_path,
        )


def adls_dataset_context(remote_prefix: str, current_working_dir: str = "") -> AdlsDatasetContext:
    """This may be a nice default place to put your datasets."""
    return AdlsDatasetContext(
        AdlsDirectory(*AdlsRoot.parse(get_datasets_storage_root()), remote_prefix),
        current_working_dir,
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
    root = AdlsRoot.parse(get_datasets_storage_root())
    # normalize to start with no slash and end with a slash.
    directory = directory if directory.endswith("/") else (directory + "/")
    directory = directory[1:] if directory.startswith("/") else directory
    for azure_filename in yield_filenames(get_global_client(root.sa, root.container), directory):
        assert azure_filename.startswith(directory)
        path = local_root_path / azure_filename[len(directory) :]
        path.parent.mkdir(exist_ok=True, parents=True)
        print(path)
        with open(path, "w") as f:
            # TODO put b64(md5) in here as well; client.get_file_client(key).get_file_properties()...
            f.write(_represent_adls_path(sa, container, azure_filename))


if __name__ == "__main__":  # pragma: nocover
    import sys

    sync_remote_to_local_as_pointers(sys.argv[1])

# TODO write utility for replacing local remote file pointers with the actual files.
