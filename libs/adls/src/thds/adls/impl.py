import asyncio
import datetime
import itertools
import logging
import os
import shutil
from collections.abc import Mapping as MappingABC
from functools import cmp_to_key, wraps
from pathlib import Path
from typing import (
    IO,
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    List,
    Mapping,
    Optional,
    TypeVar,
    Union,
)

import attr
import azure.core.exceptions
from aiostream import stream
from azure.identity.aio import DefaultAzureCredential
from azure.storage.filedatalake import FileProperties, PathProperties
from azure.storage.filedatalake.aio import DataLakeServiceClient, FileSystemClient

from thds.core import lazy, log

from ._upload import async_upload_decision_and_metadata
from .conf import CONNECTION_TIMEOUT, UPLOAD_CHUNK_SIZE
from .download import async_download_or_use_verified
from .errors import NotADirectoryError, translate_azure_error
from .file_properties import is_directory
from .ro_cache import from_cache_path_to_local, global_cache
from .shared_credential import get_credential_kwargs

LOGGER = log.getLogger(__name__)
log.getLogger("azure.core").setLevel(logging.WARNING)
log.getLogger("azure.identity").setLevel(logging.WARNING)

DEFAULT_HIVE_PREFIX = os.getenv("CORE_HIVE_PREFIX", "")
WEST_HIVE_PREFIX = "hive/warehouse"  # For easy access while we may need backwards compatibility

T = TypeVar("T")


def async_run(func: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    """Used to decorate the main runner function to avoid calling async.run too many times

    :param func: any async function
    :return:
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))  # type: ignore

    return wrapper


def base_name(remote_path: str) -> str:
    return remote_path.rstrip("/").split("/")[-1]


def _true(_):
    return True


def batcher(it: Iterable[T], size: int = 1) -> Iterable[List[T]]:
    stream = iter(it)

    def _slice():
        return list(itertools.islice(stream, size))

    yield from iter(_slice, [])


@attr.s(auto_attribs=True)
class ADLSFileSystemNotFound(ConnectionError):
    account_name: str
    file_system: str

    def __str__(self):
        return f"File system {self.file_system!r} not found under account {self.account_name!r}"


@attr.s(auto_attribs=True, frozen=True)
class PathPair:
    """Store the remote path and the corresponding local path of a file"""

    remote_path: str
    local_path: Path


@attr.s(auto_attribs=True)
class DeleteProperties:
    """Convenience class around dicts returned in file deletion."""

    path: str
    date: Optional[datetime.datetime] = None
    version: Optional[str] = None
    request_id: Optional[str] = None
    deletion_id: Optional[str] = None  # Inferring type based on request_id.
    continuation: Optional[Any] = None  # Cannot find details on this.
    exception: Optional[Exception] = None


class ADLSFileSystem:
    """A downloader that can be used to download a single file, all the files and subdirectories
    in a given directory, or all the files for a given hive table.
    """

    def __init__(
        self,
        account_name: str,
        file_system: str,
        default_batch_size: int = 64,
        cache_dir: Optional[Union[Path, str]] = None,
    ):
        self.account_name = account_name
        self.file_system = file_system
        self.default_batch_size = default_batch_size
        if not self.exists():
            raise ADLSFileSystemNotFound(account_name, file_system)

        self.cache = None if cache_dir is None else ADLSFileSystemCache(cache_dir)

    def exists(self) -> bool:
        return self._run(self._exists)

    @staticmethod
    async def _exists(file_system_client: FileSystemClient) -> bool:
        try:
            return await file_system_client.exists()
        except azure.core.exceptions.AzureError as err:
            translate_azure_error(file_system_client, "", err)

    def file_exists(self, path: str) -> bool:
        return self._run(self._path_exists, path, False)

    def dir_exists(self, path: str) -> bool:
        return self._run(self._path_exists, path, True)

    async def _path_exists(
        self, file_system_client: FileSystemClient, path: str, directory: bool
    ) -> bool:
        try:
            info = await self._get_file_info(file_system_client, path)
        except azure.core.exceptions.ResourceNotFoundError:
            return False
        except azure.core.exceptions.AzureError as err:
            translate_azure_error(file_system_client, path, err)
        return directory == is_directory(info)

    @async_run
    async def _run(self, func: Callable[..., Awaitable], *args, **kwargs):
        """Main async runner function that pass credential and account info
        to create a file system client, which can then be passed into other async functions.

        :param func: an async function
        :param args: additional args for func
        :param kwargs: addditional kwargs for func
        """
        async with DefaultAzureCredential(**get_credential_kwargs()) as credential:
            service_client = DataLakeServiceClient(
                account_url="{}://{}.dfs.core.windows.net".format("https", self.account_name),
                credential=credential,
            )
            async with service_client:
                async with service_client.get_file_system_client(
                    file_system=self.file_system
                ) as file_system_client:
                    return await func(file_system_client, *args, **kwargs)

    def _local_path_for(self, remote_path: str, local_path: Optional[Union[Path, str]]) -> Path:
        if local_path is None:
            if self.cache is None:
                # use the current working directory as the default location
                return Path(base_name(remote_path)).absolute()
            else:
                # use the cache as the default location
                return self.cache.cache_path(remote_path)
        else:
            # use the fully qualified explicit path
            return Path(local_path).absolute()

    async def _fetch_file(
        self,
        file_system_client: FileSystemClient,
        remote_path: str,
        local_path: Optional[Union[Path, str]] = None,
    ) -> Path:
        """async function that downloads a file locally given its remote path

        :returns: a local path of the downloaded file
        """
        # the local file path we will return to the caller;
        # may download into another path if there is a cache
        return_path = self._local_path_for(remote_path, local_path)
        download_path: Path

        if self.cache is None:
            download_path = return_path
        else:
            download_path = self.cache.cache_path(remote_path)

        dir_path = return_path.parent
        dir_path.mkdir(exist_ok=True, parents=True)

        locally_cached = False
        if self.cache:
            async with file_system_client.get_file_client(remote_path) as file_client:
                file_properties = await file_client.get_file_properties()
                if self.cache.is_valid_for(file_properties):
                    # local timestamp cache is up-to-date for this file; skip download
                    LOGGER.debug(f"Skipping download of cached {remote_path}")
                    locally_cached = True
        if not locally_cached:
            await async_download_or_use_verified(
                file_system_client, remote_path, download_path, cache=global_cache()
            )

        assert download_path.exists(), "File should have been downloaded by this point"
        if download_path != return_path:
            from_cache_path_to_local(download_path, return_path, link_opts=("ref", "hard"))

        return return_path

    async def _fetch_directory(
        self,
        file_system_client: FileSystemClient,
        remote_path: str,
        local_path: Optional[Union[Path, str]] = None,
        batch_size: Optional[int] = None,
        recursive: bool = True,
        path_filter: Optional[Callable[[PathProperties], bool]] = None,
    ) -> List[Path]:
        """Async function that downloads all the files within a given directory,
        including the files in the subdirectories when recursive = True

        :return: a list of the paths of the files downloaded
        """
        # normalize remote path to a standard relative dir path -
        # this ensures correctness of strip_prefix() below
        stripped_remote_path = remote_path.strip("/")
        remote_path = stripped_remote_path + "/"
        dir_path = self._local_path_for(remote_path, local_path)
        made_dir = False
        path_filter_ = _true if path_filter is None else path_filter

        # remove the remote directory prefix to determine a relative path for creation under dir_path
        def strip_prefix(name):
            return name.lstrip("/")[len(remote_path) :]

        paths = (
            PathPair(remote_path=path.name, local_path=dir_path / strip_prefix(path.name))
            async for path in file_system_client.get_paths(remote_path, recursive=recursive)
            if not path.is_directory and path_filter_(path)
        )

        # shim generator to check for file vs directory, to prevent confusing errors that happen lower down
        async def validated_paths() -> AsyncIterator[PathPair]:
            async for path_pair in paths:
                if path_pair.remote_path == stripped_remote_path:
                    raise NotADirectoryError(
                        f"Path '{stripped_remote_path}' points to a file, not a directory. "
                        f"Use fetch_file() instead."
                    )
                nonlocal made_dir
                if not made_dir:
                    dir_path.mkdir(exist_ok=True, parents=True)
                    made_dir = True
                yield path_pair

        local_paths = []
        async for batch in self._async_batch(validated_paths(), batch_size):
            local_paths.extend(
                await asyncio.gather(
                    *[
                        self._fetch_file(
                            file_system_client,
                            path_pair.remote_path,
                            path_pair.local_path,
                        )
                        for path_pair in batch
                    ]
                )
            )

        return local_paths

    async def _fetch_files(
        self,
        file_system_client: FileSystemClient,
        remote_paths: Union[Iterable[str], Mapping[str, Union[Path, str]]],
        batch_size: Optional[int] = None,
    ):
        if isinstance(remote_paths, MappingABC):
            remote_local_pairs = (
                PathPair(remote_path, Path(local_path))
                for remote_path, local_path in remote_paths.items()
            )
        else:
            remote_local_pairs = (
                PathPair(remote_path, self._local_path_for(remote_path, None))
                for remote_path in remote_paths
            )

        if batch_size is None:
            batch_size = self.default_batch_size

        local_paths = []
        for batch in iter(lambda: list(itertools.islice(remote_local_pairs, batch_size)), []):
            local_paths.extend(
                await asyncio.gather(
                    *[
                        self._fetch_file(
                            file_system_client,
                            path_pair.remote_path,
                            path_pair.local_path,
                        )
                        for path_pair in batch
                    ]
                )
            )

        return local_paths

    @staticmethod
    async def _put_file(
        file_system_client: FileSystemClient,
        local_path: Union[str, Path],
        remote_path: str,
        metadata: Optional[Mapping[str, str]] = None,
    ) -> str:
        """async function that uploads a local file to a remote path

        :returns: remote path of uploaded file
        """

        async with file_system_client.get_file_client(remote_path) as file_client:
            with open(local_path, "rb") as fp:
                decision = await async_upload_decision_and_metadata(file_client.get_file_properties, fp)
                if decision.upload_required:
                    await file_client.upload_data(
                        fp,
                        overwrite=True,
                        connection_timeout=CONNECTION_TIMEOUT(),
                        chunk_size=UPLOAD_CHUNK_SIZE(),
                        metadata={**decision.metadata, **(metadata or {})},
                    )

        return remote_path

    async def _put_directory(
        self,
        file_system_client: FileSystemClient,
        local_path: Union[str, Path],
        remote_path: str,
        recursive: bool = False,
        batch_size: Optional[int] = None,
        metadata: Optional[Mapping[str, str]] = None,
    ) -> List[str]:
        """async function that uploads all the files in a local directory to a remote path

        :returns: list of remote paths
        """

        local_path = str(local_path).rstrip("/") + "/"
        remote_path = remote_path.rstrip("/") + "/"

        if batch_size is None:
            batch_size = self.default_batch_size

        paths = []
        if recursive:
            for root, _subdirs, files in os.walk(local_path):
                for filename in files:
                    paths.append(
                        PathPair(
                            os.path.join(root, filename).replace(local_path, remote_path),
                            Path(os.path.join(root, filename)),
                        )
                    )
        else:
            for filename in os.listdir(local_path):
                if os.path.isfile(os.path.join(local_path, filename)):
                    paths.append(
                        PathPair(
                            os.path.join(remote_path, filename),
                            Path(os.path.join(local_path, filename)),
                        )
                    )

        remote_paths = []

        for batch in batcher(paths, batch_size):
            remote_paths.extend(
                await asyncio.gather(
                    *[
                        self._put_file(
                            file_system_client,
                            str(path_pair.local_path),
                            path_pair.remote_path,
                            metadata,
                        )
                        for path_pair in batch
                    ]
                )
            )

        return remote_paths

    async def _put_files(
        self,
        file_system_client: FileSystemClient,
        local_paths: Iterable[Union[str, Path]],
        remote_path: str,
        batch_size: Optional[int] = None,
        metadata: Optional[Mapping[str, str]] = None,
    ) -> List[str]:
        remote_path = remote_path.rstrip("/") + "/"

        paths: List[PathPair] = []

        for local_path in local_paths:
            file_name = os.path.basename(local_path)
            paths.append(PathPair(os.path.join(remote_path, file_name), Path(local_path)))

        if batch_size is None:
            batch_size = self.default_batch_size

        remote_paths = []

        for batch in batcher(paths, batch_size):
            remote_paths.extend(
                await asyncio.gather(
                    *[
                        self._put_file(
                            file_system_client,
                            str(path_pair.local_path),
                            path_pair.remote_path,
                            metadata,
                        )
                        for path_pair in batch
                    ]
                )
            )

        return remote_paths

    @staticmethod
    async def _get_file_info(file_system_client: FileSystemClient, remote_path: str) -> FileProperties:
        """Returns `FileProperties` for remote files.

        See :meth:`~ADLSFileSystem.get_file_info` for more details.
        """
        async with file_system_client.get_file_client(remote_path) as file_client:
            return await file_client.get_file_properties()

    async def _get_directory_info(
        self,
        file_system_client: FileSystemClient,
        remote_path: str,
        incl_subdirs: bool = False,
        batch_size: Optional[int] = None,
        recursive: bool = True,
        path_filter: Optional[Callable[[PathProperties], bool]] = None,
    ) -> List[FileProperties]:
        """Returns a list of `FileProperties` for files in a remote directory.

        See :meth:`~ADLSFileSystem.get_directory_info` for more details.
        """

        def incl_subdirs_(path: PathProperties) -> bool:
            if incl_subdirs:
                return False
            else:
                return path.is_directory

        path_filter_ = _true if path_filter is None else path_filter

        paths = (
            path.name
            async for path in file_system_client.get_paths(remote_path, recursive=recursive)
            if not incl_subdirs_(path) and path_filter_(path)
        )

        info = []

        async for batch in self._async_batch(paths, batch_size):
            info.extend(
                await asyncio.gather(*[self._get_file_info(file_system_client, path) for path in batch])
            )

        return info

    async def _get_files_info(
        self,
        file_system_client: FileSystemClient,
        remote_paths: Iterable[str],
        batch_size: Optional[int] = None,
    ) -> List[FileProperties]:
        """Returns a list of `FileProperties` for each file in a list of remote file paths.

        See :meth:`~ADLSFileSystem.get_files_info` for more details.
        """
        if batch_size is None:
            batch_size = self.default_batch_size

        info = []
        for batch in batcher(remote_paths, batch_size):
            info.extend(
                await asyncio.gather(*[self._get_file_info(file_system_client, path) for path in batch])
            )

        return info

    @staticmethod
    async def _delete_file(
        file_system_client: FileSystemClient,
        remote_path: str,
        if_modified_since: Optional[datetime.datetime] = None,
        if_unmodified_since: Optional[datetime.datetime] = None,
    ) -> DeleteProperties:
        """Deletes a remote file and returns a response details dict.

        See :meth:`~ADLSFileSystem.delete_file` for more details.
        """
        async with file_system_client.get_file_client(remote_path) as file_client:
            try:
                return DeleteProperties(
                    path=remote_path,
                    **await file_client.delete_file(
                        if_modified_since=if_modified_since,
                        if_unmodified_since=if_unmodified_since,
                    ),
                )
            except Exception as e:
                return DeleteProperties(path=remote_path, exception=e)

    @staticmethod
    async def _delete_directory(
        file_system_client: FileSystemClient,
        remote_path: str,
        if_modified_since: Optional[datetime.datetime] = None,
        if_unmodified_since: Optional[datetime.datetime] = None,
    ) -> DeleteProperties:
        """Deletes a remote directory and returns a response details dict.

        Warning: If `remote_path` is a file path, it will be deleted without raising an error.

        See :meth:`~ADLSFileSystem.delete_directory` for more details.
        """
        async with file_system_client.get_directory_client(remote_path) as directory_client:
            try:
                return DeleteProperties(
                    path=remote_path,
                    **await directory_client.delete_directory(
                        if_modified_since=if_modified_since,
                        if_unmodified_since=if_unmodified_since,
                    ),
                )
            except Exception as e:
                return DeleteProperties(path=remote_path, exception=e)

    async def _delete_in_directory(
        self,
        file_system_client: FileSystemClient,
        remote_path: str,
        if_modified_since: Optional[datetime.datetime] = None,
        if_unmodified_since: Optional[datetime.datetime] = None,
        cleanup: bool = False,
        batch_size: Optional[int] = None,
        recursive: bool = True,
        path_filter: Optional[Callable[[PathProperties], bool]] = None,
    ) -> List[DeleteProperties]:
        """Deletes files in a remote directory and returns a list of response details dicts.

        See :meth:`~ADLSFileSystem.delete_in_directory` for more details.
        """

        def cmp_subpath_relation(path1: str, path2: str) -> int:
            if path1.startswith(path2):
                return -1
            elif path2.startswith(path1):
                return 1
            return 0

        path_filter_ = _true if path_filter is None else path_filter

        file_paths = (
            path.name
            async for path in file_system_client.get_paths(remote_path, recursive=recursive)
            if not path.is_directory and path_filter_(path)
        )

        del_props = []
        async for batch in self._async_batch(file_paths, batch_size):
            del_props.extend(
                await asyncio.gather(
                    *[
                        self._delete_file(
                            file_system_client,
                            path,
                            if_modified_since,
                            if_unmodified_since,
                        )
                        for path in batch
                    ]
                )
            )

        if cleanup:
            dir_paths = [
                path.name
                async for path in file_system_client.get_paths(remote_path, recursive=recursive)
                if path.is_directory
            ]
            dir_paths.sort(key=cmp_to_key(cmp_subpath_relation))

            # Synchronous because order of operations must be maintaned.
            # Inner empty subdirs must be deleted before outer subdirs.
            for path in dir_paths:
                del_props.extend(await asyncio.gather(self._delete_file(file_system_client, path)))

        return del_props

    async def _delete_files(
        self,
        file_system_client: FileSystemClient,
        remote_paths: Iterable[str],
        batch_size: Optional[int] = None,
        if_modified_since: Optional[datetime.datetime] = None,
        if_unmodified_since: Optional[datetime.datetime] = None,
    ) -> List[DeleteProperties]:
        """Deletes each remote file in a list of remote file paths.

        See :meth:`~ADLSFileSystem.delete_files` for more details.
        """
        if batch_size is None:
            batch_size = self.default_batch_size

        del_props = []
        for batch in batcher(remote_paths, batch_size):
            del_props.extend(
                await asyncio.gather(
                    *[
                        self._delete_file(
                            file_system_client,
                            path,
                            if_modified_since,
                            if_unmodified_since,
                        )
                        for path in batch
                    ]
                )
            )

        return del_props

    async def _delete_directories(
        self,
        file_system_client: FileSystemClient,
        remote_paths: Iterable[str],
        batch_size: Optional[int] = None,
        if_modified_since: Optional[datetime.datetime] = None,
        if_unmodified_since: Optional[datetime.datetime] = None,
    ) -> List[DeleteProperties]:
        """Deletes each remote directory in a list of remote directory paths.

        Warning: If any `remote_paths` are file paths, they will also be deleted
        without raising an error.

        See :meth:`~ADLSFileSystem.delete_directories` for more details.
        """
        if batch_size is None:
            batch_size = self.default_batch_size

        del_props = []
        for batch in batcher(remote_paths, batch_size):
            del_props.extend(
                await asyncio.gather(
                    *[
                        self._delete_directory(
                            file_system_client,
                            path,
                            if_modified_since,
                            if_unmodified_since,
                        )
                        for path in batch
                    ]
                )
            )

        return del_props

    async def _async_batch(
        self, it: AsyncIterable[T], size: Optional[int] = None
    ) -> AsyncIterator[List[T]]:
        """Async batch generator"""
        # TODO - look at type ignores here
        batch_size = size if size is not None else self.default_batch_size
        async with stream.chunks(it, batch_size).stream() as streamer:  # type: ignore[arg-type,var-annotated]
            async for chunk in streamer:
                yield chunk  # type: ignore[misc]

    def fetch_files(self, remote_paths: Union[Iterable[str], Mapping[str, Union[Path, str]]]):
        return self._run(self._fetch_files, remote_paths)

    def fetch_file(self, remote_path: str, local_path: Optional[Union[Path, str]] = None) -> Path:
        """Download the given remote file and save it into a given file path (local_path).
        In case there is a cache directory, the file is downloaded to a matching path under it.
        In that case, when local_path is passed, a hard link is made to the cache copy at the
        local_path location. (a hard link ensures that clearing the cache later will not affect
        the view of the file at local_path)

        :param remote_path: path in ADLS to download
        :param local_path: path for local file; if not given, use the name from the remote path when
          there is no cache, otherwise use the path under the cache dir corresponding to remote_path
        :return: the local path where the file was downloaded
        """
        return self._run(self._fetch_file, remote_path, local_path)

    def fetch_directory(
        self,
        remote_path: str,
        local_path: Optional[Union[Path, str]] = None,
        batch_size: Optional[int] = None,
        recursive: bool = True,
        path_filter: Optional[Callable[[PathProperties], bool]] = None,
    ) -> List[Path]:
        """Download all the files in a given directory and save them in a given directory path.
        In case there is a cache directory, the remote directory is reflected in a subdirectory under it.
        The semantics of local_path are the same as for fetch_file(),
        except that it references a local directory, and the hard links are made for each downloaded
        file under it when there is a cache.

        :param remote_path: the remote directory to download from
        :param local_path: path for the local directory; if not given, use the name from the
          remote path when there is no cache, otherwise use the path under the cache dir
          corresponding to remote_path
        :param batch_size: the size of each batch
        :param recursive: recurse into subdirectories when downloading?
        :param path_filter: optional callable taking an `azure.storage.filedatalake.PathProperties`
          and returning a bool indicating whether to download the corresponding file
        :return: List of local paths that were downloaded to
        """
        return self._run(
            self._fetch_directory,
            remote_path,
            local_path,
            batch_size=batch_size,
            recursive=recursive,
            path_filter=path_filter,
        )

    def fetch_hive_table(
        self,
        table: str,
        local_path: Optional[Union[Path, str]] = None,
        batch_size: Optional[int] = None,
        hive_prefix: str = DEFAULT_HIVE_PREFIX,
    ) -> List[Path]:
        """Download all the files in the directory for a given hive table

        :param table: e.g. database.tablename
        :param local_path: if not given, the files will be saved to ./database/tablename/
        :param batch_size: the size of each batch
        :param hive_prefix: the path prefix from the container root to the Hive warehouse
        """
        database, tablename = table.split(".")
        remote_path = (
            f"{hive_prefix.strip('/')}/{database}.db/{tablename}"
            if hive_prefix
            else f"{database}.db/{tablename}"
        )
        local_path_resolved = local_path if local_path is not None else Path(f"{database}/{tablename}")

        return self.fetch_directory(remote_path, local_path_resolved, batch_size)

    def put_file(
        self,
        local_path: Union[str, Path],
        remote_path: str,
        metadata: Optional[Mapping[str, str]] = None,
    ) -> str:
        """async function that uploads a local file to a remote location

        :param local_path: The local path of the file to upload.
        :param remote_path: The remote path to which the file will be uploaded.
        :param metadata: Metadata to add to the file.
        :returns: remote path of uploaded file
        """

        return self._run(self._put_file, local_path, remote_path, metadata)

    def put_directory(
        self,
        local_path: Union[str, Path],
        remote_path: str,
        recursive: bool = False,
        batch_size: Optional[int] = None,
        metadata: Optional[Mapping[str, str]] = None,
    ) -> List[str]:
        """async function that uploads all the files in a local directory to a remote directory

        :param local_path: The local path of the directory to upload.
        :param remote_path: The remote path to which the directory will be uploaded.
        :param recursive: Recurse into subdirectories when downloading?
        :param batch_size: The size of each batch.
        :param metadata: Metadata to add to each file uploaded.
        :returns: list of remote paths
        """

        return self._run(self._put_directory, local_path, remote_path, recursive, batch_size, metadata)

    def put_files(
        self,
        local_paths: Iterable[Union[str, Path]],
        remote_path: str,
        batch_size: Optional[int] = None,
        metadata: Optional[Mapping[str, str]] = None,
    ) -> List[str]:
        """async function that uploads each in a list of files to a remote directory

        :param local_paths: The local paths of the directory to upload.
        :param remote_path: The remote path to which the files will be uploaded.
        :param batch_size: The size of each batch.
        :param metadata: Metadata to add to each file uploaded.
        :returns: list of remote paths
        """

        return self._run(self._put_files, local_paths, remote_path, batch_size, metadata)

    def delete_file(
        self,
        remote_path: str,
        if_modified_since: Optional[datetime.datetime] = None,
        if_unmodified_since: Optional[datetime.datetime] = None,
    ) -> DeleteProperties:
        """Async function that deletes a remote file.

        :param remote_path: Path to remote file location.
        :param if_modified_since: Only delete file if it has been modified since given datetime.
          Default is `None`.
        :param if_unmodified_since: Only delete file if it has been unmodified since given datetime.
          Default is `None`.
        :return: `DeleteProperties`.
        """
        return self._run(self._delete_file, remote_path, if_modified_since, if_unmodified_since)

    def delete_directory(
        self,
        remote_path: str,
        if_modified_since: Optional[datetime.datetime] = None,
        if_unmodified_since: Optional[datetime.datetime] = None,
    ) -> DeleteProperties:
        """Async function that deletes a remote directory.

        Warning: If `remote_path` is a file path, it will be deleted without raising an error.

        :param remote_path: Path to remote directory location.
        :param if_modified_since: Only delete directory if it has been modified since given datetime.
          Default is `None`.
        :param if_unmodified_since: Only delete directory if it has been unmodified since given datetime.
          Default is `None`.
        :return: `DeleteProperties`.
        """
        return self._run(self._delete_directory, remote_path, if_modified_since, if_unmodified_since)

    def delete_in_directory(
        self,
        remote_path: str,
        if_modified_since: Optional[datetime.datetime] = None,
        if_unmodified_since: Optional[datetime.datetime] = None,
        cleanup: bool = False,
        batch_size: Optional[int] = None,
        recursive: bool = True,
        path_filter: Optional[Callable[[PathProperties], bool]] = None,
    ) -> List[DeleteProperties]:
        """Async function that deletes all files in a remote directory, with the option to also delete
        subdirectories left empty afterwards.

        Note #1: Cleanup step is blocking due to the need to maintain the order in which empty
        subdirectories must be deleted. Inner empty directories have to be deleted before
        outer empty directories so the outer directories can be empty.

        Note #2: if cleanup is true, the function will attempt to delete all subdirectories,
        however non-empty subdirectories will produce an exception that gets passed and written to their
        respective response details dicts.

        :param remote_path: Path to remote directory location.
        :param if_modified_since: Only delete files if they have been modified since given datetime.
          Default is `None`.
        :param if_unmodified_since: Only delete files if they have been unmodified since given datetime.
          Default is `None`.
        :param cleanup: Whether to delete subdirectories left empty after file deletion.
          Default is `False`.
        :param batch_size: Number of files to delete in each batch.
          Default is `None`.
        :param recursive: Whether to recurse into subdirectories when deleting.
          Default is `True`.
        :param path_filter: Optional callable taking a `PathProperties` and returning a bool
          indicating whether to delete the corresponding file.
        :return: List of `DeleteProperties`.
        """
        return self._run(
            self._delete_in_directory,
            remote_path,
            if_modified_since,
            if_unmodified_since,
            cleanup,
            batch_size,
            recursive,
            path_filter,
        )

    def delete_files(
        self,
        remote_paths: Iterable[str],
        batch_size: Optional[int] = None,
        if_modified_since: Optional[datetime.datetime] = None,
        if_unmodified_since: Optional[datetime.datetime] = None,
    ) -> List[DeleteProperties]:
        """Async function that deletes each file in a list of remote file paths.

        :param remote_paths: List of paths to remote file locations.
        :param batch_size: Number of files to delete in each batch.
          Default is `None`.
        :param if_modified_since: Only delete files if they have been modified since given datetime.
          Default is `None`.
        :param if_unmodified_since: Only delete files if they have been unmodified since given datetime.
          Default is `None`.
        :return: List of `DeleteProperties`.
        """
        return self._run(
            self._delete_files,
            remote_paths,
            batch_size,
            if_modified_since,
            if_unmodified_since,
        )

    def delete_directories(
        self,
        remote_paths: Iterable[str],
        batch_size: Optional[int] = None,
        if_modified_since: Optional[datetime.datetime] = None,
        if_unmodified_since: Optional[datetime.datetime] = None,
    ) -> List[DeleteProperties]:
        """Async function that deletes each directory in a list of remote directory paths.

        Warning: If any `remote_paths` are file paths, they will also be deleted
        without raising an error.

        :param remote_paths: List of paths to remote directory locations.
        :param batch_size: Number of directories to delete in each batch.
          Default is `None`.
        :param if_modified_since: Only delete directory if it has been modified since given datetime.
          Default is `None`.
        :param if_unmodified_since: Only delete directory if it has been unmodified since given datetime.
          Default is `None`.
        :return: List of `DeleteProperties`.
        """
        return self._run(
            self._delete_directories,
            remote_paths,
            batch_size,
            if_modified_since,
            if_unmodified_since,
        )

    def get_file_info(self, remote_path: str) -> FileProperties:
        """Async function that gets `FileProperties` for a remote file.

        :param remote_path: Path to remote file location.
        :return: `FileProperties`
        """
        return self._run(self._get_file_info, remote_path)

    def get_directory_info(
        self,
        remote_path: str,
        incl_subdirs: bool = False,
        batch_size: Optional[int] = None,
        recursive: bool = True,
        path_filter: Optional[Callable[[FileProperties], bool]] = None,
    ) -> List[FileProperties]:
        """Async function that gets `FileProperties` for all files in a remote directory.

        :param remote_path: Path to a remote directory location.
        :param incl_subdirs: Whether to include `FileProperties` for the subdirectories themselves.
          Default is `False`.
        :param batch_size: Number of `FileProperties` to get in each batch.
          Default is `None`.
        :param recursive: Whether to recurse into subdirectories when getting `FileProperties`.
        :param path_filter: Optional callable taking a `PathProperties` and returning a bool
          indicating whether to delete the corresponding file.
        :return: List of `FileProperties`.
        """
        return self._run(
            self._get_directory_info,
            remote_path,
            incl_subdirs,
            batch_size,
            recursive,
            path_filter,
        )

    def get_files_info(
        self, remote_paths: Iterable[str], batch_size: Optional[int] = None
    ) -> List[FileProperties]:
        """Async function that gets `FileProperties` for each file in a list of remote file paths.

        :param remote_paths: List of paths to remote file locations.
        :param batch_size: Number of `FileProperties` to get in each batch.
          Default is `None`.
        :return: List of `FileProperties`.
        """
        return self._run(self._get_files_info, remote_paths, batch_size)


class ADLSFileSystemCache:
    def __init__(self, cache_dir: Union[Path, str]):
        self.cache_dir = Path(cache_dir).absolute()
        self._init_dir()

    def _init_dir(self):
        if self.cache_dir.exists() and not self.cache_dir.is_dir():
            raise FileExistsError(f"{self.cache_dir} exists but is not a directory; can't use as cache")
        else:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def clear(self):
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)

        self._init_dir()

    def __contains__(self, path: str) -> bool:
        """Check for existence of a path in the cache for a *blob* (not for directories)"""
        return self.cache_path(path).is_file()

    def remove(self, path: str):
        """Remove a path from the cache. This is irrespective of type (files and dirs), i.e.
        the end result should be that the cache is ready to have new content written at `path`,
        either as a file or a directory. In case a cache path corresponding to relative path `path`
        doesn't exist locally, no action is taken."""
        cache_path = self.cache_path(path)

        if cache_path.is_dir():
            shutil.rmtree(cache_path)
        elif cache_path.exists():
            os.remove(cache_path)

    def cache_path(self, path: str):
        """Return the local path in the cache corresponding to the relative ADLS path `path`"""
        # ADLS paths are always forward-slash separated, hence we don't use os.path.split here
        parts = path.split("/")
        return self.cache_dir.joinpath(*parts)

    def file_handle(self, path: str, mode: str) -> IO:
        """Return a file handle to the local path in the cache corresponding to the relative ADLS
        path, `path`, opened in mode `mode`. Closing the handle is the responsibility of the caller.
        """
        file_path = self.cache_path(path)
        dir_path = Path(file_path).parent
        dir_path.mkdir(parents=True, exist_ok=True)
        return open(file_path, mode)

    def file_properties(self, path: str) -> FileProperties:
        """Return an `azure.storage.filedatalake.FileProperties` corresponding to the properties of the
        local file."""
        cache_path = self.cache_path(path)
        if not cache_path.is_file():
            raise FileNotFoundError(f"No file at {path} in cache at {self.cache_dir}")
        cache_stat = os.stat(cache_path)
        cache_mod_time = datetime.datetime.fromtimestamp(cache_stat.st_mtime).astimezone(
            datetime.timezone.utc
        )
        fp = FileProperties(name=path)
        fp.last_modified = cache_mod_time
        fp.size = cache_stat.st_size
        return fp

    def is_valid_for(self, adls_properties: FileProperties) -> bool:
        """Check if the cache has a valid copy of a blob at a given relative ADLS path.
        This is checked by comparison of the local file properties with an
        `azure.storage.filedatalake.FileProperties` detailing the properties of the ADLS blob.
        To be valid the local cache path should:
        - exist and be a proper file
        - have a newer last-modified time than that of the ADLS blob
        - have the same size as the ADLS blob
        """
        assert adls_properties.name
        if adls_properties.name not in self:
            return False

        cache_properties = self.file_properties(adls_properties.name)
        if not cache_properties.last_modified:
            return False
        return (cache_properties.last_modified > adls_properties.last_modified) and (
            cache_properties.size == adls_properties.size
        )


def make_adls_filesystem_getter(
    account_name: str,
    file_system: str,
    default_batch_size: int = 64,
    cache_dir: Optional[Union[Path, str]] = None,
) -> Callable[[], ADLSFileSystem]:
    """Wrapper for returning a :py:class:`core.adls.ADLSFileSystem` lazily."""

    @lazy.lazy
    def get_adls_filesystem() -> ADLSFileSystem:
        return ADLSFileSystem(account_name, file_system, default_batch_size, cache_dir)

    return get_adls_filesystem
