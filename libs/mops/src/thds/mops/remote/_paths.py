import os
import typing as ty
from pathlib import Path
from uuid import uuid4

from typing_extensions import Protocol

from thds.core.log import getLogger

from ._once import Once
from .temp import tempdir

logger = getLogger(__name__)


Downloader = ty.Callable[[os.PathLike], ty.Any]


class PathStream(Protocol):
    def local_to_remote(self, __src: os.PathLike, __key: str):
        ...  # pragma: no cover

    def get_downloader(self, __key: str) -> Downloader:
        ...  # pragma: no cover


class PathUnpickler:
    def __init__(self, downloader: Downloader, remote_key: str):
        self.downloader = downloader
        self.remote_key = remote_key

    def __call__(self) -> Path:
        # we need to limit path name lengths on standard file systems
        # but we inject some randomness in order to avoid potential conflicts with
        # other threads or processess that are downloading the same file -
        # we don't want partially-overwritten files getting read somewhere else.
        local_dest = tempdir() / (uuid4().hex + self.remote_key.replace("/", "_")[-20:])
        logger.info(f"Unpickling a path from id {self.remote_key} to {local_dest}")
        self.downloader(local_dest)
        return local_dest


def pickle_path(once: Once, stream: PathStream, local_src: Path) -> ty.Union[PathUnpickler, Path]:
    if not local_src.exists():
        logger.warning(
            f"You asked us to pickle the path {local_src} "
            "but it does not exist, so it will be pickled as-is."
        )
        return local_src
    if not local_src.is_file():
        logger.warning(
            f"You asked us to pickle the Path {local_src} "
            "but it is not a file, so it will be pickled as-is."
        )
        return local_src
    # generation of path_id should be deterministic, so that
    # any additional references to it within the context/process
    # don't have to be resent, but can be used as already created.
    remote_key = str(local_src).replace("/", "_")

    def upload():
        logger.debug(
            f"Pickling path {local_src} to {remote_key} - "
            "its contents will get unpickled on the other side as a local temporary path"
        )
        stream.local_to_remote(local_src, remote_key)

    once.run_once(remote_key, upload)
    return PathUnpickler(stream.get_downloader(remote_key), remote_key)


class PathPickler:
    """Allow local file Paths to be pickled as streaming objects and then
    unpickled remotely by downloading them from a stream and then
    returning a Path object pointing to the downloaded file.
    """

    def __init__(self, stream: PathStream, once: Once):
        self.stream = stream
        self.once = once

    def __call__(self, maybe_path: ty.Any):
        if isinstance(maybe_path, Path):
            return pickle_path(self.once, self.stream, maybe_path)
        return None
