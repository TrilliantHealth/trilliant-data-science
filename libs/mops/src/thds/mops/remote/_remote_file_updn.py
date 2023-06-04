import os
import typing as ty
from functools import partial

StrOrPath = ty.Union[str, os.PathLike]
Serialized = ty.NewType("Serialized", str)
Uploader = ty.Callable[[StrOrPath], Serialized]
UriUploader = ty.Callable[[str, StrOrPath], Serialized]
NamedUriUploader = ty.Tuple[str, str]
# uploads local file and returns serialized representation of remote file location
Downloader = ty.Callable[[Serialized, StrOrPath], None]
NamedDownloader = str
# interprets the serialized string as a remote file location and downloads it to the provided local path

# TODO in 2.0 we will namespace this along with SrcFile and DestFile.
# but because they are pickled, we can't move them now without
# breaking everything for current users.

URI_UPLOADERS: ty.Dict[str, ty.Callable[[str, str], Serialized]] = {}


def reify_uploader(uploader: ty.Union[NamedUriUploader, Uploader]) -> Uploader:
    """Do not actually store this reified uploader on a SrcFile or DestFile.

    The whole point is to defer this so that the code is uncoupled
    from the serialization of the class.
    """
    if not callable(uploader):
        name, uri = uploader
        if name not in URI_UPLOADERS:
            raise ValueError(
                f"URI {name} not registered as an uploader. "
                f"Registered uploaders: {list(URI_UPLOADERS.keys())}"
            )
        return partial(URI_UPLOADERS[name], uri)
    return uploader


DOWNLOADERS: ty.Dict[str, Downloader] = {}


def reify_downloader(downloader: ty.Union[NamedDownloader, Downloader]) -> Downloader:
    if isinstance(downloader, NamedDownloader):
        if downloader not in DOWNLOADERS:
            raise ValueError(
                f"Downloader {downloader} not registered. "
                f"Registered downloaders: {list(DOWNLOADERS.keys())}"
            )
        return DOWNLOADERS[downloader]
    return downloader
