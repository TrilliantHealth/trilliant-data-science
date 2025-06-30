import os
import typing as ty
from dataclasses import dataclass
from pathlib import Path

from .. import hashing, types


@dataclass(frozen=True)
class Source(os.PathLike):
    """Source is meant to be a consistent in-memory representation for an abstract,
    **read-only** source of data that may not be present locally when an application
    starts.

    A Source uses `os.PathLike` (`__fspath__`) to support transparent `open(src)` calls,
    so in many cases it will be a drop-in replacement for Path or str filenames. If you
    need an actual Path object, you can call `path()` to get one, but you should prefer to
    defer this until the actual location of use.

    By 'wrapping' read-only data in these objects, we can unify the code around how we
    unwrap and use them, which should allow us to more easily support different execution
    environments and sources of data.

    For instance, a Source could be a file on disk, but it could also be a file in
    ADLS.

    Furthermore, libraries which build on top of this one may use this representation to
    identify opportunities for optimization, by representing the Source in a stable
    and consistent format that allows different underlying data sources to fulfill the
    request for the data based on environmental context. A library could choose to
    transparently transform a local-path-based Source into a Source representing a
    remote file, without changing the semantics of the object as observed by the code.

    One reason a Hash is part of the interface is so that libraries interacting with the
    object can use the hash as a canonical 'name' for the data, if one is available.

    Another reason is that we can add a layer of consistency checking to data we're
    working with, at the cost of a few compute cycles. Since Sources are meant to represent
    read-only data, the Hash is a meaningful and persistent marker of data identity.

    Do not call its constructor in application code. Use `from_file` or `from_uri` instead.
    """

    uri: str
    hash: ty.Optional[hashing.Hash] = None
    # hash and equality are based only on the _identity_ of the object,
    # not on the other properties that provide some caching functionality.

    @property
    def cached_path(self) -> ty.Optional[Path]:
        """This is part of the public interface as far as checking to see whether a file
        is already present locally, but its existence and value is not part of equality or
        the hash for this class - it exists purely as an optimization.
        """
        return getattr(self, "__cached_path", None)

    def _set_cached_path(self, lpath: ty.Optional[Path]):
        """protected interface for setting a cached Path since the attribute is not
        available via the constructor.
        """
        super().__setattr__("__cached_path", lpath)  # this works around dataclass.frozen.
        # https://noklam.github.io/blog/posts/2022-04-22-python-dataclass-partiala-immutable.html

    def path(self) -> Path:
        """Any Source can be turned into a local file path.

        Remember that the resulting data is meant to be read-only. If you want to mutate
        the data, you should first make a copy.

        If not already present locally, this will incur a one-time download. Then, if the
        Source has a Hash, the Hash will be validated against the downloaded file, and a
        failure will raise SourceHashMismatchError.
        """
        if self.cached_path is None or not self.cached_path.exists():
            from . import _download  # ugly circular import

            lpath = _download._get_download_handler(self.uri)(self.hash)
            # path() used to be responsible for checking the hash, but since we pass it to the downloader,
            # it really makes more sense to allow the downloader to decide how to verify its own download,
            # and we don't want to duplicate any effort that it may have already put in.
            self._set_cached_path(lpath)

        assert self.cached_path and self.cached_path.exists()
        return self.cached_path

    def __fspath__(self) -> str:
        return os.fspath(self.path())

    @staticmethod
    def from_file(
        filename: types.StrOrPath, hash: ty.Optional[hashing.Hash] = None, uri: str = ""
    ) -> "Source":
        from ._construct import from_file

        return from_file(filename, hash, uri)

    @staticmethod
    def from_uri(uri: str, hash: ty.Optional[hashing.Hash] = None) -> "Source":
        from ._construct import from_uri

        return from_uri(uri, hash)
