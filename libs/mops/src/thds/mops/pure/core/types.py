"""Core abstractions for the remote runner system."""

import typing as ty
from pathlib import Path

from typing_extensions import Protocol

from thds.core import config

T = ty.TypeVar("T")
F = ty.TypeVar("F", bound=ty.Callable)

Deserializer = ty.Callable[[], T]
Serializer = ty.Callable[[T], Deserializer]
SerializerHandler = ty.Callable[[T], ty.Union[None, Deserializer]]
# returns None if the object should be serialized normally.
# Otherwise returns a Deserializing Callable that will itself return the deserialized object when called.


class Runner(Protocol):
    """A Runner copies a function, its arguments, and discoverable
    context to a location that can be picked up from a future remote
    process, executes that process remotely, and later pulls the
    result of that remote process back to the local caller process.

    It is essentially the same abstraction as
    `concurrent.futures.Executor.submit`, or
    `multiprocessing.Pool.apply`.

    `use_runner` uses this abstraction to provide a way of wrapping a
    function and calling it elsewhere.
    """

    def __call__(
        self,
        __f: ty.Callable[..., T],
        __args: ty.Sequence,
        __kwargs: ty.Mapping[str, ty.Any],
    ) -> T:
        ...  # pragma: no cover


class NoResultAfterInvocationError(Exception):  # TODO remove in v4.
    """Runners should raise this if the remotely-invoked function does not provide any result."""


class NoResultAfterShellSuccess(NoResultAfterInvocationError):
    """Raised this if the shell returns with no error, but no result is found in the blob store.

    A better name for NoResultAfterInvocationError.
    """


class NotARunnerContext(Exception):
    """Mops may raise this if some code intended to be run under a
    Runner context is invoked outside that context.
    """


AnyStrSrc = ty.Union[ty.AnyStr, ty.Iterable[ty.AnyStr], ty.IO[ty.AnyStr], Path]


DISABLE_CONTROL_CACHE = config.item(
    "thds.mops.pure.disable_control_cache", default=False, parse=config.tobool
)
# set the above to True in order to specifically opt out of read-path caching of
# mops-created files. This can apply to a local (stack) context, or can
# apply globally to the process. The former may be used selectively within mops
# for issues of known correctness, e.g. locks, whereas the latter will be useful
# for debugging any cases where files have been remotely deleted.


class BlobStore(Protocol):
    def readbytesinto(
        self, __remote_uri: str, __stream_or_file: ty.IO[bytes], *, type_hint: str = "bytes"
    ) -> None:
        """Allows reading into any stream, including a stream-to-disk.

        May optimize reads by returning a cached version of the file if it has been seen before.
        """

    def getfile(self, __remote_uri: str) -> Path:
        """Read a remote uri directly into a path controlled by the implementation.
        Optimizations involving caches for remotes may be applied.
        The returned file is by definition read-only.
        """

    def putbytes(self, __remote_uri: str, __data: AnyStrSrc, *, type_hint: str = "bytes"):
        """Upload bytes from any stream."""

    def putfile(self, __path: Path, __remote_uri: str) -> None:
        """Upload a file that exists on the local
        filesystem. Optimizations including softlinking into caches may be
        applied.
        """

    def exists(self, __remote_uri: str) -> bool:
        """Check if a file exists. May optimize by assuming that files previously seen
        have not been deleted - since this is intended only for mops control files,
        and mops never deletes any control files.
        """

    def join(self, *parts: str) -> str:
        ...

    def split(self, uri: str) -> ty.List[str]:
        """Must return the storage root as a single string,
        followed by the path component split along the same lines that join would concatenate.
        """

    def is_blob_not_found(self, __exc: Exception) -> bool:
        ...


Args = ty.Sequence
Kwargs = ty.Mapping[str, ty.Any]
