"""Core abstractions for the remote runner system."""
import typing as ty
from pathlib import Path

from typing_extensions import Protocol

T_co = ty.TypeVar("T_co", covariant=True)
T_contra = ty.TypeVar("T_contra", contravariant=True)
T = ty.TypeVar("T")
FT = ty.TypeVar("FT", bound=ty.Callable[..., T], contravariant=True)


class IterableWithLen(Protocol[T_co]):
    def __iter__(self) -> ty.Iterator[T_co]:
        ...

    def __len__(self) -> int:
        ...


class ResultChannel(Protocol[T_contra]):
    """After remote invocation, respond with result.

    A remote invocation can succeed with a result or fail with an exception.
    """

    def result(self, __result: T_contra) -> None:
        ...  # pragma: no cover

    def exception(self, __ex: Exception) -> None:
        ...  # pragma: no cover


class Runner(Protocol):
    """A Runner copies a function, its arguments, and discoverable
    context to a location that can be picked up from a future remote
    process, executes that process remotely, and later pulls the
    result of that remote process back to the local caller process.

    It is essentially the same abstraction as
    `concurrent.futures.Executor.submit`, or
    `multiprocessing.Pool.apply`.

    `pure_remote` uses this abstraction to provide a way of wrapping a
    function and calling it elsewhere.
    """

    def __call__(
        self,
        __f: ty.Callable[..., T],
        __args: ty.Sequence,
        __kwargs: ty.Mapping[str, ty.Any],
    ) -> T:
        ...  # pragma: no cover


Shell = ty.Callable[[ty.Sequence[str]], ty.Any]
"""A Shell is a way of getting back into a Python process with enough
context to download the uploaded function and its arguments from the
location where a runner placed it, and then invoke the function. All
arguments are strings because it is assumed that this represents some
kind of command line invocation.

The Shell must be a blocking call, and its result(s) must be available
immediately after its return.
"""


# TODO: for version 2.0, ShellBuilder should be the default, and
# args,kwargs should not be expanded because that doesn't make it
# easier to process them
class _ShellBuilder(Protocol):
    def __call__(self, __f, *__args, **__kwargs) -> Shell:
        ...


class ShellBuilder(ty.NamedTuple):
    """You can also dynamically build your Shell based on the function and arguments passed.

    This allows sharing the core AdlsPickleRunner state/context between subtly different calls.
    """

    shell_builder: _ShellBuilder


class NoResultAfterInvocationError(Exception):
    """Raised if the remotely-invoked function does not provide any result."""


AnyStrSrc = ty.Union[ty.AnyStr, ty.Iterable[ty.AnyStr], ty.IO[ty.AnyStr], Path]


class BlobStore(Protocol):
    def readbytesinto(
        self, __remote_uri: str, __stream_or_file: ty.IO[bytes], *, type_hint: str = "bytes"
    ) -> None:
        """Allows reading into any stream, including a stream-to-disk."""

    def putbytes(self, __remote_uri: str, __data: AnyStrSrc, *, type_hint: str = "bytes") -> str:
        """Upload bytes from any stream, and return fully-qualified, unambiguous URI."""

    def exists(self, __remote_uri: str) -> bool:
        ...

    def join(self, *parts) -> str:
        ...

    def is_blob_not_found(self, __exc: Exception) -> bool:
        ...


Args = ty.Sequence
Kwargs = ty.Mapping[str, ty.Any]
