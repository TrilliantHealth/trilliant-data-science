"""Core abstractions for the remote runner system."""
import typing as ty

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
        self, __f: ty.Callable[..., T], __args: ty.Sequence, __kwargs: ty.Mapping[str, ty.Any]
    ) -> T:
        ...  # pragma: no cover
