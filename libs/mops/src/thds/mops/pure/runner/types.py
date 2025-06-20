import typing as ty

from thds.core import futures

from ..core.metadata import ResultMetadata
from ..core.types import Args, F, Kwargs

Shim = ty.Callable[[ty.Sequence[str]], None]
"""A runner Shim is a way of getting back into a Python process with enough
context to download the uploaded function and its arguments from the
location where a runner placed it, and then invoke the function. All
arguments are strings because it is assumed that this represents some
kind of command line invocation.

The Shim must be a blocking call, and its result(s) must be available
immediately after its return.
"""

FutureShimP = ty.Callable[[ty.Sequence[str]], futures.PFuture]


class FutureShim:
    """A FutureShim can return as soon as it likes, but must return some type of Future
    that can be passed to
    """

    def __init__(self, future_shim: FutureShimP) -> None:
        self.future_shim = future_shim

    def __call__(self, args: ty.Sequence[str]) -> futures.PFuture:
        """Call the FutureShim with the arguments to get a PFuture."""
        return self.future_shim(*args)


S = ty.TypeVar("S", Shim, FutureShim, covariant=True)


class ShimBuilder(ty.Protocol, ty.Generic[S]):
    def __call__(self, __f: F, __args: Args, __kwargs: Kwargs) -> S:
        ...  # pragma: no cover


SyncShimBuilder = ShimBuilder[Shim]
FutureShimBuilder = ShimBuilder[FutureShim]

StorageRootURI = str
SerializeArgsKwargs = ty.Callable[[StorageRootURI, F, Args, Kwargs], bytes]
SerializeInvocation = ty.Callable[[StorageRootURI, F, bytes], bytes]
# the bytes parameter is the previously-serialized args,kwargs
GetMetaAndResult = ty.Callable[[str, str], ty.Tuple[ty.Optional[ResultMetadata], ty.Any]]
# the above should probably not 'hide' the fetch of the bytes, but it is what it is for now.
