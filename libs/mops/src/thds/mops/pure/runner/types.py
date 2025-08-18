import typing as ty

from thds.core import futures

from ..core.metadata import ResultMetadata
from ..core.types import Args, F, Kwargs

FutureShim = ty.Callable[[ty.Sequence[str]], futures.PFuture]
SyncShim = ty.Callable[[ty.Sequence[str]], None]
Shim = ty.Union[SyncShim, FutureShim]
"""A runner Shim is a way of getting back into a Python process with enough
context to download the uploaded function and its arguments from the
location where a runner placed it, and then invoke the function. All
arguments are strings because it is assumed that this represents some
kind of command line invocation.

A SyncShim must be a blocking call, and its result(s) must be available
immediately after its return.
A FutureShim must return a Future (with an 'add_done_callback' method)
that, when resolved, means that the result(s) are available.
"""

S = ty.TypeVar("S", SyncShim, FutureShim, Shim, covariant=True)


class ShimBuilder(ty.Protocol, ty.Generic[S]):
    def __call__(self, __f: ty.Callable, __args: Args, __kwargs: Kwargs) -> S:
        ...  # pragma: no cover


SyncShimBuilder = ShimBuilder[SyncShim]
FutureShimBuilder = ShimBuilder[FutureShim]

StorageRootURI = str
SerializeArgsKwargs = ty.Callable[[StorageRootURI, F, Args, Kwargs], bytes]
SerializeInvocation = ty.Callable[[StorageRootURI, F, bytes], bytes]
# the bytes parameter is the previously-serialized args,kwargs
GetMetaAndResult = ty.Callable[[str, str], ty.Tuple[ty.Optional[ResultMetadata], ty.Any]]
# the above should probably not 'hide' the fetch of the bytes, but it is what it is for now.
