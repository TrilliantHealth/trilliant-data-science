import typing as ty

from ..core.metadata import ResultMetadata
from ..core.types import Args, F, Kwargs

Shell = ty.Callable[[ty.Sequence[str]], ty.Any]
"""A Shell is a way of getting back into a Python process with enough
context to download the uploaded function and its arguments from the
location where a runner placed it, and then invoke the function. All
arguments are strings because it is assumed that this represents some
kind of command line invocation.

The Shell must be a blocking call, and its result(s) must be available
immediately after its return.
"""


class ShellBuilder(ty.Protocol):
    def __call__(self, __f: F, __args: Args, __kwargs: Kwargs) -> Shell:
        ...  # pragma: no cover


StorageRootURI = str
SerializeArgsKwargs = ty.Callable[[StorageRootURI, F, Args, Kwargs], bytes]
SerializeInvocation = ty.Callable[[StorageRootURI, F, bytes], bytes]
# the bytes parameter is the previously-serialized args,kwargs
GetMetaAndResult = ty.Callable[[str, str], ty.Tuple[ty.Optional[ResultMetadata], ty.Any]]
# the above should probably not 'hide' the fetch of the bytes, but it is what it is for now.
