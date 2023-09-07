import typing as ty

from .._utils.recursive_visit import recursive_visit
from .remote_file import DestFile, SrcFile

T = ty.TypeVar("T")


def mark_as_remote(t: T) -> T:
    """After having 'arrived' in a remote context, this should be
    called to mark all SrcFiles and DestFiles as remote.

    We used to do this with a global is_remote() flag for the
    enviroment, but that forecloses the possibility of doing nested
    remote calls, since once you're 'remote' you're always
    remote. This allows us to be much more specific, only marking
    these objects immediately after they've been actually
    deserialized, which 'guarantees' that they're in a 'remote'
    context (even if that context might actually be on the same
    machine).
    """

    def visitor(obj: ty.Any):
        if isinstance(obj, SrcFile) or isinstance(obj, DestFile):
            obj._mark_as_remote()

    recursive_visit(visitor, t)
    return t
