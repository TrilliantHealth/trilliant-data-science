"""Non-object implementation details for SrcFile."""
import typing as ty
from threading import RLock

from .._recursive_visit import recursive_visit
from .remote_file import SrcFile
from .types import Args, Kwargs

_GLOBAL_SRC_FILE_UPLOAD_LOCK = RLock()
# SrcFiles are generally created on the orchestrator in order to fan
# out across multiple workers. If multiple threads launch workers
# concurrently, you don't want to cause multiple re-uploads of the same
# file.
#
# This is a simplistic approach to avoiding that, by forcing all SrcFiles
# to upload one after the other.


def trigger_src_files_upload(args: Args, kwargs: Kwargs):
    """Runner implementations making use of remote filesystems should
    call this before remote function execution.
    """

    def visitor(obj: ty.Any):
        if isinstance(obj, SrcFile):
            with _GLOBAL_SRC_FILE_UPLOAD_LOCK:
                obj._upload_if_not_already_remote()

    recursive_visit(visitor, args)
    recursive_visit(visitor, kwargs)
