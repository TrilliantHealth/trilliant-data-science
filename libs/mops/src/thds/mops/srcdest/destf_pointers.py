"""Non-object implementation details for DestFile as the abstraction continues to grow..."""
import os
import typing as ty
from pathlib import Path

from thds.core.log import getLogger

from .._utils.recursive_visit import recursive_visit
from .remote_file import DestFile

logger = getLogger(__name__)
T = ty.TypeVar("T")

# TODO for 2.0, possibly redo this as something that lives statically within the DestFile?
# The advantage of leaving it 'out' is that it doesn't impact serialization at all,
# but the disadvantage is that it's a form of indirection akin to magic.
_REMOTE_DEST_FILENAME_ADJUSTER: ty.Optional[ty.Callable[[str], str]] = None


def set_dest_filename_adjuster(func: ty.Callable[[str], str]):
    """This is an experimental feature; I'm not committing to backward compatibility here."""
    global _REMOTE_DEST_FILENAME_ADJUSTER
    _REMOTE_DEST_FILENAME_ADJUSTER = func


def _write_serialized_to_dest_placeholder(
    raw_local_filename: str, serialized_remote_pointer: str
) -> ty.Optional[Path]:
    """For use by framework code - generally a Runner implementation -
    in the local orchestrator process.

    Called during _return_ from `use_runner`.
    """
    if not serialized_remote_pointer:
        # this means that no file was written remotely.
        # therefore, no file should be written locally either
        if not Path(raw_local_filename).exists():
            logger.warning(
                f"DestFile {raw_local_filename} with empty serialization "
                "returned from use_runner-decorated function."
            )
        else:
            logger.debug("Local dest file being returned without modification")
        return None
    if _REMOTE_DEST_FILENAME_ADJUSTER:
        local_filename = _REMOTE_DEST_FILENAME_ADJUSTER(raw_local_filename)
    else:
        local_filename = raw_local_filename
    if not local_filename:
        # either the adjuster has said we don't want to serialize this one,
        # or no local filename exists at all (maybe a remote dest file?)
        return None
    logger.info(f"Writing remote file pointer to local path {local_filename}")
    Path(local_filename).parent.mkdir(exist_ok=True, parents=True)
    with open(local_filename, "w") as f:
        if not serialized_remote_pointer.endswith("\n"):
            serialized_remote_pointer += "\n"
        f.write(serialized_remote_pointer)
    return Path(local_filename)


def trigger_dest_files_placeholder_write(rval: T) -> T:
    """Runner implementations making use of remote filesystems should
    call this after remote function execution.
    """

    def visitor(obj: ty.Any):
        if isinstance(obj, DestFile):
            local_written = _write_serialized_to_dest_placeholder(
                obj._local_filename,
                obj._serialized_remote_pointer,
            )
            if local_written:
                obj._local_filename = os.fspath(local_written)

    recursive_visit(visitor, rval)  # type: ignore
    return rval
