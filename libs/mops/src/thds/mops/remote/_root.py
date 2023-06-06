import os
import socket
from datetime import datetime

from thds.core import meta, stack_context

from ..config import memo_pipeline_id

_IS_REMOTE = stack_context.StackContext("is_remote", False)


def _simple_host() -> str:
    hn = socket.gethostname()
    if hn.endswith(".local"):
        hn = hn[: -len(".local")]
    if hn.startswith("MBP-"):
        hn = hn[len("MBP-") :]
    return hn


# this is a global instead of a StackContext because we _do_ want it
# to spill over automatically into new threads.
_PIPELINE_ID = ""


def __set_or_generate_pipeline_id_if_empty():
    some_unique_name = meta.get_repo_name() or os.getenv("THDS_DOCKER_IMAGE_NAME") or ""
    clean_commit = meta.get_commit()[:7] if meta.is_clean() else ""
    named_clean_commit = (
        f"{some_unique_name}/{clean_commit}" if some_unique_name and clean_commit else ""
    )
    set_pipeline_id(
        memo_pipeline_id()
        or named_clean_commit
        or _simple_host()  # host name can be a group/directory now
        + "/"
        + "-".join(
            [
                datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                f"p{os.getpid()}",
            ]
        )
    )


def get_pipeline_id() -> str:
    """This will return the stack-local pipeline id, if set, or, if
    that is not set, will generate a global pipeline id and return
    that.

    Once a global pipeline id is generated, it will not be
    regenerated, although it can be overridden as a global with
    set_pipeline_id, and overridden for the stack with
    """
    if not _PIPELINE_ID:
        __set_or_generate_pipeline_id_if_empty()
    assert _PIPELINE_ID
    return memo_pipeline_id() or _PIPELINE_ID


def set_pipeline_id(name: str):
    """Override the current global pipeline id."""
    if not name:
        return  # quietly disallow empty strings, since we always want a value here.
    global _PIPELINE_ID
    _PIPELINE_ID = name


def is_remote() -> bool:
    return _IS_REMOTE()
