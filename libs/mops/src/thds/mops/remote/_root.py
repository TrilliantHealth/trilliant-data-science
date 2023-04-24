import os
import socket
from datetime import datetime

from thds.core.stack_context import StackContext

from ..config import memo_pipeline_id

_IS_REMOTE = StackContext("is_remote", False)


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
    set_pipeline_id(
        memo_pipeline_id()
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
    if not _PIPELINE_ID:
        __set_or_generate_pipeline_id_if_empty()
    return _PIPELINE_ID


def set_pipeline_id(name: str):
    global _PIPELINE_ID
    _PIPELINE_ID = name


def is_remote() -> bool:
    return _IS_REMOTE()
