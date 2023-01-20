import os
import socket
from datetime import datetime

from thds.core.stack_context import StackContext

_IS_REMOTE = StackContext("is_remote", False)


def _simple_host() -> str:
    hn = socket.gethostname()
    if hn.endswith(".local"):
        hn = hn[: -len(".local")]
    if hn.startswith("MBP-"):
        hn = hn[len("MBP-") :]
    return hn


_PIPELINE_ID = ""


def get_pipeline_id() -> str:
    return _PIPELINE_ID


def set_pipeline_id(name: str):
    global _PIPELINE_ID
    _PIPELINE_ID = name


set_pipeline_id(
    "-".join(
        [
            _simple_host(),
            datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            f"p{os.getpid()}",
        ]
    )
)


def is_remote() -> bool:
    return _IS_REMOTE()
