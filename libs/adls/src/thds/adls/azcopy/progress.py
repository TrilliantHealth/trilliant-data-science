import json
import typing as ty
import urllib.parse
from contextlib import contextmanager

from .. import _progress, uri


class AzCopyMessage(ty.TypedDict):
    TotalBytesEnumerated: str
    TotalBytesTransferred: str


class AzCopyJsonLine(ty.TypedDict):
    MessageType: str
    MessageContent: AzCopyMessage


def _parse_azcopy_json_output(line: str) -> AzCopyJsonLine:
    outer_msg = json.loads(line)
    return AzCopyJsonLine(
        MessageType=outer_msg["MessageType"],
        MessageContent=json.loads(outer_msg["MessageContent"]),
    )


@contextmanager
def azcopy_tracker(http_url: str, size_bytes: int) -> ty.Iterator[ty.Callable[[str], None]]:
    """Context manager that tracks progress from AzCopy JSON lines. This works for both async and sync impls."""
    tracker = _progress.get_global_download_tracker()
    adls_uri = urllib.parse.unquote(str(uri.parse_uri(http_url)))
    if size_bytes:
        tracker.add(adls_uri, total=size_bytes)

    def track(line: str):
        if not size_bytes:
            return  # no size, no progress

        if not line:
            return

        try:
            prog = _parse_azcopy_json_output(line)
            if prog["MessageType"] == "Progress":
                tracker(adls_uri, total_written=int(prog["MessageContent"]["TotalBytesTransferred"]))
        except json.JSONDecodeError:
            pass

    yield track
