# azcopy is, for whatever reason, quite a bit faster than the Python SDK for downloads.
# This code allows us to have a thin "try azcopy if it's present, fall back to the Python
# SDK if it's not" layer.
#
# However, azcopy is also quite a bit 'dumber' when it comes to progress reporting (its
# reported progress totals seem to way underestimate progress and then rubber-band at the
# very end of the download), so for local users who don't have huge bandwidth, it's likely
# a better user experience to disable this globally.
import asyncio
import json
import os
import subprocess
import typing as ty
import urllib.parse
from contextlib import contextmanager
from pathlib import Path

from azure.storage.filedatalake import DataLakeFileClient

from thds.core import cache, config, log

from .. import _progress, conf, uri

DONT_USE_AZCOPY = config.item("dont_use", default=False, parse=config.tobool)

_AZCOPY_LOGIN_WORKLOAD_IDENTITY = "azcopy login --login-type workload".split()
_AZCOPY_LOGIN_LOCAL_STATUS = "azcopy login status".split()
# device login is an interactive process involving a web browser,
# which is not acceptable for large scale automation.
# So instead of logging in, we check to see if you _are_ logged in,
# and if you are, we try using azcopy in the future.

logger = log.getLogger(__name__)


class DownloadRequest(ty.NamedTuple):
    """Use one or the other, but not both, to write the results."""

    writer: ty.IO[bytes]
    temp_path: Path


@cache.locking  # only run this once per process.
def _good_azcopy_login() -> bool:
    if DONT_USE_AZCOPY():
        return False

    try:
        subprocess.run(_AZCOPY_LOGIN_WORKLOAD_IDENTITY, check=True, capture_output=True)
        logger.info("Will use azcopy for downloads in this process...")
        return True

    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    try:
        subprocess.run(_AZCOPY_LOGIN_LOCAL_STATUS, check=True)
        logger.info("Will use azcopy for downloads in this process...", dl=None)
        return True
    except FileNotFoundError:
        logger.info("azcopy is not installed or not on your PATH, so we cannot speed up downloads")
    except subprocess.CalledProcessError as cpe:
        logger.warning(
            "You are not logged in with azcopy, so we cannot speed up downloads."
            f" Run `azcopy login` to fix this. Return code was {cpe.returncode}"
        )
    return False


def _azcopy_download_command(dl_file_client: DataLakeFileClient, path: Path) -> ty.List[str]:
    return ["azcopy", "copy", dl_file_client.url, str(path), "--output-type=json"]


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
def _track_azcopy_progress(http_url: str) -> ty.Iterator[ty.Callable[[str], None]]:
    """Context manager that tracks progress from AzCopy JSON lines. This works for both async and sync impls."""
    tracker = _progress.get_global_download_tracker()
    adls_uri = urllib.parse.unquote(str(uri.parse_uri(http_url)))

    def track(line: str):
        if not line:
            return

        try:
            prog = _parse_azcopy_json_output(line)
            if prog["MessageType"] == "Progress":
                tracker(adls_uri, total_written=int(prog["MessageContent"]["TotalBytesTransferred"]))
        except json.JSONDecodeError:
            pass

    yield track


def _restrict_mem() -> dict:
    return dict(os.environ, AZCOPY_BUFFER_GB="0.3")


def sync_fastpath(
    dl_file_client: DataLakeFileClient,
    download_request: DownloadRequest,
) -> None:
    if _good_azcopy_login():
        try:
            # Run the copy
            process = subprocess.Popen(
                _azcopy_download_command(dl_file_client, download_request.temp_path),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=_restrict_mem(),
            )
            assert process.stdout
            with _track_azcopy_progress(dl_file_client.url) as track:
                for line in process.stdout:
                    track(line)
            return  # success

        except (subprocess.SubprocessError, FileNotFoundError):
            logger.warning("Falling back to Python SDK for download")

    dl_file_client.download_file(
        max_concurrency=conf.DOWNLOAD_FILE_MAX_CONCURRENCY(),
        connection_timeout=conf.CONNECTION_TIMEOUT(),
    ).readinto(download_request.writer)


async def async_fastpath(
    dl_file_client: DataLakeFileClient,
    download_request: DownloadRequest,
) -> None:
    # technically it would be 'better' to do this login in an async subproces,
    # but it involves a lot of boilerplate, _and_ we have no nice way to cache
    # the value, which is going to be computed one per process and never again.
    # So we'll just block the async loop for a couple of seconds one time...
    if _good_azcopy_login():
        try:
            # Run the copy
            copy_proc = await asyncio.create_subprocess_exec(
                *_azcopy_download_command(dl_file_client, download_request.temp_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=_restrict_mem(),
            )
            assert copy_proc.stdout

            # Feed lines to the tracker asynchronously
            with _track_azcopy_progress(dl_file_client.url) as track:
                while True:
                    line = await copy_proc.stdout.readline()
                    if not line:  # EOF
                        break
                    track(line.decode().strip())

            # Wait for process completion
            exit_code = await copy_proc.wait()
            if exit_code != 0:
                raise subprocess.SubprocessError()

            return  # success

        except (subprocess.SubprocessError, FileNotFoundError):
            logger.warning("Falling back to Python SDK for download")

    reader = await dl_file_client.download_file(  # type: ignore[misc]
        # TODO - check above type ignore
        max_concurrency=conf.DOWNLOAD_FILE_MAX_CONCURRENCY(),
        connection_timeout=conf.CONNECTION_TIMEOUT(),
    )
    await reader.readinto(download_request.writer)
