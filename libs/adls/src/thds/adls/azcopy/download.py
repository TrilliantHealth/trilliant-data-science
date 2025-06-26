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
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from azure.storage.filedatalake import DataLakeFileClient

from thds.core import cache, config, cpus, log, scope

from .. import _progress, conf, uri

DONT_USE_AZCOPY = config.item("dont_use", default=False, parse=config.tobool)
MIN_FILE_SIZE = config.item("min_file_size", default=20 * 10**6, parse=int)  # 20 MB

_AZCOPY_LOGIN_WORKLOAD_IDENTITY = "azcopy login --login-type workload".split()
_AZCOPY_LOGIN_LOCAL_STATUS = "azcopy login status".split()
# device login is an interactive process involving a web browser,
# which is not acceptable for large scale automation.
# So instead of logging in, we check to see if you _are_ logged in,
# and if you are, we try using azcopy in the future.

logger = log.getLogger(__name__)


@dataclass
class DownloadRequest:
    temp_path: Path
    size_bytes: int


@dataclass
class SdkDownloadRequest(DownloadRequest):
    """Use one or the other, but not both, to write the results."""

    writer: ty.IO[bytes]


@cache.locking  # only run this once per process.
@scope.bound
def good_azcopy_login() -> bool:
    scope.enter(log.logger_context(dl=None))
    try:
        subprocess.run(_AZCOPY_LOGIN_WORKLOAD_IDENTITY, check=True, capture_output=True)
        logger.info("Azcopy login with workload identity, so we can use it for large downloads")
        return True

    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    try:
        subprocess.run(_AZCOPY_LOGIN_LOCAL_STATUS, check=True)
        logger.info("Azcopy login with local token, so we can use it for large downloads")
        return True

    except FileNotFoundError:
        logger.info("azcopy is not installed or not on your PATH, so we cannot speed up large downloads")
    except subprocess.CalledProcessError as cpe:
        logger.warning(
            "You are not logged in with azcopy, so we cannot speed up large downloads."
            f" Run `azcopy login` to fix this. Return code was {cpe.returncode}"
        )
    return False


def is_big_enough_for_azcopy(size_bytes: int) -> bool:
    return size_bytes >= MIN_FILE_SIZE()


def should_use_azcopy(file_size_bytes: int) -> bool:
    return is_big_enough_for_azcopy(file_size_bytes) and not DONT_USE_AZCOPY() and good_azcopy_login()


def _azcopy_download_command(dl_file_client: DataLakeFileClient, path: Path) -> ty.List[str]:
    # turns out azcopy checks md5 by default - but we we do our own checking, sometimes with faster methods,
    # and their checking _dramatically_ slows downloads on capable machines, so we disable it.
    return ["azcopy", "copy", dl_file_client.url, str(path), "--output-type=json", "--check-md5=NoCheck"]


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
def _track_azcopy_progress(http_url: str, size_bytes: int) -> ty.Iterator[ty.Callable[[str], None]]:
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


@lru_cache
def _restrict_resource_usage() -> dict:
    num_cpus = cpus.available_cpu_count()

    env = dict(os.environ)
    if "AZCOPY_BUFFER_GB" not in os.environ:
        likely_mem_gb_available = num_cpus * 4  # assume 4 GB per CPU core is available
        # o3 suggested 15% of the total available memory...
        env["AZCOPY_BUFFER_GB"] = str(likely_mem_gb_available * 0.15)
    if "AZCOPY_CONCURRENCY" not in os.environ:
        env["AZCOPY_CONCURRENCY"] = str(int(num_cpus * 2))

    logger.info(
        "AZCOPY_BUFFER_GB == %s and AZCOPY_CONCURRENCY == %s",
        env["AZCOPY_BUFFER_GB"],
        env["AZCOPY_CONCURRENCY"],
    )
    return env


def sync_fastpath(
    dl_file_client: DataLakeFileClient,
    download_request: DownloadRequest,
) -> None:
    if not isinstance(download_request, SdkDownloadRequest):
        logger.debug("Downloading %s using azcopy", dl_file_client.url)
        try:
            # Run the copy
            process = subprocess.Popen(
                _azcopy_download_command(dl_file_client, download_request.temp_path),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=_restrict_resource_usage(),
            )
            assert process.stdout
            with _track_azcopy_progress(dl_file_client.url, download_request.size_bytes) as track:
                for line in process.stdout:
                    track(line)

            process.wait()
            if process.returncode != 0:
                raise subprocess.SubprocessError(f"AzCopy failed with return code {process.returncode}")
            assert (
                download_request.temp_path.exists()
            ), f"AzCopy did not create the file at {download_request.temp_path}"
            return  # success

        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.warning("Falling back to Python SDK for download")

    logger.debug("Downloading %s using Python SDK", dl_file_client.url)
    if hasattr(download_request, "writer"):
        writer_cm = nullcontext(download_request.writer)
    else:
        writer_cm = open(download_request.temp_path, "wb")  # type: ignore[assignment]
    with writer_cm as writer:
        dl_file_client.download_file(
            max_concurrency=conf.DOWNLOAD_FILE_MAX_CONCURRENCY(),
            connection_timeout=conf.CONNECTION_TIMEOUT(),
        ).readinto(writer)


async def async_fastpath(
    dl_file_client: DataLakeFileClient,
    download_request: DownloadRequest,
) -> None:
    # technically it would be 'better' to do this login in an async subprocess,
    # but it involves a lot of boilerplate, _and_ we have no nice way to cache
    # the value, which is going to be computed one per process and never again.
    # So we'll just block the async loop for a couple of seconds one time...
    if not isinstance(download_request, SdkDownloadRequest):
        logger.debug("Downloading %s using azcopy", dl_file_client.url)
        try:
            # Run the copy
            copy_proc = await asyncio.create_subprocess_exec(
                *_azcopy_download_command(dl_file_client, download_request.temp_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=_restrict_resource_usage(),
            )
            assert copy_proc.stdout

            # Feed lines to the tracker asynchronously
            with _track_azcopy_progress(dl_file_client.url, download_request.size_bytes) as track:
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

    logger.debug("Downloading %s using Async Python SDK", dl_file_client.url)
    if hasattr(download_request, "writer"):
        writer_cm = nullcontext(download_request.writer)
    else:
        writer_cm = open(download_request.temp_path, "wb")  # type: ignore[assignment]
    with writer_cm as writer:
        reader = await dl_file_client.download_file(  # type: ignore[misc]
            # TODO - check above type ignore
            max_concurrency=conf.DOWNLOAD_FILE_MAX_CONCURRENCY(),
            connection_timeout=conf.CONNECTION_TIMEOUT(),
        )
        await reader.readinto(writer)
