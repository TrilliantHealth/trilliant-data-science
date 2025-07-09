# azcopy is, for whatever reason, quite a bit faster than the Python SDK for downloads.
# This code allows us to have a thin "try azcopy if it's present, fall back to the Python
# SDK if it's not" layer.
#
# However, azcopy is also quite a bit 'dumber' when it comes to progress reporting (its
# reported progress totals seem to way underestimate progress and then rubber-band at the
# very end of the download), so for local users who don't have huge bandwidth, it's likely
# a better user experience to disable this globally.
import asyncio
import subprocess
import typing as ty
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path

from azure.storage.filedatalake import DataLakeFileClient

from thds.core import config, log

from .. import conf
from . import login, progress, system_resources

DONT_USE_AZCOPY = config.item("dont_use", default=False, parse=config.tobool)
MIN_FILE_SIZE = config.item("min_file_size", default=20 * 10**6, parse=int)  # 20 MB

logger = log.getLogger(__name__)


@dataclass
class DownloadRequest:
    temp_path: Path
    size_bytes: ty.Optional[int]


@dataclass
class SdkDownloadRequest(DownloadRequest):
    """Use one or the other, but not both, to write the results."""

    writer: ty.IO[bytes]


def _is_big_enough_for_azcopy(size_bytes: int) -> bool:
    return size_bytes >= MIN_FILE_SIZE()


def should_use_azcopy(file_size_bytes: int) -> bool:
    return (
        _is_big_enough_for_azcopy(file_size_bytes)
        and not DONT_USE_AZCOPY()
        and login.good_azcopy_login()
    )


def _azcopy_download_command(dl_file_client: DataLakeFileClient, path: Path) -> ty.List[str]:
    # turns out azcopy checks md5 by default - but we we do our own checking, sometimes with faster methods,
    # and their checking _dramatically_ slows downloads on capable machines, so we disable it.
    return ["azcopy", "copy", dl_file_client.url, str(path), "--output-type=json", "--check-md5=NoCheck"]


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
                env=system_resources.restrict_usage(),
            )
            assert process.stdout
            output_lines = list()
            with progress.azcopy_tracker(dl_file_client.url, download_request.size_bytes or 0) as track:
                for line in process.stdout:
                    track(line)
                    output_lines.append(line.strip())

            process.wait()
            if process.returncode != 0:
                raise subprocess.CalledProcessError(
                    process.returncode,
                    f"AzCopy failed with return code {process.returncode}\n\n" + "\n".join(output_lines),
                )
            assert (
                download_request.temp_path.exists()
            ), f"AzCopy did not create the file at {download_request.temp_path}"
            return

        except (subprocess.SubprocessError, FileNotFoundError):
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
                env=system_resources.restrict_usage(),
            )
            assert copy_proc.stdout

            # Feed lines to the tracker asynchronously
            output_lines = list()
            with progress.azcopy_tracker(dl_file_client.url, download_request.size_bytes or 0) as track:
                while True:
                    line = await copy_proc.stdout.readline()
                    if not line:  # EOF
                        break
                    track(line.decode().strip())
                    output_lines.append(line.decode().strip())

            # Wait for process completion
            exit_code = await copy_proc.wait()
            if exit_code != 0:
                raise subprocess.CalledProcessError(
                    exit_code,
                    f"AzCopy failed with return code {exit_code}\n\n" + "\n".join(output_lines),
                )

            return

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
