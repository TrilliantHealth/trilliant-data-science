import subprocess
import typing as ty
from pathlib import Path

from thds.core import config

from .. import uri
from . import login, progress, system_resources

DONT_USE_AZCOPY = config.item("dont_use", default=False, parse=config.tobool)
MIN_FILE_SIZE = config.item("min_file_size", default=20 * 10**6, parse=int)  # 20 MB


def build_azcopy_upload_command(
    source_path: Path,
    dest: uri.UriIsh,
    *,
    content_type: str = "",
    metadata: ty.Mapping[str, str] = dict(),  # noqa: B006
    overwrite: bool = True,
) -> list[str]:
    """
    Build azcopy upload command as a list of strings.

    Args:
        source_path: Path to local file to upload
        dest_url: Full Azure blob URL (e.g., https://account.blob.core.windows.net/container/blob)
        content_type: MIME content type
        metadata: Mapping of metadata key-value pairs
        overwrite: Whether to overwrite existing blob

    Returns:
        List of strings suitable for subprocess.run()
    """

    cmd = ["azcopy", "copy", str(source_path), uri.to_blob_windows_url(dest)]

    if overwrite:
        cmd.append("--overwrite=true")

    if content_type:
        cmd.append(f"--content-type={content_type}")

    if metadata:
        # Format metadata as key1=value1;key2=value2
        metadata_str = ";".join(f"{k}={v}" for k, v in metadata.items())
        cmd.append(f"--metadata={metadata_str}")

    cmd.append("--output-type=json")  # for progress tracking

    return cmd


def _is_big_enough_for_azcopy(size_bytes: int) -> bool:
    """
    Determine if a file is big enough to warrant using azcopy for upload.

    Args:
        size_bytes: Size of the file in bytes

    Returns:
        True if the file is big enough, False otherwise
    """
    return size_bytes >= MIN_FILE_SIZE()


def should_use_azcopy(file_size_bytes: int) -> bool:
    return (
        _is_big_enough_for_azcopy(file_size_bytes)
        and not DONT_USE_AZCOPY()
        and login.good_azcopy_login()
    )


def run(
    cmd: ty.Sequence[str],
    dest: uri.UriIsh,
    size_bytes: int,
) -> None:
    # Run the copy
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=system_resources.restrict_usage(),
    )
    assert process.stdout
    output_lines = list()
    with progress.azcopy_tracker(uri.to_blob_windows_url(dest), size_bytes) as track:
        for line in process.stdout:
            track(line)
            output_lines.append(line.strip())

    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(
            process.returncode,
            f"AzCopy failed with return code {process.returncode}\n\n" + "\n".join(output_lines),
        )
