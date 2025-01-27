"""Just utilities for deciding whether or not to upload.

Not an officially-published API of the thds.adls library.
"""
import typing as ty
from pathlib import Path

import azure.core.exceptions
from azure.storage.blob import ContentSettings

from thds.core import hostname, log

from .md5 import AnyStrSrc, try_md5

_SKIP_ALREADY_UPLOADED_CHECK_IF_MORE_THAN_BYTES = 2 * 2**20  # 2 MB is about right


logger = log.getLogger(__name__)


def _get_checksum_content_settings(data: AnyStrSrc) -> ty.Optional[ContentSettings]:
    """Ideally, we calculate an MD5 sum for all data that we upload.

    The only circumstances under which we cannot do this are if the
    stream does not exist in its entirety before the upload begins.
    """
    md5 = try_md5(data)
    if md5:
        return ContentSettings(content_md5=md5)
    return None


def _too_small_to_skip_upload(data: AnyStrSrc, min_size_for_remote_check: int) -> bool:
    def _len() -> int:
        if isinstance(data, Path) and data.exists():
            return data.stat().st_size
        try:
            return len(data)  # type: ignore
        except TypeError as te:
            logger.debug(f"failed to get length? {repr(te)} for {data}")
            return min_size_for_remote_check + 1

    return _len() < min_size_for_remote_check


class UploadDecision(ty.NamedTuple):
    upload_required: bool
    content_settings: ty.Optional[ContentSettings]


class Properties(ty.Protocol):
    name: str
    content_settings: ContentSettings


def _co_content_settings_for_upload_unless_file_present_with_matching_checksum(
    data: AnyStrSrc, min_size_for_remote_check: int
) -> ty.Generator[bool, ty.Optional[Properties], UploadDecision]:
    local_content_settings = _get_checksum_content_settings(data)
    if not local_content_settings:
        return UploadDecision(True, None)
    if _too_small_to_skip_upload(data, min_size_for_remote_check):
        logger.debug("Too small to bother with an early call - let's just upload...")
        return UploadDecision(True, local_content_settings)
    remote_properties = yield True
    if not remote_properties:
        logger.debug("No remote properties could be fetched so an upload is required")
        return UploadDecision(True, local_content_settings)
    if remote_properties.content_settings.content_md5 == local_content_settings.content_md5:
        logger.info(f"Remote file {remote_properties.name} already exists and has matching checksum")
        return UploadDecision(False, local_content_settings)
    logger.debug("Remote file exists but MD5 does not match - upload required.")
    return UploadDecision(True, local_content_settings)


doc = """
Returns False for upload_required if the file is large and the remote
exists and has a known, matching checksum.

Returns ContentSettings if an MD5 checksum can be calculated.
"""


async def async_upload_decision_and_settings(
    get_properties: ty.Callable[[], ty.Awaitable[Properties]],
    data: AnyStrSrc,
    min_size_for_remote_check: int = _SKIP_ALREADY_UPLOADED_CHECK_IF_MORE_THAN_BYTES,
) -> UploadDecision:
    try:
        co = _co_content_settings_for_upload_unless_file_present_with_matching_checksum(
            data, min_size_for_remote_check
        )
        while True:
            co.send(None)
            try:
                co.send(await get_properties())
            except azure.core.exceptions.ResourceNotFoundError:
                co.send(None)
    except StopIteration as stop:
        return stop.value


def upload_decision_and_settings(
    get_properties: ty.Callable[[], Properties],
    data: AnyStrSrc,
    min_size_for_remote_check: int = _SKIP_ALREADY_UPLOADED_CHECK_IF_MORE_THAN_BYTES,
) -> UploadDecision:
    try:
        co = _co_content_settings_for_upload_unless_file_present_with_matching_checksum(
            data, min_size_for_remote_check
        )
        while True:
            co.send(None)
            try:
                co.send(get_properties())
            except azure.core.exceptions.ResourceNotFoundError:
                co.send(None)
    except StopIteration as stop:
        return stop.value


async_upload_decision_and_settings.__doc__ = doc
upload_decision_and_settings.__doc__ = doc


def metadata_for_upload() -> ty.Dict[str, str]:
    return {"upload_wrapper_sw": "thds.adls", "upload_hostname": hostname.friendly()}
