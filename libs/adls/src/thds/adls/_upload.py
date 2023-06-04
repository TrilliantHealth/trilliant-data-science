import typing as ty

import azure.core.exceptions
from azure.storage.blob import ContentSettings
from azure.storage.filedatalake import DataLakeFileClient, FileProperties

from thds.core.log import getLogger

from .md5 import AnyStrSrc, try_md5

_SKIP_ALREADY_UPLOADED_CHECK_IF_MORE_THAN_BYTES = 2 * 2**20  # 2 MB is about right


logger = getLogger(__name__)


def _get_checksum_content_settings(data: AnyStrSrc) -> ty.Optional[ContentSettings]:
    """Ideally, we calculate an MD5 sum for all data that we upload.

    The only circumstances under which we cannot do this are if the
    stream does not exist in its entirety before the upload begins.
    """
    md5 = try_md5(data)
    if md5:
        return ContentSettings(content_md5=md5)
    return None


def _co_content_settings_for_upload_unless_file_present_with_matching_checksum(
    data: AnyStrSrc,
) -> ty.Generator[bool, FileProperties, ty.Optional[ContentSettings]]:
    local_content_settings = _get_checksum_content_settings(data)
    if not local_content_settings:
        return ContentSettings()  # need to upload but we have no md5 info
    try:
        if len(data) < _SKIP_ALREADY_UPLOADED_CHECK_IF_MORE_THAN_BYTES:  # type: ignore
            logger.debug("Too small to bother with an early call - let's just upload...")
            return local_content_settings
    except TypeError as te:
        logger.debug(f"failed to get length? {repr(te)} for {data}")
    props = yield True
    if not props:
        logger.debug("No remote file properties could be fetched so an upload is required")
        return local_content_settings
    if props.content_settings.content_md5 == local_content_settings.content_md5:
        logger.info(f"Remote file {props.name} already exists and has matching checksum")
        return None
    logger.debug("Remote file exists but MD5 does not match - upload required.")
    return local_content_settings


doc = """
Returns ContentSettings if the upload is necessary; otherwise
returns None which means you can forgo the upload since it's
already previously occurred.
"""


async def async_content_settings_if_upload_required(
    fc: DataLakeFileClient, data: AnyStrSrc
) -> ty.Optional[ContentSettings]:
    try:
        co = _co_content_settings_for_upload_unless_file_present_with_matching_checksum(data)
        while True:
            co.send(None)
            try:
                co.send(await fc.get_file_properties())
            except azure.core.exceptions.ResourceNotFoundError:
                co.send(None)
    except StopIteration as stop:
        return stop.value


def content_settings_if_upload_required(
    fc: DataLakeFileClient, data: AnyStrSrc
) -> ty.Optional[ContentSettings]:
    try:
        co = _co_content_settings_for_upload_unless_file_present_with_matching_checksum(data)
        while True:
            co.send(None)
            try:
                co.send(fc.get_file_properties())
            except azure.core.exceptions.ResourceNotFoundError:
                co.send(None)
    except StopIteration as stop:
        return stop.value


async_content_settings_if_upload_required.__doc__ = doc
content_settings_if_upload_required.__doc__ = doc
