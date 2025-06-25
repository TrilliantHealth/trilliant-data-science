"""Just utilities for deciding whether or not to upload.

Not an officially-published API of the thds.adls library.
"""

import typing as ty
from pathlib import Path

import azure.core.exceptions

from thds.core import hashing, hostname, log

from .blake_hash import AnyStrSrc, try_blake3

_SKIP_ALREADY_UPLOADED_CHECK_IF_MORE_THAN_BYTES = 2 * 2**20  # 2 MB is about right


logger = log.getLogger(__name__)

_BLAKE_HASH_METADATA_KEY = "hash_blake3_b64"


class _BlakeHashMetadata(ty.TypedDict):
    hash_blake3_b64: str


def _get_checksum_content_settings(data: AnyStrSrc) -> ty.Optional[_BlakeHashMetadata]:
    """Ideally, we calculate a blake3 sum for all data that we upload.

    The only circumstances under which we cannot do this are if the
    stream does not exist in its entirety before the upload begins.
    """
    blake3_hash = try_blake3(data)
    if blake3_hash:
        return _BlakeHashMetadata(hash_blake3_b64=hashing.b64(blake3_hash))
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
    upload_metadata: ty.Dict[str, str]


class Properties(ty.Protocol):
    name: str
    metadata: ty.Dict[str, str]


def metadata_for_upload() -> ty.Dict[str, str]:
    return {"upload_wrapper_sw": "thds.adls", "upload_hostname": hostname.friendly()}


def _co_upload_decision_unless_file_present_with_matching_checksum(
    data: AnyStrSrc, min_size_for_remote_check: int
) -> ty.Generator[bool, ty.Optional[Properties], UploadDecision]:
    local_hash_metadata = _get_checksum_content_settings(data)
    if not local_hash_metadata:
        return UploadDecision(True, metadata_for_upload())

    metadata = dict(metadata_for_upload(), **local_hash_metadata)
    if _too_small_to_skip_upload(data, min_size_for_remote_check):
        logger.debug("Too small to bother with an early call - let's just upload...")
        return UploadDecision(True, metadata)

    remote_properties = yield True
    if not remote_properties:
        logger.debug("No remote properties could be fetched so an upload is required")
        return UploadDecision(True, metadata)

    if (
        remote_properties.metadata.get(_BLAKE_HASH_METADATA_KEY)
        == local_hash_metadata["hash_blake3_b64"]
    ):
        logger.info(f"Remote file {remote_properties.name} already exists and has matching checksum")
        return UploadDecision(False, metadata)

    logger.debug("Remote file exists but blake3 does not match - upload required.")
    return UploadDecision(True, metadata)


doc = """
Returns False for upload_required if the file is large and the remote
exists and has a known, matching checksum.

Returns a metadata dict that should be added to any upload.
"""


async def async_upload_decision_and_metadata(
    get_properties: ty.Callable[[], ty.Awaitable[Properties]],
    data: AnyStrSrc,
    min_size_for_remote_check: int = _SKIP_ALREADY_UPLOADED_CHECK_IF_MORE_THAN_BYTES,
) -> UploadDecision:
    try:
        co = _co_upload_decision_unless_file_present_with_matching_checksum(
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


def upload_decision_and_metadata(
    get_properties: ty.Callable[[], Properties],
    data: AnyStrSrc,
    min_size_for_remote_check: int = _SKIP_ALREADY_UPLOADED_CHECK_IF_MORE_THAN_BYTES,
) -> UploadDecision:
    try:
        co = _co_upload_decision_unless_file_present_with_matching_checksum(
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


async_upload_decision_and_metadata.__doc__ = doc
upload_decision_and_metadata.__doc__ = doc
