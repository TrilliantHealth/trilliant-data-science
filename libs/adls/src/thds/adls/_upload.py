"""Just utilities for deciding whether or not to upload.

Not an officially-published API of the thds.adls library.
"""

import typing as ty
from pathlib import Path

import azure.core.exceptions

from thds.core import hash_cache, hashing, hostname, log

from . import hashes
from .file_properties import PropertiesP

_SKIP_ALREADY_UPLOADED_CHECK_IF_MORE_THAN_BYTES = 2 * 2**20  # 2 MB is about right


logger = log.getLogger(__name__)


def _try_default_hash(data: hashes.AnyStrSrc) -> ty.Optional[hashing.Hash]:
    """Ideally, we calculate a hash/checksum for all data that we upload.

    The only circumstances under which we cannot do this are if the
    stream does not exist in its entirety before the upload begins.
    """
    hasher = hashes.default_hasher()
    hbytes = None
    if isinstance(data, Path):
        hbytes = hash_cache.hash_file(data, hasher)
    elif hashing.hash_anything(data, hasher):
        hbytes = hasher.digest()

    if hbytes:
        return hashing.Hash(hasher.name.lower(), hbytes)

    return None


def _too_small_to_skip_upload(data: hashes.AnyStrSrc, min_size_for_remote_check: int) -> bool:
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
    metadata: ty.Dict[str, str]


def metadata_for_upload() -> ty.Dict[str, str]:
    return {"upload_wrapper_sw": "thds.adls", "upload_hostname": hostname.friendly()}


def _co_upload_decision_unless_file_present_with_matching_checksum(
    data: hashes.AnyStrSrc, min_size_for_remote_check: int
) -> ty.Generator[bool, ty.Optional[PropertiesP], UploadDecision]:
    local_hash = _try_default_hash(data)
    if not local_hash:
        return UploadDecision(True, metadata_for_upload())

    hash_meta = hashes.metadata_hash_dict(local_hash)
    metadata = dict(metadata_for_upload(), **hash_meta)
    if _too_small_to_skip_upload(data, min_size_for_remote_check):
        logger.debug("Too small to bother with an early call - let's just upload...")
        return UploadDecision(True, metadata)

    remote_properties = yield True
    if not remote_properties:
        logger.debug("No remote properties could be fetched so an upload is required")
        return UploadDecision(True, metadata)

    remote_hashes = hashes.extract_hashes_from_props(remote_properties)
    for algo in remote_hashes:
        mkey = hashes.metadata_hash_b64_key(algo)
        if mkey in hash_meta and hashing.b64(remote_hashes[algo].bytes) == hash_meta[mkey]:
            logger.info(f"Remote file {remote_properties.name} already exists and has matching checksum")
            return UploadDecision(False, metadata)

    print(remote_hashes, hash_meta)
    logger.debug("Remote file exists but hash does not match - upload required.")
    return UploadDecision(True, metadata)


doc = """
Returns False for upload_required if the file is large and the remote
exists and has a known, matching checksum.

Returns a metadata dict that should be added to any upload.
"""


async def async_upload_decision_and_metadata(
    get_properties: ty.Callable[[], ty.Awaitable[PropertiesP]],
    data: hashes.AnyStrSrc,
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
    get_properties: ty.Callable[[], PropertiesP],
    data: hashes.AnyStrSrc,
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
