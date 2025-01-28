"""Functions for copying blobs across remote locations."""
import datetime
import random
import time
import typing as ty

from azure.storage.blob import BlobSasPermissions, BlobServiceClient, UserDelegationKey

from thds.core import cache, log, parallel, thunks

from .file_properties import exists, get_blob_properties, get_file_properties, is_directory
from .fqn import AdlsFqn
from .global_client import get_global_blob_container_client, get_global_blob_service_client
from .sas_tokens import gen_blob_sas_token, get_user_delegation_key
from .uri import UriIsh, parse_any

logger = log.getLogger(__name__)

_WAIT_TIMEOUT = 300  # seconds
# purposely set shorter than SAS token expiry as this is more of a reasonable amount of time for a human to wait
# rather than a reasonable amount of time for a file to finish copying.
# the functions using this constant use it as a default argument, so it can be changed as needed.


OverwriteMethod = ty.Literal["error", "warn", "skip", "silent"]

CopyRequest = ty.Dict[str, ty.Union[str, datetime.datetime]]


class CopyInfo(ty.NamedTuple):
    src: AdlsFqn
    dest: AdlsFqn
    request: CopyRequest

    @property
    def copy_occurred(self) -> bool:
        return bool(self.request)


def _copy_file(
    src: AdlsFqn,
    dest: AdlsFqn,
    account_key: ty.Union[str, UserDelegationKey],
    overwrite_method: OverwriteMethod = "error",
) -> CopyInfo:
    if not exists(src):
        raise ValueError(f"{src} does not exist!")

    if is_directory(get_file_properties(src)):
        raise ValueError(f"{src} is a directory!")

    src_blob_client = get_global_blob_container_client(src.sa, src.container).get_blob_client(src.path)
    dest_blob_client = get_global_blob_container_client(dest.sa, dest.container).get_blob_client(
        dest.path
    )

    def md5s_exist_and_are_equal() -> bool:
        if (
            src_md5 := src_blob_client.get_blob_properties().content_settings.content_md5
        ) and src_md5 == dest_blob_client.get_blob_properties().content_settings.content_md5:
            return True
        return False

    if dest_blob_client.exists():
        if md5s_exist_and_are_equal():
            # no point in copying if the files are the same
            logger.info(
                "%s already exists with the same md5 as the file at %s, no copy will occur", dest, src
            )
            return CopyInfo(src, dest, dict())

        logger.info(
            "%s already exists but it and/or the file at %s have unknown md5s, or the md5s differ. "
            "The overwrite method provided will determine the copy behavior.",
            dest,
            src,
        )

        if overwrite_method == "error":
            raise ValueError(f"{dest} already exists!")
        elif overwrite_method == "warn":
            logger.warning("%s will be overwritten with the file from %s", dest, src)
        elif overwrite_method == "skip":
            logger.warning("%s already exists, skipping copy from %s", dest, src)
            return CopyInfo(src, dest, dict())

    sas_token = gen_blob_sas_token(
        src, account_key=account_key, permissions=BlobSasPermissions(read=True)
    )
    # we may not always need a SAS token for the copy, but it would be hard to reason about when they are(n't)

    logger.info("Copying %s to %s...", src, dest)
    return CopyInfo(
        src, dest, dest_blob_client.start_copy_from_url(f"{src_blob_client.url}?{sas_token}")
    )


def wait_for_copy(fqn: AdlsFqn, timeout: int = _WAIT_TIMEOUT) -> AdlsFqn:
    """Set timeout to 0 or a negative int to wait indefinitely.

    Keep in mind that, if the copy is authorized using a SAS token, that token expiry exists separate from the wait
    timeout.
    """
    start_time = time.monotonic()

    while (time.monotonic() - start_time < timeout) if timeout > 0 else True:
        blob_copy_props = get_blob_properties(fqn).copy
        # blob copy properties actually show copy progress so we could add a progress bar

        if blob_copy_props.status == "success":
            return fqn
        elif blob_copy_props.status is None:
            raise ValueError(f"{fqn} is not an a pending or completed copy.")
        elif blob_copy_props.status != "pending":
            raise ValueError(
                f"The copy to {fqn} failed with the status: {blob_copy_props.status}. "
                f"See blob copy properties: {blob_copy_props}"
            )

        time.sleep(random.uniform(1, 3))

    raise TimeoutError(
        f"Copying to {fqn} did not finish within {timeout} seconds. It may still be copying."
    )


def copy_file(
    src: UriIsh,
    dest: UriIsh,
    overwrite_method: OverwriteMethod = "error",
    wait: bool = True,
    timeout: int = _WAIT_TIMEOUT,
    get_account_key: ty.Callable[
        [BlobServiceClient], ty.Union[str, UserDelegationKey]
    ] = get_user_delegation_key,
) -> CopyInfo:
    src_fqn = parse_any(src)
    dest_fqn = parse_any(dest)

    func = thunks.thunking(_copy_file)(
        src_fqn,
        dest_fqn,
        account_key=get_account_key(get_global_blob_service_client(src_fqn.sa)),
        overwrite_method=overwrite_method,
    )

    if wait:
        copy_info = func()
        wait_for_copy(dest_fqn, timeout=timeout)
        logger.info("Finished copying %s to %s", src_fqn, dest_fqn)
        return copy_info

    return func()


def copy_files(
    src_dest_pairs: ty.Collection[ty.Tuple[UriIsh, UriIsh]],
    overwrite_method: OverwriteMethod = "error",
    wait: bool = True,
    timeout: int = _WAIT_TIMEOUT,
    get_account_key: ty.Callable[
        [BlobServiceClient], ty.Union[str, UserDelegationKey]
    ] = get_user_delegation_key,
) -> ty.List[CopyInfo]:
    src_dest_fqn_pairs = [(parse_any(src), parse_any(dest)) for src, dest in src_dest_pairs]
    get_account_key = cache.locking(get_account_key)

    def copy_wrapper(src: AdlsFqn, dest: AdlsFqn) -> CopyInfo:
        return copy_file(
            src,
            dest,
            overwrite_method=overwrite_method,
            wait=wait,
            timeout=timeout,
            get_account_key=get_account_key,
        )

    # would be cool to do this async, but using threads for quicker dev
    return list(
        parallel.yield_results(
            [thunks.thunking(copy_wrapper)(src, dest) for src, dest in src_dest_fqn_pairs]
        )
    )
