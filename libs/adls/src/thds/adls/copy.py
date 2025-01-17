"""Functions for copying blobs across remote locations."""
import datetime
import random
import time
import typing as ty

from azure.storage.blob import BlobSasPermissions, BlobServiceClient, UserDelegationKey

from thds.core import cache, log, parallel, thunks

from .conf import BLOB_SAS_EXPIRY
from .file_properties import exists, get_blob_properties, get_file_properties, is_directory
from .fqn import AdlsFqn
from .global_client import get_global_blob_container_client, get_global_blob_service_client
from .sas_tokens import gen_blob_sas_token, get_user_delegation_key
from .uri import UriIsh, parse_any

logger = log.getLogger(__name__)

_COPY_TIMEOUT = BLOB_SAS_EXPIRY()  # seconds


OverwriteMethod = ty.Literal["error", "warn", "skip", "silent"]

CopyRequest = ty.Dict[str, ty.Union[str, datetime.datetime]]


def _copy_file(
    src: AdlsFqn,
    dest: AdlsFqn,
    account_key: ty.Union[str, UserDelegationKey],
    overwrite_method: OverwriteMethod = "error",
) -> CopyRequest:
    if is_directory(get_file_properties(src)):
        raise ValueError(f"{src} is a directory.")

    if exists(dest):
        if overwrite_method == "error":
            raise ValueError(f"{dest} already exists!")
        elif overwrite_method == "warn":
            logger.warning("%s already exists and will be overwritten with %s", dest, src)
        elif overwrite_method == "skip":
            logger.warning("%s already exists, no copy from %s will occur", dest, src)
            return dict()
        else:
            return dict()

    src_blob_client = get_global_blob_container_client(src.sa, src.container).get_blob_client(src.path)
    dest_blob_client = get_global_blob_container_client(dest.sa, dest.container).get_blob_client(
        dest.path
    )

    sas_token = gen_blob_sas_token(
        src, account_key=account_key, permissions=BlobSasPermissions(read=True)
    )
    # we may not always need a SAS token for the copy, but it would be hard to reason about when they are(n't)

    logger.info("Copying %s to %s...", src, dest)
    return dest_blob_client.start_copy_from_url(f"{src_blob_client.url}?{sas_token}")


def wait_for_copy(fqn: AdlsFqn, timeout: int = 300) -> AdlsFqn:
    """Set timeout to 0 or a negative int to wait indefinitely."""
    start_time = time.time()

    while (time.time() - start_time < timeout) if timeout > 0 else True:
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
    timeout: int = _COPY_TIMEOUT,
    get_account_key: ty.Callable[
        [BlobServiceClient], ty.Union[str, UserDelegationKey]
    ] = get_user_delegation_key,
) -> CopyRequest:
    src_fqn = parse_any(src)
    dest_fqn = parse_any(dest)

    func = thunks.thunking(_copy_file)(
        src_fqn,
        dest_fqn,
        account_key=get_account_key(get_global_blob_service_client(src_fqn.sa)),
        overwrite_method=overwrite_method,
    )

    if wait:
        copy_props = func()
        wait_for_copy(dest_fqn, timeout=timeout)
        logger.info("Finished copying %s to %s", src_fqn, dest_fqn)
        return copy_props

    return func()


def copy_files(
    srcs_to_dests: ty.Mapping[UriIsh, UriIsh],
    overwrite_method: OverwriteMethod = "error",
    wait: bool = True,
    timeout: int = _COPY_TIMEOUT,
    get_account_key: ty.Callable[
        [BlobServiceClient], ty.Union[str, UserDelegationKey]
    ] = get_user_delegation_key,
) -> ty.Dict[AdlsFqn, CopyRequest]:
    src_fqns_to_dest_fqns = {parse_any(k): parse_any(v) for k, v in srcs_to_dests.items()}
    get_account_key = cache.locking(get_account_key)

    def copy_wrapper(src: AdlsFqn, dest: AdlsFqn) -> ty.Tuple[AdlsFqn, CopyRequest]:
        return dest, copy_file(
            src,
            dest,
            overwrite_method=overwrite_method,
            wait=wait,
            timeout=timeout,
            get_account_key=get_account_key,
        )

    # would be cool to do this async, but using threads for quicker dev
    return dict(
        parallel.yield_results(
            [thunks.thunking(copy_wrapper)(src, dest) for src, dest in src_fqns_to_dest_fqns.items()]
        )
    )
