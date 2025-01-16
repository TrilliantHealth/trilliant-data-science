"""Functions for making copying blobs across remote locations."""
import datetime
import random
import time
import typing as ty

from azure.storage.blob import BlobSasPermissions, generate_blob_sas

from thds.core.log import getLogger

from .file_properties import exists, get_blob_properties, get_file_properties, is_directory
from .fqn import AdlsFqn
from .global_client import get_global_blob_container_client, get_global_blob_service_client
from .uri import UriIsh, parse_any

logger = getLogger(__name__)

_AUTH_EXPIRY_MINS = 15
# seems like copying a single file shouldn't take longer than 15 minutes
# not sure if the copy would fail if auth expires during the copy either


OverwriteMethod = ty.Literal["error", "warn", "skip"]


def _copy_file(
    src: AdlsFqn,
    dest: AdlsFqn,
    *,
    overwrite_method: OverwriteMethod = "error",
) -> AdlsFqn:
    if is_directory(get_file_properties(src)):
        raise ValueError(f"{src!r} is a directory.")

    if exists(dest):
        if overwrite_method == "error":
            raise ValueError(f"'{dest}' already exists!")
        elif overwrite_method == "warn":
            logger.warning("'%s' already exists and will be overwritten with '%s'", dest, src)
        else:
            logger.warning("'%s' already exists, no copy from '%s' will occur", dest, src)
            return dest

    src_bsc = get_global_blob_service_client(src.sa)
    now = datetime.datetime.now(datetime.timezone.utc)

    sas_token = generate_blob_sas(
        src.sa,
        src.container,
        src.path,
        account_key=src_bsc.get_user_delegation_key(  # type: ignore[arg-type]
            key_start_time=now, key_expiry_time=now + datetime.timedelta(minutes=_AUTH_EXPIRY_MINS)
        ),  # does this key need to live for the life of the token(s) it signs?
        # on the other hand could also try to cache this key to prevent too frequent requests for a key
        permission=BlobSasPermissions(read=True),
        expiry=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(minutes=_AUTH_EXPIRY_MINS),
    )
    # TODO - make this a generic function

    src_blob_client = src_bsc.get_blob_client(src.container, src.path)
    dest_blob_client = get_global_blob_container_client(dest.sa, dest.container).get_blob_client(
        dest.path
    )

    logger.info("Copying '%s' to '%s'...", src, dest)
    dest_blob_client.start_copy_from_url(f"{src_blob_client.url}?{sas_token}")

    return dest


def wait_for_copy(fqn: AdlsFqn, timeout: int = 300) -> AdlsFqn:
    start_time = time.time()

    while time.time() - start_time < timeout:
        copy_props = get_blob_properties(fqn).copy
        if copy_props.status == "success":
            # blob properties actually shows copy progress so we could add a progress bar
            return fqn
        elif copy_props.status is None:
            raise ValueError(f"A copy to '{fqn}', is not currently in progress.")
        elif copy_props.status != "pending":
            raise ValueError(
                f"The copy to '{fqn}' {copy_props.status}. See copy properties: {copy_props}"
            )

        time.sleep(random.randint(1, 5))

    raise TimeoutError(f"Copying to '{fqn}' did not finish within {timeout} seconds.")


def copy_file(
    src: UriIsh,
    dest: UriIsh,
    *,
    overwrite_method: OverwriteMethod = "error",
    wait: bool = True,
    timeout: int = 300,
) -> AdlsFqn:
    src_fqn = parse_any(src)
    dest_fqn = parse_any(dest)

    if wait:
        copy = wait_for_copy(
            _copy_file(src_fqn, dest_fqn, overwrite_method=overwrite_method), timeout=timeout
        )
        logger.info("Finished copying '%s' to '%s'", src_fqn, dest_fqn)
        return copy

    return _copy_file(src_fqn, dest_fqn, overwrite_method=overwrite_method)
