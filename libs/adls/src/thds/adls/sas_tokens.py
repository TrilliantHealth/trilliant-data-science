import datetime
import typing as ty

from azure.storage.blob import (
    BlobSasPermissions,
    BlobServiceClient,
    UserDelegationKey,
    generate_blob_sas,
)

from .conf import BLOB_SAS_EXPIRY, USER_DELEGATION_KEY_EXPIRY
from .fqn import AdlsFqn


def get_user_delegation_key(
    blob_service_client: BlobServiceClient,
    expiry: ty.Union[int, ty.Callable[[], int]] = USER_DELEGATION_KEY_EXPIRY,
) -> UserDelegationKey:
    """For using as an account key to generate SAS tokens.

    This key must remain valid for the SAS tokens it signs to remain valid.

    `expiry` is in seconds.
    """
    now = datetime.datetime.now(datetime.timezone.utc)

    return blob_service_client.get_user_delegation_key(
        key_start_time=now,
        key_expiry_time=now + datetime.timedelta(seconds=expiry() if callable(expiry) else expiry),
    )


def gen_blob_sas_token(
    fqn: AdlsFqn,
    account_key: ty.Union[str, UserDelegationKey],
    permissions: BlobSasPermissions,
    expiry: ty.Union[int, ty.Callable[[], int]] = BLOB_SAS_EXPIRY,
) -> str:
    """Generates a SAS token for the blob present at the `fqn`.

    If `account_key` is a `UserDelegationKey`, the expiry will be set to the `UserDelegationKey`'s expiry,
    as when that key expires, the SAS tokens it signs will also expire.

    `expiry` is in seconds.
    """
    expiry_datetime: ty.Union[ty.Optional[str], datetime.datetime]

    if isinstance(account_key, UserDelegationKey):
        expiry_datetime = account_key.signed_expiry
    else:
        expiry_datetime = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            seconds=expiry() if callable(expiry) else expiry
        )

    return generate_blob_sas(
        fqn.sa,
        fqn.container,
        fqn.path,
        account_key=account_key,  # type: ignore[arg-type]
        # the Azure SDK has too restrictive a type here
        permission=permissions,
        expiry=expiry_datetime,
    )
