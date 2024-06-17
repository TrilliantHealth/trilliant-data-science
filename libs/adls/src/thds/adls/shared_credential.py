import logging
import os
import threading
from typing import Dict

from azure.identity import DefaultAzureCredential
from azure.identity._constants import EnvironmentVariables

from thds.core.lazy import Lazy

# suppress noisy Azure identity logs
logging.getLogger("azure.identity").setLevel(logging.WARNING)


class ThreadSafeAzureCredential(DefaultAzureCredential):
    """This is a workaround for how terrible the Azure SDK is.

    They do _not_ play nicely with many threads all trying to load the
    credentials at once. Thankfully we can just shim in there and make
    sure that we're not making thousands of parallel requests to the
    refresh token API...
    """

    def __init__(self, *args, **kwargs):
        self.lock = threading.Lock()
        super().__init__(*args, **kwargs)

    def get_token(self, *args, **kwargs):
        with self.lock:
            return super().get_token(*args, **kwargs)

    def close(self):
        with self.lock:
            return super().close()


def _has_workload_identity_creds() -> bool:
    workload_identity_vars = [
        EnvironmentVariables.AZURE_TENANT_ID,
        EnvironmentVariables.AZURE_CLIENT_ID,
        EnvironmentVariables.AZURE_FEDERATED_TOKEN_FILE,
    ]
    return all(var in os.environ for var in workload_identity_vars)


def get_credential_kwargs() -> Dict[str, bool]:
    if _has_workload_identity_creds():
        # in K8s, we use various forms of credentials, but not the EnvironmentCredential,
        # and that one gets tried early and then warns us about something we don't care about.
        return dict(exclude_environment_credential=True)
    # exclusion due to storage explorer credentials issue when trying to hit East SAs
    return dict(exclude_shared_token_cache_credential=True)


def _SharedCredential() -> DefaultAzureCredential:
    return ThreadSafeAzureCredential(**get_credential_kwargs())


SharedCredential = Lazy(_SharedCredential)
