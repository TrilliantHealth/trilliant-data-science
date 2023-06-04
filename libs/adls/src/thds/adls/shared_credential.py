import logging
import os
import threading

from azure.identity import DefaultAzureCredential

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


def _SharedCredential() -> DefaultAzureCredential:
    if "KUBERNETES_SERVICE_HOST" in os.environ:
        # in K8s, we use various forms of credentials, but not the EnvironmentCredential,
        # and that one gets tried early and then warns us about something we don't care about.
        return ThreadSafeAzureCredential(exclude_environment_credential=True)

    # exclusion due to storage explorer credentials issue when trying to hit East SAs
    return ThreadSafeAzureCredential(exclude_shared_token_cache_credential=True)


SharedCredential = Lazy(_SharedCredential)
