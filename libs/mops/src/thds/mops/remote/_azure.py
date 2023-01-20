import logging
import threading

import lazy_object_proxy
from azure.identity import DefaultAzureCredential

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
    return ThreadSafeAzureCredential(exclude_shared_token_cache_credential=True)


SharedCredential = lazy_object_proxy.Proxy(_SharedCredential)
