import json
import logging
import os
import platform
import random
import threading
import time
from pathlib import Path
from typing import Dict

from azure.core.credentials import AccessToken, TokenCredential
from azure.identity import AzureCliCredential, DefaultAzureCredential
from azure.identity._constants import EnvironmentVariables

from thds.core import config, files, log
from thds.core.lazy import Lazy

# suppress noisy Azure identity logs
logging.getLogger("azure.identity").setLevel(logging.WARNING)
logger = log.getLogger(__name__)
DISABLE_FAST_CACHED_CREDENTIAL = config.item("disable_fast", default=False, parse=config.tobool)


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


_CACHED_TOKEN_PATH = "~/.azure/thds-cache-azure-cli-tokens"


class FastCachedAzureCliCredential(AzureCliCredential):
    # AzureCliCredential only allows a single scope per token, so even though
    # the abstract concept might name *args as *scopes, for our implementation
    # it would be an error to try to fetch more than one scope at a time anyway.
    def get_token(self, scope: str, *args, **kwargs):
        cached_path = Path(os.path.expanduser(_CACHED_TOKEN_PATH)) / scope.replace("/", "|")
        # the | (pipe) is just to make the local file paths not-weird.
        # i chose it essentially at random, and while technically this is a form of compression,
        # I very much doubt any collisions are possible in the real set of scopes Azure offers.
        try:
            token_dict = json.loads(open(cached_path).read())
            expires_on = token_dict["expires_on"]
            if expires_on - random.randint(50, 150) < time.time():
                # we conservatively grab a new token even if we're within 50 to 150 seconds
                # _before_ the current token expires.
                # The randomness helps avoid a whole bunch of processes all grabbing it at once,
                # assuming there's a _lot_ of Azure activity going on in parallel.
                # There is no correctness concern with this; just optimizations.
                raise ValueError("Expired Token")
            return AccessToken(**token_dict)
        except (FileNotFoundError, TypeError, KeyError, ValueError):
            fresh_token = super().get_token(scope, *args, **kwargs)
            # we write atomically b/c we know other processes may be reading or writing
            # this file.  it's really just a best practice for almost all file writes
            # where there's any chance of other readers or writers.
            with files.atomic_write_path(cached_path) as wp, open(wp, "w") as wf:
                wf.write(json.dumps(fresh_token._asdict()))
            return fresh_token
        except Exception as e:
            logger.exception(f"failed to get fast credential: {e}")
            raise


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


def _SharedCredential() -> TokenCredential:
    if platform.system() == "Darwin" and not DISABLE_FAST_CACHED_CREDENTIAL():
        # only try this crazy optimization on our local laptops
        return FastCachedAzureCliCredential()  # type: ignore
    return ThreadSafeAzureCredential(**get_credential_kwargs())


SharedCredential = Lazy(_SharedCredential)
