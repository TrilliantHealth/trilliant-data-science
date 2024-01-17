from datetime import timedelta
from getpass import getuser
from pathlib import Path

import tomli

from thds.core import config


def namespace(input_str: str) -> str:
    # lowercase and replace all non-alphanumeric characters with dashes
    return "".join(c if c.isalnum() else "-" for c in input_str.lower())


def _user_namespace() -> str:
    try:
        return getuser()
    except OSError:
        return "CICD-Runner"


k8s_namespace = config.item("mops.k8s.namespace", _user_namespace(), parse=namespace)
k8s_namespace_env_var_key = config.item("mops.k8s.namespace_env_var_key", "K8S_NAMESPACE")
# the above is used to embed the current namespace _inside_ the container as an
# environment variable.  it will not affect how your namespace is selected in the first
# place.

k8s_acr_url = config.item("mops.k8s.acr_url", "")
k8s_job_retry_count = config.item("mops.k8s.job.retry_count", 6, parse=int)
k8s_job_cleanup_ttl_seconds_after_completion = config.item(
    "mops.k8s.job.cleanup_ttl_seconds", int(timedelta(minutes=60).total_seconds()), parse=int
)
k8s_job_timeout_seconds = config.item(
    "mops.k8s.job.timeout_seconds", int(timedelta(minutes=3).total_seconds()), parse=int
)
k8s_monitor_delay = config.item("mops.k8s.monitor.delay_seconds", 5, parse=int)
k8s_monitor_max_attempts = config.item("mops.k8s.monitor.max_attempts", 100, parse=int)

# In the East, we use the newer pod managed identity by default,
# which provides access to a metadata endpoint that Azure clients know
# how to access automatically.
# https://docs.microsoft.com/en-us/azure/aks/use-azure-ad-pod-identity
aad_pod_managed_identity = config.item("mops.k8s.aad_pod_managed_identity", "")

# but there's an even newer, better type of auth called Workload
# Identity, which unfortunately requires specific infrastructure
# configuration that lives outside this library.
# https://azure.github.io/azure-workload-identity/docs/introduction.html
namespaces_supporting_workload_identity = config.item(
    "mops.k8s.azure.namespaces_supporting_workload_identity", ["default"]
)


# load our 'standard' config - someday maybe this moves into a dedicated module
config.set_global_defaults(tomli.load(open(Path(__file__).parent / "east_config.toml", "rb")))
