"""This entire module is/could be a generic stack-configuration layer
built over top of static application config that allows it to be
selectively overridden on a per-stack/thread basis. It would get moved
to `thds.core` without the `tomli` dependency or the `mops`-specific
config.

I just need to finish abstracting it and give it a nicer API.

"""
import os
import typing as ty
from datetime import timedelta
from getpass import getuser
from importlib.resources import read_text
from pathlib import Path

import tomli

from thds.core.stack_context import StackContext

_WDIR_TOML = Path(".mops.toml").resolve()
_TOML_TOP_LEVEL_TABLE = "mops."


def _reload_current_config():
    global _CONFIG
    env_toml_config_path = Path(os.environ.get("MOPS_CONFIG", f"{Path.home()}/.mops.toml"))
    if _WDIR_TOML.exists():
        return tomli.load(open(_WDIR_TOML, "rb"))
    elif env_toml_config_path.exists():
        return tomli.load(open(env_toml_config_path, "rb"))
    return tomli.loads(read_text("thds.mops", "east_config.toml"))


_CONFIG = _reload_current_config()


def _get_config():
    return _CONFIG


def set_config(dict_like_config):
    """Call this to replace the default config with a pre-parsed
    dict-like such as a Dynaconf settings object that your application
    already manages. It will only override the specific keys that you
    provide.
    """
    global _CONFIG
    _CONFIG = {**_CONFIG, **dict_like_config}


def _nested_lazy_get(dotted_path: str, default=None):
    convert_val = type(default) if default is not None else lambda x: x
    assert callable(convert_val)
    convert_val(default)

    def _nested_get_(d: ty.Union[ty.Callable[[], dict], dict]):
        assert callable(convert_val)
        dd = d() if callable(d) else d
        for path_part in dotted_path.split("."):
            if path_part in dd:
                dd = dd[path_part]
            else:
                if callable(default):
                    return convert_val(default())
                return convert_val(default)
        return dd

    return _nested_get_


T = ty.TypeVar("T")


def _make_stack_config(dotted_path: str, default: T) -> StackContext[T]:
    return StackContext(
        dotted_path,
        _nested_lazy_get(_TOML_TOP_LEVEL_TABLE + dotted_path, default=default)(_get_config),
    )


# k8s namespace will default to your OS username
try:
    _K8S_NAMESPACE = (os.getenv("MOPS_K8S_NAMESPACE") or getuser()).replace(".", "-")
except OSError:
    _K8S_NAMESPACE = "CICD-Runner"


# Kubernetes config stuff
k8s_cluster_name = _make_stack_config("k8s.cluster.name", "")
k8s_cluster_resource_group = _make_stack_config("k8s.cluster.resource_group", "")
k8s_cluster_url = _make_stack_config("k8s.cluster.url", "")
k8s_cluster_api_version = _make_stack_config("k8s.cluster.api_version", "")

k8s_namespace = _make_stack_config("k8s.namespace", _K8S_NAMESPACE)

k8s_namespace_env_var_key = _make_stack_config("k8s.namespace_env_var_key", "K8S_NAMESPACE")
# for embedding the namespace in an env var in the pod/container
k8s_job_retry_count = _make_stack_config("k8s.job.retry_count", 6)
k8s_job_cleanup_ttl_seconds_after_completion = _make_stack_config(
    "k8s.job.cleanup_ttl_seconds", int(timedelta(minutes=60).total_seconds())
)
k8s_job_timeout_seconds = _make_stack_config(
    "k8s.job.timeout_seconds", int(timedelta(minutes=3).total_seconds())
)
k8s_monitor_delay = _make_stack_config("k8s.monitor.delay_seconds", 5)
k8s_monitor_max_attempts = _make_stack_config("k8s.monitor.max_attempts", 100)

# In the East, we use the newer pod managed identity by default,
# which provides access to a metadata endpoint that Azure clients know
# how to access automatically.
# https://docs.microsoft.com/en-us/azure/aks/use-azure-ad-pod-identity
aad_pod_managed_identity = _make_stack_config("k8s.azure.aad_pod_managed_identity", "")

# but there's an even newer, better type of auth called Workload
# Identity, which unfortunately requires specific infrastructure
# configuration that lives outside this library.
# https://azure.github.io/azure-workload-identity/docs/introduction.html
namespaces_supporting_workload_identity = _make_stack_config(
    "k8s.azure.namespaces_supporting_workload_identity", ["default"]
)

adls_remote_tmp_sa = _make_stack_config("adls.remote.tmp_sa", "")
adls_remote_tmp_container = _make_stack_config("adls.remote.tmp_container", "")

adls_max_clients = _make_stack_config("adls.max_clients", 8)
# 8 clients has been obtained experimentally via the `stress_test`
# application running on a Mac M1 laptop running 200 parallel 5 second
# tasks, though no significant difference was obtained between 5 and
# 20 clients. Running a similar stress test from your orchestrator may
# be a good idea if you are dealing with hundreds of micro (<20
# second) remote tasks.

adls_skip_already_uploaded_check_if_smaller_than_bytes = _make_stack_config(
    "adls.skip_already_uploaded_check_if_smaller_than_bytes", 2 * 2**20
)  # 2 MB is about right for how slow ADLS is to respond to individual requests.
adls_remote_datasets_sa = _make_stack_config("adls.remote.datasets_sa", "")
adls_remote_datasets_container = _make_stack_config("adls.remote.datasets_container", "")

# Container registry stuff
acr_url = _make_stack_config("acr.url", "")

open_files_limit = _make_stack_config("resources.max_open_files", 10000)
