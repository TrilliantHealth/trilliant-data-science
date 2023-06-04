"""This entire module is/could be a generic stack-configuration layer
built over top of static application config that allows it to be
selectively overridden on a per-stack/thread basis. It would get moved
to `thds.core` without the `tomli` dependency or the `mops`-specific
config.

I just need to finish abstracting it and give it a nicer API.

"""
import contextlib
import copy
import os
import typing as ty
from datetime import timedelta
from getpass import getuser
from importlib.resources import read_text
from pathlib import Path

import tomli

from thds.adls import AdlsFqn
from thds.core.stack_context import StackContext

_WDIR_TOML = Path(".mops.toml").resolve()
_ROOT_CFG_KEY = "mops"


def _load_config() -> ty.Dict[str, ty.Any]:
    paths = [
        Path(os.environ.get("MOPS_CONFIG", "")),
        _WDIR_TOML,
        Path(f"{Path.home()}/.mops.toml"),
    ]
    for path in paths:
        if path and path.exists() and path.is_file():
            return tomli.load(open(path, "rb"))
    return tomli.loads(read_text("thds.mops", "east_config.toml"))


_CONFIG = StackContext("MOPS_CONFIG", _load_config())


def _set_val_at_path(d: dict, value: ty.Any, *path: str):
    if path[0] != _ROOT_CFG_KEY:
        path = (_ROOT_CFG_KEY, *path)

    loc = d
    for path_part in path[:-1]:
        if path_part not in loc:
            loc[path_part] = dict()
        loc = loc[path_part]
    loc[path[-1]] = value


def set_global_config(*path: str) -> ty.Callable[[ty.Any], None]:
    """Call this to replace pieces of the default config with a value
    from wherever you might be loading your own config.

    If you have a pre-parsed dict-like such as a Dynaconf settings
    object that your application already manages, you could do something like:

    for key, value in settings.items():
        set_global_config(*key.split('.'))(value)
    """

    def set_global_config_(value: ty.Any) -> None:
        _set_val_at_path(_CONFIG(), value, *path)

    return set_global_config_


def set_config(
    *path: str,
) -> ty.Callable[[ty.Any], contextlib._GeneratorContextManager]:
    @contextlib.contextmanager
    def set_config_(value: ty.Any) -> ty.Generator[None, None, None]:
        config = copy.deepcopy(_CONFIG())
        _set_val_at_path(config, value, *path)
        with _CONFIG.set(config):
            yield

    return set_config_


T = ty.TypeVar("T")


def config_at_path(default: T, *path) -> T:
    """Dynamic lookup of mops config."""
    convert_val = type(default) if default is not None else lambda x: x
    assert callable(convert_val)
    convert_val(default)  # type: ignore

    config = _CONFIG()
    for path_part in path:
        if path_part not in config:
            return default
        config = config[path_part]
    return convert_val(config)  # type: ignore


class _StackConfig(ty.Generic[T]):
    def __init__(self, default: T, *path: str):
        self.default = default
        self.path = path

    def __call__(self) -> T:  # get current config value
        return config_at_path(self.default, *self.path)

    @contextlib.contextmanager
    def set(self, value) -> ty.Iterator[None]:
        with set_config(*self.path)(value):
            yield


def _make_stack_config(dotted_path: str, default: T) -> _StackConfig[T]:
    return _StackConfig(default, _ROOT_CFG_KEY, *dotted_path.split("."))


# k8s namespace will default to your OS username
try:
    _K8S_NAMESPACE = (os.getenv("MOPS_K8S_NAMESPACE") or getuser()).replace(".", "-")
except OSError:
    _K8S_NAMESPACE = "CICD-Runner"


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

# TODO eliminate these for 2.0 in favor of `memo_storage_root`. `tmp`
# is an implementation detail; what it is is 'memoization storage'.
adls_remote_tmp_sa = _make_stack_config("adls.remote.tmp_sa", "")
adls_remote_tmp_container = _make_stack_config("adls.remote.tmp_container", "")

memo_storage_root = _make_stack_config("memo.storage_root", "")
# use this instead of the remote_tmp config objects.


def get_memo_storage_root() -> str:
    return memo_storage_root() or str(AdlsFqn.of(adls_remote_tmp_sa(), adls_remote_tmp_container()))


memo_pipeline_id = _make_stack_config("memo.pipeline_id", "")

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

# TODO eliminate these for 2.0 in favor of `datasets_storage_root`
adls_remote_datasets_sa = _make_stack_config("adls.remote.datasets_sa", "")
adls_remote_datasets_container = _make_stack_config("adls.remote.datasets_container", "")

datasets_storage_root = _make_stack_config("datasets.storage_root", "")


def get_datasets_storage_root() -> str:
    return datasets_storage_root() or str(
        AdlsFqn.of(adls_remote_datasets_sa(), adls_remote_datasets_container())
    )


# Container registry stuff
acr_url = _make_stack_config("acr.url", "")

open_files_limit = _make_stack_config("resources.max_open_files", 10000)
