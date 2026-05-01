import os
import typing as ty
from threading import RLock

from cachetools import TTLCache
from kubernetes import client, config

from thds.core import fretry, log, scope

from .._utils.locked_cache import locked_cached
from .config import kubeconfig_context

logger = log.getLogger(__name__)


def _retry_config(exc: Exception) -> bool:
    if isinstance(exc, config.ConfigException):
        logger.debug("Retrying config load...")
        return True
    return False


empty_config_retry = fretry.retry_sleep(_retry_config, fretry.expo(retries=3, delay=0.2))

_AUTH_RLOCK = RLock()


def _load_kube_config() -> None:
    # In-pod, kubernetes.config.load_config tries ~/.kube/config first, fails,
    # then falls back to in-cluster - emitting a root-logger WARNING on every
    # call ("kube_config_path not provided ... Using inCluster Config"). The
    # TTL cache keeps it bounded but it still recurs on token refresh. Skip
    # the kube_config probe when KUBERNETES_SERVICE_HOST is set.
    if "KUBERNETES_SERVICE_HOST" in os.environ:
        config.load_incluster_config()
        return

    ctx = kubeconfig_context()
    if not ctx:
        config.load_config()
        return

    try:
        config.load_kube_config(context=ctx)
    except config.ConfigException as e:
        raise config.ConfigException(
            f"`mops` is configured to use the kubeconfig context {ctx!r} but it was not"
            f" found in your kubeconfig. Use your cloud provider's CLI to add credentials"
            f" for that cluster, then retry. (original error: {e})"
        ) from e


# load_config gets called all over the place and way too often.
@locked_cached(TTLCache(1, ttl=120), lock=_AUTH_RLOCK)
def load_config() -> None:
    logger.debug("Loading Kubernetes config...")
    try:
        empty_config_retry(_load_kube_config)()
    except config.ConfigException:
        logger.error("Failed to load kube-config")


@scope.bound
def upsert_namespace(namespace: str, created_cache: ty.Set[str] = set()) -> None:  # noqa: B006
    scope.enter(_AUTH_RLOCK)
    if namespace in created_cache:
        return
    logger.debug("Creating namespace if not exists: %s" % namespace)
    load_config()
    kubeapi = client.CoreV1Api()
    ns_obj = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
    namespaces = set([item.metadata.name for item in kubeapi.list_namespace().items])
    if namespace not in namespaces:
        logger.info(f"Creating namespace {namespace}")
        kubeapi.create_namespace(ns_obj)
    created_cache.add(namespace)


def core_client() -> client.CoreV1Api:
    """Returns a CoreV1Api client, ensuring that the Kubernetes config is loaded."""
    load_config()
    return client.CoreV1Api()
