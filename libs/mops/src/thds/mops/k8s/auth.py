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


def _new_api_client(kubeconfig_context: str) -> client.ApiClient:
    if "KUBERNETES_SERVICE_HOST" in os.environ:
        # in-pod there is exactly one cluster; the context is irrelevant. (Skipping the
        # kube-config probe also avoids the kubernetes library's root-logger WARNING about
        # falling back to inCluster config, which otherwise recurs on every token refresh.)
        config.load_incluster_config()
        return client.ApiClient()

    if not kubeconfig_context:
        return config.new_client_from_config()

    try:
        return config.new_client_from_config(context=kubeconfig_context)
    except config.ConfigException as e:
        raise config.ConfigException(
            f"`mops` was asked to use the kubeconfig context {kubeconfig_context!r} but it was"
            f" not found in your kubeconfig. Use your cloud provider's CLI to add credentials"
            f" for that cluster, then retry. (original error: {e})"
        ) from e


# TTL'd because exec-plugin credentials (kubelogin) expire, and rebuilding the client
# re-resolves them. Keyed per context, so launches targeting different clusters each get a
# client bound to their own cluster, independent of the process-global default configuration.
@locked_cached(TTLCache(8, ttl=120), lock=_AUTH_RLOCK)
def api_client(kubeconfig_context: str = "") -> client.ApiClient:
    logger.debug("Building Kubernetes ApiClient for context %r...", kubeconfig_context)
    return empty_config_retry(_new_api_client)(kubeconfig_context)


@locked_cached(TTLCache(1, ttl=120), lock=_AUTH_RLOCK)
def load_config() -> None:
    """__Here for backwards-compatibility.__

    Set the kubernetes library's process-global default Configuration - what bare
    `client.FooApi()` constructors read - to the configured context's configuration.

    This exists for code outside the mops launch->watch->logs pipeline (`apply_yaml`, external
    bare-client callers); mops's own machinery passes `api_client(context)` explicitly. It is
    just a promotion of `api_client`'s configuration, so both views share one loader, one
    retry policy, and one credential-refresh cadence."""
    logger.debug("Loading Kubernetes config...")
    try:
        client.Configuration.set_default(api_client(kubeconfig_context()).configuration)
    except config.ConfigException:
        logger.error("Failed to load kube-config")


def cache_clear() -> None:
    """Manually clearing the cache is hacky, but we found a bug that necessitated it."""
    load_config.cache_clear()  # type: ignore[attr-defined]
    api_client.cache_clear()  # type: ignore[attr-defined]


@scope.bound
def upsert_namespace(
    namespace: str,
    kubeconfig_context: str = "",
    created_cache: ty.Set[ty.Tuple[str, str]] = set(),  # noqa: B006
) -> None:
    scope.enter(_AUTH_RLOCK)
    key = (kubeconfig_context, namespace)
    if key in created_cache:
        return
    logger.debug("Creating namespace if not exists: %s" % namespace)
    kubeapi = client.CoreV1Api(api_client=api_client(kubeconfig_context))
    ns_obj = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
    namespaces = set([item.metadata.name for item in kubeapi.list_namespace().items])
    if namespace not in namespaces:
        logger.info(f"Creating namespace {namespace}")
        kubeapi.create_namespace(ns_obj)
    created_cache.add(key)


def core_client() -> client.CoreV1Api:
    """Returns a CoreV1Api client bound to the configured kubeconfig context."""
    return client.CoreV1Api(api_client=api_client(kubeconfig_context()))
    # Passing the config item's value (rather than "") preserves load_config's behavior of
    # honoring `mops.k8s.kubeconfig_context` - api_client("") means kubernetes-default loading,
    # which deliberately does NOT consult the config item.
