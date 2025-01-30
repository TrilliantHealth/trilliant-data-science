import typing as ty
from threading import RLock

from cachetools import TTLCache
from kubernetes import client, config

from thds.core import fretry, log, scope

from .._utils.locked_cache import locked_cached

logger = log.getLogger(__name__)


def _retry_config(exc: Exception) -> bool:
    if isinstance(exc, config.ConfigException):
        logger.debug("Retrying config load...")
        return True
    return False


empty_config_retry = fretry.retry_sleep(_retry_config, fretry.expo(retries=3, delay=0.2))

_AUTH_RLOCK = RLock()


# load_config gets called all over the place and way too often.
@locked_cached(TTLCache(1, ttl=120), lock=_AUTH_RLOCK)
def load_config() -> None:
    logger.debug("Loading Kubernetes config...")
    try:
        empty_config_retry(config.load_config)()
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
