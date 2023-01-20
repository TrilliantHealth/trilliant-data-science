import logging
import typing as ty
from threading import RLock

from cachetools import TTLCache
from kubernetes import client, config
from retry import retry

from thds.core import scope
from thds.core.log import getLogger

from ..locked_cache import locked_cached

logger = getLogger(__name__)


empty_config_retry = retry(
    config.ConfigException, delay=0.2, tries=3, logger=ty.cast(logging.Logger, logger)
)

_AUTH_RLOCK = RLock()


# load_config gets called all over the place and way too often.
@locked_cached(TTLCache(1, ttl=120), lock=_AUTH_RLOCK)
def load_config():
    logger.info("Loading Kubernetes config...")
    try:
        empty_config_retry(config.load_config)()
    except config.ConfigException:
        logger.error("Failed to load kube-config")


@scope.bound
def upsert_namespace(namespace: str, created_cache=set()):  # noqa: B006
    scope.enter(_AUTH_RLOCK)
    if namespace in created_cache:
        return
    logger.info("Creating namespace if not exists: %s" % namespace)
    load_config()
    kubeapi = client.CoreV1Api()
    ns_obj = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
    namespaces = set([item.metadata.name for item in kubeapi.list_namespace().items])
    if namespace not in namespaces:
        logger.info(f"Creating namespace {namespace}")
        kubeapi.create_namespace(ns_obj)
    created_cache.add(namespace)
