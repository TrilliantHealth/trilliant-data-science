"""Provider discovery and externally configurable provider loading."""

import importlib
import typing as ty

from kubernetes import client

from thds.core import config, log

from . import azure, model

logger = log.getLogger(__name__)

PROVIDER = config.item("mops.k8s.cost.provider", default="auto", parse=str)

_BUILT_INS: dict[str, model.Provider] = {azure.PROVIDER.name: azure.PROVIDER}


Resolver = tuple[model.Provider, ...]


def _external(import_path: str) -> model.Provider:
    module_path, name = import_path.rsplit(".", 1)
    factory = ty.cast(model.ProviderFactory, getattr(importlib.import_module(module_path), name))
    return factory()


def node(resolver: Resolver, raw: client.V1Node) -> model.Node:
    """Classify a node with the first matching provider."""
    for provider in resolver:
        if classified := provider.node(raw):
            return classified
    return model.unclassified(raw)


def price(resolver: Resolver, value: model.Node) -> model.Price:
    """Price a classified node with its matching provider."""
    for provider in resolver:
        if provider.name == value.provider:
            return provider.price(value)
    return model.Price(None, "USD", "", f"no cost provider matched {value.name}")


def resolve(configured: str = "") -> Resolver:
    """Resolve `auto`, a built-in name, or a dotted provider-factory path."""
    selected = configured or PROVIDER()
    if selected == "auto":
        return tuple(_BUILT_INS.values())
    if selected in _BUILT_INS:
        return (_BUILT_INS[selected],)
    try:
        return (_external(selected),)
    except Exception:
        logger.warning("Could not load Kubernetes cost provider '%s'.", selected, exc_info=True)
        return ()
