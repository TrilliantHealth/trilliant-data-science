"""The (cluster, namespace) coordinates that a mops k8s operation targets.

Resolved once at launch time (`resolve`) and then threaded explicitly through job creation,
watching, and log scraping - the background threads those spawn never re-read ambient config,
so launches targeting different clusters or namespaces can coexist in one process.
"""

import typing as ty

from . import config


class K8sTarget(ty.NamedTuple):
    kubeconfig_context: str
    # ^ empty string means "whatever kubernetes.config.load_config picks by default"
    namespace: str

    def __str__(self) -> str:
        return f"{self.kubeconfig_context or '<default>'}/{self.namespace}"


Resolvable = ty.Union[str, ty.Callable[[], str], None]
# ^ a value, a zero-arg callable producing one (e.g. a config item), or None for
# "read the mops config item at resolve time".


def _resolve(value: Resolvable, default: ty.Callable[[], str]) -> str:
    if value is None:
        return default()
    if callable(value):
        return value()
    return value


def resolve_target(kubeconfig_context: Resolvable = None, namespace: Resolvable = None) -> K8sTarget:
    """Resolve per-invocation target coordinates, defaulting to the mops config items."""
    return K8sTarget(
        kubeconfig_context=_resolve(kubeconfig_context, config.kubeconfig_context),
        namespace=_resolve(namespace, config.k8s_namespace),
    )
