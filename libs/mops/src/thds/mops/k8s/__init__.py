"""Trilliant Health abstraction around launching K8S Jobs."""

try:
    from kubernetes import client as _  # noqa
except ModuleNotFoundError as mnf:
    raise ModuleNotFoundError(
        "Please install mops with the `k8s` extra to use `thds.mops.k8s`."
    ) from mnf

from .container_registry import autocr  # noqa: F401
from .launch import K8sJobFailedError, launch, shim  # noqa
from .node_selection import (  # noqa
    NodeNarrowing,
    ResourceDefinition,
    require_gpu,
    tolerates_64cpu,
    tolerates_gpu,
    tolerates_spot,
)

try:
    from . import thds_std  # noqa: F401
except ModuleNotFoundError:
    pass


mops_shell = shim  # deprecated alias
