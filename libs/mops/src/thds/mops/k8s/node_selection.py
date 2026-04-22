import typing as ty

from kubernetes import client
from typing_extensions import TypedDict


class ResourceDefinition(TypedDict, total=False):
    """
    This works for both limits and requests.

    https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
    """

    memory: str
    """E.g., 10G"""
    cpu: str
    """E.g., 4.5, or 4500m (millicores)"""


class NodeNarrowing(TypedDict, total=False):
    """This is a more transparent interface for selecting nodes that your job can run on.

    You don't have to provide each key, but any key/value you pair you provide must be the proper type.
    """

    resource_requests: ResourceDefinition
    resource_limits: ResourceDefinition
    # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
    node_selector: ty.Mapping[str, str]
    # https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/
    node_affinity: client.V1NodeAffinity
    # https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#node-affinity
    # Use when node_selector's single-value equality isn't enough - e.g. when a pod
    # should be eligible for any of several node pools. node_affinity and node_selector
    # compose: the scheduler ANDs them, so setting both is fine.
    tolerations: ty.Sequence[client.V1Toleration]
    # https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/


def tolerates_spot() -> client.V1Toleration:
    """Return our custom spot instance toleration configuration."""
    return client.V1Toleration(
        key="kubernetes.azure.com/scalesetpriority", value="spot", effect="NoSchedule"
    )


def tolerates_gpu() -> client.V1Toleration:
    """Apply this toleration to enable use of GPUs."""
    return client.V1Toleration(key="dedicated", value="gpu", effect="NoSchedule")


def tolerates_64cpu() -> client.V1Toleration:
    """These node pools often do not scale up well or quickly, so by
    default they're disabled. If that changes in the future, or if you
    are requesting more than 32 CPUs for your Pod, you should apply
    this toleration.
    """
    return client.V1Toleration(key="dedicated", value="64cpu", effect="NoSchedule")


def require_gpu() -> NodeNarrowing:
    """Merge this with any additional NodeNarrowing (e.g. resource_requests) to run on GPUs."""
    return dict(node_selector={"instance-type": "gpu"}, tolerations=[tolerates_gpu()])


def require_node_labels(labels: ty.Mapping[str, ty.Sequence[str]]) -> client.V1NodeAffinity:
    """Hard requirement that each node label's value is in the given set.

    AND across keys, OR within each key (standard `In` operator semantics). Use this
    when a pod should be eligible for any of several node pools - e.g.
    ``{"initiative": ["pool-a", "pool-b"]}``. For single-value equality,
    ``node_selector`` is simpler.

    For unusual cases (preferred affinity, NotIn, key-exists-only), build a
    ``V1NodeAffinity`` directly and pass it to ``NodeNarrowing["node_affinity"]``.
    """
    return client.V1NodeAffinity(
        required_during_scheduling_ignored_during_execution=client.V1NodeSelector(
            node_selector_terms=[
                client.V1NodeSelectorTerm(
                    match_expressions=[
                        client.V1NodeSelectorRequirement(key=k, operator="In", values=list(v))
                        for k, v in labels.items()
                    ],
                ),
            ],
        ),
    )
