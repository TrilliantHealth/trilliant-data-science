"""This helps me guard against typos..."""
from thds.mops.config import k8s_namespace, namespaces_supporting_workload_identity


def test_use_workload_identity():
    with k8s_namespace.set("demand-forecast"):
        assert k8s_namespace() in namespaces_supporting_workload_identity()
