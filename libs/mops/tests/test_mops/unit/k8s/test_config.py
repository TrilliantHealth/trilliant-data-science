"""This helps me guard against typos..."""

from thds.mops.k8s.config import k8s_namespace, namespaces_supporting_workload_identity
from thds.mops.k8s.namespace import parse_namespace


def test_use_workload_identity():
    with k8s_namespace.set_local("demand-forecast"):
        assert k8s_namespace() in namespaces_supporting_workload_identity()


def test_namespace():
    assert "yu-zhang" == parse_namespace("Yu_Zhang")
