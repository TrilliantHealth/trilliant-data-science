from thds.mops.k8s import config
from thds.mops.k8s.target import K8sTarget, resolve_target


def test_resolve_defaults_to_config_items():
    with config.kubeconfig_context.set_local("cluster-a"), config.k8s_namespace.set_local("ns-a"):
        assert resolve_target() == K8sTarget(kubeconfig_context="cluster-a", namespace="ns-a")


def test_resolve_explicit_values_win_over_config():
    with config.kubeconfig_context.set_local("cluster-a"), config.k8s_namespace.set_local("ns-a"):
        assert resolve_target("cluster-b", "ns-b") == K8sTarget(
            kubeconfig_context="cluster-b", namespace="ns-b"
        )


def test_resolve_callables_are_called():
    assert resolve_target(lambda: "cluster-c", lambda: "ns-c") == K8sTarget(
        kubeconfig_context="cluster-c", namespace="ns-c"
    )
