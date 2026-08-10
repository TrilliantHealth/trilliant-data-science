import importlib

from thds.mops.k8s import config, runtime_context


def test_a_pod_reports_where_it_is_running(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "job-abc-x7f2q")
    monkeypatch.setenv("MOPS_K8S_NAMESPACE", "ml-prod")
    monkeypatch.setenv("MOPS_K8S_JOB_NAME", "job-abc")

    assert runtime_context.k8s_context() == (
        "k8s",
        {"pod_name": "job-abc-x7f2q", "namespace": "ml-prod", "job_name": "job-abc"},
    )


def test_the_namespace_variable_is_the_one_the_launcher_was_told_to_set(monkeypatch):
    """`_launch` names this variable from config, so a deployment that renamed it would
    otherwise report every pod without a namespace - and a namespace is required to reach
    the logs."""
    monkeypatch.setenv("HOSTNAME", "pod-1")
    monkeypatch.setenv("SOMEWHERE_ELSE", "ml-prod")

    with config.k8s_namespace_env_var_key.set_local("SOMEWHERE_ELSE"):
        assert runtime_context.k8s_context().coordinates["namespace"] == "ml-prod"


def test_off_cluster_there_is_nothing_to_report(monkeypatch):
    for name in ("HOSTNAME", "MOPS_K8S_NAMESPACE", "MOPS_K8S_JOB_NAME"):
        monkeypatch.delenv(name, raising=False)

    assert runtime_context.k8s_context().coordinates == {
        "pod_name": "",
        "namespace": "",
        "job_name": "",
    }
    # empty values are dropped by `runtime.current`, not here: a provider reports what it
    # reads, and one place decides what counts as absent.


def test_the_provider_path_imports_back_to_this_function():
    """`_launch` ships this string to the remote, which imports it. A rename that did not
    update the string would fail only on the cluster."""
    module_path, name = runtime_context.PROVIDER.rsplit(".", 1)

    assert getattr(importlib.import_module(module_path), name) is runtime_context.k8s_context
