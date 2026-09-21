from thds.core.log import env as log_env
from thds.mops.pure.core import th_datadog


def test_log_query_scopes_to_the_pod_by_default():
    assert th_datadog.log_query("ns", "pod-1", "job-1") == "kube_namespace:ns pod_name:pod-1"
    assert th_datadog.log_query("ns", "", "job-1") == "kube_namespace:ns kube_job_name:job-1"


def test_log_query_narrows_to_one_invocation_when_the_context_names_it():
    """A pod running many invocations holds all their lines; the logger context carried
    into its process by the environment is what tells them apart."""
    context = log_env.from_env(log_env.as_env(mops_fn="mod--fn", mops_args="AbC123", mqclass="train"))
    assert (
        th_datadog.log_query("ns", "pod-1", "job-1", context)
        == 'kube_namespace:ns pod_name:pod-1 "mops_args=AbC123"'
    )
