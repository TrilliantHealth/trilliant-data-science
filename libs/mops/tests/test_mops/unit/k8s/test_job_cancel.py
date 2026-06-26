from unittest import mock

from kubernetes import client

from thds.core import futures
from thds.mops.k8s import jobs
from thds.mops.k8s.job_future import _CancellableJobFuture


def test_delete_job_returns_true_on_success():
    with mock.patch.object(client, "BatchV1Api") as api:
        assert jobs.delete_job("job-x", "ns-y") is True
        api.return_value.delete_namespaced_job.assert_called_once_with(
            name="job-x", namespace="ns-y", propagation_policy="Foreground"
        )


def test_delete_job_returns_false_when_already_gone():
    with mock.patch.object(client, "BatchV1Api") as api:
        api.return_value.delete_namespaced_job.side_effect = client.exceptions.ApiException(status=404)
        assert jobs.delete_job("job-x", "ns-y") is False


def test_delete_job_returns_false_when_forbidden():
    # 403 = the orchestrator SA lacks delete on jobs.batch. cancel() must stay
    # infallible (like stdlib Future.cancel), so this is False (logged), not a raise.
    with mock.patch.object(client, "BatchV1Api") as api:
        api.return_value.delete_namespaced_job.side_effect = client.exceptions.ApiException(status=403)
        assert jobs.delete_job("job-x", "ns-y") is False


def test_delete_job_returns_false_on_any_api_error():
    # mops doesn't classify shim failures - any ApiException collapses to False.
    with mock.patch.object(client, "BatchV1Api") as api:
        api.return_value.delete_namespaced_job.side_effect = client.exceptions.ApiException(status=500)
        assert jobs.delete_job("job-x", "ns-y") is False


def test_cancellable_job_future_cancel_deletes_job():
    inner: "futures.PFuture[bool]" = futures.resolved(True)
    fut = _CancellableJobFuture(inner, job_name="job-x", namespace="ns-y")
    with mock.patch.object(jobs.client, "BatchV1Api") as api:
        assert fut.cancel() is True
        api.return_value.delete_namespaced_job.assert_called_once_with(
            name="job-x", namespace="ns-y", propagation_policy="Foreground"
        )


def test_cancellable_job_future_is_cancellable_via_try_cancel():
    # The chain reaches this via futures.try_cancel; confirm it qualifies.
    fut = _CancellableJobFuture(futures.resolved(True), job_name="j", namespace="n")
    with mock.patch.object(jobs.client, "BatchV1Api"):
        assert futures.try_cancel(fut) is True


def test_cancellable_job_future_delegates_result():
    fut = _CancellableJobFuture(futures.resolved(True), job_name="j", namespace="n")
    assert fut.result() is True
    assert fut.done()
