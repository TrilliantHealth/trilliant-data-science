import typing as ty
from datetime import datetime, timezone

from kubernetes import client

from ._shared import logger
from .retry import k8s_sdk_retry
from .watch import EventType, WatchingObjectSource, watch_forever


@k8s_sdk_retry()
def _get_job(namespace: str, job_name: str) -> ty.Optional[client.models.V1Job]:
    logger.debug(f"Reading job {job_name}")
    return client.BatchV1Api().read_namespaced_job(
        namespace=namespace,
        name=job_name,
    )


_JOB_SOURCE = WatchingObjectSource(
    lambda _, __: client.BatchV1Api().list_namespaced_job,
    lambda job: job.metadata.name,  # type: ignore
    _get_job,
    typename="Job",
)


def job_source() -> WatchingObjectSource[client.models.V1Job]:
    return _JOB_SOURCE


def get_job(job_name: str, namespace: str = "") -> ty.Optional[client.models.V1Job]:
    return _JOB_SOURCE.get(job_name, namespace=namespace)


@k8s_sdk_retry()
def delete_job(job_name: str, namespace: str) -> bool:
    """Delete a Job, cascading to its pod(s). Returns True if the Job was
    deleted, False if it couldn't be - for ANY reason. `propagation_policy=
    "Foreground"` deletes the Job's dependents (the pod) too - deleting just the
    pod is wrong, the Job controller would recreate it.

    Never raises: this backs a future's `cancel()`, and like
    `concurrent.futures.Future.cancel()` (pure local state, bool-only) cancel
    must be infallible from the caller's view. mops does not try to classify the
    many ways the shim's delete can fail (forbidden, throttled, API-server down,
    admission webhook, ...) into a raise-vs-return taxonomy - any failure to
    effect the delete collapses to False. The distinction the caller never gets
    here is instead preserved in the logs: a 404 (Job already gone) is the one
    expected non-success and logs quietly; everything else logs with a full
    stack trace so nothing is swallowed silently (e.g. a 403 = the orchestrator
    SA lacks `delete` on jobs.batch, an RBAC gap worth fixing)."""
    try:
        client.BatchV1Api().delete_namespaced_job(
            name=job_name, namespace=namespace, propagation_policy="Foreground"
        )
        logger.info(f"Deleted job {namespace}/{job_name}")
        return True
    except client.exceptions.ApiException as e:
        if e.status == 404:
            logger.info(f"Job {namespace}/{job_name} already gone; nothing to delete")
            return False

        logger.exception(f"Failed to delete job {namespace}/{job_name}; the pod will run to completion")
        return False


# https://github.com/kubernetes/kubernetes/issues/68712#issuecomment-514008330
# https://kubernetes.io/docs/concepts/workloads/controllers/job/#terminal-job-conditions


def job_completion_time(job: client.models.V1Job) -> ty.Optional[datetime]:
    if not job.status:
        return None

    if job.status.completion_time:
        return job.status.completion_time

    for condition in job.status.conditions or tuple():
        if condition.type == "Complete" and condition.status == "True":
            return (
                condition.last_transition_time
                if condition.last_transition_time
                else datetime.now(tz=timezone.utc)
            )

    return None


def is_job_succeeded(job: client.models.V1Job) -> bool:
    return bool(job_completion_time(job))


def is_job_failed(job: client.models.V1Job) -> bool:
    if not job.status:
        return False

    for condition in job.status.conditions or tuple():
        if condition.type == "Failed" and condition.status == "True":
            return True

    return False


def is_mops_exception_failure(job: client.models.V1Job) -> bool:
    """True when the job failed because the user function raised an exception.

    The mops entry point exits with MOPS_EXCEPTION_EXIT_CODE, which triggers the
    podFailurePolicy FailJob rule. k8s records this as reason='PodFailurePolicy'.
    The serialized exception is already in blob storage — the caller should retrieve
    it via the normal result-reading path rather than raising K8sJobFailedError.
    """
    for condition in job.status.conditions or tuple():
        if condition.reason == "PodFailurePolicy" and condition.status == "True":
            return True

    return False


def watch_jobs(
    namespace: str, timeout: ty.Optional[int] = None
) -> ty.Iterator[ty.Tuple[client.models.V1Job, EventType]]:
    yield from watch_forever(
        lambda _, __: client.BatchV1Api().list_namespaced_job,
        namespace,
        typename="Job",
        timeout=timeout,
    )


def watch_jobs_cli() -> None:
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(description="Watch Kubernetes jobs")
    parser.add_argument("namespace", help="Kubernetes namespace to watch")
    parser.add_argument("--timeout", type=int, help="Timeout in seconds", default=None)
    args = parser.parse_args()

    for job, event_type in watch_jobs(args.namespace, timeout=args.timeout):
        completion_time = job_completion_time(job)
        creation_time = job.metadata.creation_timestamp

        status = ""
        if is_job_failed(job):
            status = "FAILED"
        elif completion_time := job_completion_time(job):
            job_duration = completion_time - creation_time
            status = f"completed_after={job_duration}"
        else:
            status = "incomplete" + " " * 20

        print(
            datetime.now().isoformat(),
            f"{event_type:<10}",
            f"{job.metadata.name:<64}",
            job.metadata.creation_timestamp,
            status,
        )


if __name__ == "__main__":
    watch_jobs_cli()
