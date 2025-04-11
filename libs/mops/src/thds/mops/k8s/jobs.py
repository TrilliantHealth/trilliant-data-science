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


def get_job(job_name: str, namespace: str = "") -> ty.Optional[client.models.V1Job]:
    return _JOB_SOURCE.get(job_name, namespace=namespace)


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
