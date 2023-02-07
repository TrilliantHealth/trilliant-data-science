import typing as ty

from kubernetes import client

from ._shared import logger
from .retry import k8s_sdk_retry
from .watch import WatchingObjectSource


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
