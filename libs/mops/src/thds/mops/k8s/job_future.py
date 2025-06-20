import time
import typing as ty

from kubernetes import client

from thds.core import futures, log
from thds.termtool.colorize import colorized

from . import config, counts
from .jobs import is_job_failed, is_job_succeeded, job_source
from .uncertain_future import FutureDone

logger = log.getLogger(__name__)

UNUSUAL = colorized(fg="white", bg="yellow")
SUCCEEDED = colorized(fg="white", bg="blue")
FAILED = colorized(fg="white", bg="red")


class K8sJobFailedError(Exception):
    """Raised by `launch` when a Job is seen to terminate in a Failed state."""


def make_job_completion_future(job_name: str, *, namespace: str = "") -> futures.PFuture[bool]:
    """This is a natural boundary for a serializable lazy future - something that represents
    work being done across process boundaries (since Kubernetes jobs will be listed via an API.
    """

    def job_completion_interpreter(
        job: ty.Optional[client.models.V1Job], last_seen_at: float
    ) -> ty.Optional[FutureDone[bool]]:
        if not job:
            if last_seen_at:
                logger.warning(
                    UNUSUAL(f"Previously-seen job {job_name} no longer exists - assuming success!")
                )
                return FutureDone(True)

            time_since_last_seen = time.monotonic() - last_seen_at
            if time_since_last_seen > config.k8s_monitor_max_attempts() * config.k8s_monitor_delay():
                raise TimeoutError(
                    f"Job {job_name} has not been seen for {time_since_last_seen:.1f} seconds - assuming failure!"
                )

            return None  # we don't know what's going on but things aren't truly stale yet.

        def fmt_counts() -> str:
            launched = counts.LAUNCH_COUNT.value
            return f"- ({launched - counts.FINISH_COUNT.inc()} unfinished of {launched})"

        if is_job_succeeded(job):
            logger.info(SUCCEEDED(f"Job {job_name} Succeeded! {fmt_counts()}"))
            return FutureDone(True)

        if is_job_failed(job):
            logger.error(FAILED(f"Job {job_name} Failed! {fmt_counts()}"))
            raise K8sJobFailedError(f"Job {job_name} has failed with status: {job.status}")

        return None  # job is still in progress

    return job_source().create_future(
        job_completion_interpreter,
        job_name,
        namespace=namespace or config.k8s_namespace(),
    )
