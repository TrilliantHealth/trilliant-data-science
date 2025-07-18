import threading
import typing as ty

from kubernetes import client

from thds.core import futures, log
from thds.termtool.colorize import colorized

from . import config, counts, uncertain_future
from .jobs import is_job_failed, is_job_succeeded, job_source

logger = log.getLogger(__name__)

UNUSUAL = colorized(fg="white", bg="yellow")
SUCCEEDED = colorized(fg="white", bg="blue")
FAILED = colorized(fg="white", bg="red")


_FINISHED_JOBS = set[str]()
_FINISHED_JOBS_LOCK = threading.Lock()


def _check_newly_finished(job_name: str, namespace: str = "") -> str:
    # I don't believe it's possible to ever have a Job that both succeeds and fails.
    namespace = namespace or config.k8s_namespace()
    job_full = f"{namespace}/{job_name}"
    if job_full in _FINISHED_JOBS:
        return ""

    with _FINISHED_JOBS_LOCK:
        if job_full in _FINISHED_JOBS:
            return ""

        _FINISHED_JOBS.add(job_full)

    launched = counts.LAUNCH_COUNT.value
    return f"- ({launched - counts.inc(counts.FINISH_COUNT)} unfinished of {launched})"


class K8sJobFailedError(Exception):
    """Raised by `launch` when a Job is seen to terminate in a Failed state."""


def make_job_completion_future(job_name: str, *, namespace: str = "") -> futures.PFuture[bool]:
    """This is a natural boundary for a serializable lazy future - something that represents
    work being done across process boundaries (since Kubernetes jobs will be listed via an API.

    If True is returned, the Job has definitely succeeded.

    If False is returned, the Job may have succeeded but we saw no evidence of it.

    If the Job definitely failed, an Exception will be raised.
    """

    JOB_SEEN = False

    def job_completion_interpreter(
        job: ty.Optional[client.models.V1Job], last_seen_at: float
    ) -> ty.Union[uncertain_future.NotYetDone, bool]:
        nonlocal JOB_SEEN
        if not job:
            if JOB_SEEN:
                logger.warning(
                    UNUSUAL(f"Previously-seen job {job_name} no longer exists - assuming success!")
                )
                # we hereby indicate an unusual success to the Future waiter.
                return False

            time_since_last_seen = uncertain_future.official_timer() - last_seen_at
            if time_since_last_seen > config.k8s_watch_object_stale_seconds():
                # this is 5 minutes by default as of 2025-07-15.
                raise TimeoutError(
                    f"Job {job_name} has not been seen for {time_since_last_seen:.1f} seconds - assuming failure!"
                )

            # we don't know what's going on but things aren't truly stale yet.
            return uncertain_future.NotYetDone()

        JOB_SEEN = True

        if is_job_succeeded(job):
            newly_succeeded = _check_newly_finished(job_name, namespace)
            if newly_succeeded:
                logger.info(SUCCEEDED(f"Job {job_name} Succeeded! {newly_succeeded}"))
            return True

        if is_job_failed(job):
            newly_failed = _check_newly_finished(job_name, namespace)
            if newly_failed:
                logger.error(FAILED(f"Job {job_name} Failed! {newly_failed}"))
            raise K8sJobFailedError(f"Job {job_name} has failed with status: {job.status}")

        return uncertain_future.NotYetDone()  # job is still in progress

    return job_source().create_future(
        job_completion_interpreter,
        job_name,
        namespace=namespace or config.k8s_namespace(),
    )


def make_lazy_completion_future(job_name: str, *, namespace: str = "") -> futures.LazyFuture[bool]:
    """This is a convenience function that will create a job completion future and then
    immediately process it, returning the result. See docs on function above.
    """
    return futures.make_lazy(make_job_completion_future)(
        job_name,
        namespace=namespace or config.k8s_namespace(),
    )
