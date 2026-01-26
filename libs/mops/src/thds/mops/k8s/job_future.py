import threading
import typing as ty

from kubernetes import client

from thds.core import futures, log
from thds.termtool.colorize import colorized

from . import config, counts, uncertain_future
from .jobs import get_job, is_job_failed, is_job_succeeded, job_source

logger = log.getLogger(__name__)

UNUSUAL = colorized(fg="white", bg="orange")
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


def _check_job_before_timeout(
    job_name: str, namespace: str, job_seen: bool, time_since_last_seen: float
) -> ty.Union[bool, uncertain_future.NotYetDone, None]:
    """Check actual job state before timing out.

    This is called when we haven't seen updates for a job in a while (staleness timeout).
    The purpose is to recover from watch unreliability - we do a direct API fetch to see
    what's really happening with the job.

    Returns:
        True: Job definitely succeeded - resolve future successfully
        False: Unusual success (currently unused)
        NotYetDone: Job is still running - keep waiting, don't timeout
        None: Can't determine state - proceed with timeout (job doesn't exist or fetch failed)
    """
    try:
        # get_job uses the existing cache + backup fetch abstraction
        fetched = get_job(job_name, namespace)
        if fetched:
            if is_job_succeeded(fetched):
                logger.warning(
                    UNUSUAL(
                        f"Job {job_name} was not seen by watch but EXISTS and SUCCEEDED in k8s "
                        f"(JOB_SEEN={job_seen}, stale_for={time_since_last_seen:.1f}s) - recovering"
                    )
                )
                return True

            if is_job_failed(fetched):
                logger.error(
                    f"Job {job_name} was not seen by watch but EXISTS and FAILED in k8s "
                    f"(JOB_SEEN={job_seen}, stale_for={time_since_last_seen:.1f}s)"
                )
                raise K8sJobFailedError(
                    f"Job {job_name} failed (found via backup fetch): {fetched.status}"
                )

            # Job exists and is still running - the watch just isn't giving us updates,
            # but the job is fine. Keep waiting rather than timing out.
            logger.info(
                f"Job {job_name} still running (backup fetch confirmed) - "
                f"watch stale for {time_since_last_seen:.1f}s but job is healthy"
            )
            return uncertain_future.NotYetDone()

        # Job doesn't exist in k8s - this is a real problem, proceed with timeout
        logger.error(
            f"Job {job_name} does not exist in k8s "
            f"(JOB_SEEN={job_seen}, stale_for={time_since_last_seen:.1f}s)"
        )
    except K8sJobFailedError:
        raise  # re-raise job failures
    except Exception as e:
        logger.error(
            f"Backup fetch failed for {job_name}: {type(e).__name__}: {e} "
            f"(JOB_SEEN={job_seen}, stale_for={time_since_last_seen:.1f}s)"
        )

    return None  # proceed with timeout


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
                # Before timing out, check the actual job state - we might be able to recover
                ns = namespace or config.k8s_namespace()
                recovery_result = _check_job_before_timeout(job_name, ns, JOB_SEEN, time_since_last_seen)
                if recovery_result is not None:
                    return recovery_result

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
