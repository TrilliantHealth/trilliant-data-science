"""Wait for a Job to finish."""

import time
from datetime import timedelta
from timeit import default_timer

from thds.core import scope
from thds.core.log import logger_context
from thds.termtool.colorize import colorized

from . import config
from ._shared import logger
from .jobs import get_job, is_job_failed, is_job_succeeded

UNUSUAL = colorized(fg="white", bg="yellow")


def _max_no_job_wait() -> timedelta:
    return timedelta(seconds=config.k8s_monitor_max_attempts() * config.k8s_monitor_delay())


@scope.bound
def wait_for_job(job_name: str, short_name: str = "") -> bool:
    """Return True if Job completed, False if it failed.

    May raise an exception if something truly unusual happened.

    A _lot_ has gone in to trying to make this robust against common
    failure patterns. My apologies for the resulting shape of the
    code. :/
    """
    scope.enter(logger_context(job=job_name))
    log_name = f"Job {short_name}" if short_name else "Job"
    logger.debug(f"Waiting for {log_name} to finish...")
    start_time = default_timer()

    def _wait_for_job() -> bool:
        nonlocal start_time
        found_at_least_once = False
        while True:
            time.sleep(0.5 if found_at_least_once else 10.0)
            job = get_job(job_name)
            if not job:
                if found_at_least_once:
                    logger.warning(UNUSUAL(f"Known job {job_name} no longer exists - assuming success!"))
                    return True
                max_wait_seconds = _max_no_job_wait().total_seconds()
                if default_timer() - start_time > max_wait_seconds:
                    logger.error(
                        UNUSUAL(
                            f"Job {job_name} has not been seen for {max_wait_seconds:.1f} seconds"
                            " - assuming failure!"
                        )
                    )
                    return False

                logger.debug("%s not found... retrying.", job_name)
                continue

            if is_job_succeeded(job):
                return True

            if is_job_failed(job):
                logger.error(
                    UNUSUAL(f"A Kubernetes Job is reporting an actual failed status: {job_name}")
                )
                return False

            found_at_least_once = True
            start_time = default_timer()  # restart timer since the job has been found.

    return _wait_for_job()
