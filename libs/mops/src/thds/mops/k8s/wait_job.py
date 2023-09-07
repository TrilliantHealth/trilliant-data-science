"""Wait for a Job to finish."""
import time
from datetime import timedelta
from timeit import default_timer

from thds.core import scope
from thds.core.log import logger_context

from .. import config
from .._utils.colorize import colorized
from ._shared import logger
from .jobs import get_job

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
        found = False
        while True:
            time.sleep(0.5 if found else 10.0)
            job = get_job(job_name)
            if not job:
                if found:
                    logger.warning(UNUSUAL(f"Known {log_name} no longer exists - assuming success!"))
                    return True
                if default_timer() - start_time > _max_no_job_wait().total_seconds():
                    logger.error(UNUSUAL(f"{log_name} has never been found - assuming failure!"))
                    return False

                logger.debug(f"{log_name} not yet found... retrying.")
                continue

            found = True
            start_time = default_timer()  # restart timer since the job has been found.
            status = job.status  # type: ignore
            if not status:
                continue

            if status.completion_time:
                return True

            if not status.active and status.failed:
                return False

    return _wait_for_job()
