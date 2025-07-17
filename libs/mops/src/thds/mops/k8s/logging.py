"""Handles things having to do with getting logs out of the Pods of a Job."""

import enum
import random
import threading
import time
import typing as ty
from timeit import default_timer

import cachetools
import urllib3.exceptions
from kubernetes import client, watch

from thds import core
from thds.core.log import logger_context
from thds.termtool.colorize import colorized, make_colorized_out, next_color

from .._utils.locked_cache import locked_cached
from . import config
from ._shared import logger
from .jobs import get_job
from .retry import k8s_sdk_retry

NO_K8S_LOGS = core.config.item("mops.no_k8s_logs", parse=core.config.tobool, default=False)
# non-empty if you want to completely disable k8s pod logs.
K8S_LOG_POD_FRACTION = core.config.item("mops.k8s.log_pod_fraction", parse=float, default=1.0)
# fraction of pods to log. 1.0 means all pods.

BOINK = colorized(fg="white", bg="magenta")
# this module has tons of logs. occasionally you want to find a needle
# in that haystack when you're debugging something. Wrap the logged
# string in this and it'll stand out.


class JobLogWatcher:
    """Will spawn one or more daemon threads.

    Each pod scraped will get its own randomly-selected ANSI color for
    logs printed to the terminal.

    When pods enter a failure state, a new check for pods will be
    launched, in the hopes that the Job is planning to create new Pods
    to replace them.

    If the Job goes away entirely, this may or may not eventually
    terminate. Because the threads are daemon threads, this will not
    affect the logic of your program, but it's possible you may see
    some spurious logging messages.
    """

    def __init__(self, job_name: str, num_pods_expected: int = 1) -> None:
        self.job_name = job_name
        self.num_pods_expected = num_pods_expected
        self.pods_being_scraped: ty.Set[str] = set()
        self.pod_colors: ty.Dict[str, ty.Callable[[str], ty.Any]] = dict()
        self.job_pods_discovery_lock = threading.Lock()

    @k8s_sdk_retry()
    @core.scope.bound
    def start(self, failed_pod_name: str = "") -> None:
        """Call this one time - it will spawn threads as needed."""
        if NO_K8S_LOGS():
            return

        if random.random() > K8S_LOG_POD_FRACTION():
            logger.info(f"Skipping log watcher for {self.job_name} due to fraction.")
            return

        core.scope.enter(self.job_pods_discovery_lock)
        # we lock here because some of the threads we spawn may
        # eventually call this same method, and we only want one
        # instance of this running at a time.
        core.scope.enter(logger_context(log=self.job_name))
        logger.debug("Starting log watcher")
        if failed_pod_name:
            logger.info(
                BOINK(f"Failed to scrape logs in pod {failed_pod_name}, looking for new pods...")
            )
            self.pods_being_scraped.discard(failed_pod_name)
            # this one can be retried if it's still out there.
        time.sleep(config.k8s_monitor_delay())
        for pod in _yield_running_pods_for_job(
            self.job_name,
            self.num_pods_expected if not self.pods_being_scraped else 1,
        ):
            pod_name = pod.metadata.name
            if pod_name not in self.pods_being_scraped:
                # don't start new threads for pods we've already previously discovered - they have their own thread.
                self.pods_being_scraped.add(pod_name)
                if pod_name not in self.pod_colors:
                    self.pod_colors[pod_name] = make_colorized_out(
                        colorized(fg=next_color()), fmt_str=pod_name + " {}"
                    )
                log_thread = threading.Thread(
                    target=_scrape_pod_logs,
                    args=(
                        self.pod_colors[pod_name],
                        pod_name,
                        self.start,
                    ),
                    daemon=True,
                )
                log_thread.start()


# we really don't want many threads calling the K8S API a billion times all at once
@locked_cached(cachetools.TTLCache(maxsize=1, ttl=2))
def _list_pods_in_our_namespace() -> ty.List[client.models.V1Pod]:
    return client.CoreV1Api().list_namespaced_pod(namespace=config.k8s_namespace()).items


class K8sPodStatus(enum.Enum):
    PENDING = "Pending"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    UNKNOWN = "Unknown"


def _yield_running_pods_for_job(
    job_name: str, expected_number_of_pods: int = 1
) -> ty.Iterator[client.models.V1Pod]:
    """TODO: stop polling if the Job cannot be found at all."""
    attempt = 0
    yielded = 0
    logger.debug("Polling for pods created by job: %s", job_name)
    while attempt < config.k8s_monitor_max_attempts():
        for pod in _list_pods_in_our_namespace():
            owner_refs = pod.metadata.owner_references
            if not owner_refs:
                # this is a rare and undocumented case where a pod
                # will have owner_references=None if it was manually created.
                # since we're looking for pods created by jobs, we can safely skip these.
                continue

            if len(owner_refs) > 1:
                logger.warning("Found multiple owner references for a pod. Taking first one...")
            owner_ref = owner_refs[0]
            if owner_ref.name == job_name:
                if pod.status.phase in {
                    K8sPodStatus.RUNNING.value,
                    K8sPodStatus.UNKNOWN.value,
                }:
                    logger.debug(f"Found a pod {pod.metadata.name} in phase {pod.status.phase}")
                    yielded += 1
                    yield pod
        if yielded >= expected_number_of_pods:
            logger.debug("Found all expected running pods.")
            return
        if not get_job(job_name):
            logger.warning("Job not found; not a good sign for pod logs")
            attempt += 50
        logger.debug("Didn't find enough pods yet, sleeping for a moment...")
        time.sleep(config.k8s_monitor_delay())
        attempt += 1


def _get_pod_phase(pod_name: str) -> str:
    return (
        client.CoreV1Api()
        .read_namespaced_pod(
            namespace=config.k8s_namespace(),
            name=pod_name,
            _request_timeout=(
                config.k8s_watch_connection_timeout_seconds(),
                config.k8s_watch_read_timeout_seconds(),
            ),
        )
        .status.phase
    )


def _await_pod_phases(phases: ty.Set[K8sPodStatus], pod_name: str) -> str:
    while True:
        phase = _get_pod_phase(pod_name)
        if phase in {phase.value for phase in phases}:
            return phase
        time.sleep(config.k8s_monitor_delay())


@core.scope.bound
def _scrape_pod_logs(
    out: ty.Callable[[str], ty.Any],
    pod_name: str,
    failure_callback: ty.Callable[[str], ty.Any],
) -> None:
    """Contains its own retry error boundary b/c this is notoriously unreliable."""
    core.scope.enter(logger_context(log=pod_name))

    last_scraped_at = default_timer()
    base_kwargs = dict(
        name=pod_name,
        namespace=config.k8s_namespace(),
        _request_timeout=(
            config.k8s_logs_watch_connection_timeout_seconds(),
            config.k8s_logs_watch_read_timeout_seconds(),
            # we want these numbers fairly high, otherwise a pod that's temporarily silent
            # will cause the stream to end, which is noisy and inefficient.
        ),
        # i'm occasionally seeing the `stream()` call below hang
        # indefinitely if logs don't come back from the pod for a
        # while. Which is ironic, since most of this code is here to
        # help us make sure we keep retrying if no logs happen on the
        # pod for a while, since frequently `stream()` will just end
        # quietly when that happens.  In any case, at this point,
        # we're better-equipped to handle all kinds of retries, so
        # using the (connect, read) _request timeout tuple is probably
        # what we want to try next.
    )

    def get_retry_kwargs(_: int) -> ty.Tuple[tuple, dict]:
        return tuple(), dict(base_kwargs, since_seconds=int(default_timer() - last_scraped_at))

    def scrape_logs(*_args: ty.Any, **kwargs: ty.Any) -> None:
        nonlocal last_scraped_at
        _await_pod_phases(
            {K8sPodStatus.RUNNING, K8sPodStatus.SUCCEEDED, K8sPodStatus.FAILED},
            pod_name,
        )
        logger.debug("Watching pod log stream...")
        while True:
            for e in watch.Watch().stream(
                client.CoreV1Api().read_namespaced_pod_log,
                **kwargs,
            ):
                out(e)
                last_scraped_at = default_timer()
            time.sleep(config.k8s_monitor_delay())
            pod_phase = _get_pod_phase(pod_name)
            if pod_phase == K8sPodStatus.SUCCEEDED.value:
                logger.debug("Done scraping pod logs")
                return
            if pod_phase == K8sPodStatus.FAILED.value:
                logger.warning("Pod failed - calling callback")
                failure_callback(pod_name)
                return
            logger.debug("Pod is not complete - will retry the log watch")

    def should_retry(ex: Exception) -> bool:
        return isinstance(ex, urllib3.exceptions.ReadTimeoutError)

    try:
        k8s_sdk_retry(get_retry_kwargs, should_retry=should_retry)(scrape_logs)(**base_kwargs)
    except Exception:
        logger.exception(BOINK("Pod log scraping failed utterly. Pod may have died?"))
        # at least let the caller know something went horribly wrong
        failure_callback(pod_name)
