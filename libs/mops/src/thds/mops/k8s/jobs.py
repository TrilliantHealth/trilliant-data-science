import threading
import time
import typing as ty

import urllib3.exceptions
from kubernetes import client, watch

from .. import config
from ..colorize import colorized
from ._shared import logger
from .auth import load_config
from .retry import k8s_sdk_retry

STARTING = colorized(fg="white", bg="orange")


@k8s_sdk_retry()
def _get_job(namespace: str, job_name: str) -> ty.Optional[client.models.V1Job]:
    logger.debug(f"Reading job {job_name}")
    return client.BatchV1Api().read_namespaced_job(
        namespace=namespace,
        name=job_name,
    )


def _name(namespace: str, job_name: str) -> str:
    return f"{namespace}/{job_name}"


class JobSource:
    def __init__(self):
        self._jobs_by_name: ty.Dict[str, client.models.V1Job] = dict()
        self._lock = threading.RLock()
        self._threads: ty.Dict[str, threading.Thread] = dict()

    def _start(self, namespace: str):
        if namespace not in self._threads:
            with self._lock:
                if namespace not in self._threads:
                    self._threads[namespace] = threading.Thread(
                        target=self._watcher_thread, args=(namespace,), daemon=True
                    )
                    self._threads[namespace].start()

    def _add_job(self, job: client.models.V1Job) -> client.models.V1Job:
        assert job
        name = _name(job.metadata.namespace, job.metadata.name)
        with self._lock:
            logger.debug(f"Job {name} updated")
            self._jobs_by_name[name] = job

    def _watcher_thread(self, namespace: str):
        while True:  # is a daemon thread and never exits once started
            load_config()
            try:
                logger.info(STARTING(f"Watching jobs in namespace: {namespace}"))
                for e in watch.Watch().stream(
                    client.BatchV1Api().list_namespaced_job,
                    namespace=namespace,
                    _request_timeout=(10, config.k8s_job_timeout_seconds()),
                ):
                    self._add_job(e["object"])
            except urllib3.exceptions.ProtocolError:
                pass
            except Exception:
                logger.exception("Sleeping before we retry Job scraping...")
                time.sleep(config.k8s_monitor_delay())

    def get_job(self, job_name: str) -> ty.Optional[client.models.V1Job]:
        namespace = config.k8s_namespace()
        name = _name(namespace, job_name)
        if name in self._jobs_by_name:
            return self._jobs_by_name[name]

        self._start(namespace)
        time.sleep(config.k8s_monitor_delay())
        if name in self._jobs_by_name:
            return self._jobs_by_name[name]

        logger.info(f"Fetching job {name} manually...")
        try:
            job = _get_job(namespace, job_name)
            if job:
                return self._add_job(job)
        except Exception:
            pass
        return None


_JOB_SOURCE = JobSource()


def get_job(job_name: str) -> ty.Optional[client.models.V1Job]:
    return _JOB_SOURCE.get_job(job_name)
