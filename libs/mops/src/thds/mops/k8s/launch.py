"""Provides an abstraction for launching Docker images on Kubernetes and waiting until they finish."""
import os
import threading
import typing as ty
import uuid

from kubernetes import client

from thds.core import scope
from thds.core.log import logger_context

from .. import config
from ..colorize import colorized
from ._shared import logger
from .auth import load_config, upsert_namespace
from .logging import JobLogWatcher
from .node_selection import NodeNarrowing, ResourceDefinition
from .retry import k8s_sdk_retry
from .wait_job import wait_for_job

LAUNCHED = colorized(fg="white", bg="green")
COMPLETE = colorized(fg="white", bg="blue")
FAILED = colorized(fg="white", bg="red")


class K8sJobFailedError(Exception):
    """Raised by `launch` when a Job is seen to terminate in a Failed state."""


def autocr(container_image_name: str) -> str:
    """Prefix the container with the configured container registry URL."""
    return config.acr_url() + "/" + container_image_name


class Counter:
    def __init__(self):
        self.value = 0
        self._lock = threading.Lock()

    def inc(self) -> int:
        with self._lock:
            self.value += 1
            return self.value


_LAUNCH_COUNT = Counter()
_FINISH_COUNT = Counter()
_SIMULTANEOUS_LAUNCHES = threading.BoundedSemaphore(20)


@scope.bound
def launch(
    container_image: str,
    args: ty.Sequence[str],
    *,
    name_prefix: str = "",
    node_narrowing: ty.Optional[NodeNarrowing] = None,
    dry_run: bool = False,
    fire_and_forget: bool = False,
    suppress_logs: bool = False,
    container_name: str = "jobcontainer",
    env_vars: ty.Optional[ty.Mapping[str, str]] = None,
) -> None:
    """Launch a Kubernetes job.

    Required parameters are the container_image and the arguments to
    that image, just as if you were running this directly with Docker.

    Unless fire_and_forget=True, will poll until Job completes and
    will raise K8sJobFailedError if the Job fails. None is returned
    if the Job succeeds.

    `name_prefix` is an optional parameter for debugging/developer
    convenience. A generated suffix will be added to it.

    """
    job_num = f"{_LAUNCH_COUNT.inc():0>3}"
    name = "-".join([name_prefix, str(os.getpid()), job_num, str(uuid.uuid4())[:8]])
    scope.enter(logger_context(job=name))
    node_narrowing = node_narrowing or dict()

    @k8s_sdk_retry()
    def assemble_job() -> client.models.V1Job:
        logger.debug(f"Assembling job named `{name}` on image `{container_image}`")
        logger.debug("Fire and forget: %s", fire_and_forget)
        logger.debug("Loading kube configs ...")
        load_config()
        logger.debug("Populating job object ...")
        body = client.V1Job(api_version="batch/v1", kind="Job")
        logger.debug("Setting object meta ...")
        body.metadata = client.V1ObjectMeta(namespace=config.k8s_namespace(), name=name)

        body.status = client.V1JobStatus()
        logger.debug("Creating pod template ...")
        template = client.V1PodTemplate()

        labels = dict()
        if config.aad_pod_managed_identity():
            logger.debug("Adding AAD pod managed identity")
            labels["aadpodidbinding"] = config.aad_pod_managed_identity()
        template.template = client.V1PodTemplateSpec(metadata=client.V1ObjectMeta(labels=labels))

        logger.debug("Applying environment variables ...")
        env_list = []
        if env_vars is not None:
            for env_name, env_value in env_vars.items():
                env_list.append(client.V1EnvVar(name=env_name, value=env_value))
        env_list.append(
            client.V1EnvVar(name=config.k8s_namespace_env_var_key(), value=config.k8s_namespace())
        )

        logger.debug("Creating container definition ...")
        logger.debug("Setting container CPU/RAM requirements ...")
        v1_container_args = dict(
            args=args,
            name=container_name,
            image=container_image,
            env=env_list,
            image_pull_policy="Always",  # default is IfNotPresent, which leads to staleness when reusing a tag.
            # https://kubernetes.io/docs/concepts/containers/images/#updating-images
        )

        assert node_narrowing is not None
        resource_requests: ResourceDefinition = node_narrowing.get("resource_requests", dict())
        resource_limits: ResourceDefinition = node_narrowing.get("resource_limits", dict())
        if resource_requests or resource_limits:
            v1_container_args["resources"] = client.V1ResourceRequirements(
                requests=resource_requests,
                limits=resource_limits,
            )

        container = client.V1Container(**v1_container_args)
        logger.debug("Creating podspec definition ...")
        template.template.spec = client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            node_selector=node_narrowing.get("node_selector", dict()),
            tolerations=node_narrowing.get("tolerations", list()),
        )
        logger.debug("Creating job definition ...")
        body.spec = client.V1JobSpec(
            backoff_limit=config.k8s_job_retry_count(),
            completions=1,
            ttl_seconds_after_finished=config.k8s_job_cleanup_ttl_seconds_after_completion(),
            template=template.template,
        )
        logger.debug("Finished creating job definition ...")
        return body

    body = assemble_job()

    if dry_run:
        logger.info("Dry run assembly successful; not launching...")
        return

    @k8s_sdk_retry()
    def launch_job() -> client.models.V1Job:
        with _SIMULTANEOUS_LAUNCHES:
            upsert_namespace(config.k8s_namespace())
            return client.BatchV1Api().create_namespaced_job(namespace=config.k8s_namespace(), body=body)

    job = launch_job()
    logger.info(LAUNCHED(f"Job {job_num} launched!") + f" - {name} on {container_image}")
    if not suppress_logs:
        threading.Thread(  # fire and forget a log watching thread
            target=JobLogWatcher(job.metadata.name, len(job.spec.template.spec.containers)).start,
            daemon=True,
        ).start()

    if not fire_and_forget:

        def counts() -> str:
            launched = _LAUNCH_COUNT.value
            return f"- ({launched - _FINISH_COUNT.inc()} unfinished of {launched})"

        job_name = job.metadata.name
        del body, job  # trying to save memory here while we wait...
        if not wait_for_job(job_name, short_name=job_num):
            logger.error(FAILED(f"Job {job_num} Failed! {counts()}"))
            raise K8sJobFailedError(f"Job {job_name} failed.")
        logger.info(COMPLETE(f"Job {job_num} Complete! {counts()}"))


def k8s_shell(
    container_image: str,
    **outer_kwargs,
) -> ty.Callable[[ty.Sequence[str]], None]:
    """Return a closure that can launch the given configuration with specific arguments."""
    assert "args" not in outer_kwargs

    def launch_container_on_k8s_with_args(args: ty.Sequence[str], **inner_kwargs):
        assert "args" not in inner_kwargs
        launch(container_image, args, **{**outer_kwargs, **inner_kwargs})

    return launch_container_on_k8s_with_args
