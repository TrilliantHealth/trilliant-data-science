"""Provides an abstraction for launching Docker images on Kubernetes and waiting until they finish."""

import os
import threading
import typing as ty
import uuid

from kubernetes import client

from thds.core import scope
from thds.core.log import logger_context
from thds.mops.pure.pickling.memoize_only import _threadlocal_shell

from .._utils.colorize import colorized
from . import config
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


def autocr(container_image_name: str, cr_url: str = "") -> str:
    """Prefix the container with the configured container registry URL.

    Idempotent, so it will not apply if called a second time.
    """
    cr_url = cr_url or config.k8s_acr_url()
    assert cr_url, "No container registry URL configured."
    prefix = cr_url + "/" if cr_url and not cr_url.endswith("/") else cr_url
    if not container_image_name.startswith(prefix):
        return prefix + container_image_name
    return container_image_name


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
    node_narrowing: ty.Optional[NodeNarrowing] = None,
    container_name: str = "jobcontainer",
    env_vars: ty.Optional[ty.Mapping[str, str]] = None,
    # arguments below are for launching; arguments above are for
    # building.  these should get separated in a future change.
    name_prefix: str = "",
    dry_run: bool = False,
    fire_and_forget: bool = False,
    suppress_logs: bool = False,
    transform_job: ty.Callable[[client.models.V1Job], client.models.V1Job] = lambda x: x,
    service_account_name: str = "",
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
    if not container_image:
        raise ValueError("container_image (the fully qualified Docker tag) must not be empty.")
    job_num = f"{_LAUNCH_COUNT.inc():0>3}"
    name = "-".join([name_prefix, str(os.getpid()), job_num, str(uuid.uuid4())[:8]]).lstrip("-")
    scope.enter(logger_context(job=name))
    node_narrowing = node_narrowing or dict()

    # TODO move this entire function out to be separately callable
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

        use_azure_workload_identity = (
            config.k8s_namespace() in config.namespaces_supporting_workload_identity()
        )
        if use_azure_workload_identity:
            logger.debug(
                "Using Azure Workload Identity," " which is the most reliable form of auth as of Q1 2023"
            )
            labels = {"azure.workload.identity/use": "true"}
            # this is in fact supposed to be string 'true', not value True.
            # https://azure.github.io/azure-workload-identity/docs/quick-start.html#7-deploy-workload
        elif config.aad_pod_managed_identity():
            logger.debug("Adding AAD pod managed identity")
            labels = {"aadpodidbinding": config.aad_pod_managed_identity()}
        else:
            logger.warning(
                "No automatic Azure identity being assigned. This might cause authorization issues"
            )
            labels = dict()

        template.template = client.V1PodTemplateSpec(metadata=client.V1ObjectMeta(labels=labels))

        logger.debug("Applying environment variables ...")
        env_list = [
            client.V1EnvVar(name="MOPS_IMAGE_FULL_TAG", value=container_image),
            client.V1EnvVar(name="K8S_NAMESPACE", value=config.k8s_namespace()),
            # by setting these, things will be 'reentrant' if it is necessary to launch jobs within this job.
        ]
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
        if service_account_name:
            template.template.spec.service_account_name = service_account_name
        elif use_azure_workload_identity:
            template.template.spec.service_account_name = "ds-standard"

        logger.debug("Creating job definition ...")
        body.spec = client.V1JobSpec(
            backoff_limit=config.k8s_job_retry_count(),
            completions=1,
            ttl_seconds_after_finished=config.k8s_job_cleanup_ttl_seconds_after_completion(),
            template=template.template,
        )
        logger.debug("Finished creating job definition ...")
        return body

    body = transform_job(assemble_job())

    if dry_run:
        logger.info("Dry run assembly successful; not launching...")
        return

    @k8s_sdk_retry()
    def launch_job() -> client.models.V1Job:
        with _SIMULTANEOUS_LAUNCHES:
            upsert_namespace(config.k8s_namespace())
            return client.BatchV1Api().create_namespaced_job(namespace=config.k8s_namespace(), body=body)

    job = launch_job()
    logger.info(LAUNCHED(f"Job {job_num} launched!") + f" on {container_image}")
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


def mops_shell(
    container_image: ty.Union[str, ty.Callable[[], str]],
    disable_remote: ty.Callable[[], bool] = lambda: False,
    **outer_kwargs,
) -> ty.Callable[[ty.Sequence[str]], None]:
    """Return a closure that can launch the given configuration and run a mops pure function.

    Now supports callables that return a container image name; the
    goal being to allow applications to perform this lazily on the
    first actual use of the k8s shell. The passed callable will be
    called each time, so if you want it to be called only once, you'll
    need to wrap it yourself.

    Supports an optional callable argument `disable_remote` which when evaluated to True
    causes the mops pure function to be run in a local shell.
    """
    assert (
        "args" not in outer_kwargs
    ), "Passing 'args' as a keyword argument will cause conflicts with the closure."

    if disable_remote():
        return _threadlocal_shell

    if isinstance(container_image, str):
        get_container_image = lambda: container_image  # noqa: E731
    else:
        get_container_image = container_image

    def launch_container_on_k8s_with_args(args: ty.Sequence[str], **inner_kwargs):
        assert "args" not in inner_kwargs
        launch(
            get_container_image(),
            ["python", "-m", "thds.mops.pure.core.entry.main", *args],
            **{**outer_kwargs, **inner_kwargs},
        )

    return launch_container_on_k8s_with_args
