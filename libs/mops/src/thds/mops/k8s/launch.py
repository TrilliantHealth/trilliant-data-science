"""Provides an abstraction for launching Docker images on Kubernetes and waiting until they finish."""

import os
import threading
import typing as ty
import uuid

from kubernetes import client

from thds.core import scope
from thds.core.log import logger_context
from thds.mops.pure.runner.simple_shims import samethread_shim

from .._utils.colorize import colorized
from . import config
from ._shared import logger
from .auth import load_config, upsert_namespace
from .logging import JobLogWatcher
from .node_selection import NodeNarrowing, ResourceDefinition
from .retry import k8s_sdk_retry
from .thds_std import embed_thds_auth
from .wait_job import wait_for_job

LAUNCHED = colorized(fg="white", bg="green")
COMPLETE = colorized(fg="white", bg="blue")
FAILED = colorized(fg="white", bg="red")


class K8sJobFailedError(Exception):
    """Raised by `launch` when a Job is seen to terminate in a Failed state."""


class Counter:
    def __init__(self) -> None:
        self.value = 0
        self._lock = threading.Lock()

    def inc(self) -> int:
        with self._lock:
            self.value += 1
            return self.value


def sanitize_str(name: str) -> str:
    # you can't have anything other than lowercase alphanumeric characters or dashes.
    # you can't start with a dash. I don't know if you can end with a dash, but i see no point to that.
    return "".join([c if c.isalnum() or c == "-" else "-" for c in name.lower()]).strip("-")


def construct_job_name(user_prefix: str, job_num: str) -> str:
    # we want some consistency here, but also some randomness in case the prefixes don't exist or aren't unique.
    mops_name_part = "-".join([str(os.getpid()), sanitize_str(job_num), str(uuid.uuid4())[:8]])
    if len(mops_name_part) > 63:
        # this should be _impossible_, because having a job num longer than even 20 digits would be an impossibly large
        # number of jobs. but just in case, we'll truncate it to the last 63 characters.
        mops_name_part = mops_name_part[-63:]  # keep the most random part, to avoid collisions

    user_prefix = sanitize_str(user_prefix)
    if user_prefix:
        name = f"{user_prefix[:63 - 1 - len(mops_name_part)]}-{mops_name_part}"
    else:
        name = mops_name_part
    name = sanitize_str(name)
    assert len(name) <= 63, f"Job name `{name}` is too long ({len(name)}); max length is 63 characters."
    return name


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
    transform_job: ty.Callable[[client.models.V1Job], client.models.V1Job] = embed_thds_auth,
    # this is a default for now. later if we share this code we'll need to have a wrapper interface
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
    name = construct_job_name(name_prefix, job_num)
    scope.enter(logger_context(job=name))
    node_narrowing = node_narrowing or dict()

    # TODO move this entire function out to be separately callable
    @k8s_sdk_retry()
    def assemble_base_job() -> client.models.V1Job:
        logger.debug(f"Assembling job named `{name}` on image `{container_image}`")
        logger.debug("Fire and forget: %s", fire_and_forget)
        logger.debug("Loading kube configs ...")
        load_config()
        logger.debug("Populating job object ...")
        v1_job_body = client.V1Job(api_version="batch/v1", kind="Job")
        logger.debug("Setting object meta ...")
        v1_job_body.metadata = client.V1ObjectMeta(namespace=config.k8s_namespace(), name=name)

        v1_job_body.status = client.V1JobStatus()
        logger.debug("Creating pod template ...")
        pod_template = client.V1PodTemplate()

        pod_template.template = client.V1PodTemplateSpec(metadata=client.V1ObjectMeta(labels=dict()))
        # we make empty labels just in case a later transformer wants to add some.

        logger.debug("Applying environment variables ...")
        env_list = [
            client.V1EnvVar(name="MOPS_IMAGE_FULL_TAG", value=container_image),
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
        pod_template.template.spec = client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            node_selector=node_narrowing.get("node_selector", dict()),
            tolerations=node_narrowing.get("tolerations", list()),
            service_account_name=service_account_name,
        )

        logger.debug("Creating job definition ...")
        v1_job_body.spec = client.V1JobSpec(
            backoff_limit=config.k8s_job_retry_count(),
            completions=1,
            ttl_seconds_after_finished=config.k8s_job_cleanup_ttl_seconds_after_completion(),
            template=pod_template.template,
        )
        logger.debug("Finished creating base job definition ...")
        return v1_job_body

    def job_with_all_transforms() -> client.models.V1Job:
        return transform_job(assemble_base_job())

    if dry_run:
        job_with_all_transforms()
        logger.info("Dry run assembly successful; not launching...")
        return

    @k8s_sdk_retry()
    def launch_job() -> client.models.V1Job:
        with _SIMULTANEOUS_LAUNCHES:
            upsert_namespace(config.k8s_namespace())
            # we do the job transform after actually upserting the namespace so that
            # the transform can use the namespace if necessary.
            return client.BatchV1Api().create_namespaced_job(
                namespace=config.k8s_namespace(), body=job_with_all_transforms()
            )

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
        del job  # trying to save memory here while we wait...
        if not wait_for_job(job_name, short_name=job_num):
            logger.error(FAILED(f"Job {job_num} Failed! {counts()}"))
            raise K8sJobFailedError(f"Job {job_name} failed.")
        logger.info(COMPLETE(f"Job {job_num} Complete! {counts()}"))


def shim(
    container_image: ty.Union[str, ty.Callable[[], str]],
    disable_remote: ty.Callable[[], bool] = lambda: False,
    **outer_kwargs: ty.Any,
) -> ty.Callable[[ty.Sequence[str]], None]:
    """Return a closure that can launch the given configuration and run a mops pure function.

    Now supports callables that return a container image name; the
    goal being to allow applications to perform this lazily on the
    first actual use of the k8s runtime shim. The passed callable will be
    called each time, so if you want it to be called only once, you'll
    need to wrap it yourself.

    Supports an optional callable argument `disable_remote` which when evaluated to True
    causes the mops pure function to be run in a local shell.
    """
    assert (
        "args" not in outer_kwargs
    ), "Passing 'args' as a keyword argument will cause conflicts with the closure."

    if disable_remote():
        return samethread_shim

    if isinstance(container_image, str):
        get_container_image: ty.Callable[[], str] = lambda: container_image  # noqa: E731
    else:
        get_container_image = container_image

    def launch_container_on_k8s_with_args(args: ty.Sequence[str], **inner_kwargs: ty.Any) -> None:
        assert "args" not in inner_kwargs
        launch(
            get_container_image(),
            ["python", "-m", "thds.mops.pure.core.entry.main", *args],
            **{**outer_kwargs, **inner_kwargs},
        )

    return launch_container_on_k8s_with_args
