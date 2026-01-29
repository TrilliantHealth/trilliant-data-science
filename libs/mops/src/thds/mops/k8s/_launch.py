"""Provides an abstraction for launching Docker images on Kubernetes and waiting until they finish."""

import importlib
import os
import threading
import typing as ty
import uuid
from functools import partial

from kubernetes import client

from thds import core
from thds.mops.pure.core.metadata import EXTRA_METADATA_GENERATOR
from thds.mops.pure.runner.simple_shims import samethread_shim
from thds.termtool.colorize import colorized

from . import config, counts, job_future, logging
from ._shared import logger
from .auth import load_config, upsert_namespace
from .node_selection import NodeNarrowing, ResourceDefinition
from .retry import k8s_sdk_retry

JobTransform = ty.Callable[["client.models.V1Job"], "client.models.V1Job"]

JOB_TRANSFORM = core.config.item("mops.k8s.job_transform", default="")
# Dotted import path to a callable that transforms the V1Job before launch.
# Signature: (V1Job) -> V1Job. Used for auth embedding (e.g., Azure Workload Identity).
# If not set, the job is launched without transformation.
# TH users get this configured via east_config.toml to point to thds_std.embed_thds_auth.


def _identity_transform(v1_job_body: "client.models.V1Job") -> "client.models.V1Job":
    return v1_job_body


def _load_job_transform() -> JobTransform:
    """Load the configured job transform function, or return identity transform."""
    import_path = JOB_TRANSFORM()
    if not import_path:
        return _identity_transform

    try:
        module_path, func_name = import_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return ty.cast(JobTransform, getattr(module, func_name))
    except (ValueError, ImportError, AttributeError) as e:
        logger.warning(f"Failed to load job transform '{import_path}': {e}")
        return _identity_transform


LAUNCHED = colorized(fg="white", bg="green")


def sanitize_str(name: str) -> str:
    # you can't have anything other than lowercase alphanumeric characters or dashes.
    # you can't start with a dash. I don't know if you can end with a dash, but i see no point to that.
    return "".join([c if c.isalnum() or c == "-" else "-" for c in name.lower()]).strip("-")


def construct_job_name(user_prefix: str, job_num: str) -> str:
    # we want some consistency here, but also some randomness in case the prefixes don't exist or aren't unique.
    mops_name_part = "-".join([sanitize_str(job_num), str(uuid.uuid4())[:8]])
    if len(mops_name_part) > 63:
        # this should be _unlikely_, because having a job num longer than even 20 digits would be an impossibly large
        # number of jobs. but just in case, we'll truncate it to the last 63 characters.
        mops_name_part = mops_name_part[-63:]  # prefer the most random part, to avoid collisions

    user_prefix = sanitize_str(user_prefix)
    if user_prefix and len(mops_name_part) < 62:
        name = f"{user_prefix[:63 - 1 - len(mops_name_part)]}-{mops_name_part}"
    else:
        name = mops_name_part
    name = sanitize_str(name)
    assert len(name) <= 63, f"Job name `{name}` is too long ({len(name)}); max length is 63 characters."
    return name


_SIMULTANEOUS_LAUNCHES = threading.BoundedSemaphore(20)
JOB_NAME = core.stack_context.StackContext("job_name", "")


@core.scope.bound
def launch(  # noqa: C901
    container_image: str,
    args: ty.Sequence[str],
    *,
    node_narrowing: ty.Optional[NodeNarrowing] = None,
    container_name: str = "jobcontainer",
    env_vars: ty.Optional[ty.Mapping[str, str]] = None,
    # arguments below are for launching; arguments above are for
    # building.  these should get separated in a future change.
    name_prefix: str = "",
    full_name: str = "",
    dry_run: bool = False,
    suppress_logs: bool = False,
    transform_job: ty.Optional[JobTransform] = None,
    # If None, loads from config (mops.k8s.job_transform). TH users get embed_thds_auth via
    # east_config.toml. OSS users can configure their own or leave empty for no transform.
    service_account_name: str = "",
) -> core.futures.LazyFuture[bool]:
    """Launch a Kubernetes job.

    Required parameters are the container_image and the arguments to
    that image, just as if you were running this directly with Docker.

    Returns a Future that will resolve to True when the Job completes successfully, or
    raise K8sJobFailedError if the Job fails.

    `name_prefix` is an optional parameter for debugging/developer
    convenience. A generated suffix will be added to it.
    """
    if not container_image:
        raise ValueError("container_image (the fully qualified Docker tag) must not be empty.")

    full_name = full_name or JOB_NAME()
    # in certain cases, it may be necessary to set the job name
    # via a StackContext, so we check that here, and prefer it over name_prefix.

    if full_name and name_prefix:
        raise ValueError("You cannot specify both full_name and name_prefix; use one or the other.")

    if not full_name:
        name = construct_job_name(
            "-".join([name_prefix, str(os.getpid())]), counts.to_name(counts.inc(counts.LAUNCH_COUNT))
        )
    else:
        name = full_name

    core.scope.enter(core.log.logger_context(job=name))
    node_narrowing = node_narrowing or dict()

    # TODO move this entire function out to be separately callable
    @k8s_sdk_retry()
    def assemble_base_job() -> client.models.V1Job:
        logger.debug(f"Assembling job named `{name}` on image `{container_image}`")
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
            client.V1EnvVar(name="MOPS_IMAGE_RECURSIVE_REF", value=container_image),
            # by setting these, things will be 'reentrant' if it is necessary to launch jobs within this job.
        ]

        def add_env_var(name: str, value: ty.Any) -> None:
            env_list.append(client.V1EnvVar(name=name, value=str(value)))

        if env_vars is not None:
            for env_name, env_value in env_vars.items():
                add_env_var(env_name, env_value)

        add_env_var(config.k8s_namespace_env_var_key(), config.k8s_namespace())
        add_env_var("MOPS_K8S_JOB_NAME", name)
        # Pass the extra_metadata_generator config to the remote side via env var.
        # This is needed because east_config.toml is only loaded on the local side
        # (when thds_std is imported). Only set if configured - OSS users won't have this.
        if extra_meta_gen := EXTRA_METADATA_GENERATOR():
            add_env_var("MOPS_METADATA_EXTRA_GENERATOR", extra_meta_gen)

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
            if resource_requests:
                if cpu_request := resource_requests.get("cpu"):
                    add_env_var(core.cpus.GUARANTEE.envname, cpu_request)
                if memory_request := resource_requests.get("memory"):
                    add_env_var("MOPS_K8S_MEMORY_GUARANTEE", memory_request)
            if resource_limits:
                if cpu_limit := resource_limits.get("cpu"):
                    add_env_var(core.cpus.LIMIT.envname, cpu_limit)
                if memory_limit := resource_limits.get("memory"):
                    add_env_var("MOPS_K8S_MEMORY_LIMIT", memory_limit)

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
        actual_transform = transform_job if transform_job is not None else _load_job_transform()
        return actual_transform(assemble_base_job())

    if dry_run:
        job_with_all_transforms()
        logger.info("Dry run assembly successful; not launching...")
        return core.futures.LazyFuture(partial(core.futures.ResolvedFuture, True))

    @k8s_sdk_retry()
    def launch_job() -> client.models.V1Job:
        with _SIMULTANEOUS_LAUNCHES:
            # This ensures the config is loaded before the the batch API client is created.
            load_config()
            upsert_namespace(config.k8s_namespace())
            # we do the job transform after actually upserting the namespace so that
            # the transform can use the namespace if necessary.
            return client.BatchV1Api().create_namespaced_job(
                namespace=config.k8s_namespace(), body=job_with_all_transforms()
            )

    job = launch_job()
    logger.info(LAUNCHED(f"Job {name} launched!") + f" on {container_image}")
    return core.futures.make_lazy(_launch_logs_and_create_future)(  # see below for implementation
        job.metadata.name,
        num_pods_expected=len(job.spec.template.spec.containers),
        namespace=config.k8s_namespace(),
        suppress_logs=suppress_logs,
    )


# this function has to be a top level def because it will sometimes be transferred across process boundaries,
# and Python/pickle in its infinite wisdom does not allow nested functions to be pickled.
def _launch_logs_and_create_future(
    job_name: str, *, num_pods_expected: int, namespace: str, suppress_logs: bool
) -> core.futures.PFuture[bool]:
    if not suppress_logs:
        logging.maybe_start_job_thread(job_name, num_pods_expected)
    return job_future.make_job_completion_future(job_name, namespace=namespace)


def create_lazy_job_logging_future(
    job_name: str, *, namespace: str = "", num_pods_expected: int = 1
) -> core.futures.LazyFuture[bool]:
    return core.futures.make_lazy(_launch_logs_and_create_future)(
        job_name,
        num_pods_expected=num_pods_expected,
        namespace=namespace or config.k8s_namespace(),
        suppress_logs=False,
    )


def shim(
    container_image: ty.Union[str, ty.Callable[[], str]],
    disable_remote: ty.Callable[[], bool] = lambda: False,
    **outer_kwargs: ty.Any,
) -> ty.Callable[[ty.Sequence[str]], core.futures.LazyFuture[bool]]:
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
        return samethread_shim  # type: ignore[return-value]

    if isinstance(container_image, str):
        get_container_image: ty.Callable[[], str] = lambda: container_image  # noqa: E731
    else:
        get_container_image = container_image

    def launch_container_on_k8s_with_args(
        args: ty.Sequence[str], **inner_kwargs: ty.Any
    ) -> core.futures.LazyFuture[bool]:
        assert "args" not in inner_kwargs
        return launch(
            get_container_image(),
            ["python", "-m", "thds.mops.pure.core.entry.main", *args],
            **{**outer_kwargs, **inner_kwargs},
        )

    return launch_container_on_k8s_with_args
