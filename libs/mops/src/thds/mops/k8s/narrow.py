"""Narrowing for kubernetes model fields that newer client versions type as Optional."""

import typing as ty

from kubernetes import client

T = ty.TypeVar("T")


def required(value: ty.Optional[T], what: str) -> T:
    """Raise if the field is absent; callers need it populated."""
    if value is None:
        raise ValueError(f"Kubernetes object is missing {what}")

    return value


def name_of(obj: ty.Any) -> str:
    return required(required(obj.metadata, "metadata").name, "metadata.name")


def pod_template(job: client.models.V1Job) -> client.models.V1PodTemplateSpec:
    return required(required(job.spec, "spec").template, "spec.template")


def pod_spec(job: client.models.V1Job) -> client.models.V1PodSpec:
    return required(pod_template(job).spec, "spec.template.spec")


def phase_of(pod: client.models.V1Pod) -> str:
    return required(pod.status, "status").phase or ""


def namespace_of(obj: ty.Any) -> str:
    return required(required(obj.metadata, "metadata").namespace, "metadata.namespace")
