"""Cloud-neutral values exchanged by Kubernetes cost providers."""

import datetime as dt
import typing as ty

from kubernetes import client


class Node(ty.NamedTuple):
    name: str
    uid: str
    provider: str
    allocation_group: str
    instance_type: str
    region: str
    purchase_option: str
    created_at: dt.datetime


class Price(ty.NamedTuple):
    hourly_rate: None | float
    currency: str
    source: str
    error: str = ""
    hourly_rate_candidates: tuple[float, ...] = ()


class Provider(ty.NamedTuple):
    """Cloud-specific node classification and pricing functions."""

    name: str
    node: ty.Callable[[client.V1Node], None | Node]
    price: ty.Callable[[Node], Price]


ProviderFactory = ty.Callable[[], Provider]


def identity(raw: client.V1Node) -> tuple[str, str, dt.datetime]:
    """The provider-independent identity shared by every Kubernetes node."""
    name = raw.metadata.name or ""
    created = raw.metadata.creation_timestamp or dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    if created.tzinfo is None:
        created = created.replace(tzinfo=dt.timezone.utc)
    return name, raw.metadata.uid or name, created


def unclassified(raw: client.V1Node) -> Node:
    name, uid, created = identity(raw)
    labels = raw.metadata.labels or {}
    return Node(
        name=name,
        uid=uid,
        provider="unknown",
        allocation_group="",
        instance_type=labels.get("node.kubernetes.io/instance-type", ""),
        region=labels.get("topology.kubernetes.io/region", ""),
        purchase_option="",
        created_at=created,
    )
