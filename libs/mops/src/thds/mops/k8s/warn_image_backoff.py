import threading
import typing as ty
from datetime import datetime, timezone

from kubernetes import client

from thds.core.log import getLogger
from thds.termtool.colorize import colorized

from . import auth
from .target import K8sTarget, resolve_target
from .watch import K8sList, OneShotLimiter, yield_objects_from_list

logger = getLogger(__name__)

OnCoreEvent = ty.Callable[[client.CoreV1Event], ty.Any]

YIKES = colorized(fg="black", bg="yellow")


def _emit_basic(event: client.CoreV1Event) -> None:
    logger.error(YIKES(event.message))


def _warn_image_pull_backoff(target: K8sTarget, on_backoff: OnCoreEvent = _emit_basic) -> None:
    """Log scary errors when ImagePullBackoff is observed."""
    start_dt = datetime.now(tz=timezone.utc)
    for _target, obj, _event_type in yield_objects_from_list(
        target,
        lambda target_, __: ty.cast(
            # do NOT use client.EventsV1Api here - for some reason
            # it does not return the right 'types' of events.
            # why? who the heck knows? How much time did I spend
            # trying to figure this out? Also who knows.
            K8sList[client.CoreV1Event],
            client.CoreV1Api(
                api_client=auth.api_client(target_.kubeconfig_context)
            ).list_namespaced_event,
        ),
        object_type_hint="backoff-warnings",
        field_selector="reason=BackOff",
    ):
        if None is obj.last_timestamp or obj.last_timestamp > start_dt:
            on_backoff(obj)


_WARN_IMAGE_PULL_BACKOFF = OneShotLimiter()


def start_warn_image_pull_backoff_thread(
    namespace: str = "",
    on_backoff: ty.Optional[OnCoreEvent] = None,
    kubeconfig_context: str = "",
) -> None:
    """Limit 1 thread per (cluster, namespace) per application.

    You can pass an additional message context
    """
    target = resolve_target(kubeconfig_context or None, namespace or None)

    _WARN_IMAGE_PULL_BACKOFF(
        target,
        lambda t: threading.Thread(
            target=_warn_image_pull_backoff,
            args=(target, on_backoff or _emit_basic),
            daemon=True,
        ).start(),
    )
