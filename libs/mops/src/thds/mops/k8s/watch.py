"""K8s SDK watching is very unreliable for lots of reasons.

This is a general-purpose fix for using watchers in a thread reliably.
"""
import threading
import time
import typing as ty

import urllib3
from kubernetes import client
from kubernetes import watch as k8s_watch

from thds.core import scope
from thds.core.log import getLogger, logger_context

from .. import config
from ..colorize import colorized
from .auth import load_config
from .too_old_resource_version import parse_too_old_resource_version

logger = getLogger(__name__)

T = ty.TypeVar("T")


class V1List(ty.Protocol[T]):
    api_version: str
    items: ty.List[T]
    kind: str
    metadata: client.models.V1ListMeta


class K8sList(ty.Protocol[T]):
    def __call__(self, *args, namespace: str, **kwargs) -> V1List[T]:
        ...


# If this does not return a K8sList API method, the loop will exit
GetListMethod = ty.Callable[[str, ty.Optional[Exception]], ty.Optional[K8sList[T]]]
# if this returns True, the loop will exit.
OnEvent = ty.Callable[[str, T], ty.Optional[bool]]


def yield_objects_from_list(
    namespace: str,
    get_list_method: GetListMethod[T],
    server_timeout: int = 10,
    **kwargs,
) -> ty.Iterator[ty.Tuple[str, T]]:
    ex = None
    while True:
        try:
            load_config()
            list_method = get_list_method(namespace, ex)
            if not list_method:
                logger.info("No longer yielding events")
                break
            initial_list = list_method(namespace=namespace)
            logger.info(f"Fetched {len(initial_list.items)} items from {namespace}")
            for item in initial_list.items:
                yield namespace, item
            if initial_list.metadata._continue:
                logger.warning("We did not fetch the whole list the first time...")
            for evt in k8s_watch.Watch().stream(
                list_method,
                namespace=namespace,
                resource_version=initial_list.metadata.resource_version,
                **kwargs,
                _request_timeout=(server_timeout, config.k8s_job_timeout_seconds()),
            ):
                object = evt.get("object")
                if object:
                    yield namespace, object
                # once we've received events, let the resource version
                # be managed automatically if possible.
        except urllib3.exceptions.ProtocolError:
            ex = None
        except urllib3.exceptions.ReadTimeoutError:
            ex = None
        except Exception as e:
            too_old = parse_too_old_resource_version(e)
            if too_old:
                logger.info(f"Immediately retrying {too_old}")
            else:
                logger.exception("Unexpected exception while listing objects")
            ex = e


def callback_events(on_event: OnEvent[T], event_yielder: ty.Iterable[ty.Tuple[str, T]]):
    """Suitable for use with a daemon thread."""
    for namespace, event in event_yielder:
        should_exit = on_event(namespace, event)
        if should_exit:
            break


def _make_name(namespace: str, name: str) -> str:
    return f"{namespace}/{name}"


def _default_get_name(obj: ty.Any) -> str:
    return obj.metadata.name


def _default_get_namespace(obj: ty.Any) -> str:
    return obj.metadata.namespace


STARTING = colorized(fg="white", bg="orange")


class OneShotLimiter:
    """Do an action once per provided name. Does not wait for it to complete."""

    def __init__(self):
        self._lock = threading.RLock()
        self._names: ty.Set[str] = set()

    def __call__(self, name: str, shoot: ty.Callable[[str], ty.Any]):
        """Shoot if the name has not already been shot."""
        if name in self._names:
            return
        with self._lock:
            if name in self._names:
                return
            shoot(name)
            self._names.add(name)


class WatchingObjectSource(ty.Generic[T]):
    """Efficiently 'get' objects by reliably watching for changes to all such objects in a given namespace.

    This is network-efficient for observing many different objects,
    but not memory efficient if you really only need to fetch details
    for a few objects.
    """

    def __init__(
        self,
        get_list_method: GetListMethod[T],
        get_name: ty.Callable[[T], str] = ty.cast(  # noqa: B008
            ty.Callable[[T], str], _default_get_name
        ),
        backup_fetch: ty.Optional[ty.Callable[[str, str], T]] = None,
        typename: str = "obj",
        starting: ty.Callable[[str], str] = STARTING,
    ):
        self.get_list_method = get_list_method
        self.get_name = get_name
        self.backup_fetch = backup_fetch
        self.typename = typename
        self._objs_by_name: ty.Dict[str, T] = dict()
        # ^ is a possibly big/expensive local cache of the most recent
        # state for all of the event type in the namespace.  Don't use
        # this class if you can't afford the memory overhead of
        # observing everything in your namespace and keeping the last
        # known copy of everything forever.
        self._limiter = OneShotLimiter()

    def _start_thread(self, namespace: str):
        logger.info(f"Launching {self.typename}-watching thread in {namespace}")
        threading.Thread(
            target=callback_events,
            args=(
                self._add_object,
                yield_objects_from_list(namespace, self._get_list_method_on_restart),
            ),
            daemon=True,
        ).start()

    def _add_object(self, namespace: str, obj: T):
        if not obj:
            logger.warning("Received null/empty object")
            return
        name = _make_name(namespace, self.get_name(obj))
        logger.debug(f"{self.typename} {name} updated")
        self._objs_by_name[name] = obj

    def _get_list_method_on_restart(self, namespace: str, exc: ty.Optional[Exception]):
        suffix = ""
        if exc:
            too_old = parse_too_old_resource_version(exc)
            if not too_old:
                logger.exception(f"Sleeping before we retry {self.typename} scraping...")
                time.sleep(config.k8s_monitor_delay())
                suffix = f" after {type(exc).__name__}: {exc}"
        self._objs_by_name = dict()  # clear the whole cache
        logger.info(STARTING(f"Watching {self.typename}s in namespace: {namespace}{suffix}"))
        return self.get_list_method(namespace, exc)

    @scope.bound
    def get(self, obj_name: str, namespace: str = "") -> ty.Optional[T]:
        namespace = namespace or config.k8s_namespace()
        name = _make_name(namespace, obj_name)
        scope.enter(logger_context(name=obj_name, namespace=namespace))
        if name in self._objs_by_name:
            return self._objs_by_name[name]

        self._limiter(namespace, self._start_thread)
        time.sleep(config.k8s_monitor_delay())
        if name in self._objs_by_name:
            # here
            return self._objs_by_name[name]

        if self.backup_fetch:
            logger.info(f"Manually fetching {self.typename}...")
            try:
                obj = self.backup_fetch(namespace, obj_name)
                if obj:
                    return self._add_object(namespace, obj)
            except Exception:
                logger.exception(f"Unexpected error during manual fetch of {self.typename}.")
        return None
