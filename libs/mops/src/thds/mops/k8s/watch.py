"""K8s SDK watching is very unreliable for lots of reasons.

This is a general-purpose fix for using watchers in a thread reliably.
"""

import queue
import threading
import time
import typing as ty

import urllib3
from kubernetes import client
from kubernetes import watch as k8s_watch

from thds.core import futures, scope
from thds.core.log import getLogger, logger_context
from thds.termtool.colorize import colorized

from . import config
from .auth import load_config
from .too_old_resource_version import parse_too_old_resource_version
from .uncertain_future import FutureInterpreter, UncertainFuturesTracker

logger = getLogger(__name__)

T = ty.TypeVar("T")
K = ty.TypeVar("K")
R = ty.TypeVar("R")


class V1List(ty.Protocol[T]):
    api_version: str
    items: ty.List[T]
    kind: str
    metadata: client.models.V1ListMeta


class K8sList(ty.Protocol[T]):
    def __call__(self, *args: ty.Any, namespace: str, **kwargs: ty.Any) -> V1List[T]:
        ...


# If this does not return a K8sList API method, the loop will exit
GetListMethod = ty.Callable[[str, ty.Optional[Exception]], ty.Optional[K8sList[T]]]
# if this returns True, the loop will exit.
EventType = ty.Literal["FETCH", "ADDED", "MODIFIED", "DELETED"]
OnEvent = ty.Callable[[str, T, EventType], ty.Optional[bool]]


def yield_objects_from_list(
    namespace: str,
    get_list_method: GetListMethod[T],
    *,
    server_timeout: int = config.k8s_watch_server_timeout_seconds(),
    connection_timeout: int = config.k8s_watch_connection_timeout_seconds(),
    read_timeout: int = config.k8s_watch_read_timeout_seconds(),
    # connection and read timeout should generally be fairly aggressive so that we retry
    # quickly if we don't hear anything for a while, and the config defaults are.
    object_type_hint: str = "items",
    init: ty.Optional[ty.Callable[[], None]] = None,
    **kwargs: ty.Any,
) -> ty.Iterator[ty.Tuple[str, T, EventType]]:
    ex = None
    if init:
        init()
    while True:
        try:
            load_config()
            list_method = get_list_method(namespace, ex)
            if not list_method:
                logger.debug(f"Stopped watching {object_type_hint} events in namespace: {namespace}")
                return

            initial_list = list_method(namespace=namespace)
            logger.debug(
                f"Listed {len(initial_list.items)} {object_type_hint} in namespace: {namespace}"
            )
            for object in initial_list.items:
                yield namespace, object, "FETCH"

            if initial_list.metadata._continue:
                logger.warning(
                    f"We did not fetch the whole list of {object_type_hint} the first time..."
                )
            for evt in k8s_watch.Watch().stream(
                list_method,
                namespace=namespace,
                resource_version=initial_list.metadata.resource_version,
                **kwargs,
                timeout_seconds=server_timeout,
                _request_timeout=(connection_timeout, read_timeout),
            ):
                object = evt.get("object")
                if object:
                    yield namespace, object, evt["type"]
                # once we've received events, let the resource version
                # be managed automatically if possible.
        except urllib3.exceptions.ProtocolError:
            ex = None
        except urllib3.exceptions.ReadTimeoutError:
            ex = None
        except Exception as e:
            too_old = parse_too_old_resource_version(e)
            if too_old:
                logger.debug(f"Immediately retrying {too_old}")
            else:
                logger.exception(f"Unexpected exception while listing {object_type_hint}")
            ex = e


def callback_events(
    on_event: OnEvent[T], event_yielder: ty.Iterable[ty.Tuple[str, T, EventType]]
) -> None:
    """Suitable for use with a daemon thread."""
    for namespace, obj, event in event_yielder:
        should_exit = on_event(namespace, obj, event)
        if should_exit:
            break


def _default_get_name(obj: ty.Any) -> str:
    return obj.metadata.name


def _default_get_namespace(obj: ty.Any) -> str:
    return obj.metadata.namespace


STARTING = colorized(fg="black", bg="orange")


class OneShotLimiter:
    """Do an action once per provided name. Does not wait for it to complete."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._names: ty.Set[str] = set()

    def __call__(self, name: str, shoot: ty.Callable[[str], ty.Any]) -> None:
        """Shoot if the name has not already been shot."""
        if name in self._names:
            return
        with self._lock:
            if name in self._names:
                return
            shoot(name)
            self._names.add(name)


def _watch_timer() -> float:
    # in this context, monotonicity (actual timing) is most useful because we don't need sentinels.
    return time.monotonic()


def is_stale(api_last_update_time: float, obj_last_seen_time: float) -> bool:
    now = _watch_timer()
    allowed_stale_seconds = config.k8s_watch_object_stale_seconds()
    # about 5 minutes by default as of 2025-07-15.
    if (time_since_api_update := now - api_last_update_time) > allowed_stale_seconds:  # noqa: F841
        # we haven't heard anything from the API in a while; probably
        # the API is down. Ignore object staleness to avoid false positives.
        return False

    if not obj_last_seen_time:
        return False  # false positives aren't worth it

    return (time_since_obj_update := now - obj_last_seen_time) > allowed_stale_seconds  # noqa: F841


def _wrap_get_list_method_with_too_old_check(
    typename: str,
    get_list_method: GetListMethod[T],
) -> GetListMethod[T]:
    def wrapped_get_list_method(namespace: str, exc: ty.Optional[Exception]) -> ty.Optional[K8sList[T]]:
        suffix = ""
        if exc:
            too_old = parse_too_old_resource_version(exc)
            if not too_old:
                logger.exception(f"Not fatal, but sleeping before we retry {typename} scraping...")
                time.sleep(config.k8s_monitor_delay())
                suffix = f" after {type(exc).__name__}: {exc}"
                logger.info(f"Watching {typename}s in namespace: {namespace}{suffix}")
        return get_list_method(namespace, exc)

    return wrapped_get_list_method


def create_watch_thread(
    get_list_method: GetListMethod[T],
    callback: ty.Callable[[str, T, EventType], None],
    namespace: str,
    *,
    typename: str = "object",
) -> threading.Thread:
    return threading.Thread(
        target=callback_events,
        args=(
            callback,
            yield_objects_from_list(
                namespace,
                _wrap_get_list_method_with_too_old_check(typename, get_list_method),
                # arguably this wrapper could be composed externally, but i see no use cases so far where we'd want that.
                object_type_hint=typename + "s",
                init=lambda: logger.info(STARTING(f"Watching {typename}s in {namespace}")),
            ),
        ),
        daemon=True,
    )


def watch_forever(
    get_list_method: GetListMethod[T],
    namespace: str,
    *,
    typename: str = "object",
    timeout: ty.Optional[int] = None,
) -> ty.Iterator[ty.Tuple[T, EventType]]:
    q: queue.Queue[ty.Tuple[T, EventType]] = queue.Queue()

    def put_queue(namespace: str, obj: T, event_type: EventType) -> None:
        q.put((obj, event_type))

    create_watch_thread(get_list_method, put_queue, namespace, typename=typename).start()
    while True:
        try:
            yield q.get(timeout=timeout)
        except queue.Empty:
            break


class _SeenObjectContainer(ty.Generic[K, T]):
    """Splits some of the logic for 'get' out of WatchingObjectSource
    so that we can have it be a simpler container for both this and the UncertainFuturesTracker.
    """

    def __init__(
        self,
        backup_fetch: ty.Optional[ty.Callable[[K], ty.Optional[T]]] = None,
    ) -> None:
        self._objs: ty.Dict[K, T] = dict()
        # ^ is a possibly big/expensive local cache of the most recent
        # state for all of the event type in the namespace.  Don't use
        # this class if you can't afford the memory overhead of
        # observing everything in your namespace and keeping the last
        # known copy of everything forever.
        self._last_seen_times: ty.Dict[K, float] = dict()
        self._last_api_update_time = 0.0
        self.backup_fetch = backup_fetch

    def set_object(self, key: K, obj: T) -> None:
        """Set an object in the cache, updating the last seen time."""
        now = _watch_timer()
        self._last_api_update_time = now
        self._last_seen_times[key] = now
        self._objs[key] = obj

    def _is_stale(self, key: K) -> bool:
        return is_stale(self._last_api_update_time, self._last_seen_times.get(key) or 0)

    def get(self, key: K) -> ty.Optional[T]:
        # first try is looking in our local cache
        if (obj := self._objs.get(key)) and not self._is_stale(key):
            return obj

        # second try is making sure the namespace watcher is running, sleeping, and then looking in the cache again.
        # This is much more efficient than a manual fetch.
        time.sleep(config.k8s_monitor_delay())
        if (obj := self._objs.get(key)) and not self._is_stale(key):
            return obj

        # if that doesn't work, try a manual fetch.
        if self.backup_fetch:
            logger.warning(f"Manually fetching {key}...")
            # doing a lot of manual fetches may indicate that the k8s API is having trouble keeping up...
            try:
                if obj := self.backup_fetch(key):
                    self.set_object(key, obj)  # updates last seen, too
                    return obj

            except Exception:
                logger.exception(f"Unexpected error during manual fetch of {key}.")

        if self._is_stale(key):
            logger.warning(
                f"Could not refresh {key}, and our record of it is stale - dropping stale object!"
            )
            self._objs.pop(key, None)
            self._last_seen_times.pop(key, None)

        return None


class WatchingObjectSource(ty.Generic[T]):
    """Efficiently 'get' objects by launching a single thread to
    watch for changes to all such objects in a given namespace.

    Also provide a way to create a future that will be resolved according to the logic
    provided by the caller whenever an object is updated, or if the object has not been
    updated in a while.

    Importantly, the Futures are only prevented from deadlocking (never awakening their
    condition variable) by the fact that we very occasionally will go through the list
    of seen objects and raise Exceptions for objects that have not been updated in a while.
    This is vaguely akin to garbage collection, in that it will occasionally
    cause a 'pause' in the watcher thread as it tries to collect stale objects.

    This is network-efficient for observing many different objects,
    but not memory efficient if you really only need to fetch details
    for a few objects, because we retain the last known state for every observed object indefinitely.
    """

    def __init__(
        self,
        get_list_method: GetListMethod[T],
        get_name: ty.Callable[[T], str] = ty.cast(  # noqa: B008
            ty.Callable[[T], str], _default_get_name
        ),
        backup_fetch: ty.Optional[ty.Callable[[str, str], ty.Optional[T]]] = None,
        typename: str = "object",
    ) -> None:
        self.get_list_method = get_list_method
        self.get_name = get_name
        self.typename = typename
        self._limiter = OneShotLimiter()
        self._uncertain_futures = UncertainFuturesTracker[tuple[str, str], T](
            config.k8s_watch_object_stale_seconds()
        )
        self._seen_objects = _SeenObjectContainer[tuple[str, str], T](
            lambda namespace_and_name: backup_fetch(*namespace_and_name) if backup_fetch else None
        )

    def _add_object(self, namespace: str, obj: T, _event_type: EventType) -> None:
        """This is where we receive updates from the k8s API."""
        if not obj:
            logger.warning(f"Received null/empty {self.typename}")
            return

        key = (namespace, self.get_name(obj))
        self._seen_objects.set_object(key, obj)
        self._uncertain_futures.update(key, obj)
        logger.debug("%s %s updated", self.typename, key)

    def _start_namespace_watcher_thread(self, namespace: str) -> None:
        create_watch_thread(
            self.get_list_method, self._add_object, namespace, typename=self.typename
        ).start()

    @scope.bound
    def get(self, obj_name: str, namespace: str = "") -> ty.Optional[T]:
        """May block for a little while if a manual fetch is required."""
        namespace = namespace or config.k8s_namespace()
        scope.enter(logger_context(name=obj_name, namespace=namespace))
        self._limiter(namespace, self._start_namespace_watcher_thread)
        return self._seen_objects.get((namespace, obj_name))

    def create_future(
        self,
        interpreter: FutureInterpreter[T, R],
        obj_name: str,
        *,
        namespace: str = "",
    ) -> futures.PFuture[R]:
        """Create a future that will be resolved when the object is available according to
        the interpreter.

        The FutureInterpreter must:
         - raise an exception if it wishes the future to raise.
         - return a Done with the result if it wishes the future to resolve successfully.
          -return None if the status is still in progress.
        """
        namespace = namespace or config.k8s_namespace()
        self._limiter(namespace, self._start_namespace_watcher_thread)
        return self._uncertain_futures.create((namespace, obj_name), interpreter)
