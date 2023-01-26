"""K8s SDK watching is very unreliable for lots of reasons.

This is a general-purpose fix for using watchers in a thread reliably.

You do have to be willing to
"""
import threading
import time
import typing as ty

import urllib3
from kubernetes import watch as k8s_watch

from thds.core.log import getLogger

from .. import config
from ..colorize import colorized
from .auth import load_config

logger = getLogger(__name__)

T = ty.TypeVar("T")


class K8sList(ty.Protocol[T]):
    def __call__(self, *args, namespace: str, **kwargs) -> ty.List[T]:
        ...


# If this does not return a K8sList API method, the loop will exit
GetApiMethod = ty.Callable[[str, ty.Optional[Exception]], ty.Optional[K8sList[T]]]
# if this returns True, the loop will exit.
OnEvent = ty.Callable[[str, T], ty.Optional[bool]]


def yield_events(
    namespace: str,
    get_api_method: GetApiMethod[T],
    server_timeout: int = 10,
    **kwargs,
) -> ty.Iterator[ty.Tuple[str, T]]:
    while True:
        load_config()
        ex = None
        try:
            api_method = get_api_method(namespace, ex)
            if not api_method:
                break
            for evt in k8s_watch.Watch().stream(
                api_method,
                namespace=namespace,
                **kwargs,
                _request_timeout=(server_timeout, config.k8s_job_timeout_seconds()),
            ):
                yield namespace, evt
        except urllib3.exceptions.ProtocolError:
            pass
        except urllib3.exceptions.ReadTimeoutError:
            pass
        except Exception as e:
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


class DaemonLimiter:
    """Launch a single thread per submitted name."""

    def __init__(self):
        self._lock = threading.RLock()
        self._threads: ty.Dict[str, threading.Thread] = dict()

    def __call__(self, name: str, target: ty.Callable, args: ty.Tuple[ty.Any, ...]):
        """Launch a daemon thread if this name is not already running."""
        if name in self._threads:
            return
        with self._lock:
            if name in self._threads:
                return
            self._threads[name] = threading.Thread(
                target=target,
                args=args,
                daemon=True,
            )
            self._threads[name].start()


class WatchingObjectSource(ty.Generic[T]):
    """Efficiently 'get' objects by reliably watching for changes to all such objects in a given namespace.

    This is network-efficient for observing many different objects,
    but not memory efficient if you really only need to fetch details
    for a few objects.
    """

    def __init__(
        self,
        get_api_method: GetApiMethod[T],
        get_name: ty.Callable[[T], str] = ty.cast(  # noqa: B008
            ty.Callable[[T], str], _default_get_name
        ),
        backup_fetch: ty.Optional[ty.Callable[[str, str], T]] = None,
        typename: str = "obj",
        starting: ty.Callable[[str], str] = STARTING,
    ):
        self.get_api_method = get_api_method
        self.get_name = get_name
        self.backup_fetch = backup_fetch
        self.typename = typename
        self._objs_by_name: ty.Dict[str, T] = dict()
        self._limit_daemons = DaemonLimiter()

    def _start(self, namespace: str):
        self._limit_daemons(
            namespace,
            target=callback_events,
            args=(
                self._add_object,
                yield_events(namespace, self._get_api_method_on_restart),
            ),
        )

    def _add_object(self, namespace: str, evt: ty.Any):
        obj = evt["object"]
        assert obj
        name = _make_name(namespace, self.get_name(obj))
        logger.debug(f"{self.typename} {name} updated")
        self._objs_by_name[name] = obj

    def _get_api_method_on_restart(self, namespace: str, exc: ty.Optional[Exception]):
        if exc:
            logger.exception(f"Sleeping before we retry {self.typename} scraping...")
            time.sleep(config.k8s_monitor_delay())
        STARTING(
            f"Watching {self.typename}s in namespace: {namespace}" + (" after {exc}" if exc else "")
        )
        return self.get_api_method(namespace, exc)

    def get(self, obj_name: str, namespace: str = "") -> ty.Optional[T]:
        namespace = namespace or config.k8s_namespace()
        name = _make_name(namespace, obj_name)
        if name in self._objs_by_name:
            return self._objs_by_name[name]

        self._start(namespace)
        time.sleep(config.k8s_monitor_delay())
        if name in self._objs_by_name:
            return self._objs_by_name[name]

        if self.backup_fetch:
            logger.info(f"Fetching {self.typename} {name} manually...")
            try:
                obj = self.backup_fetch(namespace, obj_name)
                if obj:
                    return self._add_object(namespace, obj)
            except Exception:
                pass
        return None
