"""Process-local registry of in-flight lease-owning invocations.

When submit() loses the lease race to a thread in this same process, the lease waiter
subscribes to the owner's completion and wakes the moment it settles, instead of waiting
out the timed storage-polling backoff. The registry never decides who invokes - the
blob-store lease does - and waiters keep timed polling as their backstop, so a missed or
raced lookup costs at most one backoff cycle, never correctness.

Entries are weakly held: an owner future abandoned without ever resolving is
garbage-collected and its entry evaporates, so the registry cannot pin results or grow
beyond the invocations currently in flight. No locking: the lease guarantees at most one
owner per memo URI at a time, so registration is a plain overwrite, and readers tolerate
staleness.
"""

import typing as ty
import weakref

from thds.core import log

logger = log.getLogger(__name__)


class Completes(ty.Protocol):
    """The one behavior a waiter needs from an in-flight holder."""

    def add_done_callback(self, fn: ty.Callable[[ty.Any], None]) -> None: ...


_IN_FLIGHT: "weakref.WeakValueDictionary[str, ty.Any]" = weakref.WeakValueDictionary()


def register(memo_uri: str, holder: Completes) -> None:
    """Owners only, immediately after lease acquisition. Re-registering (replacing a
    placeholder with the real invocation future) is a plain overwrite."""
    _IN_FLIGHT[memo_uri] = holder


def peek(memo_uri: str) -> Completes | None:
    return _IN_FLIGHT.get(memo_uri)


def subscribe(
    memo_uri: str, subscribed: Completes | None, wake: ty.Callable[[], None]
) -> Completes | None:
    """Attach `wake` to the completion of the in-process holder of `memo_uri`, if there
    is one we aren't already subscribed to. Returns the holder now subscribed to, if any.

    Purely an accelerator for the subscriber's own timed polling: a lookup that races the
    owner's registration just means no subscription until a later attempt, and a holder
    GC'd without ever settling (its callbacks never fire) strands nothing.
    """
    holder = _IN_FLIGHT.get(memo_uri)
    if holder is None or holder is subscribed:
        return subscribed

    logger.info("The lease for %s is held by this very process - waking on its completion.", memo_uri)
    holder.add_done_callback(lambda _f: wake())
    return holder
