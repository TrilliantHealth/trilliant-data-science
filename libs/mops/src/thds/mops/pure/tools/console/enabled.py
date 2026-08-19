"""Whether this run reports events at all.

Both halves are on by default and independently switchable: an orchestrator writing to a
local directory, and remotes writing small objects to the shared blob store. Turning either
off is a matter of config, not of code paths.
"""

import datetime as dt

from thds.core import cache, log

from . import blob_sink, events, run_name, runtime, upload, writer
from .blob_sink import CONSOLE_REMOTE_EVENTS
from .events import Event, finished, started
from .writer import CONSOLE_EVENTS_DIR, emit

logger = log.getLogger(__name__)


def current_run_name() -> str:
    """The name every event in this run is filed under, or empty when nothing records.

    Minting a name is what tells remotes to report, so it must not happen when both halves
    are off - otherwise remotes would write objects nobody asked for.
    """
    return run_name.current(bool(CONSOLE_EVENTS_DIR().name) or CONSOLE_REMOTE_EVENTS())


@cache.locking
def _link_halves(events_root_uri: str) -> None:
    writer.record_remote_events_uri(events_root_uri)


def _note_run_location(memo_uri: str) -> None:
    """Record where this run's remotes report, so a reader given the local half can find
    the remote half.

    Called per invocation but does its work once - `memo_uri` differs every time, so the
    blob root is resolved first and that is what the cache keys on.
    """
    name = current_run_name()
    if not name or not CONSOLE_REMOTE_EVENTS():
        return

    try:
        upload.start(memo_uri, name)
        _link_halves(blob_sink.events_root(memo_uri, name))
        # publishing the orchestrator's own events, so a second observer sees invocations
        # rather than only the remotes' side of the run.
    except Exception:
        logger.debug(
            "Could not resolve the remote events uri for %s;"
            " a console will read this run's local events only.",
            memo_uri,
        )
        # a memo uri that will not parse costs the console a cross-reference, never an
        # invocation.


def invoked(memo_uri: str, *, attempt_id: str, at: dt.datetime) -> None:
    """Tell the console an invocation has been handed to the shim.

    One call rather than two, because an invocation is one fact: the caller should not have
    to know that recording it also involves noting where this run publishes.

    Nothing here reaches the blob store. The event goes onto the writer's queue, and the
    location is a small local file written once per process - the batch that does upload
    goes out later, on the writer's drain thread (which also calls `upload.flush`).
    """
    _note_run_location(memo_uri)
    emit(events.invoked(memo_uri, attempt_id=attempt_id, at=at))


def memoized(
    memo_uri: str,
    *,
    at: dt.datetime,
    was_error: bool = False,
    invoked_at: str = "",
    started_at: str = "",
    ended_at: str = "",
    run_name: str = "",
) -> None:
    """Tell the console a result was served from the cache.

    Always goes through the local queue. It is published only if this run later performs
    an actual invocation: a memoized-only run is a local checklist, not a new shared run.
    If a miss does arrive, the writer uploads its local history from the beginning, so
    cache hits observed before the miss are not lost to a remote observer.
    """
    emit(
        events.memoized(
            memo_uri,
            at=at,
            was_error=was_error,
            invoked_at=invoked_at,
            started_at=started_at,
            ended_at=ended_at,
            run_name=run_name,
        )
    )


def remote_started(
    memo_uri: str, run_name_: str, attempt_id: str, at: dt.datetime, invoked_at: str
) -> None:
    """A remote reporting that it has begun, with where it is running.

    Where it is running is asked of whichever runtime launched it, because only that
    runtime can say - and only now, while the execution it names still exists.
    """
    _emit_to_blob(
        memo_uri,
        run_name_,
        started(
            memo_uri,
            attempt_id=attempt_id,
            at=at,
            where=runtime.current(),
            invoked_at=invoked_at,
        ),
        attempt_id,
        "started",
    )


def remote_finished(
    memo_uri: str, run_name_: str, attempt_id: str, at: dt.datetime, was_error: bool, started_at: str
) -> None:
    """A remote reporting its outcome, and when the attempt that produced it began."""
    _emit_to_blob(
        memo_uri,
        run_name_,
        finished(memo_uri, attempt_id=attempt_id, at=at, was_error=was_error, started_at=started_at),
        attempt_id,
        "finished",
    )


def _emit_to_blob(memo_uri: str, run_name_: str, event: Event, attempt_id: str, kind: str) -> None:
    """No-op unless the orchestrator named a run, which it only does when the console is on.

    The object name carries the attempt id so concurrent attempts at one invocation cannot
    overwrite each other's events.
    """
    if not run_name_:
        return

    blob_sink.emit_to_blob(
        memo_uri, run_name_, event, blob_sink.object_name(event, f"{attempt_id}-{kind}")
    )
