"""Per-process batching writer for run events.

One JSONL file per process, appended by a single background thread. Orchestrators may make
tens of thousands of invocations, so a file per event is not viable - and the write must
never delay the invocation that triggered it.

Emission is fire-and-forget onto a bounded queue. When the queue is full, events are
dropped and counted rather than blocking a caller: a stale row in a UI is always
preferable to slowing down the work being observed.

Events land beside the summary tree the same run writes - `.mops/events/<run name>/`
alongside `.mops/summary/<run name>/` - so the two views of one run share a key and a
working directory. Setting the directory to empty disables writing entirely.
"""

import atexit
import json
import os
import queue
import threading
import time
from pathlib import Path

from thds.core import config, log

from . import run_name, throwaway, upload
from .events import Event

CONSOLE_EVENTS_DIR = config.item(
    "thds.mops.console.events_dir", default=Path(".mops/events"), parse=Path
)
# relative by default, matching `thds.mops.summary.dir`, so a run's events land under the
# directory it was launched from rather than somewhere global. Set empty to disable.

_MAX_QUEUE = 10_000
# bounded so a slow or stuck disk cannot grow memory without limit.

_DRAIN_POLL_SECONDS = 1.0
_NOTHING_ARRIVED: Event = {}
# a sentinel distinct from both an event and the None that means stop.

logger = log.getLogger(__name__)


class _Writer:
    """Owns one file and one thread. Created at most once per process.

    Mutable by necessity - it wraps an OS file handle and a drain thread.
    """

    def __init__(self, path: Path) -> None:
        self._queue: queue.Queue[None | Event] = queue.Queue(maxsize=_MAX_QUEUE)
        self._dropped = 0
        self._path = path
        self._thread = threading.Thread(target=self._drain, name="mops-console-events", daemon=True)
        self._thread.start()
        atexit.register(self.close)

    def emit(self, event: Event) -> None:
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            self._dropped += 1

    def _drain(self) -> None:
        last_upload = time.monotonic()
        with open(self._path, "a", encoding="utf-8") as f:
            while True:
                try:
                    event = self._queue.get(timeout=_DRAIN_POLL_SECONDS)
                except queue.Empty:
                    event = _NOTHING_ARRIVED
                    # waking on a timer as well as on arrival, so that a run which goes
                    # quiet still publishes its last batch instead of holding it until
                    # the process exits.

                if event is None:
                    f.flush()
                    upload.flush(self._known_roots())
                    return

                if event is not _NOTHING_ARRIVED:
                    try:
                        f.write(json.dumps(event) + "\n")
                    except (OSError, TypeError, ValueError):
                        logger.exception("Failed to write a mops console event")
                    upload.add([event])

                if self._queue.empty():
                    f.flush()
                    # flush when caught up rather than per event, so a reader tailing the
                    # file sees whole lines promptly without paying a syscall per append.

                if time.monotonic() - last_upload >= upload.UPLOAD_INTERVAL_SECONDS():
                    upload.flush(self._known_roots())
                    last_upload = time.monotonic()
                    # publishing happens on this thread rather than its own: it is already
                    # off the invocation path, and a batch is only worth sending once
                    # enough events have accumulated to make the request worthwhile.

    def _known_roots(self) -> list[str]:
        """Every events root any process of this run has recorded locally.

        Workers of one run may each write to a different blob root and share none - but
        they all share this run directory. Handing the pointer file's contents to each
        manifest is what lets a remote watcher entering any single root find the rest.
        """
        return remote_events_uris(self._path.parent)

    def close(self) -> None:
        if self._dropped:
            logger.warning(
                "Dropped %d mops console events; the UI's view of this run is incomplete.",
                self._dropped,
            )
        self._queue.put(None)
        self._thread.join(timeout=5)


_UNUSABLE = "unusable"
# a sentinel distinct from None, so a directory we failed to open is never retried.
# Without it, every emit on a broken directory would re-attempt mkdir and re-log -
# tens of thousands of failed syscalls on the hot path we are trying not to disturb.

_WRITER: None | _Writer | str = None
_WRITER_LOCK = threading.Lock()


def _hold_locks_for_fork() -> None:
    """Take both module locks so no thread holds either across the fork.

    A lock held by another thread at fork time is copied into the child in its locked
    state, with no thread left to release it - the child's first emit would then block
    forever. Acquiring here makes the fork wait out any in-flight writer creation
    instead.
    """
    _WRITER_LOCK.acquire()
    upload._UPLOADERS_LOCK.acquire()


def _release_locks_in_parent() -> None:
    upload._UPLOADERS_LOCK.release()
    _WRITER_LOCK.release()


def _reset_after_fork() -> None:
    """A forked child inherits the parent's _WRITER object and queue but not its drain
    thread. Emissions would queue forever and never reach disk. Reset so the child
    creates its own writer (and uploader) on first use, with fresh locks."""
    global _WRITER, _WRITER_LOCK
    _WRITER = None
    _WRITER_LOCK = threading.Lock()
    upload._reset()


os.register_at_fork(
    before=_hold_locks_for_fork,
    after_in_parent=_release_locks_in_parent,
    after_in_child=_reset_after_fork,
)


_REMOTE_POINTER = "remote-events-uri.txt"


def events_dir() -> Path:
    """This run's event directory, named for the run rather than for the process.

    Named by the console's own run name, which every process in a run shares. The summary
    tree's name cannot serve here: it carries the writing process's pid, so a run that
    dispatches from a process pool would scatter its events across one directory per
    worker while claiming to describe a single run.

    A throwaway run goes to a sibling directory, so a project that uses `mops` inside its
    own tests does not have to pick its real runs out from among them.
    """
    configured = CONSOLE_EVENTS_DIR()
    if not configured.name:
        return configured / run_name.current(True)
        # the empty path that disables local events entirely - nothing is written here, and
        # there is no name to mark.

    return configured.with_name(throwaway.suffixed(configured.name)) / run_name.current(True)
    # the leaf is renamed rather than the root, so this still lands beside a configured
    # location instead of back under the default one.


def record_remote_events_uri(uri: str) -> None:
    """Leave the blob prefix the remotes report to beside the local events.

    The two halves of a run are named differently - the local tree by the summary run
    name, the remote prefix by the console run name - so without this a reader holding one
    could not find the other. It is also the string you hand someone else so they can watch
    your run from their own machine.

    Appends rather than overwrites: a run that spans blob roots records each one, and a
    reader gets all of them. Deduplicated on read rather than on write, since the append is
    on the invocation path and a set lookup is not free at tens of thousands of calls.
    """
    if not CONSOLE_EVENTS_DIR().name or not uri:
        return

    try:
        directory = events_dir()
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / _REMOTE_POINTER).open("a") as f:
            f.write(uri + "\n")
    except OSError:
        logger.debug("Could not record the remote events uri; the console will read local events only.")


def remote_events_uris(run_dir: Path) -> list[str]:
    """Every blob prefix a local run's remotes reported to, deduplicated, in discovery order."""
    try:
        seen: set[str] = set()
        result: list[str] = []
        for line in (run_dir / _REMOTE_POINTER).read_text().splitlines():
            stripped = line.strip()
            if stripped and stripped not in seen:
                seen.add(stripped)
                result.append(stripped)

        return result
    except OSError:
        return []


def _writer() -> None | _Writer:
    global _WRITER
    if _WRITER is not None:
        return _WRITER if isinstance(_WRITER, _Writer) else None

    with _WRITER_LOCK:
        if _WRITER is None:
            directory = events_dir()
            try:
                directory.mkdir(parents=True, exist_ok=True)
                _WRITER = _Writer(directory / f"events-{os.getpid()}.jsonl")
            except OSError:
                logger.exception("Could not open mops console events dir '%s'; disabling.", directory)
                _WRITER = _UNUSABLE

    return _WRITER if isinstance(_WRITER, _Writer) else None


def emit(event: Event) -> None:
    """Never raises, never blocks. Safe to call from the invocation hot path."""
    if not CONSOLE_EVENTS_DIR().name:
        return
        # the disabled case costs one config read and nothing else.

    writer = _writer()
    if writer:
        writer.emit(event)
