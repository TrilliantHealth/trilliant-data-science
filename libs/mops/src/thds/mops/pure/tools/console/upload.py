"""Publishing the orchestrator's events so someone else can follow the run.

An orchestrator's events - the invocations - are written to local disk, which is cheap and
cannot fail in a way that touches the work. But local disk is reachable by exactly one
person. Without publishing them, a second observer sees remotes starting and finishing
work that, as far as they can tell, was never invoked: no queue times, no count of what is
still waiting.

The unit of upload is a batch, never an event. An orchestrator may invoke tens of thousands
of functions, and the whole reason its events go to a local queue first is that it is the
expensive producer. Batches are uploaded on a timer, so the cost is a handful of objects
per minute regardless of invocation rate.

Each batch is named by the time it was written, like every other object in the events
prefix, so a reader resumes across both kinds with one listing.

An orchestrator killed outright never flushes its last batch, so a remote reader can miss
up to one interval of invocations - the local file has them, since that is written as the
queue drains rather than on a timer. A console watching locally closes the gap without
being asked: it folds those events and carries them into the snapshot it publishes.
"""

import json
import os
import threading
import typing as ty

from thds.core import config, log

from ...core import uris
from .blob_sink import events_root, object_name
from .events import Event

CONSOLE_UPLOAD_EVENTS = config.item("thds.mops.console.upload_events", default=True, parse=config.tobool)
# on by default, and off is a real choice: a run nobody will watch from elsewhere gains
# nothing from publishing, and a blob store may be one an orchestrator would rather not
# write to on a timer.

UPLOAD_INTERVAL_SECONDS = config.item(
    "thds.mops.console.upload_interval_seconds", default=15.0, parse=float
)
# how stale a second observer's view of invocations may be. Lower costs more objects for
# the same number of events, since a batch is one object however many events it holds.

logger = log.getLogger(__name__)


def _root_of(memo_uri: str) -> str:
    from ...core.memo import function_memospace

    return function_memospace.parse_memo_uri(memo_uri).runner_prefix.rsplit("/", 1)[0]


class _Uploader:
    """Accumulates events and flushes them on a timer.

    Mutable because it is a buffer: it exists to hold events that have not been published
    yet, and to know when enough time has passed to be worth a request.
    """

    def __init__(self, memo_uri: str, run_name: str) -> None:
        self._memo_uri = memo_uri
        self._run_name = run_name
        self._pending: list[Event] = []
        self._lock = threading.Lock()
        self._seq = 0
        self._pid = os.getpid()

    @property
    def events_root_uri(self) -> str:
        try:
            return events_root(self._memo_uri, self._run_name)
        except (ValueError, AssertionError):
            return ""

    def add(self, events: ty.Iterable[Event]) -> None:
        with self._lock:
            self._pending.extend(events)

    def flush(self) -> None:
        """Never raises. A failed upload costs a second observer some freshness and must
        never touch the run - the events are still on local disk either way."""
        with self._lock:
            batch, self._pending = self._pending, []

        if not batch:
            return

        try:
            blob_store = uris.lookup_blob_store(self._memo_uri)
            blob_store.putbytes(
                blob_store.join(
                    events_root(self._memo_uri, self._run_name),
                    "events",
                    object_name(batch[-1], f"orchestrator-{self._pid}-{self._seq}", lines=True),
                ),
                "\n".join(json.dumps(event) for event in batch).encode(),
                type_hint="application/mops-console-events",
            )
            self._seq += 1
        except Exception:
            logger.debug("Could not publish a batch of console events; continuing.", exc_info=True)


_UPLOADERS: dict[str, _Uploader] = {}
_UPLOADERS_LOCK = threading.Lock()


def _key(memo_uri: str) -> str:
    try:
        return _root_of(memo_uri)
    except (ValueError, AssertionError):
        return memo_uri


def start(memo_uri: str, run_name: str) -> None:
    """Begin publishing this run's orchestrator events for one blob root.

    Called per invocation; a run that spans roots calls this with different memo URIs and
    each root gets its own uploader. The root is derived from the memo URI.
    """
    if not CONSOLE_UPLOAD_EVENTS() or not run_name:
        return

    key = _key(memo_uri)
    if key in _UPLOADERS:
        return

    with _UPLOADERS_LOCK:
        if key not in _UPLOADERS:
            _UPLOADERS[key] = _Uploader(memo_uri, run_name)


def _snapshot() -> tuple[_Uploader, ...]:
    with _UPLOADERS_LOCK:
        return tuple(_UPLOADERS.values())


def add(events_batch: ty.Sequence[Event]) -> None:
    """Route events to the right uploader by root. No-op until `start` has been called."""
    uploaders = _snapshot()
    if not uploaders:
        return

    if len(uploaders) == 1:
        uploaders[0].add(events_batch)
        return

    by_root: dict[str, list[Event]] = {}
    for event in events_batch:
        key = _key(event.get("memo_uri", ""))
        if key in _UPLOADERS:
            by_root.setdefault(key, []).append(event)

    for key, batch in by_root.items():
        _UPLOADERS[key].add(batch)


def flush(known_roots: ty.Sequence[str] = ()) -> None:
    """Publish whatever has accumulated, across all roots.

    `known_roots` names events roots this process learned about from elsewhere - the
    run's local pointer file, which every process appends to. Workers of one run may
    each write to a different blob root and share none; the pointer file is what they do
    share, so publishing a manifest of its contents under every root is what lets a
    remote watcher entering any root discover them all.
    """
    for uploader in _snapshot():
        uploader.flush()

    if CONSOLE_UPLOAD_EVENTS():
        _publish_manifest(known_roots)
        # gated here rather than assumed off by having no uploaders: `known_roots` alone
        # would otherwise have a run publish manifests its owner asked it not to.


_MANIFEST_WRITTEN: tuple[frozenset[str], frozenset[str]] = (frozenset(), frozenset())
_ROOTS_DIR = "roots"


def _publish_manifest(known_roots: ty.Sequence[str] = ()) -> None:
    """Write this process's root list so any entry point discovers all roots.

    Each process writes its own `roots/{pid}.json`; the reader unions them. This avoids
    the clobber a single shared `roots.json` would suffer when multiple processes (e.g.
    spawn-pool workers) each see a subset of the roots. The manifest names every root
    this process knows of - its own and `known_roots` - and is written under every one
    of them: backfilling a root some other process wrote, and may already have exited
    from, is what lets a reader entering that root alone discover the rest. Best-effort,
    since nothing guarantees write access to a root someone else chose - a refusal costs
    a debug line, and discovery from that root stays incomplete unless a process that
    can write there publishes the full set.

    Re-published when the root set grows, or when a previous attempt failed to write to
    every root. `_MANIFEST_WRITTEN` tracks both the root set and the set of roots that
    have the current manifest, so a partial failure retries on the next flush.
    """
    global _MANIFEST_WRITTEN
    targets = frozenset(u.events_root_uri for u in _snapshot() if u.events_root_uri) | frozenset(
        known_roots
    )
    prev_roots, prev_written = _MANIFEST_WRITTEN
    if targets == prev_roots and targets == prev_written:
        return

    manifest_name = f"{os.getpid()}.json"
    manifest = json.dumps({"roots": sorted(targets)}, indent=2).encode()
    written: set[str] = set()
    for root in sorted(targets):
        if targets == prev_roots and root in prev_written:
            written.add(root)
            continue

        try:
            blob_store = uris.lookup_blob_store(root)
            blob_store.putbytes(
                blob_store.join(root, _ROOTS_DIR, manifest_name),
                manifest,
                type_hint="application/json",
            )
            written.add(root)
        except Exception:
            logger.debug("Could not publish a roots manifest to %s; continuing.", root, exc_info=True)

    _MANIFEST_WRITTEN = (targets, frozenset(written))


def roots() -> list[str]:
    return [uri for u in _snapshot() if (uri := u.events_root_uri)]


def _reset() -> None:
    """Discard inherited state after fork. Called by writer._reset_after_fork.

    The lock is replaced rather than released: the fork hooks hold it across the fork,
    so the child inherits it acquired, owned by a thread that does not exist here.
    """
    global _MANIFEST_WRITTEN, _UPLOADERS_LOCK
    _UPLOADERS.clear()
    _UPLOADERS_LOCK = threading.Lock()
    _MANIFEST_WRITTEN = (frozenset(), frozenset())
