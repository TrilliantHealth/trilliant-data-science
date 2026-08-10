"""Event writing from a remote process.

A remote has no access to the orchestrator's disk, so its events go to the blob store both
sides already share - under `<blob root>/mops/console/<run_name>/`, a sibling of the
`mops2-mpf` memoization namespace rather than a member of it.

A remote emits a couple of events across a job lasting minutes to hours, so writing one
small object each is cheap relative to its own work. That asymmetry is deliberate: the
orchestrator, which may make tens of thousands of invocations, batches to local disk
instead (see `writer`).
"""

import datetime as dt
import json

from thds.core import config, log

from ...core import uris
from ...core.memo import function_memospace
from . import throwaway
from .events import Event

CONSOLE_DIRNAME = "mops/console"

CONSOLE_REMOTE_EVENTS = config.item("thds.mops.console.remote_events", default=True, parse=config.tobool)
# on by default: a remote writes two small objects across a job lasting minutes to hours,
# and without them nothing can report the invoked-to-started gap, which is the reason the
# console exists. Set false to silence a remote entirely.

logger = log.getLogger(__name__)


def console_dirname() -> str:
    """`mops/console`, or the throwaway sibling for a run nobody will look for."""
    return throwaway.suffixed(CONSOLE_DIRNAME)


def events_root(memo_uri: str, run_name: str) -> str:
    """Where one run's events live, under the same blob root the invocation itself used.

    Derived from the memo URI rather than from `uris.get_root`, which returns the blob
    store's *configured* root - not necessarily the root this invocation was written to.
    """
    runner_prefix = function_memospace.parse_memo_uri(memo_uri).runner_prefix
    blob_root = runner_prefix.rsplit("/", 1)[0]  # strip the trailing 'mops2-mpf'
    return uris.lookup_blob_store(memo_uri).join(blob_root, console_dirname(), run_name)


def object_name(event: Event, suffix: str, lines: bool = False) -> str:
    """A name that sorts by when the event happened, so a reader can resume a listing.

    Object stores can only resume a listing at a name, never at a time, so time has to be
    *in* the name for an incremental read to mean anything. Fixed width and zero-padded, so
    lexicographic order is chronological order.

    The ordering is only as good as the writing machine's clock, and nothing can fix that -
    there is no shared clock across a pod fleet and no server-assigned sequence to borrow.
    Readers therefore resume from a margin behind their watermark rather than trusting the
    order exactly; see `read.resume_at`.

    `suffix` distinguishes events written in the same millisecond and must be unique within
    the run - concurrent writers would otherwise overwrite each other.

    `lines` names a batch of newline-delimited events rather than a single one, so the
    extension says which of the two an object holds. Readers sniff the content either way;
    this is for whoever opens one by hand.
    """
    at = event.get("at", "")
    return f"{_sortable(at)}-{suffix}{'.jsonl' if lines else '.json'}"


def _sortable(at: str) -> str:
    try:
        stamp = dt.datetime.fromisoformat(at).astimezone(dt.timezone.utc)
    except ValueError:
        return "0" * 18
        # an unparseable timestamp sorts first, where it will be re-read rather than
        # skipped. Losing an event to a bad clock reading is worse than re-reading it.

    return stamp.strftime("%Y%m%dT%H%M%S%f")[:18]


def emit_to_blob(memo_uri: str, run_name: str, event: Event, name: str) -> None:
    """Never raises - an unwritable event must not fail the work it describes.

    `name` must be unique within the run: separate processes write concurrently, and a
    blob store's last-writer-wins would silently drop one of them.
    """
    if not CONSOLE_REMOTE_EVENTS():
        return

    try:
        blob_store = uris.lookup_blob_store(memo_uri)
        blob_store.putbytes(
            blob_store.join(events_root(memo_uri, run_name), "events", name),
            json.dumps(event, indent=2).encode(),
            # one event per blob, so nothing parses these line by line and the indentation
            # costs about 3% - worth it for a file someone opens in a browser to read.
            type_hint="application/mops-console-event",
        )
    except Exception:
        logger.exception("Could not write a mops console event; continuing.")
        # deliberately broad: this is diagnostic-only, and every blob store raises
        # something different. Nothing here may ever interrupt the actual invocation.
