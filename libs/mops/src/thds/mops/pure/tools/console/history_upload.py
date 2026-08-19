"""Replay a process's local event history once a run has shared work.

The local JSONL is the source of truth for orchestrator events. Reading from byte zero
when a root first appears includes cache hits that preceded the first invocation without
holding an undecided run in memory. The first invocation opens the run as a whole; after
that, events publish to every blob root they reference, including roots with cache hits
but no new work.
"""

import json
import typing as ty
from pathlib import Path

from thds.core import log

from . import blob_sink, upload
from .events import Event

logger = log.getLogger(__name__)


def _events_since(path: Path, offset: int) -> tuple[list[Event], int]:
    """Complete local records after `offset`, plus the next safe byte position."""
    try:
        with path.open("rb") as events_file:
            events_file.seek(offset)
            tail = events_file.read()
    except OSError:
        logger.debug("Could not read local console events for upload.")
        return [], offset

    complete = tail.rfind(b"\n") + 1
    events: list[Event] = []
    for line in tail[:complete].splitlines():
        try:
            event = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.debug("Skipping an unreadable local console event during upload.")
            continue

        if isinstance(event, dict):
            events.append(ty.cast(Event, event))

    return events, offset + complete


def _event_root(event: Event, run_name: str) -> str:
    try:
        return blob_sink.events_root(event.get("memo_uri", ""), run_name)
    except (ValueError, AssertionError):
        return ""


def upload_new(
    path: Path,
    run_name: str,
    activating_roots: ty.Sequence[str],
    uploaded_offsets: ty.Mapping[str, int],
) -> dict[str, int]:
    """Publish new local records once any actual invocation has activated the run."""
    if not activating_roots:
        return dict(uploaded_offsets)

    scan_from = min(uploaded_offsets.values(), default=0)
    new_events, _ = _events_since(path, scan_from)
    discovered = {_event_root(event, run_name) for event in new_events}
    roots = list(
        dict.fromkeys((*uploaded_offsets, *activating_roots, *(root for root in discovered if root)))
    )
    next_offsets = dict(uploaded_offsets)
    for root in roots:
        upload.start_root(root, run_name)
        events, offset = _events_since(path, uploaded_offsets.get(root, 0))
        events = [event for event in events if _event_root(event, run_name) == root]

        upload.add_to(root, events)
        next_offsets[root] = offset

    upload.flush(roots)
    return next_offsets
