"""Pointers from a run's events root to the work queues feeding it.

A queue root is chosen by the application and need not share a prefix with the events
root, so a reader cannot derive one from the other. The orchestrator records each queue
root it uses under `<events root>/queues/`, one small object per queue.
"""

import hashlib
import json

from thds.core import log

from ...core import types, uris
from . import blob_sink, enabled

QUEUES_DIR = "queues"

logger = log.getLogger(__name__)


def publish(memo_uri: str, queue_root: str) -> None:
    """Idempotent: the object's name is derived from `queue_root`. Never raises, since
    the run does not depend on its console."""
    name = enabled.current_run_name()
    if not name or not blob_sink.CONSOLE_REMOTE_EVENTS():
        return

    try:
        store = uris.lookup_blob_store(memo_uri)
        store.putbytes(
            store.join(
                blob_sink.events_root(memo_uri, name),
                QUEUES_DIR,
                hashlib.sha256(queue_root.encode()).hexdigest()[:12] + ".json",
            ),
            json.dumps({"queue_root": queue_root}).encode(),
            type_hint="application/json",
        )
    except Exception:
        logger.warning("Could not record queue root %s for the console", queue_root, exc_info=True)


def read(events_root: str) -> list[str]:
    """The queue roots recorded under one events root; empty when the run has no queue."""
    store = uris.lookup_blob_store(events_root)
    if not isinstance(store, types.ListableBlobStore):
        return []

    return sorted(
        json.loads(uris.get_bytes(entry.uri, "mops-console-queue-root"))["queue_root"]
        for entry in store.list(store.join(events_root, QUEUES_DIR), "")
    )
