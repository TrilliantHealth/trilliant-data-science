"""Events roots this process has been refused permission to write.

A run publishes to every root it touched, and it may touch roots it can only read - a
shared cache someone else computed into. Retrying a refusal cannot change the answer, so
the first one drops the root for the rest of the process.

Inherited across fork: a child holds the same credentials as its parent.
"""

import threading

from thds.core import log

logger = log.getLogger(__name__)

_REFUSED: set[str] = set()
_LOCK = threading.Lock()


def refused(events_root: str) -> bool:
    return events_root in _REFUSED


def note(events_root: str, err: PermissionError) -> None:
    with _LOCK:
        if events_root in _REFUSED:
            return

        _REFUSED.add(events_root)

    logger.warning(
        "Console publishing to %s was refused (%s); this run will not be visible from there.",
        events_root,
        err,
    )


def _reset_for_test() -> None:
    _REFUSED.clear()
