"""Describing the run at each root it touches, each on a thread of its own.

The request comes from an invocation, and describing takes several blob store round trips.
A store that is slow to answer, or slow to give up, delays that root's description and
never the invocation, nor the description at any other root.
"""

import atexit
import os
import threading
import time

from . import run_metadata

_EXIT_GRACE_SECONDS = 5.0

_LOCK = threading.Lock()
_PENDING: dict[str, threading.Thread] = {}
_DONE: set[str] = set()
# roots this process has finished describing, or has no part in describing. A root that
# failed is in neither, so the next request tries it again.
_EXIT_HOOKED = False


def _describe(events_root: str, run_name: str) -> None:
    try:
        if run_metadata.publish_root(events_root, run_name):
            _DONE.add(events_root)
    finally:
        with _LOCK:
            _PENDING.pop(events_root, None)


def _join_pending(timeout: float) -> None:
    deadline = time.monotonic() + timeout
    with _LOCK:
        threads = list(_PENDING.values())
    for thread in threads:
        thread.join(timeout=max(0.0, deadline - time.monotonic()))


def request(events_root: str, run_name: str) -> None:
    """Never blocks on the blob store. Cheap to repeat: every invocation calls it."""
    global _EXIT_HOOKED
    if events_root in _DONE:
        return

    with _LOCK:
        if events_root in _PENDING:
            return

        thread = threading.Thread(
            target=_describe, args=(events_root, run_name), name="mops-console-describer", daemon=True
        )
        _PENDING[events_root] = thread
        thread.start()
        if not _EXIT_HOOKED:
            atexit.register(_join_pending, _EXIT_GRACE_SECONDS)
            _EXIT_HOOKED = True


def _reset() -> None:
    """A forked child inherits the bookkeeping but none of the threads it names."""
    global _LOCK
    _LOCK = threading.Lock()
    _PENDING.clear()
    _DONE.clear()


def _wait_for_test() -> None:
    _join_pending(10.0)


def _reset_for_test() -> None:
    _wait_for_test()
    _reset()


os.register_at_fork(after_in_child=_reset)
