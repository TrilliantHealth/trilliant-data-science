import collections
import threading
import time
import typing as ty

import pytest

from thds.mops.k8s.uncertain_future import (
    UncertainFuturesTracker,
    _FutureInterpretationShim,
    _FuturesState,
    official_timer,
)


class SignalingODict(collections.OrderedDict):
    def __init__(self, initial: ty.Mapping, snapshot_ready: threading.Event):
        super().__init__()
        super().update(initial)
        self.snapshot_ready = snapshot_ready

    def values(self):
        # Signal after we've yielded at least one item, so we're actively iterating
        for i, val in enumerate(super().values()):
            if i == 1:  # Signal after the second item
                self.snapshot_ready.set()
                # Give the worker thread a tiny window to start mutating
                time.sleep(0.001)
            yield val


@pytest.mark.timeout(3)
def test_concurrent_mutation_during_active_iteration_does_not_raise():
    """Test that mutations during OrderedDict iteration don't cause RuntimeError
    when using the snapshot approach."""

    allowed_stale_seconds = 0.01
    tracker = UncertainFuturesTracker[ty.Any, ty.Any](allowed_stale_seconds=allowed_stale_seconds)

    # Pre-populate with enough stale states to ensure we iterate past the first item
    with tracker._lock:
        now = official_timer()
        for i in range(10):
            tracker._keyed_futures_state[f"pre{i}"] = _FuturesState(set(), last_seen_at=now - 1.0)

    # Replace the internal dict with our signaling subclass
    snapshot_ready = threading.Event()
    with tracker._lock:
        tracker._keyed_futures_state = SignalingODict(
            initial=tracker._keyed_futures_state,
            snapshot_ready=snapshot_ready,
        )

    mutation_completed = threading.Event()
    exception_caught = threading.Event()

    def mutator_worker():
        try:
            # Wait until we're actively iterating the OrderedDict
            assert snapshot_ready.wait(timeout=2.0), "snapshot_ready not signaled"

            # Now perform mutations that would cause "RuntimeError: OrderedDict mutated during iteration"
            # if we were iterating the live dict instead of a snapshot
            for j in range(5):
                tracker.update(f"mut{j}", None)  # This calls move_to_end() under the lock
                time.sleep(0.001)  # Small delay to increase chance of interleaving

        except Exception:
            exception_caught.set()
            raise
        finally:
            mutation_completed.set()

    mutator_thread = threading.Thread(target=mutator_worker, daemon=True)
    mutator_thread.start()

    # This should NOT raise "RuntimeError: OrderedDict mutated during iteration"
    # because we iterate over a snapshot, not the live dict
    try:
        tracker.gc_stale()
    except RuntimeError as e:
        if "mutated during iteration" in str(e):
            pytest.fail(f"Got the race condition we're trying to prevent: {e}")
        raise

    # Ensure the worker actually ran and completed
    assert mutation_completed.wait(timeout=2.0), "mutation thread did not complete"
    assert not exception_caught.is_set(), "mutation thread raised an exception"

    mutator_thread.join(timeout=1.0)


def test_interpret_tolerates_already_resolved_future():
    """If a future is resolved externally before interpret() runs, it should
    return self (unregister) rather than raising InvalidStateError."""
    shim: _FutureInterpretationShim[ty.Any, int] = _FutureInterpretationShim(lambda r_0, last_seen: 42)
    shim.future.set_result(99)  # simulate external resolution (e.g. batch submit path)

    result = shim.interpret("anything", official_timer())
    assert result is shim
    assert shim.future.result() == 99  # original result preserved


def test_interpret_tolerates_concurrent_resolution():
    """If the interpreter returns a result but another thread resolves the future
    between the done() check and set_result(), interpret should not raise."""
    resolved = threading.Event()

    def _slow_interpreter(r_0: ty.Any, last_seen: float) -> int:
        resolved.wait(timeout=2.0)  # block until the future is resolved externally
        return 42

    shim = _FutureInterpretationShim(_slow_interpreter)

    def _resolve_externally():
        shim.future.set_result(99)
        resolved.set()

    t = threading.Thread(target=_resolve_externally, daemon=True)
    t.start()

    result = shim.interpret("anything", official_timer())
    t.join(timeout=2.0)
    assert result is shim
    assert shim.future.result() == 99
