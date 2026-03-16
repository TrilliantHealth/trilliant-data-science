"""Tests for the progress Tracker, including a reproduction of the race
condition from issue #4114.

The real bug: when multiple threads call Tracker methods concurrently, the
reporter callback (TqdmReporter/DumbReporter) can be invoked simultaneously
from different threads. These reporters have their own mutable state
(tqdm bar, last_reported timestamp) that isn't thread-safe. The Tracker's
lock serializes all access, preventing concurrent reporter invocations.
"""

import io
import sys
import threading
import time

from thds.adls._progress import ProgressState, Tracker, report_download_progress


def test_lock_prevents_concurrent_reporter_invocations():
    """Prove the lock serializes reporter calls.

    Without the lock, multiple threads calling tracker.add() or tracker()
    simultaneously will invoke the reporter callback concurrently. The real
    reporters (TqdmReporter, DumbReporter) mutate their own state and are
    not thread-safe — concurrent invocations corrupt their internal state,
    producing the wrong file sizes in progress output.

    This test uses a reporter that detects concurrent invocations. With the
    lock in place, the reporter should never see overlapping calls.
    """
    concurrent_violations = []
    active_count = 0
    count_lock = threading.Lock()

    def concurrency_detecting_reporter(states: list[ProgressState]) -> None:
        nonlocal active_count
        with count_lock:
            active_count += 1
            current = active_count

        if current > 1:
            concurrent_violations.append(current)

        # Simulate real reporter work (tqdm update, logging, etc.)
        # This sleep widens the window for concurrent entry.
        time.sleep(0.001)

        with count_lock:
            active_count -= 1

    tracker = Tracker(concurrency_detecting_reporter)
    n_threads = 16
    barrier = threading.Barrier(n_threads)

    def worker(thread_id: int) -> None:
        key = f"file_{thread_id}"
        total = 1000
        barrier.wait()
        tracker.add(key, total=total)
        for i in range(1, 11):
            tracker(key, total_written=i * 100)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert tracker._progresses == {}, f"Stale entries: {tracker._progresses}"
    assert not concurrent_violations, (
        f"Reporter was invoked concurrently {len(concurrent_violations)} times "
        f"(max overlap: {max(concurrent_violations)}). "
        f"This means the lock is not protecting the reporter callback."
    )


def test_concurrent_download_progress_reports_consistent_sizes():
    """Stress test: N threads doing add + incremental writes with aggressive switching."""
    n_threads = 16
    chunk_size = 1024
    chunks_per_file = 20
    file_sizes = {f"file_{i}": chunk_size * chunks_per_file + i for i in range(n_threads)}

    snapshots: list[list[ProgressState]] = []
    snap_lock = threading.Lock()

    def recording_reporter(states: list[ProgressState]) -> None:
        with snap_lock:
            snapshots.append(list(states))

    tracker = Tracker(recording_reporter)
    barrier = threading.Barrier(n_threads)
    registered_totals = set(file_sizes.values())

    old_interval = sys.getswitchinterval()

    def download_worker(file_key: str, total: int) -> None:
        barrier.wait()
        tracker.add(file_key, total=total)
        written = 0
        while written < total:
            write_amt = min(chunk_size, total - written)
            written += write_amt
            tracker(file_key, written=write_amt)

    try:
        sys.setswitchinterval(1e-6)
        threads = [
            threading.Thread(target=download_worker, args=(key, size))
            for key, size in file_sizes.items()
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        sys.setswitchinterval(old_interval)

    assert tracker._progresses == {}, f"Stale entries remain: {tracker._progresses}"

    for i, snap in enumerate(snapshots):
        for state in snap:
            if state.total and state.total not in registered_totals:
                raise AssertionError(f"Snapshot {i} has unregistered total {state.total} (size leakage)")
            if state.total and state.n > state.total:
                raise AssertionError(f"Snapshot {i}: progress {state.n} exceeds total {state.total}")

    for i, snap in enumerate(snapshots):
        totals_in_snap = [s.total for s in snap if s.total]
        assert len(totals_in_snap) == len(set(totals_in_snap)), (
            f"Snapshot {i} has duplicate totals (size leaked between files): {snap}"
        )


def test_report_download_progress_wraps_stream_writes():
    """Verify report_download_progress wires up the stream -> tracker path correctly."""
    states_seen: list[list[ProgressState]] = []

    def reporter(states: list[ProgressState]) -> None:
        states_seen.append(list(states))

    tracker = Tracker(reporter)
    import thds.adls._progress as mod

    original = mod._GLOBAL_DN_TRACKER
    mod._GLOBAL_DN_TRACKER = tracker
    try:
        buf = io.BytesIO()
        total = 256
        wrapped = report_download_progress(buf, "test_file", total)

        wrapped.write(b"\x00" * 128)
        wrapped.write(b"\x00" * 128)

        assert "test_file" not in tracker._progresses
        last_with_entry = [s for s in states_seen if any(st.total == total for st in s)]
        assert last_with_entry, "Reporter never saw our file's progress"
    finally:
        mod._GLOBAL_DN_TRACKER = original


def test_tracker_add_and_complete():
    """Basic single-threaded sanity: add a key, write its total, key gets removed."""
    states_seen: list[list[ProgressState]] = []

    def reporter(states: list[ProgressState]) -> None:
        states_seen.append(list(states))

    tracker = Tracker(reporter)
    tracker.add("a", total=100)
    assert "a" in tracker._progresses

    tracker("a", total_written=100)
    assert "a" not in tracker._progresses, "Key should be removed after reaching total"
