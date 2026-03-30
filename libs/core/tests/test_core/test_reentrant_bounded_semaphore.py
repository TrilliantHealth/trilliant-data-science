import threading
import time

from thds.core.concurrency import ReentrantBoundedSemaphore


def test_basic_acquire_release():
    sem = ReentrantBoundedSemaphore(2)
    sem.acquire()
    sem.release()


def test_context_manager():
    sem = ReentrantBoundedSemaphore(2)
    with sem:
        pass


def test_reentrant_same_thread():
    sem = ReentrantBoundedSemaphore(1)
    with sem:
        with sem:
            with sem:
                pass


def test_reentrant_does_not_consume_extra_slots():
    """With only 1 slot, reentrancy must not try to acquire a second."""
    sem = ReentrantBoundedSemaphore(1)
    with sem:
        # if this blocked, we'd deadlock — the test passing proves it doesn't
        acquired = sem.acquire(blocking=False)
        assert acquired
        sem.release()


def test_different_threads_compete_for_slots():
    sem = ReentrantBoundedSemaphore(1)
    results = []
    barrier = threading.Barrier(2)

    def worker(label: str):
        barrier.wait()
        with sem:
            results.append(f"{label}-acquired")
            time.sleep(0.05)
        results.append(f"{label}-released")

    t1 = threading.Thread(target=worker, args=("a",))
    t2 = threading.Thread(target=worker, args=("b",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # both threads acquired and released; one must have waited for the other
    assert "a-acquired" in results
    assert "b-acquired" in results
    assert len(results) == 4


def test_release_without_acquire_raises():
    sem = ReentrantBoundedSemaphore(1)
    try:
        sem.release()
        raise AssertionError("should have raised RuntimeError")
    except RuntimeError:
        pass


def test_nonblocking_acquire_returns_false_when_full():
    sem = ReentrantBoundedSemaphore(1)
    held = threading.Event()
    done = threading.Event()

    def holder():
        with sem:
            held.set()
            done.wait()

    t = threading.Thread(target=holder)
    t.start()
    held.wait()

    # from a different thread, non-blocking acquire should fail
    assert not sem.acquire(blocking=False)

    done.set()
    t.join()


def test_reentrant_across_nested_thread_pools():
    """Reproduces the APB deadlock scenario: outer thread holds the semaphore,
    inner lazy evaluation needs the semaphore from the same thread."""
    sem = ReentrantBoundedSemaphore(1)
    result = []

    def inner_work():
        # this runs on the same thread that already holds the semaphore
        with sem:
            result.append("inner")

    with sem:
        inner_work()
        result.append("outer")

    assert result == ["inner", "outer"]


def test_depth_isolated_across_threads():
    """One thread's depth counter must not affect another thread."""
    sem = ReentrantBoundedSemaphore(2)
    errors = []

    def thread_a():
        with sem:
            with sem:
                # depth is 2 here — thread B should still see depth 0
                time.sleep(0.1)

    def thread_b():
        time.sleep(0.05)
        # thread B should be able to acquire normally, unaffected by A's depth
        acquired = sem.acquire(blocking=False)
        if not acquired:
            errors.append("thread B couldn't acquire despite available slot")
            return

        sem.release()

    t1 = threading.Thread(target=thread_a)
    t2 = threading.Thread(target=thread_b)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert not errors, errors
