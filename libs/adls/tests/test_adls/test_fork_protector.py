import multiprocessing as mp
import typing as ty
from functools import wraps

# Assuming the provided code is in a file named `fork_safe_cache.py`
from thds.adls._fork_protector import F, fork_safe_cached

# --- Test Setup ---


def make_simple_cache_decorator(cache_storage: ty.Dict[ty.Any, ty.Any]) -> ty.Callable[[F], F]:
    """A factory for a simple in-memory cache decorator for testing purposes."""

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # A real cache would properly handle kwargs, but this is fine for the test.
            key = (args, tuple(sorted(kwargs.items())))
            if key not in cache_storage:
                cache_storage[key] = func(*args, **kwargs)
            return cache_storage[key]

        return ty.cast(F, wrapper)

    return decorator


def target_function_in_child(cached_func: ty.Callable[[int], int], call_counter: ty.Any):
    """This function runs in the child process."""
    # This call should result in a CACHE MISS if the decorator works,
    # as the PID will be different from the parent's.
    result = cached_func(100)

    # Confirm the function executed and returned the correct value
    assert result == 100
    # The counter should now be 3 (two from parent test, one from child)
    assert call_counter.value == 3


# --- The Test ---


def test_fork_safe_cached():
    """
    Tests that the fork_safe_cached decorator correctly invalidates the cache
    across process forks.
    """
    # 1. Setup a shared counter and a simple cache
    call_counter = mp.Value("i", 0)
    cache_storage: dict = {}
    simple_cache_deco = make_simple_cache_decorator(cache_storage)

    # 2. Define a simple function to decorate
    def identity_and_count(x: int) -> int:
        """A simple function that increments a counter on each execution."""
        with call_counter.get_lock():
            call_counter.value += 1
        return x

    # 3. Apply the fork_safe_cached decorator
    cached_func = fork_safe_cached(simple_cache_deco, identity_and_count)

    # --- Part 1: Test caching within a single process ---
    assert call_counter.value == 0

    # First call: should execute the function and cache the result
    res1 = cached_func(100)
    assert res1 == 100
    assert call_counter.value == 1
    assert len(cache_storage) == 1

    # Second call with same args: should be a CACHE HIT
    res2 = cached_func(100)
    assert res2 == 100
    assert call_counter.value == 1  # Unchanged!
    assert len(cache_storage) == 1

    # Call with different args: should be a CACHE MISS
    res3 = cached_func(200)
    assert res3 == 200
    assert call_counter.value == 2  # Incremented!
    assert len(cache_storage) == 2

    # --- Part 2: Test caching across a process fork ---

    # This part of the test is only meaningful on systems that use 'fork'
    mp_fork = mp.get_context("fork")
    # We expect the child process to inherit the cache_storage dict.
    # The fork_safe_cached decorator's job is to prevent the child from
    # using the parent's cached entries.

    # 4. Create and run a child process
    child_process = mp_fork.Process(target=target_function_in_child, args=(cached_func, call_counter))
    child_process.start()
    child_process.join(timeout=5)

    assert child_process.exitcode == 0, "Child process failed"

    # 5. Final assertion: The counter should have been incremented by the child
    # This proves the child had a cache miss and re-executed the function.
    assert call_counter.value == 3, "Cache was not invalidated after fork"
