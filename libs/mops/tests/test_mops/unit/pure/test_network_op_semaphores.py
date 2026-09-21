"""The network-op semaphores are sized from `max_concurrent_network_ops` on first use, so an
application entry point can raise the limit after importing mops and still get what it asked for."""

import typing as ty

from thds.mops.pure.runner import get_results, local


def _clear(cached: ty.Callable) -> None:
    ty.cast(ty.Any, cached).clear_cache()


def test_before_invocation_semaphore_is_sized_when_first_used(monkeypatch):
    _clear(local._before_invocation_semaphore)
    monkeypatch.setattr(local, "max_concurrent_network_ops", lambda: 37)
    try:
        assert local._before_invocation_semaphore()._sem._value == 37
        assert local._before_invocation_semaphore() is local._before_invocation_semaphore()
    finally:
        _clear(local._before_invocation_semaphore)


def test_after_invocation_semaphore_is_three_times_the_configured_ops(monkeypatch):
    _clear(get_results._after_invocation_semaphore)
    monkeypatch.setattr(get_results, "max_concurrent_network_ops", lambda: 5)
    try:
        assert get_results._after_invocation_semaphore()._sem._value == 15
    finally:
        _clear(get_results._after_invocation_semaphore)
