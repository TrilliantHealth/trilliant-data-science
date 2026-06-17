import concurrent.futures

from thds.mops.pure.runner import simple_shims


def test_future_subprocess_shim_dispatches_to_subprocess_shim(monkeypatch):
    """The future runs subprocess_shim (a real child process via `python -m ...entry.main`),
    not the in-process samethread_shim.

    Running the child process and blocking the worker on subprocess.check_call is what
    makes the result durable before the future resolves: check_call returns only once the
    child has exited. The original implementation ran samethread_shim inside a
    ProcessPoolExecutor whose only reference was dropped, so GC could reclaim the worker
    mid-write -> a clean exit with no result blob (NoResultAfterShimSuccess).
    """
    seen = []
    monkeypatch.setattr(simple_shims, "subprocess_shim", lambda shim_args: seen.append(shim_args))

    future = simple_shims.future_subprocess_shim(("memo_uri_a",))
    assert isinstance(future, concurrent.futures.Future)
    assert future.result() is None
    assert seen == [("memo_uri_a",)]
