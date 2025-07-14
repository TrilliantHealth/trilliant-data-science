"""There are times when what you want is a lazy-ish resource, but you don't want it to live forever - just for the duration of the current stack's usage.
At the same time, if another thread or stack is currently using the resource, you want to reuse it.

A good example of using this might be a ThreadPoolExecutor where multiple other threads might be doing similar work concurrently, and it would help if they could all share the same pool:

```
from concurrent.futures import ThreadPoolExecutor
from thds.core import refcount


shared_thread_pool = refcount.Resource(lambda: ThreadPoolExecutor())


def do_work(thunks):
    with shared_thread_pool.get() as thread_pool:
        for res in parallel.yield_results(thunks, executor_cm=thread_pool):
            print(res)
        ...
```
"""

import threading
import typing as ty
from contextlib import contextmanager

from . import log

R = ty.TypeVar("R")
logger = log.getLogger(__name__)


class _ContextProxy(ty.Generic[R]):
    """A proxy to wrap a resource and neuter its context management methods."""

    def __init__(self, resource: R):
        self._resource = resource

    def __getattr__(self, name: str) -> ty.Any:
        return getattr(self._resource, name)

    def __enter__(self) -> R:
        # The user might still do `with proxy: ...`. We allow it,
        # but it does nothing and just returns the underlying resource.
        return self._resource

    def __exit__(self, *args: ty.Any) -> None:
        # This is a no-op; the real cleanup is handled by _RefCountResource
        pass


class _RefCountResource(ty.Generic[R]):
    def __init__(self, factory: ty.Callable[[], ty.ContextManager[R]]) -> None:
        self._factory = factory
        self._rcm__exit__: ty.Optional[ty.Callable[[ty.Any, ty.Any, ty.Any], ty.Optional[bool]]] = None
        self._resource: ty.Optional[R] = None
        self._ref_count: int = 0
        self._lock = threading.RLock()

    @contextmanager
    def get(self) -> ty.Iterator[R]:
        with self._lock:
            assert self._ref_count >= 0, "Reference count should not be negative prior to incrementing"
            if self._ref_count == 0:
                assert (
                    self._rcm__exit__ is None
                ), "Resource CM __exit__ should be None when ref count is zero"
                assert self._resource is None, "Resource should be None when ref count is zero"
                resource_cm = self._factory()
                resource = resource_cm.__enter__()
                self._rcm__exit__ = resource_cm.__exit__
                if id(resource) == id(resource_cm):
                    logger.info("Patching self-managing resource to avoid double exit: %s", resource)
                    # this is one of those context managers that returns itself
                    # since we manage this resource, we need to prevent others from trying to enter or exit it.
                    resource = _ContextProxy(resource)  # type: ignore[assignment]
                self._resource = resource
            self._ref_count += 1
            assert self._resource is not None, "Resource should not be None after incrementing ref count"
            resource = self._resource
        try:
            yield resource

        finally:
            with self._lock:
                self._ref_count -= 1
                assert self._ref_count >= 0, "Reference count should not be negative after decrementing"
                if self._ref_count == 0:
                    assert (
                        self._rcm__exit__ is not None
                    ), "Resource CM __exit__ should not be None when ref count is zero"
                    assert (
                        self._resource is not None
                    ), "Resource should not be None when ref count is zero"
                    self._rcm__exit__(None, None, None)
                    self._resource = None
                    self._rcm__exit__ = None


Resource = _RefCountResource  # probably preferable to use this name
