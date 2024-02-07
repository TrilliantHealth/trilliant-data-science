"""A thread-safe lazy callable."""
import typing as ty
from threading import Lock, local

R = ty.TypeVar("R")
_LOCK_LOCK = Lock()  # for thread local storage, you need to create the lock on each thread.


def _get_lock(storage) -> Lock:
    if hasattr(storage, "lock"):
        return storage.lock
    with _LOCK_LOCK:
        if hasattr(storage, "lock"):
            return storage.lock
        # creating a lock is itself very fast, whereas the source() callable may be slow.
        storage.lock = Lock()
    return storage.lock


class Lazy(ty.Generic[R]):
    def __init__(self, source: ty.Callable[[], R], storage=None):
        self._source = source
        self._storage = storage if storage is not None else lambda: 0
        self._lock = Lock()

    def __call__(self) -> R:
        if hasattr(self._storage, "cached"):
            return self._storage.cached
        with _get_lock(self._storage):
            if hasattr(self._storage, "cached"):
                return self._storage.cached
            self._storage.cached = self._source()
        return self._storage.cached


class ThreadLocalLazy(Lazy[R]):
    def __init__(self, source: ty.Callable[[], R]):
        # local() creates a brand new instance every time it is called,
        # so this does not cause issues with storage being shared across multiple TTLazies
        super().__init__(source, storage=local())


def lazy(source: ty.Callable[[], R]) -> ty.Callable[[], R]:
    return Lazy(source)


def threadlocal_lazy(source: ty.Callable[[], R]) -> ty.Callable[[], R]:
    return ThreadLocalLazy(source)
