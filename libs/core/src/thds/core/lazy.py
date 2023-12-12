"""A thread-safe lazy callable."""
import typing as ty
from threading import Lock, local

R = ty.TypeVar("R")


class Lazy(ty.Generic[R]):
    def __init__(self, source: ty.Callable[[], R], storage=None):
        self._source = source
        self._storage = storage if storage is not None else lambda: 0
        self._lock = Lock()

    def __call__(self) -> R:
        try:
            return self._storage.cached
        except AttributeError:
            with self._lock:
                try:
                    return self._storage.cached
                except AttributeError:
                    self._storage.cached = self._source()

        return self._storage.cached


class ThreadLocalLazy(Lazy[R]):
    def __init__(self, source: ty.Callable[[], R]):
        # local() creates a brand new instance every time it is called,
        # so this does not cause issues with storage being shared across multiple TTLazies
        super().__init__(source, storage=local())
