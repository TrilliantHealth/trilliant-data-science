"""A thread-safe lazy callable."""
import typing as ty
from threading import RLock, local

R = ty.TypeVar("R")


_NotSourced = object()  # singleton sentinel value


class Lazy(ty.Generic[R]):
    def __init__(self, source: ty.Callable[[], R], storage=None):
        self._source = source
        self._storage = storage if storage is not None else lambda: 0
        self._storage.cached = _NotSourced
        self._lock = RLock()

    def __call__(self) -> R:
        if self._storage.cached is _NotSourced:
            with self._lock:
                if self._storage.cached is _NotSourced:
                    self._storage.cached = self._source()

        return self._storage.cached


class ThreadLocalLazy(Lazy[R]):
    def __init__(self, source: ty.Callable[[], R]):
        # local() creates a brand new instance every time it is called,
        # so this does not cause issues with storage being shared across multiple TTLazies
        super().__init__(source, storage=local())
