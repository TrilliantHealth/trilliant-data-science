"""A thread-safe lazy callable."""

import typing as ty
from threading import Lock, local

R = ty.TypeVar("R")
_LOCK_LOCK = Lock()  # for thread local storage, you need to create the lock on each thread.


def _get_or_create_lock(storage) -> Lock:
    """Ensures a lock is available on this storage object. Holds a global lock to make
    sure there is only ever one lock created for this storage object.

    Storage can be any object that can have an attribute assigned with __setattr__.
    """
    if hasattr(storage, "lock"):
        return storage.lock
    with _LOCK_LOCK:
        if hasattr(storage, "lock"):
            return storage.lock
        # creating a lock is itself very fast, whereas the source() callable may be slow.
        storage.lock = Lock()
    return storage.lock


class Lazy(ty.Generic[R]):
    """Ensures that the zero-argument callable (thunk) is called either 0 or 1 times for
    the lifetime of this wrapper and its internal storage.

    Most commonly, this wraps a singleton defined at module scope, but it could also be
    used for shorter-lifetime singletons.

    If thread-local storage is provided, then the wrapper will be called 0 or 1 times per
    thread.
    """

    def __init__(self, source: ty.Callable[[], R], storage=None):
        self._source = source
        self._storage = storage if storage is not None else lambda: 0
        self._storage.lock = Lock()
        # we store the Lock on the storage, because in some cases the storage may be
        # thread-local, and we need a separate lock per thread. However, we also create
        # the first lock in the constructor so that in most cases, we never need to use
        # the global _LOCK_LOCK, which will cause some very minor contention.

    def __call__(self) -> R:
        if hasattr(self._storage, "cached"):
            return self._storage.cached
        with _get_or_create_lock(self._storage):
            if hasattr(self._storage, "cached"):
                return self._storage.cached
            self._storage.cached = self._source()
        return self._storage.cached

    def __repr__(self) -> str:
        return f"Lazy({self._source})"

    if not ty.TYPE_CHECKING:
        # if I don't 'guard' it this way, mypy (unhelpfully) allows all attribute access (as Any)
        def __getattr__(self, name: str) -> ty.NoReturn:
            raise AttributeError(
                f"{self} has no attribute '{name}' -"
                f" did you mean to instantiate the object before access, i.e. `().{name}`?"
            )


class ThreadLocalLazy(Lazy[R]):
    """A Lazy (see docs above), but with thread-local storage."""

    def __init__(self, source: ty.Callable[[], R]):
        # local() creates a brand new instance every time it is called,
        # so this does not cause issues with storage being shared across multiple TTLazies
        super().__init__(source, storage=local())


def lazy(source: ty.Callable[[], R]) -> ty.Callable[[], R]:
    """Wraps a thunk so that it is called at most once, and the result is cached."""
    return Lazy(source)


def threadlocal_lazy(source: ty.Callable[[], R]) -> ty.Callable[[], R]:
    """Wraps a thunk so that it is called at most once per thread, and the result is cached."""
    return ThreadLocalLazy(source)
