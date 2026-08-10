import multiprocessing as mp
import threading
import typing as ty

T = ty.TypeVar("T")


class MpValue(ty.Protocol[T]):
    def get_lock(self) -> ty.Any: ...

    value: T


def inc(mp_val: MpValue[int]) -> int:
    with mp_val.get_lock():
        mp_val.value += 1
        return mp_val.value


_COUNTS: dict[str, MpValue[int]] = {}
_COUNTS_LOCK = threading.Lock()
# these are spooky - they're global and mutable, and may in fact get overwritten by code
# using specific multiprocessing contexts.


def _counter(name: str) -> MpValue[int]:
    """A shared counter, allocated the first time it is actually wanted.

    Allocating at import time spawns multiprocessing's resource tracker as a side effect of
    `import`, which fails outright in a process whose standard file descriptors are not
    what `posix_spawn` expects - a curses or full-screen terminal application, for one.
    Merely importing this package should not require a process to be forkable.
    """
    with _COUNTS_LOCK:
        if name not in _COUNTS:
            _COUNTS[name] = ty.cast(MpValue[int], mp.Value("i", 0))

        return _COUNTS[name]


def __getattr__(name: str) -> MpValue[int]:
    """`counts.LAUNCH_COUNT` keeps working, and keeps being reassignable.

    A module-level `__getattr__` runs only for names not already in the module dict, so an
    assignment to `counts.LAUNCH_COUNT` shadows this permanently - which is what
    `batching` relies on when it swaps in a counter from its own context.
    """
    if name in ("LAUNCH_COUNT", "FINISH_COUNT"):
        return _counter(name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def to_name(count: int) -> str:
    """Convert a count to a name."""
    return f"{count:0>4}"
