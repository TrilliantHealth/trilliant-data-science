import multiprocessing as mp
import typing as ty

T = ty.TypeVar("T")


class MpValue(ty.Protocol[T]):
    def get_lock(self) -> ty.Any:
        ...

    value: T


def inc(mp_val: MpValue[int]) -> int:
    with mp_val.get_lock():
        mp_val.value += 1
        return mp_val.value


LAUNCH_COUNT = mp.Value("i", 0)
FINISH_COUNT = mp.Value("i", 0)
# these are spooky - they're global and mutable, and may in fact get overwritten by code
# using specific multiprocessing contexts.


def to_name(count: int) -> str:
    """Convert a count to a name."""
    return f"{count:0>4}"
