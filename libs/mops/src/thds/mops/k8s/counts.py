import multiprocessing as mp
import typing as ty

T = ty.TypeVar("T")


class MpValue(ty.Protocol[T]):
    def get_lock(self) -> ty.Any:
        ...

    value: T


class Counter:
    def __init__(self, mp_val: MpValue[int]) -> None:
        self.counter = mp_val

    def inc(self) -> int:
        with self.counter.get_lock():
            self.counter.value += 1
            return self.counter.value


LAUNCH_COUNT = Counter(mp.Value("i", 0))
FINISH_COUNT = Counter(mp.Value("i", 0))
