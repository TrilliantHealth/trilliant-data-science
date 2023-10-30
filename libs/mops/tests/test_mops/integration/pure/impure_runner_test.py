import threading
import typing as ty

from thds.mops.pure import Args, Kwargs
from thds.mops.pure.pickling.impure_runner import ImpureRunner

from ...config import TEST_TMP_URI

_A_LOCK_ISNT_PICKLABLE = threading.Lock()


def _func_whose_args_cant_be_pickled(num: int, lock: threading.Lock):
    print(f"Calling actual function with {num} and {lock}")
    return (num, 17)


def memo_key(func: ty.Callable, args: Args, kwargs: Kwargs) -> ty.Tuple[ty.Callable, Args, Kwargs]:
    def unwrap_ak(num: int, lock: threading.Lock) -> ty.Tuple[Args, Kwargs]:
        # does some stuff with num and removes lock so it doesn't get used.
        return (num, num + 1), dict()

    return func, *unwrap_ak(*args, **kwargs)


def test_impure_runner():
    num = 1
    result = ImpureRunner(
        TEST_TMP_URI,
        memo_key,
    )(_func_whose_args_cant_be_pickled, (num,), dict(lock=_A_LOCK_ISNT_PICKLABLE))

    assert result == (num, 17)
