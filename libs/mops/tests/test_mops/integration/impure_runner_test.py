import threading
import typing as ty

from thds.mops import impure
from thds.mops.pure import Args, Kwargs

from ..config import TEST_TMP_URI

_A_LOCK_ISNT_PICKLABLE = threading.Lock()


def _func_whose_args_cant_be_pickled(num: int, lock: threading.Lock):
    print(f"Calling actual function with {num} and {lock}")
    return (num, 17)


def memo_key(c: ty.Callable, args: Args, kwargs: Kwargs) -> ty.Tuple[ty.Callable, Args, Kwargs]:
    def unwrap_ak(num: int, lock: threading.Lock) -> ty.Tuple[Args, Kwargs]:
        # does some stuff with num and removes lock so it doesn't get used.
        return (num, num + 1), dict()

    return c, *unwrap_ak(*args, **kwargs)


def test_impure_runner():
    num = 1
    result = impure.KeyedLocalRunner(
        TEST_TMP_URI,
        keyfunc=memo_key,
    )(_func_whose_args_cant_be_pickled, (num,), dict(lock=_A_LOCK_ISNT_PICKLABLE))

    assert result == (num, 17)


def test_impure_runner_with_auto_memo_key():
    """This is basically just a simpler way of approaching writing the keyfunc."""
    num = 3
    result = impure.KeyedLocalRunner(TEST_TMP_URI, keyfunc=impure.nil_args("lock"))(
        _func_whose_args_cant_be_pickled, (num,), dict(lock=_A_LOCK_ISNT_PICKLABLE)
    )
    assert result == (num, 17)
