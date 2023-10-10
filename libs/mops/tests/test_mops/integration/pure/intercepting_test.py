import threading

from thds.mops.pure.pickling.intercept import InterceptingRunner

_A_LOCK_ISNT_PICKLABLE = threading.Lock()


def _func_whose_args_cant_be_pickled(num: int, lock: threading.Lock):
    print(f"Calling actual function with {num} and {lock}")
    return (num, 17)


def memo_key(func, num, lock):
    # does some stuff with num and removes lock so it doesn't get used.
    return func, (num, num + 1), dict()


def test_intercepting_runner():
    num, _17 = InterceptingRunner(
        "adls://thdsscratch/tmp/",
        memo_key,
    )(_func_whose_args_cant_be_pickled, (1,), dict(lock=_A_LOCK_ISNT_PICKLABLE))

    assert num == 1
    assert _17 == 17
