"""This just tests that our abstraction can easily be implemented with other 'Runners'"""
import concurrent.futures
import multiprocessing as mp

from thds.mops.pure import use_runner


def a_function(a, b, *, c=3):
    return a * b + c


def test_multiprocessing_pool_apply():
    with mp.Pool() as pool:
        assert 15 == use_runner(pool.apply)(a_function)(4, 2, c=7)


def test_concurrent_futures_executor_submit():
    with concurrent.futures.ThreadPoolExecutor() as tpe:

        def to_submit(f, args, kwargs):
            return tpe.submit(f, *args, **kwargs).result()

        assert 15 == use_runner(to_submit)(a_function)(4, 2, c=7)
