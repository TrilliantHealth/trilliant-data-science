"""Test that marking a specific object for shared serialization will work correctly"""
import typing as ty

from thds.mops.parallel import Thunk, parallel_yield_results

from ...config import TEST_TMP_URI
from ._util import MemoizingPicklingRunner, _subprocess_remote, use_runner

_our_runner = MemoizingPicklingRunner(_subprocess_remote, TEST_TMP_URI)
adls_remote = use_runner(_our_runner)


def multiply_arraysum(arr_of_nums: ty.List[int], mult: int) -> int:
    return sum(arr_of_nums) * mult


class List(list):
    """Lists (among other things) are not natively weak-ref-able."""


def test_multiple_threads_share_large_array():
    big_arr = List(range(100000))

    _our_runner.shared(bigarray=big_arr)

    ts = [
        Thunk(adls_remote(multiply_arraysum), big_arr, 1),
        Thunk(adls_remote(multiply_arraysum), big_arr, 2),
        Thunk(adls_remote(multiply_arraysum), big_arr, 3),
    ]

    base_sum = 4999950000  # sum(range(100000))
    assert sorted(parallel_yield_results(ts)) == [base_sum, base_sum * 2, base_sum * 3]
