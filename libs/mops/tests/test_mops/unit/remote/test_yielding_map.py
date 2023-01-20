from thds.mops.remote import Thunk, YieldingMapWithLen, parallel_yield_results


def mult_2p2(f: float) -> float:
    """CPU hungry operation - usually needs to be module-level to be pickleable"""
    return f * 2.2


def test_yielding_map_with_len():
    """YieldingMapWithLen lets you do _some_ work in serial before
    launching each task with parallel_yield_results;

    Handy for cases where you need to constrain peak memory usage of
    some preparatory work, but still want to pickle and launch in
    parallel.
    """

    def inc_and_float(i: int) -> float:
        """Memory-hungry operation - does not need to be module-level."""
        return float(i + 100)

    def thunkify(i: int) -> Thunk[float]:
        # return a Thunk of the CPU-hungry operation after performing
        # the memory-hungry operation to construct its arguments.
        return Thunk(mult_2p2, inc_and_float(i))

    results = set(parallel_yield_results(YieldingMapWithLen(thunkify, range(20))))
    for i in range(20):
        assert mult_2p2(inc_and_float(i)) in results
