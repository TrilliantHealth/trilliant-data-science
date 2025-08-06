from timeit import default_timer

from thds.core.iterators import titrate


def demo():
    start = default_timer()
    for x in titrate(range(1, 20), at_rate=0.2, until_nth=10):
        print(f"{default_timer() - start:.2f}", x)


demo()
