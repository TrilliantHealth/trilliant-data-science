import random
from functools import partial
from itertools import accumulate
from operator import add
from typing import Callable, Iterator, Mapping, Optional, Sequence, Tuple, TypeVar, Union

T = TypeVar("T")
U = TypeVar("U")

Gen = Callable[[], T]


def repeat(gen: Gen[T]) -> Iterator[T]:
    while True:
        yield gen()


def repeat_gen(gen: Gen[T]) -> Gen[Iterator[T]]:
    return partial(repeat, gen)


def juxtapose(gen1: Gen[T], gen2: Gen[U]) -> Tuple[T, U]:
    # e.g. for key-value pairs
    return gen1(), gen2()


def juxtapose_gen(gen1: Gen[T], gen2: Gen[U]) -> Gen[Tuple[T, U]]:
    return partial(juxtapose, gen1, gen2)


def either(gen1: Gen[T], gen2: Gen[U], choose1: Gen[bool]) -> Union[T, U]:
    return gen1() if choose1() else gen2()


def either_gen(gen1: Gen[T], gen2: Gen[U], choose1: Gen[bool]) -> Gen[Union[T, U]]:
    return partial(either, gen1, gen2, choose1)


def choice(values: Sequence[T], cum_weights: Optional[Sequence[float]] = None) -> T:
    """optimized to use the cum_weights arg of random.choices"""
    return (
        random.choice(values)
        if cum_weights is None
        else random.choices(values, cum_weights=cum_weights, k=1)[0]
    )


def choice_gen(values: Union[Sequence[T], Mapping[T, float]]) -> Gen[T]:
    if isinstance(values, Mapping):
        values_, weights = zip(*values.items())
        total = sum(weights)
        cum_weights = tuple(w / total for w in accumulate(weights, add))
        return partial(choice, values_, cum_weights)
    else:
        return partial(choice, values)
