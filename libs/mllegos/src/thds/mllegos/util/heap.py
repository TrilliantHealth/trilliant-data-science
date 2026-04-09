from typing import Tuple, TypeVar

# Interval utils

# Key for ordered comparison
K = TypeVar("K")
# Object associated with a heap item
Obj = TypeVar("Obj")


class HeapItem(Tuple[Obj, K]):
    def __lt__(self, other):
        return self[1] < other[1]

    def __gt__(self, other):
        return self[1] > other[1]

    def __le__(self, other):
        return self[1] <= other[1]

    def __ge__(self, other):
        return self[1] >= other[1]
