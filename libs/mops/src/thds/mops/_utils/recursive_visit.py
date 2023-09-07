import typing as ty
from io import BytesIO
from pickle import Pickler


def recursive_visit(visitor: ty.Callable[[ty.Any], bool], obj: ty.Any):
    """A hilarious abuse of pickle to do nearly effortless object recursion in Python for us.

    Because use_runner depends on serializability, in general this
    will not cause significant limitations that would not have been
    incurred during serialization later. This is, however, purely an
    implementation detail, so it could be replaced with some other
    approach later.
    """

    class PickleVisit(Pickler):
        def __init__(self, file):
            super().__init__(file)

        def reducer_override(self, obj: ty.Any):
            visitor(obj)
            return NotImplemented

    PickleVisit(BytesIO()).dump(obj)
