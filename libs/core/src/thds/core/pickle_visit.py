import typing as ty
from io import BytesIO
from pickle import Pickler


def recursive_visit(visitor: ty.Callable[[ty.Any], ty.Any], obj: ty.Any) -> None:
    """A hilarious abuse of pickle to do nearly effortless recursive object 'visiting' in
    Python for us.  In other words, if you want to 'see' everything inside an object but
    don't actually care about serializing.

    This can only work for objects that are fully recursively picklable. If yours isn't,
    a pickling error will be raised and only some of the object will be visited.
    """

    class PickleVisit(Pickler):
        def __init__(self, file):
            super().__init__(file)
            self.file = file

        def reducer_override(self, obj: ty.Any):
            visitor(obj)
            return NotImplemented

    PickleVisit(BytesIO()).dump(obj)
