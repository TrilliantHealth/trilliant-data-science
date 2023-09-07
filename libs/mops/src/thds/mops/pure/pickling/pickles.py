"""This module is a good place to define actual objects that need to
pickled in a backward-compatible way - i.e., we want to remember not
to refactor their names or the name of the module they live in so as
to maintain backward-compatibility more easily.
"""
import importlib
import io
import pickle
import typing as ty
from pathlib import Path

from thds.core import log

from ..core.uris import get_bytes, lookup_blob_store

logger = log.getLogger(__name__)


class NestedFunctionPickle(ty.NamedTuple):
    """By pickling args-kwargs on its own, we can get a hash of just those."""

    f: ty.Callable
    args_kwargs_pickle: bytes


class PicklableFunction:
    """The main 'issue' this is working around is that decorated
    functions aren't picklable because of something having to do with
    the way the function gets 'replaced' at decoration time.

    There may be other solutions to this, but this seems to work fine.
    """

    def __init__(self, f):
        if f.__module__ == "__main__":
            raise ValueError(
                f"Cannot pickle function {f} that is in the __main__ module"
                " because it will not be able to be found in a different process with a different __main__."
                " Please move it to a different module."
            )
        self.fmod = f.__module__
        self.fname = f.__name__
        self.f = None

    def __str__(self) -> str:
        return f"{self.fmod}.{self.fname}"

    def __call__(self, *args, **kwargs):
        logger.debug(f"Dynamically importing function {str(self)}")
        mod = importlib.import_module(self.fmod)
        self.f = getattr(mod, self.fname)
        return self.f(*args, **kwargs)


class UnpickleSimplePickleFromUri:
    def __init__(self, uri: str):
        self.uri = uri  # serializable as a pure string for simplicity
        self._cached = None

    def __call__(self) -> object:
        # i don't believe there's any need for thread safety here, since pickle won't use threads.
        if self._cached is None:
            self._cached = pickle.load(io.BytesIO(get_bytes(self.uri, type_hint="simple-uri-pickle")))
        return self._cached


class UnpicklePathFromUri(ty.NamedTuple):
    uri: str

    def __call__(self) -> Path:
        return lookup_blob_store(self.uri).getfile(self.uri)
