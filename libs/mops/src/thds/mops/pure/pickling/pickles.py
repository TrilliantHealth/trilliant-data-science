"""This module is a good place to define actual objects that need to
pickled in a backward-compatible way - i.e., we want to remember not
to refactor their names or the name of the module they live in so as
to maintain backward-compatibility more easily.
"""

import importlib
import io
import pickle
import typing as ty
from dataclasses import dataclass
from pathlib import Path

from thds.core import hashing, log, source

from ..core.script_support import add_main_module_function, get_main_module_function
from ..core.source import source_from_hashref, source_from_source_result
from ..core.uris import get_bytes, lookup_blob_store

logger = log.getLogger(__name__)


@dataclass
class Invocation:
    """Basically, NestedFunctionPickle was the v2. This is v3. By switching to dataclass,
    we can more easily add new optional attributes later on.
    """

    func: ty.Callable
    args_kwargs_pickle: bytes
    # this is pickled separately so that we can hash it separately.
    # the identity of the function is represented by the name part of the blob path.


class NestedFunctionPickle(ty.NamedTuple):
    """Not in use - retained for mops-inspect backward-compatibility"""

    f: ty.Callable
    args_kwargs_pickle: bytes


class PicklableFunction:
    """The main 'issue' this is working around is that decorated
    functions aren't picklable because of something having to do with
    the way the function gets 'replaced' at decoration time.

    There may be other solutions to this, but this seems to work fine.
    """

    def __init__(self, f: ty.Callable) -> None:
        if f.__module__ == "__main__":
            add_main_module_function(f.__name__, f)
        self.fmod = f.__module__
        self.fname = f.__name__
        self.f = None

    def __str__(self) -> str:
        return f"{self.fmod}.{self.fname}"

    def __repr__(self) -> str:
        return str(self)

    @property
    def __name__(self) -> str:
        return self.fname

    def __call__(self, *args: ty.Any, **kwargs: ty.Any) -> ty.Any:
        logger.debug(f"Dynamically importing function {str(self)}")
        if self.fmod == "__main__":
            self.f = get_main_module_function(self.fname)  # type: ignore
        else:
            mod = importlib.import_module(self.fmod)
            self.f = getattr(mod, self.fname)
        assert self.f
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


class UnpickleSourceUriArgument(ty.NamedTuple):
    """The URI fully specifies this type of source. Nothing fancy happens here.  We just
    return a new Source object that represents the URI.
    """

    uri: str

    def __call__(self) -> source.Source:
        return source.from_uri(self.uri)


class UnpickleSourceHashrefArgument(ty.NamedTuple):
    """Represents the root for a single file hashref. May be either local or remote.

    For stability, the module name and the class name must not change.

    This only applies to arguments _into_ a function. Results _from_ a function should
    have a different form.
    """

    hash: hashing.Hash

    def __call__(self) -> source.Source:
        return source_from_hashref(self.hash)


class UnpickleSourceResult(ty.NamedTuple):
    """Stability for this is not critical, as it will only ever exist in the result
    payload, which does not participate in memoization.
    """

    remote_uri: str
    hash: ty.Optional[hashing.Hash]
    file_uri: str

    def __call__(self) -> source.Source:
        return source_from_source_result(*self)


class UnpickleFunctionWithLogicKey(ty.NamedTuple):
    """When a mops-memoized function receives, in standard "functional programming" style,
    a function as an argument (whether partially-applied or not), we need to make
    sure to represent any function-logic-key on that callable as part of what gets serialized,
    so that memoization does not happen when unexpected/undesired.

    The function itself must be picklable in the natural way.
    """

    func_bytes: bytes
    function_logic_key: str

    def __call__(self) -> ty.Callable:
        return pickle.loads(self.func_bytes)
