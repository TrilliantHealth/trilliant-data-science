"""Definitions of basic keyfuncs."""
import inspect
import typing as ty

from ..types import Args, Kwargs


class Keyfunc(ty.Protocol):
    """A function which, when called with (function, args, kwargs),
    returns either the same or a different function, and the same or
    different args and kwargs, such that the returned three-tuple is
    what will get used to construct the full memoization key.

    The args, kwargs returned _must_ be bindable to the 4parameters
    function returned.  However, since they will not get used to
    actually call the function, it is not important that they bind in
    a semantically meaningful way - if you're just trying to drop
    certain arguments that can't be pickled, your best bet will be to
    return a `None` placeholder for those.

    The identity function (lambda f, a, k: f, a k) is equivalent to
    the unchanged default behavior from MemoizingPicklingRunner.
    """

    def __call__(
        self, func: ty.Callable, __args: Args, __kwargs: Kwargs
    ) -> ty.Tuple[ty.Callable, Args, Kwargs]:
        ...  # pragma: nocover


ArgsOnlyKeyfunc = ty.Callable[..., ty.Tuple[Args, Kwargs]]


def args_only(keyfunc: ty.Union[ArgsOnlyKeyfunc, Keyfunc]) -> Keyfunc:
    def funcpassthrough_keyfunc(
        func: ty.Callable, args: Args, kwargs: Kwargs
    ) -> ty.Tuple[ty.Callable, Args, Kwargs]:
        return func, *keyfunc(*args, **kwargs)  # type: ignore

    return ty.cast(Keyfunc, funcpassthrough_keyfunc)


def autowrap_args_only_keyfunc(keyfunc: ty.Union[ArgsOnlyKeyfunc, Keyfunc]) -> Keyfunc:
    """This exists only to 'sweeten' the API, so that in most cases a
    'normal-looking' function can be passed in that does not have
    access to the `func` parameter and gets Pythonic access to the
    splatted args and kwargs, rather than a tuple and a dictionary.
    """
    keyfunc_params = inspect.signature(keyfunc).parameters
    is_full_keyfunc = len(keyfunc_params) == 3 and next(iter(keyfunc_params.values())).name == "func"
    if is_full_keyfunc:
        return ty.cast(Keyfunc, keyfunc)
    return args_only(keyfunc)
