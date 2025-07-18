"""Builds on top of the pure.MemoizingPicklingRunner to provide
impure, customizable memoization.
"""

import typing as ty

from typing_extensions import ParamSpec

from thds.core import log
from thds.core.stack_context import StackContext

from ..pure.core.memo.keyfunc import ArgsOnlyKeyfunc, Keyfunc, autowrap_args_only_keyfunc
from ..pure.core.types import Args, Kwargs
from ..pure.core.uris import UriResolvable
from ..pure.pickling.mprunner import NO_REDIRECT, MemoizingPicklingRunner, Redirect
from ..pure.runner.simple_shims import samethread_shim

logger = log.getLogger(__name__)


R = ty.TypeVar("R")
P = ParamSpec("P")
F_Args_Kwargs = ty.Tuple[ty.Callable, Args, Kwargs]
_ORIGINAL_F_ARGS_KWARGS: StackContext[ty.Optional[F_Args_Kwargs]] = StackContext("f_args_kwargs", None)


def _perform_original_invocation(*_args: ty.Any, **_kwargs: ty.Any) -> ty.Any:
    f_args_kwargs = _ORIGINAL_F_ARGS_KWARGS()
    assert (
        f_args_kwargs is not None
    ), "_perform_original_invocation() must be called from within a runner"
    f, args, kwargs = f_args_kwargs
    return f(*args, **kwargs)


class KeyedLocalRunner(MemoizingPicklingRunner):
    """The only purpose for using this is to reify/memoize your results.

    Allows changing the memoization key, at the expense of
    (theoretical) purity, since now we're memoizing on something you
    made up, rather than something directly derived from the full set
    of arguments passed to your function.

    When the 'remote' side is reached, the original (args, kwargs)
    will be passed to the result of change_function, or the original
    function if change_function is the default (identity).

    This runs the 'remote' function in the same process - your
    function, if no memoized result is found, will execute in the same
    thread where it was originally called. This runner will use the
    return values of change_key_elements _only_ for the purposes of
    keying the cache.
    """

    def __init__(
        self,
        blob_storage_root: UriResolvable,
        *,
        keyfunc: ty.Union[ArgsOnlyKeyfunc, Keyfunc],
        redirect: Redirect = NO_REDIRECT,
    ):
        self._impure_keyfunc = autowrap_args_only_keyfunc(keyfunc)
        self._pre_pickle_redirect = redirect
        super().__init__(
            samethread_shim,
            blob_storage_root,
            redirect=lambda _f, _args, _kwargs: _perform_original_invocation,
        )

    def __call__(self, raw_func: ty.Callable[P, R], raw_args: P.args, raw_kwargs: P.kwargs) -> R:
        actual_function_to_call = self._pre_pickle_redirect(raw_func, raw_args, raw_kwargs)
        with _ORIGINAL_F_ARGS_KWARGS.set((actual_function_to_call, raw_args, raw_kwargs)):
            return super().__call__(*self._impure_keyfunc(raw_func, raw_args, raw_kwargs))
