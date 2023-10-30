import typing as ty

from thds.core import log
from thds.core.stack_context import StackContext

from ..core.types import Args, Kwargs
from ..core.uris import UriResolvable
from .memoize_only import _threadlocal_shell
from .runner.orchestrator_side import NO_REDIRECT, MemoizingPicklingRunner, Redirect

logger = log.getLogger(__name__)


F_Args_Kwargs = ty.Tuple[ty.Callable, Args, Kwargs]
_ORIGINAL_F_ARGS_KWARGS: StackContext[ty.Optional[F_Args_Kwargs]] = StackContext("f_args_kwargs", None)


def _perform_original_invocation(*_args, **_kwargs) -> ty.Any:
    f_args_kwargs = _ORIGINAL_F_ARGS_KWARGS()
    assert (
        f_args_kwargs is not None
    ), "_perform_original_invocation() must be called from within a runner"
    f, args, kwargs = f_args_kwargs
    print(f_args_kwargs)
    return f(*args, **kwargs)


class ImpureKeyFunc(ty.Protocol):
    """A function which, when called with (function, args, kwargs),
    returns either the same or a different function, and the same or
    different args and kwargs, such that the returned three-tuple is
    what will get used to construct the full memoization key.

    The identity function (lambda f, a, k: f, a k) is equivalent to
    the unchanged default behavior from MemoizingPicklingRunner.
    """

    def __call__(
        self, __func: ty.Callable, __args: Args, __kwargs: Kwargs
    ) -> ty.Tuple[ty.Callable, Args, Kwargs]:
        ...  # pragma: nocover


class ImpureRunner(MemoizingPicklingRunner):
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
        impure_key_func: ImpureKeyFunc,
        redirect: Redirect = NO_REDIRECT,
    ):
        self._impure_key_func = impure_key_func
        self._pre_pickle_redirect = redirect
        super().__init__(
            _threadlocal_shell,
            blob_storage_root,
            redirect=lambda _f, _args, _kwargs: _perform_original_invocation,
        )

    def __call__(self, raw_func: ty.Callable, raw_args: Args, raw_kwargs: Kwargs):
        actual_function_to_call = self._pre_pickle_redirect(raw_func, raw_args, raw_kwargs)
        with _ORIGINAL_F_ARGS_KWARGS.set((actual_function_to_call, raw_args, raw_kwargs)):
            return super().__call__(*self._impure_key_func(raw_func, *raw_args, **raw_kwargs))
