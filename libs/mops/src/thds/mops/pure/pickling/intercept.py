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
    return f(*args, **kwargs)


class InterceptingLocalThreadRunner(MemoizingPicklingRunner):
    """Allows changing the cache key
    This only works in the same process.

    It will use the return values of change_key_elements only for the purposes of
    keying the cache.

    When the 'remote' side is reached, the original
    args, kwargs will be passed to the result of change_function, or the
    original function if change_function is the default (identity).
    """

    def __init__(
        self,
        blob_storage_root: UriResolvable,
        change_key_elements: ty.Callable[
            [ty.Callable, Args, Kwargs], ty.Tuple[ty.Callable, Args, Kwargs]
        ],
        redirect: Redirect = NO_REDIRECT,
    ):
        super().__init__(
            _threadlocal_shell,
            blob_storage_root,
            redirect=lambda _f, _args, _kwargs: _perform_original_invocation,
        )
        self._change_key_elements = change_key_elements
        self._redirect = redirect

    def __call__(self, raw_func: ty.Callable, raw_args: Args, raw_kwargs: Kwargs):
        actual_function_to_call = self._redirect(raw_func, raw_args, raw_kwargs)
        with _ORIGINAL_F_ARGS_KWARGS.set((actual_function_to_call, raw_args, raw_kwargs)):
            return super().__call__(*self._change_key_elements(raw_func, *raw_args, **raw_kwargs))
