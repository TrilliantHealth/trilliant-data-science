import typing as ty

from thds.core import log
from thds.core.stack_context import StackContext

from ..core.types import Args, Kwargs
from ..core.uris import UriResolvable
from .memoize_only import _threadlocal_shell
from .runner import MemoizingPicklingRunner

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


class InterceptingRunner(MemoizingPicklingRunner):
    """This only works in the same process, and by definition will use
    the return values of memo_key only for the purposes of keying the
    cache - when the 'remote' side is reached, the original invocation
    will be performed directly.
    """

    def __init__(
        self,
        blob_storage_root: UriResolvable,
        memo_keyer: ty.Callable[[ty.Callable, Args, Kwargs], ty.Tuple[ty.Callable, Args, Kwargs]],
    ):
        super().__init__(
            _threadlocal_shell,
            blob_storage_root,
            redirect=lambda f, _args, _kwargs: _perform_original_invocation,
        )
        self._memo_keyer = memo_keyer

    def __call__(self, func: ty.Callable, args: Args, kwargs: Kwargs):
        with _ORIGINAL_F_ARGS_KWARGS.set((func, args, kwargs)):
            return super().__call__(*self._memo_keyer(func, *args, **kwargs))
