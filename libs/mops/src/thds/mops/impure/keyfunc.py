"""Impure keyfunctions that are useful for common cases."""
import typing as ty

from ..pure.core.memo.keyfunc import Args, Keyfunc, Kwargs
from ..pure.core.memo.overwrite_params import parameter_overwriter


def nil_args(*named_parameters: str) -> Keyfunc:
    def nil_args_impure_keyfunc(
        c: ty.Callable, args: Args, kwargs: Kwargs
    ) -> ty.Tuple[ty.Callable, Args, Kwargs]:
        return c, *parameter_overwriter(c, {name: None for name in named_parameters})(args, kwargs)

    return nil_args_impure_keyfunc
